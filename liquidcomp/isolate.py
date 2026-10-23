"""Remote command execution with independent lease and cancellation monitoring."""

import shlex
import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional
from uuid import uuid4

from cantok import AbstractToken, DefaultToken
from semver import Version
from throng.abstracts.abstract_isolate import AbstractIsolate
from throng.abstracts.meta import (
    BenchmarkResult,
    Benchmarks,
    IsolateResourcesDescription,
)
from throng.errors import CannotInstallDependencyError, InterruptedInstallationError

from liquidcomp.api import API, Reply
from liquidcomp.errors import (
    CancellationTimeoutError,
    IsolateClosedError,
    IsolateUnavailableError,
    LiquidError,
    MetadataError,
    OperationTimeoutError,
    ProtocolError,
)
from liquidcomp.models import LiquidResult
from liquidcomp.protocol import (
    identifier,
    number,
    object_value,
    optional_text,
    text,
    when,
)

TERMINAL = frozenset(('completed', 'cancelled', 'timed_out', 'isolate_failed', 'failed'))
PENDING = frozenset(('queued', 'dispatched', 'running', 'cancel_requested'))


@dataclass
class _Pending:
    identifier: str
    token: AbstractToken
    deadline: float
    cancel_deadline: Optional[float] = None
    cancelled_by_token: bool = False


class _Lease:
    """Own no isolate reference, so the worker cannot keep a forgotten isolate alive."""

    def __init__(self, api: API, isolate_id: str, reply: Reply, payload: Dict[str, object]) -> None:
        self.api = api
        self.isolate_id = isolate_id
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.pending: Optional[_Pending] = None
        self.error: Optional[BaseException] = None
        self.deadline = reply.deadline(when(payload.get('expires_at')))
        if self.deadline <= time.monotonic():
            raise IsolateUnavailableError('The API returned an expired isolate lease.')
        self.renew_at = time.monotonic() + min(float(api.settings.renew_interval), (self.deadline - time.monotonic()) / 3)
        self.thread = threading.Thread(target=self._run, name='liquid-lease-' + isolate_id, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def check(self) -> None:
        with self.lock:
            if self.error is not None:
                raise self.error
            pending = self.pending
            if pending is not None and pending.cancel_deadline is not None and time.monotonic() >= pending.cancel_deadline:
                raise CancellationTimeoutError('Command cancellation was not confirmed by the API.')

    def set_pending(self, pending: Optional[_Pending]) -> None:
        with self.lock:
            self.pending = pending

    def fail(self, error: BaseException) -> None:
        with self.lock:
            self.error = error
        self.stop.set()

    def close(self) -> None:
        self.stop.set()
        self.thread.join()

    def _wait(self) -> bool:
        now = time.monotonic()
        with self.lock:
            cancellation = self.pending.cancel_deadline if self.pending is not None else None
        deadline = self.deadline if cancellation is None else min(self.deadline, cancellation)
        delay = max(0.0, min(float(self.api.settings.poll_interval), self.renew_at - now, deadline - now))
        return self.stop.wait(delay)

    def _run(self) -> None:
        try:
            while not self._wait():
                self.check()
                now = time.monotonic()
                if now >= self.deadline:
                    raise IsolateUnavailableError('The isolate lease expired without renewal.')
                if now >= self.renew_at:
                    reply = self.api.request('POST', 'isolates/' + self.isolate_id + '/renew')
                    payload = reply.mapping()
                    if identifier(payload.get('id')) != self.isolate_id:
                        raise ProtocolError('Renewal returned a different isolate.')
                    self.deadline = reply.deadline(when(payload.get('expires_at')))
                    self.renew_at = time.monotonic() + min(float(self.api.settings.renew_interval), (self.deadline - time.monotonic()) / 3)
                with self.lock:
                    pending = self.pending
                if pending is not None and pending.cancel_deadline is None:
                    cancelled_by_token = not pending.token
                    if not cancelled_by_token and now < pending.deadline:
                        continue
                    with self.lock:
                        if self.pending is not pending:
                            continue
                        pending.cancelled_by_token = cancelled_by_token
                        pending.cancel_deadline = time.monotonic() + self.api.settings.cancellation_timeout
                    try:
                        self.api.request('POST', 'commands/cancel', payload={'ids': [pending.identifier]})
                    except LiquidError:
                        with self.lock:
                            # A completed command no longer owns cancellation errors.
                            if self.pending is pending:
                                raise
        except BaseException as error:  # noqa: BLE001 - wake callers on any worker termination.
            self.fail(error)


class LiquidIsolate(AbstractIsolate):
    def __init__(self, api: API, isolate_id: str, computer_id: str, lease: _Lease) -> None:
        self._closed = True
        self._state_lock = threading.Lock()
        self._operations = threading.RLock()
        self._kill_lock = threading.Lock()
        self._api = api
        self._id = isolate_id
        self._computer_id = computer_id
        self._lease = lease
        self._closed = False

    def __del__(self) -> None:
        try:
            self.kill()
        except Exception:  # noqa: BLE001
            pass

    def _check_alive(self) -> None:
        with self._state_lock:
            if self._closed:
                raise IsolateClosedError('The Liquid isolate has been closed.')
        self._lease.check()

    def get_id(self) -> str:
        return 'liquid:' + self._api.base_url + ':' + self._id

    def get_resources_description(self) -> IsolateResourcesDescription:
        return IsolateResourcesDescription(shared=True, shared_with=None)

    def kill(self) -> None:
        with self._kill_lock:
            with self._state_lock:
                if self._closed:
                    return
                self._closed = True
            self._lease.close()
            try:
                self._api.request('DELETE', 'isolates/' + self._id)
            finally:
                with self._operations:
                    self._api.close()

    def _run(self, command: str, token: AbstractToken = DefaultToken()) -> LiquidResult:  # noqa: B008
        with self._operations:
            self._check_alive()
            if not isinstance(command, str) or not command.strip() or len(command) > 65536 or '\0' in command:
                raise ValueError('Command must be nonempty text of at most 65536 characters without NUL.')
            if not token:
                return LiquidResult(success=False, killed_by_token=True)
            settings = self._api.settings
            command_id: Optional[str] = None
            try:
                reply = self._api.request('POST', 'isolates/' + self._id + '/commands', key=uuid4().hex, payload={
                    'command': command, 'shell': settings.shell, 'cwd': settings.cwd,
                    'env': settings.env, 'timeout_seconds': settings.command_timeout,
                })
                payload = reply.mapping()
                command_id = identifier(payload.get('id'))
                pending = _Pending(command_id, token, reply.deadline(when(payload.get('queued_deadline'))) + settings.command_timeout)
                self._lease.set_pending(pending)
                while True:
                    self._check_alive()
                    rows = self._api.request('POST', 'commands/results', payload={'ids': [command_id]}).array()
                    if len(rows) != 1:
                        raise ProtocolError('Expected exactly one command result.')
                    row = object_value(rows[0])
                    if identifier(row.get('id')) != command_id:
                        raise ProtocolError('Result belongs to a different command.')
                    status = text(row.get('status'))
                    if status in TERMINAL:
                        result_payload = object_value(row.get('result'))
                        if identifier(result_payload.get('command_id')) != command_id:
                            raise ProtocolError('Terminal result belongs to a different command.')
                        break
                    if status not in PENDING or row.get('result') is not None:
                        raise ProtocolError('Invalid command status/result combination.')
                    self._lease.stop.wait(float(settings.poll_interval))
            except BaseException as error:
                self._lease.fail(error)
                if command_id is not None:
                    try:
                        self._api.request('POST', 'commands/cancel', payload={'ids': [command_id]})
                    except Exception:  # noqa: BLE001
                        pass
                raise
            finally:
                self._lease.set_pending(None)
            return self._result(command_id, status, result_payload, pending)

    def _result(self, command_id: str, status: str, payload: Dict[str, object], pending: _Pending) -> LiquidResult:
        if type(payload.get('success')) is not bool:
            raise ProtocolError('Invalid command result success flag.')
        code = payload.get('returncode')
        if code is not None and type(code) is not int:
            raise ProtocolError('Invalid command return code.')
        if payload['success'] is True and (status != 'completed' or code != 0):
            raise ProtocolError('The command success flag contradicts its terminal outcome.')
        stdout = optional_text(payload.get('stdout'))
        stderr = optional_text(payload.get('stderr'))
        output_bytes: Optional[bytes] = None
        error_bytes: Optional[bytes] = None
        stdout_id = payload.get('stdout_artifact_id')
        stderr_id = payload.get('stderr_artifact_id')
        if stdout_id is not None:
            output_bytes = self._api.download('artifacts/' + identifier(stdout_id) + '/content')
            stdout = output_bytes.decode('utf-8', 'replace')
        if stderr_id is not None:
            error_bytes = self._api.download('artifacts/' + identifier(stderr_id) + '/content')
            stderr = error_bytes.decode('utf-8', 'replace')
        return LiquidResult(
            success=payload['success'] is True, returncode=code, stdout=stdout, stderr=stderr,
            id=command_id, status=status, error=optional_text(payload.get('error')),
            killed_by_token=pending.cancelled_by_token and status in ('cancelled', 'timed_out'),
            stdout_bytes=output_bytes, stderr_bytes=error_bytes,
        )

    def read(self) -> bytes:
        with self._operations:
            self._check_alive()
            operation = self._api.request('POST', 'isolates/' + self._id + '/snapshots', key=uuid4().hex).mapping()
            operation_id = identifier(operation.get('id'))
            deadline = time.monotonic() + self._api.settings.operation_timeout
            while True:
                self._check_alive()
                payload = self._api.request('GET', 'operations/' + operation_id).mapping()
                if identifier(payload.get('id')) != operation_id:
                    raise ProtocolError('Snapshot response belongs to a different operation.')
                status = text(payload.get('status'))
                if status == 'completed':
                    return self._api.download('artifacts/' + identifier(payload.get('artifact_id')) + '/content')
                if status in ('failed', 'obsolete'):
                    raise IsolateUnavailableError('The snapshot operation failed.')
                if status not in ('pending', 'dispatched', 'accepted'):
                    raise ProtocolError('Invalid snapshot operation status.')
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise OperationTimeoutError('Timed out waiting for the snapshot.')
                self._lease.stop.wait(min(float(self._api.settings.poll_interval), remaining))

    def install(self, *packages: str, token: AbstractToken = DefaultToken()) -> None:  # noqa: B008
        with self._operations:
            self._check_alive()
            for package in packages:
                if not token:
                    raise InterruptedInstallationError('Package installation was canceled.')
                if not package.strip() or '\0' in package:
                    raise ValueError('Package names must be nonempty strings without NUL.')
                result = self.run('python3 -m pip install -- ' + shlex.quote(package), token=token)
                if not result.success:
                    raise CannotInstallDependencyError('Remote package installation failed.', result)

    def get_benchmarks(self) -> Benchmarks:
        with self._operations:
            self._check_alive()
            try:
                rows = self._api.request('GET', 'computers/' + self._computer_id + '/benchmark-results?latest_only=true').array()
                known: Dict[str, BenchmarkResult] = {}
                versions = set()
                for raw in rows:
                    row = object_value(raw)
                    if identifier(row.get('computer_id')) != self._computer_id:
                        raise ProtocolError('Benchmark belongs to a different computer.')
                    version = text(row.get('benchmarker_version'))
                    try:
                        Version.parse(version)
                    except ValueError:
                        raise ProtocolError('Invalid benchmark version.') from None
                    versions.add(version)
                    name = text(row.get('benchmark_name'))
                    measurement = BenchmarkResult(number=number(row.get('number')), when=when(row.get('created_at')), benchmarker_version=version)
                    if name in ('single_core', 'multi_core'):
                        if name in known:
                            raise ProtocolError('Duplicate benchmark measurement.')
                        known[name] = measurement
                if len(versions) > 1:
                    raise ProtocolError('Latest benchmark results mix different versions.')
                return Benchmarks(single_core=known.get('single_core'), multi_core=known.get('multi_core'))
            except LiquidError as error:
                raise MetadataError('Could not retrieve valid benchmark metadata.') from error
