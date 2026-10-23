"""The throng manager defers credential checks until remote allocation."""

import hashlib
import time
from contextlib import closing
from pathlib import Path
from typing import List, Optional, Union
from uuid import uuid4

from cantok import AbstractToken, DefaultToken
from throng import AbstractManager

from liquidcomp.api import API
from liquidcomp.auth import Credentials, validate_token
from liquidcomp.errors import (
    APIError,
    InvalidTokenError,
    IsolateUnavailableError,
    KeyringError,
    MissingTokenError,
    OperationCancelledError,
    OperationTimeoutError,
    PermissionDeniedError,
    ProtocolError,
    ServerUnavailableError,
    StateError,
)
from liquidcomp.isolate import LiquidIsolate, _Lease
from liquidcomp.models import Status
from liquidcomp.protocol import identifier, text
from liquidcomp.settings import Settings
from liquidcomp.state import read_state


class LiquidManager(AbstractManager):
    def __init__(self, path: Union[str, Path] = '.', exclude: Optional[List[str]] = None, prepare: Optional[List[str]] = None, packages: Optional[List[str]] = None, *, settings: Optional[Settings] = None) -> None:
        super().__init__(path, exclude, prepare, packages)
        self.settings = settings if settings is not None else Settings()
        self._credentials = Credentials(self.settings)

    def init(self) -> None:
        self._credentials.init()

    def read(self) -> bytes:
        return read_state(self.path, self.exclude, self.settings.max_artifact_bytes)

    def _get(self, state: bytes, token: AbstractToken = DefaultToken()) -> LiquidIsolate:  # noqa: B008, PLR0915
        if not token:
            raise OperationCancelledError('Isolate allocation was canceled.')
        if not isinstance(state, bytes) or len(state) > self.settings.max_artifact_bytes:
            raise StateError('State must be bytes within the configured artifact limit.')
        try:
            credential = self._credentials.get()
        except MissingTokenError:
            if not self.settings.ask_for_token:
                raise
            self.init()
            credential = self._credentials.get()
        api = API(self.settings, credential)
        isolate_id: Optional[str] = None
        artifact_id: Optional[str] = None
        lease: Optional[_Lease] = None
        try:
            api.check()
            if not token:
                raise OperationCancelledError('Isolate allocation was canceled.')
            artifact = api.request('POST', 'artifacts', content=state, retry=False).mapping()
            artifact_id = identifier(artifact.get('id'))
            if artifact.get('sha256') != hashlib.sha256(state).hexdigest():
                raise ProtocolError('Uploaded state checksum does not match.')
            if not token:
                raise OperationCancelledError('Isolate allocation was canceled.')
            settings = self.settings
            reply = api.request('POST', 'isolates', key=uuid4().hex, payload={
                'state_artifact_id': artifact_id, 'image': settings.image,
                'os': 'linux', 'architecture': 'arm64', 'runtime': 'powercell-ubuntu-arm64-v1',
                'cpu_millis': round(settings.cpu * 1000), 'memory_bytes': int(settings.ram * 1024 ** 3),
                'disk_bytes': int(settings.disk * 1024 ** 3), 'retry_allowed': False,
            })
            payload = reply.mapping()
            isolate_id = identifier(payload.get('id'))
            deadline = time.monotonic() + settings.operation_timeout
            while True:
                if not token:
                    raise OperationCancelledError('Isolate allocation was canceled.')
                if identifier(payload.get('id')) != isolate_id:
                    raise ProtocolError('Provisioning returned a different isolate.')
                status = text(payload.get('status'))
                if status not in ('ready', 'provisioning'):
                    raise IsolateUnavailableError('The remote isolate could not be provisioned.')
                if lease is None:
                    worker = _Lease(api, isolate_id, reply, payload)
                    worker.start()
                    lease = worker
                lease.check()
                if status == 'ready':
                    return LiquidIsolate(api, isolate_id, identifier(payload.get('computer_id')), lease)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise OperationTimeoutError('Timed out waiting for a remote isolate.')
                time.sleep(min(float(settings.poll_interval), remaining))
                reply = api.request('GET', 'isolates/' + isolate_id)
                payload = reply.mapping()
        except BaseException:
            try:
                if lease is not None:
                    lease.close()
                if isolate_id is not None:
                    api.request('DELETE', 'isolates/' + isolate_id)
                elif artifact_id is not None:
                    api.request('DELETE', 'artifacts/' + artifact_id)
            except Exception:  # noqa: BLE001
                pass
            finally:
                api.close()
            raise

    def status(self) -> Status:
        connected, ready = False, False
        available = False
        valid: Optional[bool] = None
        source: Optional[str] = None
        error: Optional[str] = None
        with closing(API(self.settings, '')) as api:
            try:
                ready = api.request('GET', '', authenticated=False).mapping().get('status') == 'ready'
                connected = True
            except APIError as failure:
                connected = failure.status_code is not None
                error = str(failure)
            except ProtocolError as failure:
                connected = True
                error = str(failure)
        try:
            credential, source = self._credentials.peek()
            available = credential is not None
            if credential is not None:
                validate_token(credential)
                with closing(API(self.settings, credential)) as api:
                    try:
                        api.check()
                        valid, connected = True, True
                    except ServerUnavailableError as failure:
                        connected = connected or failure.status_code is not None
                        error = str(failure)
                    except (InvalidTokenError, PermissionDeniedError) as failure:
                        connected = True
                        valid = False
                        error = str(failure)
                    except (APIError, ProtocolError) as failure:
                        connected = True
                        error = str(failure)
        except InvalidTokenError as failure:
            valid = False
            error = str(failure)
        except KeyringError as failure:
            error = str(failure)
        return Status(connected, ready, available, valid, source, error)
