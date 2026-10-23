"""Failure boundaries, protocol validation and cleanup remain observable to callers."""

import gc
import hashlib
import os
import stat
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from io import BytesIO
from pathlib import Path
from tarfile import open as open_tar
from uuid import uuid4

import httpx
import pytest
from cantok import SimpleToken
from throng.errors import InterruptedInstallationError

from liquidcomp import LiquidManager, Settings
from liquidcomp.api import API, Reply
from liquidcomp.errors import (
    APIError,
    IsolateUnavailableError,
    MetadataError,
    MissingTokenError,
    OperationCancelledError,
    OperationTimeoutError,
    ProtocolError,
    StateError,
)
from tests.units.test_execution import result


def test_changed_api_endpoint_uses_its_own_keyring_namespace(api_stub, clean_environment):
    """Changing a manager's settings must not send a previous server's stored token elsewhere."""
    clean_environment.values['liquidcomp:https://api.mutating.tech/v1', 'LIQUID_TOKEN'] = 'old-server-secret'
    manager = LiquidManager()
    manager.settings.api_url = 'https://another.invalid/v1'
    with pytest.raises(MissingTokenError):
        manager.get(b'state')
    assert not api_stub.requests


@pytest.mark.parametrize('phase', ['stat', 'resolve', 'walk'])
def test_unreadable_subdirectory_does_not_silently_disappear(phase, tmp_path, monkeypatch):
    """Prevent root permission errors from escaping StateError or silently producing partial state."""
    manager = LiquidManager(tmp_path)
    def walk(_root, *, followlinks, onerror=None):
        if onerror is not None:
            onerror(PermissionError('unreadable source directory'))
        return iter([])
    def denied(_path):
        raise PermissionError('unreadable state root')
    with monkeypatch.context() as patch:
        if phase == 'walk':
            patch.setattr(os, 'walk', walk)
        else:
            patch.setattr(Path, 'is_dir' if phase == 'stat' else 'resolve', denied)
        with pytest.raises(StateError):
            manager.read()


@pytest.mark.parametrize('absolute', [False, True])
@pytest.mark.parametrize('nested', [False, True])
@pytest.mark.parametrize('target_kind', ['file', 'directory'])
def test_internal_symlinks_remain_portable(tmp_path, absolute, nested, target_kind):
    """Prevent invalid Windows relative links and duplicated directory-link archive entries."""
    target = tmp_path / 'target'
    if target_kind == 'directory':
        target.mkdir()
        (target / 'data').write_text('payload')
    else:
        target.write_text('payload')
    directory = tmp_path / 'nested' if nested else tmp_path
    directory.mkdir(exist_ok=True)
    expected = '../target' if nested else 'target'
    try:
        (directory / 'link').symlink_to(target if absolute else Path(expected), target_is_directory=target_kind == 'directory')
    except OSError:
        pytest.skip('Symlink privileges unavailable')
    with open_tar(fileobj=BytesIO(LiquidManager(tmp_path).read())) as archive:
        link = archive.getmember('nested/link' if nested else 'link')
        assert link.issym()
        assert link.linkname == expected
        names = {'target', 'nested/link' if nested else 'link'}
        if nested:
            names.add('nested')
        if target_kind == 'directory':
            names.add('target/data')
        assert set(archive.getnames()) == names


@pytest.mark.parametrize('mode', ['file', 'header', 'padding'])
def test_state_size_limit_includes_archive_overhead(tmp_path, mode):
    """A configured cap includes tar headers and final padding as well as file bytes."""
    if mode != 'padding':
        (tmp_path / 'data').write_bytes(b'x' * (1024 if mode == 'file' else 0))
    with pytest.raises(StateError):
        LiquidManager(tmp_path, settings=Settings(max_artifact_bytes=128)).read()


@pytest.mark.parametrize('native', [False, True])
def test_special_file_is_not_uploaded(tmp_path, monkeypatch, native):
    """Prevent special files from blocking state reads, including hosts without mkfifo."""
    pipe = tmp_path / 'pipe'
    if native:
        if not hasattr(os, 'mkfifo'):
            pytest.skip('Named pipes unavailable')
        os.mkfifo(pipe)
    else:
        pipe.touch()
        original_lstat = Path.lstat
        def lstat(path):
            info = original_lstat(path)
            return os.stat_result((stat.S_IFIFO, *info[1:])) if path == pipe else info
        monkeypatch.setattr(Path, 'lstat', lstat)
    with pytest.raises(StateError, match='unsupported special file'):
        LiquidManager(tmp_path).read()


@pytest.mark.parametrize('state', ['not bytes', b'oversized'])
def test_invalid_explicit_state_fails_before_auth_or_network(state, configured, api_stub):
    configured.settings.max_artifact_bytes = 3
    with pytest.raises(StateError):
        configured.get(state)
    assert not api_stub.requests


@pytest.mark.parametrize('stage', ['auth', 'upload', 'provisioning', 'provisioning_poll'])
def test_cancellation_during_allocation_cleans_known_resources(stage, configured, api_stub):
    """Prevent cancellation during provisioning polls from returning a live allocation."""
    token = SimpleToken()
    path = {
        'auth': '/v1/auth/check', 'upload': '/v1/artifacts', 'provisioning': '/v1/isolates',
        'provisioning_poll': '/v1/isolates/' + api_stub.isolate_id,
    }[stage]
    if stage == 'provisioning_poll':
        api_stub.routes['POST', '/v1/isolates'] = lambda _request: httpx.Response(202, json={
            'id': api_stub.isolate_id, 'status': 'provisioning', 'expires_at': '2099-01-01T00:00:00Z',
        })
    def reply(request):
        token.cancel()
        if stage == 'auth':
            return httpx.Response(200, json={'valid': True, 'scopes': ['read', 'execute']})
        if stage == 'upload':
            return httpx.Response(201, json={'id': api_stub.artifact_id, 'sha256': hashlib.sha256(request.content).hexdigest()})
        return httpx.Response(200 if stage == 'provisioning_poll' else 202, json={
            'id': api_stub.isolate_id, 'computer_id': api_stub.computer_id,
            'status': 'ready' if stage == 'provisioning_poll' else 'provisioning', 'expires_at': '2099-01-01T00:00:00Z',
        })
    api_stub.routes['GET' if stage in ('auth', 'provisioning_poll') else 'POST', path] = reply
    with pytest.raises(OperationCancelledError):
        configured.get(b'state', token=token)
    deleted = [r.url.path for r in api_stub.requests if r.method == 'DELETE']
    assert deleted == ([] if stage == 'auth' else ['/v1/' + ('artifacts/' + api_stub.artifact_id if stage == 'upload' else 'isolates/' + api_stub.isolate_id)])


def test_failed_cleanup_preserves_original_allocation_error(configured, api_stub, monkeypatch):
    """Prevent a failed cleanup from masking allocation failure or leaking its HTTP client."""
    clients = []
    def api(*args, **kwargs):
        instance = API(*args, **kwargs)
        clients.append(instance._client)
        return instance
    monkeypatch.setattr('liquidcomp.manager.API', api)
    api_stub.routes['POST', '/v1/isolates'] = lambda _request: httpx.Response(409)
    api_stub.routes['DELETE', '/v1/artifacts/' + api_stub.artifact_id] = lambda _request: httpx.Response(503)
    try:
        with pytest.raises(APIError) as caught:
            configured.get(b'state')
        assert caught.value.status_code == 409
        assert len(clients) == 1
        assert clients[0].is_closed
    finally:
        for client in clients:
            client.close()


def test_provisioning_cannot_switch_isolate_identity(configured, api_stub):
    api_stub.routes['POST', '/v1/isolates'] = lambda _request: httpx.Response(202, json={'id': api_stub.isolate_id, 'status': 'provisioning', 'expires_at': '2099-01-01T00:00:00Z'})
    api_stub.routes['GET', '/v1/isolates/' + api_stub.isolate_id] = lambda _request: httpx.Response(200, json={'id': str(uuid4()), 'status': 'ready'})
    with pytest.raises(ProtocolError):
        configured.get(b'state')


@pytest.mark.parametrize('kind', ['expired', 'wrong_id'])
def test_invalid_lease_is_not_silently_used(kind, configured, api_stub):
    configured.settings.renew_interval = 0.01
    if kind == 'expired':
        api_stub.routes['POST', '/v1/isolates'] = lambda _request: httpx.Response(202, json={
            'id': api_stub.isolate_id, 'computer_id': api_stub.computer_id, 'status': 'ready', 'expires_at': '2000-01-01T00:00:00Z',
        })
        with pytest.raises(IsolateUnavailableError):
            configured.get(b'state')
    else:
        api_stub.routes['POST', '/v1/isolates/' + api_stub.isolate_id + '/renew'] = lambda _request: httpx.Response(200, json={'id': str(uuid4()), 'expires_at': '2099-01-01T00:00:00Z'})
        with configured.scope as isolate:
            assert isolate._lease.stop.wait(2)
            with pytest.raises(ProtocolError):
                isolate.run('work')


@pytest.mark.parametrize('header', ['garbage', 'Wed, 21 Oct 2026 12:00:00', 'Fri, 31 Dec 9999 23:59:59 GMT'])
def test_invalid_http_date_is_a_protocol_error(header):
    with pytest.raises(ProtocolError):
        Reply(b'', {'date': header}, time.monotonic()).deadline(datetime.now(timezone.utc))


def test_http_date_rounding_cannot_overestimate_remote_lease():
    """Prevent clock skew from expiring live leases or extending them past server expiry."""
    current = datetime(2026, 10, 21, 12, 0, tzinfo=timezone.utc)
    deadline = Reply(b'', {'date': format_datetime(current)}, 100).deadline(current + timedelta(seconds=60))
    assert deadline == 159


@pytest.mark.parametrize('command', ['', ' ', 'x\0y', pytest.param('x' * 65537, id='too-long'), None])
def test_invalid_command_does_not_contact_command_api(command, configured, api_stub):
    """Reject invalid commands without overflowing Windows environment limits in pytest metadata."""
    with configured.scope as isolate:
        count = len(api_stub.requests)
        with pytest.raises(ValueError, match='Command must'):
            isolate.run(command)
        assert len(api_stub.requests) == count


@pytest.mark.parametrize('body', [[], [{}, {}], [{'id': 'bad'}], [{'id': None}], [{'status': 'running', 'result': {}}]])
def test_command_result_envelope_is_strict(body, configured, api_stub):
    for row in body:
        row.setdefault('id', api_stub.command_id)
    api_stub.routes['POST', '/v1/commands/results'] = lambda _request: httpx.Response(200, json=body)
    with configured.scope as isolate, pytest.raises(ProtocolError):
        isolate.run('work')


def test_failed_cancellation_does_not_replace_original_poll_failure(configured, api_stub):
    api_stub.routes['POST', '/v1/commands/results'] = lambda _request: httpx.Response(200, json=[])
    api_stub.routes['POST', '/v1/commands/cancel'] = lambda _request: httpx.Response(409)
    with configured.scope as isolate, pytest.raises(ProtocolError):
        isolate.run('work')


@pytest.mark.parametrize('mode', ['pending', 'failed', 'obsolete', 'unknown', 'wrong_id'])
def test_snapshot_waiting_and_failures_are_bounded(mode, configured, api_stub):
    """Prevent a long poll pause from overrunning the snapshot completion budget."""
    configured.settings.operation_timeout = 0.02
    if mode == 'pending':
        configured.settings.poll_interval = 60
    api_stub.routes['GET', '/v1/operations/' + api_stub.operation_id] = lambda _request: httpx.Response(200, json={
        'id': str(uuid4()) if mode == 'wrong_id' else api_stub.operation_id, 'status': mode,
    })
    expected = OperationTimeoutError if mode == 'pending' else IsolateUnavailableError if mode in ('failed', 'obsolete') else ProtocolError
    with configured.scope as isolate, ThreadPoolExecutor(max_workers=1) as pool:
        snapshot = pool.submit(isolate.read)
        try:
            with pytest.raises(expected):
                snapshot.result(timeout=2)
        finally:
            isolate._lease.stop.set()


@pytest.mark.parametrize('phase', ['before', 'between'])
def test_package_installation_canceled_before_launch(phase, configured, api_stub):
    """Prevent cancellation between packages from being reported as a pip failure or launching another package."""
    token = SimpleToken()
    if phase == 'before':
        token.cancel()
    else:
        def complete(_request):
            token.cancel()
            return result(api_stub)
        api_stub.routes['POST', '/v1/commands/results'] = complete
    with configured.scope as isolate:
        with pytest.raises(InterruptedInstallationError):
            isolate.install('package', 'next-package', token=token)
        for package in ('', ' ', 'bad\0package'):
            with pytest.raises(ValueError, match='Package names'):
                isolate.install(package)
        assert sum(r.url.path.endswith('/commands') for r in api_stub.requests) == (0 if phase == 'before' else 1)


@pytest.mark.parametrize('mutation', ['wrong_computer', 'huge_score', 'invalid_time', 'wrong_name', 'missing_row', 'deep_json'])
def test_benchmark_boundary_errors_are_metadata_errors(mutation, configured, api_stub):
    """Prevent malformed or deeply nested benchmark JSON from escaping MetadataError or poisoning commands."""
    row = {'computer_id': api_stub.computer_id, 'benchmark_name': 'single_core', 'number': 1,
           'created_at': '2026-10-21T12:00:00Z', 'benchmarker_version': '1.0.0'}
    if mutation == 'wrong_computer':
        row['computer_id'] = str(uuid4())
    elif mutation == 'huge_score':
        row['number'] = 10 ** 400
    elif mutation == 'invalid_time':
        row['created_at'] = 'yesterday'
    elif mutation == 'wrong_name':
        row['benchmark_name'] = 42
    response = (
        httpx.Response(200, content=b'[' * 10000 + b'0' + b']' * 10000)
        if mutation == 'deep_json' else httpx.Response(200, json=[None if mutation == 'missing_row' else row])
    )
    api_stub.routes['GET', '/v1/computers/' + api_stub.computer_id + '/benchmark-results'] = lambda _request: response
    with configured.scope as isolate:
        with pytest.raises(MetadataError):
            isolate.get_benchmarks()
        assert isolate.run('work').success


def test_expired_lease_unblocks_callers(configured):
    with configured.scope as isolate:
        isolate._lease.deadline = time.monotonic() - 1
        assert isolate._lease.stop.wait(2)
        with pytest.raises(IsolateUnavailableError):
            isolate.read()


@pytest.mark.parametrize('cleanup', ['explicit', 'garbage_collection', 'kill'])
def test_remote_cleanup_failure_closes_local_resources(cleanup, configured, api_stub):
    """Prevent leaks or swallowed explicit DELETE errors while destructors suppress cleanup failures."""
    isolate = configured.get(b'state')
    client, thread = isolate._api._client, isolate._lease.thread
    reference = weakref.ref(isolate)
    route = ('DELETE', '/v1/isolates/' + api_stub.isolate_id)
    api_stub.routes[route] = lambda _request: httpx.Response(409)
    try:
        if cleanup == 'explicit':
            isolate.__del__()
        elif cleanup == 'kill':
            with pytest.raises(APIError) as caught:
                isolate.kill()
            assert caught.value.status_code == 409
            isolate.kill()
        else:
            del isolate
            gc.collect()
            assert reference() is None
        assert client.is_closed
        assert not thread.is_alive()
        assert sum(r.method == 'DELETE' for r in api_stub.requests) == 1
    finally:
        survivor = reference()
        if survivor is not None and not client.is_closed:
            api_stub.routes[route] = lambda _request: httpx.Response(202)
            survivor.kill()


@pytest.mark.parametrize('operation', ['metadata', 'snapshot'])
def test_close_waits_for_inflight_reads(operation, configured, api_stub):
    """Prevent closing HTTPX while a metadata or snapshot operation still needs the client."""
    started, release, deleted = threading.Event(), threading.Event(), threading.Event()
    def benchmarks(_request):
        started.set()
        assert release.wait(3)
        return httpx.Response(200, json=[] if operation == 'metadata' else {
            'id': api_stub.operation_id, 'status': 'completed', 'artifact_id': api_stub.artifact_id,
        })
    def delete(_request):
        deleted.set()
        return httpx.Response(202)
    path = '/v1/computers/' + api_stub.computer_id + '/benchmark-results' if operation == 'metadata' else '/v1/operations/' + api_stub.operation_id
    api_stub.routes['GET', path] = benchmarks
    api_stub.routes['DELETE', '/v1/isolates/' + api_stub.isolate_id] = delete
    isolate = configured.get(b'state')
    with ThreadPoolExecutor(max_workers=2) as pool:
        reading = pool.submit(isolate.get_benchmarks if operation == 'metadata' else isolate.read)
        assert started.wait(2)
        closing = pool.submit(isolate.kill)
        try:
            assert deleted.wait(2)
            with pytest.raises(FutureTimeout):
                closing.result(timeout=0.05)
            assert not isolate._api._client.is_closed
        finally:
            release.set()
        output = reading.result(timeout=2)
        if operation == 'metadata':
            assert output.single_core is None
        else:
            assert output == api_stub.content
        closing.result(timeout=2)
    assert isolate._api._client.is_closed


def test_excluded_file_is_not_archived(tmp_path):
    (tmp_path / 'secret').write_text('never upload')
    with open_tar(fileobj=BytesIO(LiquidManager(tmp_path, exclude=['secret']).read())) as archive:
        assert archive.getnames() == []


@pytest.mark.parametrize('bound', ['interval', 'lease'])
def test_lease_renewal_deadline_is_independent_of_result_poll_interval(bound, configured, api_stub):
    """Prevent slow polling or a long renew interval from missing initial or renewed short leases."""
    configured.settings.poll_interval = 1
    configured.settings.renew_interval = 0.01 if bound == 'interval' else 30
    def payload():
        return {
            'id': api_stub.isolate_id, 'computer_id': api_stub.computer_id, 'status': 'ready',
            'expires_at': (datetime.now(timezone.utc) + timedelta(seconds=0.6)).isoformat(),
        }
    if bound == 'lease':
        api_stub.routes['POST', '/v1/isolates'] = lambda _request: httpx.Response(202, json=payload())
    renewed = threading.Event()
    count = 0
    def renew(_request):
        nonlocal count
        count += 1
        if count == 2:
            renewed.set()
        return httpx.Response(200, json=payload())
    api_stub.routes['POST', '/v1/isolates/' + api_stub.isolate_id + '/renew'] = renew
    with configured.scope:
        assert renewed.wait(2)
