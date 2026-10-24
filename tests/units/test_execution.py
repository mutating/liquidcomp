"""Remote lifecycle, cancellation and metadata contracts precede implementation."""

import hashlib
import json
import ntpath
import os
import stat
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from datetime import datetime, timedelta, timezone
from io import BytesIO
from tarfile import open as open_tar
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from cantok import DefaultToken, SimpleToken, TimeoutToken
from throng import throng
from throng.errors import (
    CannotInstallDependencyError,
    InterruptedInstallationError,
    NotSuccessfulRunError,
    PreparationCommandFailedError,
)

from liquidcomp import LiquidManager, Settings, manager as manager_module
from liquidcomp.errors import (
    APIError,
    CancellationTimeoutError,
    IsolateClosedError,
    IsolateUnavailableError,
    MetadataError,
    OperationCancelledError,
    OperationTimeoutError,
    ProtocolError,
    StateError,
)
from liquidcomp.isolate import _Lease


def result(api, status='completed', **changes):
    """Construct an API result independently of the plugin's deserializer."""
    payload = {'command_id': api.command_id, 'success': status == 'completed', 'returncode': 0,
               'stdout': 'out', 'stderr': '', 'error': None,
               'stdout_artifact_id': None, 'stderr_artifact_id': None}
    payload.update(changes)
    return httpx.Response(200, json=[{'id': api.command_id, 'status': status, 'result': payload}])


@pytest.mark.parametrize(('mode', 'exact_cap'), [(0o644, False), (0o755, False), (0o755, True)])
def test_directory_state_preserves_files_empty_directories_and_exclusions(tmp_path, mode, exact_cap):
    """Prevent lost permissions, excluded-file leaks or rejection of a state exactly at its limit."""
    (tmp_path / 'empty').mkdir()
    (tmp_path / 'source.txt').write_text('payload')
    (tmp_path / 'source.txt').chmod(mode)
    (tmp_path / 'ignored').mkdir()
    (tmp_path / 'ignored' / 'secret').write_text('private')
    manager = LiquidManager(tmp_path, exclude=['ignored/'])
    if exact_cap:
        manager.settings.max_artifact_bytes = len(manager.read())
    with open_tar(fileobj=BytesIO(manager.read())) as archive:
        assert set(archive.getnames()) == {'empty', 'source.txt'}
        assert archive.extractfile('source.txt').read() == b'payload'
        assert archive.getmember('source.txt').mode == stat.S_IMODE((tmp_path / 'source.txt').stat().st_mode)


def test_directory_state_rejects_missing_root(tmp_path):
    """State errors are raised before any credential or network operation."""
    with pytest.raises(StateError):
        LiquidManager(tmp_path / 'missing').read()


@pytest.mark.parametrize('target', ['outside', 'outside_prefix', 'dangling', 'loop', 'cross_drive'])
@pytest.mark.parametrize('excluded', [False, True])
def test_directory_state_rejects_escaping_links(tmp_path, target, excluded, monkeypatch):
    """Protect StateError for unsafe links, including different Windows drives, while honoring exclusions."""
    link = tmp_path / 'outside'
    prefix_target = tmp_path.with_name(tmp_path.name + '-private')
    if target == 'outside_prefix':
        prefix_target.mkdir()
    try:
        link.symlink_to({'outside': tmp_path.parent, 'outside_prefix': prefix_target, 'dangling': tmp_path / 'missing', 'loop': link, 'cross_drive': tmp_path.parent}[target])
    except OSError:
        pytest.skip('Symlink privileges unavailable')
    manager = LiquidManager(tmp_path, exclude=['outside'] if excluded else None)
    def cross_drive(_paths):
        return ntpath.commonpath((r'C:\state', r'D:\outside'))
    with monkeypatch.context() as patch:
        if target == 'cross_drive':
            patch.setattr(os.path, 'commonpath', cross_drive)
        if excluded:
            with open_tar(fileobj=BytesIO(manager.read())) as archive:
                assert archive.getnames() == []
        else:
            with pytest.raises(StateError):
                manager.read()


@pytest.mark.parametrize(('resources', 'expected', 'timeout'), [
    ({}, (1000, 536870912, 1073741824), 3600),
    ({'cpu': 2.125, 'ram': 0.75, 'disk': 2.5}, (2125, 805306368, 2684354560), 3600),
    ({}, (1000, 536870912, 1073741824), 86400),
])
def test_fixed_runtime_resources_and_command_settings(resources, expected, timeout, configured, api_stub):
    """Protect requested resources, the fixed runtime and acceptance of the full 24-hour timeout."""
    for name, value in resources.items():
        setattr(configured.settings, name, value)
    if timeout != 3600:
        configured.settings.command_timeout = timeout
    isolate = configured.get(b'state')
    try:
        output = isolate.run('echo hello')
        assert output.success
        assert output.returncode == 0
        assert output.stdout == 'ok\n'
        creates = [r for r in api_stub.requests if r.url.path == '/v1/isolates']
        body = json.loads(creates[0].content)
        assert (body['os'], body['architecture'], body['runtime']) == ('linux', 'arm64', 'powercell-ubuntu-arm64-v1')
        assert (body['cpu_millis'], body['memory_bytes'], body['disk_bytes']) == expected
        assert body['retry_allowed'] is False
        assert 'company_id' not in body
        assert creates[0].headers['Idempotency-Key']
        command = next(r for r in api_stub.requests if r.url.path.endswith('/commands'))
        assert json.loads(command.content) == {'command': 'echo hello', 'shell': False, 'cwd': '.', 'env': {}, 'timeout_seconds': timeout}
        assert api_stub.requests[0].url.path == '/v1/auth/check'
    finally:
        isolate.kill()


def test_runtime_has_no_settings_override():
    """The first plugin version does not silently accept unsupported runtimes."""
    for name in ['os', 'architecture', 'runtime']:
        with pytest.raises(KeyError):
            Settings(**{name: 'other'})


@pytest.mark.parametrize('status', ['completed', 'failed', 'cancelled', 'timed_out', 'isolate_failed'])
def test_terminal_results_and_standard_throng_exception_mode(status, configured, api_stub):
    """Preserve server truth even for cancellation and process-launch failure."""
    api_stub.routes['POST', '/v1/commands/results'] = lambda _request: result(
        api_stub, status, success=False, returncode=None, stdout=None, stderr='failure', error='failed',
    )
    with configured.scope as isolate:
        output = isolate.run('false')
        assert not output.success
        assert output.returncode is None
        assert output.stderr == 'failure'
        assert output.stdout is None
        assert output.error == 'failed'
        assert output.status == status
        with pytest.raises(NotSuccessfulRunError) as caught:
            isolate.run('false', exception=True)
        assert caught.value.result.status == status


@pytest.mark.parametrize('mutation', ['null', 'unknown_status', 'wrong_id', 'bad_bool', 'bad_returncode', 'missing_result', 'bad_stdout', 'bad_stderr', 'bad_error'])
def test_malformed_command_responses_are_not_success(mutation, configured, api_stub):
    """Prevent malformed output/diagnostics or mismatched replies from becoming successful results."""
    def malformed(_request):
        payload = json.loads(result(api_stub).content)
        if mutation == 'null':
            return httpx.Response(200, json=None)
        if mutation == 'unknown_status':
            payload[0]['status'] = 'future_unknown_state'
        elif mutation == 'wrong_id':
            payload[0]['id'] = str(uuid4())
        elif mutation == 'bad_bool':
            payload[0]['result']['success'] = 'true'
        elif mutation == 'bad_returncode':
            payload[0]['result']['returncode'] = True
        elif mutation == 'missing_result':
            payload[0]['result'] = None
        elif mutation in ('bad_stdout', 'bad_stderr', 'bad_error'):
            payload[0]['result'][mutation[4:]] = {'invalid': 'object'}
        return httpx.Response(200, json=payload)
    api_stub.routes['POST', '/v1/commands/results'] = malformed
    with configured.scope as isolate, pytest.raises(ProtocolError):
        isolate.run('echo hello')


def test_cancel_before_create_does_not_contact_server(configured, api_stub):
    """An already canceled allocation does not upload anything or consume capacity."""
    with pytest.raises(OperationCancelledError):
        configured.get(b'state', token=TimeoutToken(0))
    assert not api_stub.requests


def test_cancel_before_run_does_not_submit_command(configured, api_stub):
    """A canceled call returns a normal unsuccessful throng result without a remote launch."""
    with configured.scope as isolate:
        output = isolate.run('never', token=TimeoutToken(0))
        assert not output.success
        assert output.killed_by_token
        assert output.returncode is None
        assert not any(r.url.path.endswith('/commands') for r in api_stub.requests)


@pytest.mark.parametrize(('pending_status', 'operation'), [
    ('queued', 'run'), ('dispatched', 'run'), ('running', 'run'), ('cancel_requested', 'run'), ('running', 'install'),
])
def test_cancellation_waits_for_confirmation_and_preserves_winning_completion(pending_status, operation, configured, api_stub):
    """Prevent losing install cancellation tokens or treating a cancellation acknowledgement as completion."""
    token = SimpleToken()
    cancel_seen = threading.Event()
    polls = []
    def cancel(_request):
        cancel_seen.set()
        return httpx.Response(202, json=[{'id': api_stub.command_id, 'status': 'cancel_requested'}])
    def poll(request):
        polls.append(request)
        assert len(polls) < 100, 'The remote command never received cancellation.'
        token.cancel()
        if cancel_seen.is_set() and len(polls) > 2:
            return result(api_stub, stdout='completed despite cancellation')
        return httpx.Response(200, json=[{'id': api_stub.command_id, 'status': pending_status, 'result': None}])
    api_stub.routes['POST', '/v1/commands/results'] = poll
    api_stub.routes['POST', '/v1/commands/cancel'] = cancel
    with configured.scope as isolate:
        output = getattr(isolate, operation)('work', token=token)
        assert cancel_seen.is_set()
        if operation == 'run':
            assert output.success
            assert output.stdout == 'completed despite cancellation'
        else:
            assert output is None
        assert len(polls) > 2


@pytest.mark.parametrize('poll_interval', [0.01, 60])
def test_unconfirmed_cancellation_is_bounded(poll_interval, configured, api_stub):
    """Prevent a long polling pause from hiding an expired cancellation budget."""
    configured.settings.cancellation_timeout = 0.05
    configured.settings.poll_interval = poll_interval
    api_stub.routes['POST', '/v1/isolates'] = lambda _request: httpx.Response(202, json={
        'id': api_stub.isolate_id, 'computer_id': api_stub.computer_id, 'status': 'ready',
        'expires_at': (datetime.now(timezone.utc) + timedelta(seconds=0.6)).isoformat(),
    })
    cancel_seen = threading.Event()
    def cancel(_request):
        cancel_seen.set()
        return httpx.Response(202)
    api_stub.routes['POST', '/v1/commands/cancel'] = cancel
    api_stub.routes['POST', '/v1/commands/results'] = lambda _request: httpx.Response(
        200, json=[{'id': api_stub.command_id, 'status': 'running', 'result': None}],
    )
    token = SimpleToken()
    def command(_request):
        token.cancel()
        return httpx.Response(202, json={'id': api_stub.command_id, 'queued_deadline': (datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat()})
    api_stub.routes['POST', f'/v1/isolates/{api_stub.isolate_id}/commands'] = command
    with configured.scope as isolate, ThreadPoolExecutor(max_workers=1) as pool:
        execution = pool.submit(isolate.run, 'work', token=token)
        try:
            assert cancel_seen.wait(2)
            with pytest.raises(CancellationTimeoutError):
                execution.result(timeout=2)
        finally:
            isolate._lease.stop.set()


def test_failed_provisioning_is_cleaned_up(configured, api_stub):
    """A known remote allocation is destroyed when provisioning fails."""
    api_stub.routes['POST', '/v1/isolates'] = lambda _request: httpx.Response(202, json={'id': api_stub.isolate_id, 'status': 'failed'})
    with pytest.raises(IsolateUnavailableError):
        configured.get(b'state')
    assert any(r.method == 'DELETE' and r.url.path.endswith(api_stub.isolate_id) for r in api_stub.requests)


@pytest.mark.parametrize('poll_interval', [0.01, 60])
def test_provisioning_timeout_is_bounded_and_cleaned_up(poll_interval, configured, api_stub, monkeypatch):
    """Protect the provisioning budget from long sleeps and prevent client or worker leaks."""
    workers = []
    original_start = _Lease.start
    def start(worker):
        workers.append(worker)
        original_start(worker)
    monkeypatch.setattr(_Lease, 'start', start)
    configured.settings.operation_timeout = 0.03
    configured.settings.poll_interval = poll_interval
    clock = [0.0]
    def sleep(delay):
        assert delay <= configured.settings.operation_timeout
        clock[0] += delay
    monkeypatch.setattr(manager_module, 'time', SimpleNamespace(monotonic=lambda: clock[0], sleep=sleep))
    for method, path in [('POST', '/v1/isolates'), ('GET', f'/v1/isolates/{api_stub.isolate_id}')]:
        api_stub.routes[method, path] = lambda _request: httpx.Response(202, json={'id': api_stub.isolate_id, 'status': 'provisioning', 'expires_at': '2099-01-01T00:00:00Z'})
    try:
        with pytest.raises(OperationTimeoutError):
            configured.get(b'state')
        assert any(r.method == 'DELETE' for r in api_stub.requests)
        assert len(workers) == 1
        assert not workers[0].thread.is_alive()
        assert workers[0].api._client.is_closed
    finally:
        for worker in workers:
            worker.close()
            worker.api.close()


@pytest.mark.parametrize('phase', ['idle', 'running'])
def test_lease_renewal_is_independent_of_command_execution(phase, configured, api_stub):
    """Prevent ignored renewal expiry or canceling a running command when only its queue window elapsed."""
    configured.settings.renew_interval = 0.02
    heartbeat = threading.Condition()
    renewals = 0
    def renew(_request):
        nonlocal renewals
        with heartbeat:
            renewals += 1
            heartbeat.notify_all()
        return httpx.Response(200, json={'id': api_stub.isolate_id, 'expires_at': '2099-01-01T00:00:00Z'})
    def poll(_request):
        nonlocal renewals
        with heartbeat:
            renewals = 0
            assert heartbeat.wait_for(lambda: renewals >= 2, timeout=2)
        return result(api_stub)
    if phase == 'running':
        api_stub.routes['POST', '/v1/isolates/' + api_stub.isolate_id + '/commands'] = lambda _request: httpx.Response(202, json={
            'id': api_stub.command_id, 'queued_deadline': (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(),
        })
        api_stub.routes['POST', '/v1/commands/results'] = poll
    api_stub.routes['POST', f'/v1/isolates/{api_stub.isolate_id}/renew'] = renew
    with configured.scope as isolate:
        if phase == 'running':
            assert isolate.run('work').success
        with heartbeat:
            assert heartbeat.wait_for(lambda: renewals >= 2, timeout=2)
        assert isolate._lease.deadline > time.monotonic() + 86400
        assert not any(r.url.path == '/v1/commands/cancel' for r in api_stub.requests)
    before = len(api_stub.requests)
    time.sleep(0.05)
    assert len(api_stub.requests) == before


def test_renewal_failure_unblocks_waiting_command(configured, api_stub):
    """A lost lease fails the waiter instead of leaving a daemon failure invisible."""
    configured.settings.renew_interval = 0.02
    api_stub.routes['POST', f'/v1/isolates/{api_stub.isolate_id}/renew'] = lambda _request: httpx.Response(409)
    api_stub.routes['POST', '/v1/commands/results'] = lambda _request: httpx.Response(200, json=[{'id': api_stub.command_id, 'status': 'running', 'result': None}])
    with configured.scope as isolate, pytest.raises((IsolateUnavailableError, APIError)):
        isolate.run('work')


def test_kill_is_idempotent_and_blocks_further_operations(configured, api_stub):
    """A dead isolate cannot launch commands, snapshot state, or fetch live metadata."""
    isolate = configured.get(b'state')
    identity = isolate.get_id()
    isolate.kill()
    isolate.kill()
    assert isolate.get_id() == identity
    assert sum(r.method == 'DELETE' for r in api_stub.requests) == 1
    for operation in [lambda: isolate.run('x'), isolate.read, isolate.get_benchmarks]:
        with pytest.raises(IsolateClosedError):
            operation()


@pytest.mark.parametrize(('pending_status', 'exact_cap'), [
    (None, False), ('pending', False), ('dispatched', False), ('accepted', False), (None, True),
])
def test_snapshot_round_trip_and_checksum(pending_status, exact_cap, configured, api_stub):
    """Prevent rejecting a snapshot at its size limit or valid intermediate states before completion."""
    statuses = iter([pending_status, 'completed'] if pending_status else ['completed'])
    polls = []
    def poll(request):
        polls.append(request)
        return httpx.Response(200, json={'id': api_stub.operation_id, 'status': next(statuses), 'artifact_id': api_stub.artifact_id})
    api_stub.routes['GET', '/v1/operations/' + api_stub.operation_id] = poll
    with configured.scope as isolate:
        if exact_cap:
            configured.settings.max_artifact_bytes = len(api_stub.content)
        assert isolate.read() == api_stub.content
    assert len(polls) == (2 if pending_status else 1)
    assert any(r.url.path.endswith('/snapshots') and r.headers.get('Idempotency-Key') for r in api_stub.requests)


@pytest.mark.parametrize('phase', ['poll', 'logs'])
def test_commands_on_one_isolate_are_serialized_through_result(phase, configured, api_stub):
    """Prevent concurrent runs from replacing the pending command during polling or log retrieval."""
    blocked, release, second_started = threading.Event(), threading.Event(), threading.Event()
    submits = []
    def submit(request):
        submits.append(json.loads(request.content)['command'])
        return httpx.Response(202, json={'id': api_stub.command_id, 'queued_deadline': '2099-01-01T00:00:00Z'})
    def poll(_request):
        if submits == ['first']:
            if phase == 'poll':
                blocked.set()
                assert release.wait(3)
            else:
                return result(api_stub, stdout_artifact_id=api_stub.artifact_id)
        return result(api_stub)
    def download(_request):
        blocked.set()
        assert release.wait(3)
        return httpx.Response(200, content=b'logs', headers={'X-Content-SHA256': hashlib.sha256(b'logs').hexdigest()})
    def second(isolate):
        second_started.set()
        return isolate.run('second')
    api_stub.routes['POST', '/v1/isolates/' + api_stub.isolate_id + '/commands'] = submit
    api_stub.routes['POST', '/v1/commands/results'] = poll
    api_stub.routes['GET', '/v1/artifacts/' + api_stub.artifact_id + '/content'] = download
    with configured.scope as isolate, ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(isolate.run, 'first')
        try:
            assert blocked.wait(2)
            waiting = pool.submit(second, isolate)
            assert second_started.wait(2)
            with pytest.raises(FutureTimeout):
                waiting.result(timeout=0.05)
            assert submits == ['first']
        finally:
            release.set()
        assert first.result(timeout=2).success
        assert waiting.result(timeout=2).success
    assert submits == ['first', 'second']


@pytest.mark.parametrize('stream', ['stdout', 'stderr'])
def test_output_artifact_overrides_inline_preview_and_keeps_binary(stream, configured, api_stub):
    """Prevent either stream from losing full binary output or incorrectly retaining its inline preview."""
    api_stub.content = b'full output\xff'
    api_stub.routes['POST', '/v1/commands/results'] = lambda _request: result(
        api_stub, **{stream: 'preview', stream + '_artifact_id': api_stub.artifact_id},
    )
    with configured.scope as isolate:
        output = isolate.run('work')
        assert getattr(output, stream) == 'full output\ufffd'
        assert getattr(output, stream + '_bytes') == api_stub.content


@pytest.mark.parametrize('failure', ['checksum', 'oversize', 'oversize_stream'])
def test_invalid_artifact_is_not_returned(failure, configured, api_stub, byte_stream):
    """Prevent corrupt snapshots or oversized artifacts with valid checksums from being returned."""
    content = b'payload'
    checksum = hashlib.sha256(b'other' if failure == 'checksum' else content).hexdigest()
    api_stub.routes['GET', f'/v1/artifacts/{api_stub.artifact_id}/content'] = lambda _request: (
        httpx.Response(200, stream=byte_stream([content[:4], content[4:]]), headers={'X-Content-SHA256': checksum})
        if failure == 'oversize_stream' else httpx.Response(200, content=content, headers={'X-Content-SHA256': checksum})
    )
    with configured.scope as isolate:
        if failure.startswith('oversize'):
            configured.settings.max_artifact_bytes = len(content) - 1
        with pytest.raises(ProtocolError):
            isolate.read()


@pytest.mark.parametrize(('phase', 'error'), [('prepare', PreparationCommandFailedError), ('packages', InterruptedInstallationError)])
@pytest.mark.parametrize('entrypoint', ['direct', 'plugin'])
def test_setup_failure_uses_throng_cleanup(phase, error, entrypoint, configured, api_stub):
    """Prevent constructor or registered-plugin setup arguments from being dropped or leaking allocations."""
    manager = (
        LiquidManager(settings=configured.settings, **{phase: ['failing']}) if entrypoint == 'direct'
        else throng['liquid']('.', **{phase: ['failing']})['liquid']
    )
    api_stub.routes['POST', '/v1/commands/results'] = lambda _request: result(api_stub, success=False, returncode=1)
    with pytest.raises(error):
        manager.get(b'state')
    assert any(r.method == 'DELETE' for r in api_stub.requests)


@pytest.mark.parametrize('extra_package', [False, True])
def test_package_installation_quotes_arguments_and_propagates_failure(extra_package, configured, api_stub):
    """Prevent dropping later packages, shell injection or continuing installation after a pip failure."""
    packages = ['example>=1; echo bad'] + (['second-package'] if extra_package else [])
    expected = ["python3 -m pip install -- 'example>=1; echo bad'"] + (['python3 -m pip install -- second-package'] if extra_package else [])
    with configured.scope as isolate:
        isolate.install(*packages)
        commands = [json.loads(r.content)['command'] for r in api_stub.requests if r.url.path.endswith('/commands')]
        assert commands == expected
        api_stub.routes['POST', '/v1/commands/results'] = lambda _request: result(api_stub, success=False, returncode=1)
        with pytest.raises(CannotInstallDependencyError):
            isolate.install('example', 'never-installed')
        commands = [json.loads(r.content)['command'] for r in api_stub.requests if r.url.path.endswith('/commands')]
        assert commands == [*expected, 'python3 -m pip install -- example']


@pytest.mark.parametrize('records', [[], ['single_core'], ['multi_core'], ['single_core', 'multi_core']])
@pytest.mark.parametrize(('score_pair', 'cpu'), [((0, 123.5), 1), ((123.5, 0), 1), ((123.5, 456), 2.5)])
def test_benchmarks_are_optional_independently_and_fresh(records, score_pair, cpu, configured, api_stub):
    """Protect independent scores and dates without rescaling, dropping zeros or inventing data."""
    configured.settings.cpu = cpu
    scores = {'single_core': score_pair[0], 'multi_core': score_pair[1]}
    dates = {'single_core': '2026-10-19T12:30:00Z', 'multi_core': '2026-10-19T12:31:00Z'}
    rows = [{'computer_id': api_stub.computer_id, 'benchmark_name': name,
             'number': scores[name], 'created_at': dates[name], 'benchmarker_version': '0.0.2'} for name in records]
    api_stub.routes['GET', f'/v1/computers/{api_stub.computer_id}/benchmark-results'] = lambda _request: httpx.Response(200, json=rows)
    with configured.scope as isolate:
        metadata = isolate.meta
        assert metadata.id == isolate.get_id()
        assert metadata.resources.shared is True
        assert metadata.resources.shared_with is None
        for name in ['single_core', 'multi_core']:
            value = getattr(metadata.resources.benchmarks, name)
            assert (value is None) == (name not in records)
            if value is not None:
                assert value.number == scores[name]
                assert value.benchmarker_version == '0.0.2'
                assert value.benchmarker == 'benchset'
                assert value.when == datetime.fromisoformat(dates[name].replace('Z', '+00:00'))
        rows.clear()
        assert isolate.meta.resources.benchmarks.single_core is None


@pytest.mark.parametrize('bad', ['negative', 'nan', 'true_score', 'false_score', 'version', 'naive_time', 'duplicate', 'mixed_versions'])
def test_malformed_benchmark_records_are_errors_not_missing_data(bad, configured, api_stub):
    """Prevent malformed measurements, including JSON booleans, from becoming numeric scores."""
    row = {'computer_id': api_stub.computer_id, 'benchmark_name': 'single_core', 'number': 1.0,
           'created_at': '2026-10-19T12:30:00Z', 'benchmarker_version': '0.0.2'}
    rows = [row]
    if bad == 'negative':
        row['number'] = -1
    elif bad == 'nan':
        row['number'] = 'NaN'
    elif bad in ('true_score', 'false_score'):
        row['number'] = bad == 'true_score'
    elif bad == 'version':
        row['benchmarker_version'] = 'not-a-version'
    elif bad == 'naive_time':
        row['created_at'] = '2026-10-19T12:30:00'
    elif bad == 'duplicate':
        rows.append(dict(row))
    else:
        rows.append({**row, 'benchmark_name': 'multi_core', 'benchmarker_version': '0.0.3'})
    api_stub.routes['GET', f'/v1/computers/{api_stub.computer_id}/benchmark-results'] = lambda _request: httpx.Response(200, json=rows)
    with configured.scope as isolate, pytest.raises(MetadataError):
        isolate.get_benchmarks()


@pytest.mark.parametrize('status', [401, 403, 404, 503])
def test_metadata_access_errors_do_not_stop_command_execution(status, configured, api_stub):
    """An inaccessible measurement endpoint is distinct from an empty benchmark suite."""
    api_stub.routes['GET', f'/v1/computers/{api_stub.computer_id}/benchmark-results'] = lambda _request: httpx.Response(status)
    with configured.scope as isolate:
        with pytest.raises(MetadataError):
            isolate.get_benchmarks()
        assert isolate.run('work').success


@pytest.mark.parametrize('field', ['single_core', 'multi_core'])
def test_unknown_future_benchmarks_do_not_break_metadata(field, configured, api_stub):
    """Prevent unknown benchmark names from being misclassified into either known score field."""
    api_stub.routes['GET', f'/v1/computers/{api_stub.computer_id}/benchmark-results'] = lambda _request: httpx.Response(200, json=[{
        'computer_id': api_stub.computer_id, 'benchmark_name': 'future_gpu', 'number': 9,
        'created_at': '2026-10-19T12:30:00Z', 'benchmarker_version': '1.0.0',
    }])
    with configured.scope as isolate:
        assert getattr(isolate.get_benchmarks(), field) is None


@pytest.mark.parametrize(('status', 'code'), [('failed', 0), ('cancelled', 0), ('completed', 1), ('completed', None)])
def test_contradictory_success_flag_is_a_protocol_error(status, code, configured, api_stub):
    """A success flag cannot turn a canceled or failed process into successful execution."""
    api_stub.routes['POST', '/v1/commands/results'] = lambda _request: result(api_stub, status, success=True, returncode=code)
    with configured.scope as isolate, pytest.raises(ProtocolError):
        isolate.run('work')


@pytest.mark.parametrize('status', ['timed_out', 'cancelled'])
@pytest.mark.parametrize('late_cancel', ['never', 'cancel_request', 'result', 'artifact'])
def test_watchdog_cancellation_is_not_reported_as_user_token_cancellation(status, late_cancel, configured, api_stub):
    """The watchdog's cause survives token cancellation after its decision, including log retrieval."""
    configured.settings.command_timeout = 1
    cancelled = threading.Event()
    user_token = SimpleToken()
    token = DefaultToken() if late_cancel == 'never' else user_token
    api_stub.routes['POST', f'/v1/isolates/{api_stub.isolate_id}/commands'] = lambda _request: httpx.Response(202, json={'id': api_stub.command_id, 'queued_deadline': '2000-01-01T00:00:00Z'})
    def cancel(_request):
        assert token, 'The watchdog must initiate cancellation before the token expires.'
        if late_cancel == 'cancel_request':
            user_token.cancel()
        cancelled.set()
        return httpx.Response(202)
    def poll(_request):
        if not cancelled.is_set():
            return httpx.Response(200, json=[{'id': api_stub.command_id, 'status': 'running', 'result': None}])
        if late_cancel == 'result':
            user_token.cancel()
        return result(api_stub, status, stdout_artifact_id=api_stub.artifact_id)
    def download(_request):
        if late_cancel == 'artifact':
            user_token.cancel()
        return httpx.Response(200, content=api_stub.content, headers={'X-Content-SHA256': hashlib.sha256(api_stub.content).hexdigest()})
    api_stub.routes['POST', '/v1/commands/cancel'] = cancel
    api_stub.routes['POST', '/v1/commands/results'] = poll
    api_stub.routes['GET', '/v1/artifacts/' + api_stub.artifact_id + '/content'] = download
    with configured.scope as isolate:
        output = isolate.run('work', token=token)
        assert cancelled.is_set()
        assert output.status == status
        assert output.killed_by_token is False
        assert output.stdout_bytes == api_stub.content


@pytest.mark.parametrize('failure', ['bad_deadline', 'uncertain_response'])
def test_uncertain_submission_blocks_further_commands_and_cancels_known_id(failure, configured, api_stub):
    submitted = []
    def submit(request):
        submitted.append(request)
        if failure == 'uncertain_response':
            raise httpx.ReadError('lost reply', request=request)
        return httpx.Response(202, json={'id': api_stub.command_id, 'queued_deadline': 'invalid'})
    api_stub.routes['POST', f'/v1/isolates/{api_stub.isolate_id}/commands'] = submit
    expected = ProtocolError if failure == 'bad_deadline' else APIError
    with configured.scope as isolate:
        with pytest.raises(expected):
            isolate.run('first')
        before = len(submitted)
        with pytest.raises(expected):
            isolate.run('second')
        assert len(submitted) == before
        assert isolate._lease.stop.is_set()
        cancelled = [r for r in api_stub.requests if r.url.path == '/v1/commands/cancel']
        assert bool(cancelled) is (failure == 'bad_deadline')
        if cancelled:
            assert json.loads(cancelled[0].content) == {'ids': [api_stub.command_id]}


def test_provisioning_renews_lease_and_transfers_worker_to_ready_isolate(configured, api_stub):
    renewed = threading.Event()
    configured.settings.renew_interval = 0.02
    configured.settings.operation_timeout = 0.3
    payload = {'id': api_stub.isolate_id, 'computer_id': api_stub.computer_id, 'status': 'provisioning', 'expires_at': '2099-01-01T00:00:00Z'}
    api_stub.routes['POST', '/v1/isolates'] = lambda _request: httpx.Response(202, json=payload)
    api_stub.routes['GET', '/v1/isolates/' + api_stub.isolate_id] = lambda _request: httpx.Response(200, json={**payload, 'status': 'ready' if renewed.is_set() else 'provisioning'})
    def renew(_request):
        renewed.set()
        return httpx.Response(200, json=payload)
    api_stub.routes['POST', '/v1/isolates/' + api_stub.isolate_id + '/renew'] = renew
    with configured.scope as isolate:
        assert renewed.is_set()
        assert isolate._lease.thread.is_alive()
        assert isolate.run('work').success
    assert not isolate._lease.thread.is_alive()


def test_provisioning_renew_failure_is_reported_and_cleaned_up(configured, api_stub):
    configured.settings.renew_interval = 0.01
    configured.settings.operation_timeout = 0.2
    payload = {'id': api_stub.isolate_id, 'status': 'provisioning', 'expires_at': '2099-01-01T00:00:00Z'}
    api_stub.routes['POST', '/v1/isolates'] = lambda _request: httpx.Response(202, json=payload)
    api_stub.routes['GET', '/v1/isolates/' + api_stub.isolate_id] = lambda _request: httpx.Response(200, json=payload)
    api_stub.routes['POST', '/v1/isolates/' + api_stub.isolate_id + '/renew'] = lambda _request: httpx.Response(409)
    with pytest.raises(APIError) as caught:
        configured.get(b'state')
    assert caught.value.status_code == 409
    assert any(r.method == 'DELETE' and r.url.path.endswith(api_stub.isolate_id) for r in api_stub.requests)


@pytest.mark.parametrize('trigger', ['token', 'watchdog', 'inflight_error', 'detached_candidate'])
def test_terminal_command_is_not_monitored_while_logs_download(trigger, configured, api_stub, monkeypatch):  # noqa: PLR0915 - deterministic lifecycle interleavings share the same gates.
    """Only running commands are cancellable; stale cancel failures cannot poison a live lease."""
    configured.settings.renew_interval = 0.01
    configured.settings.retry_attempts = 1
    candidate_seen, finish_candidate = threading.Event(), threading.Event()
    monitor_thread = None

    class PausingToken(SimpleToken):
        def __bool__(self):
            if trigger == 'detached_candidate' and threading.current_thread() is monitor_thread:
                candidate_seen.set()
                assert finish_candidate.wait(3)
            return super().__bool__()

    token = PausingToken()
    download_started, finish_download = threading.Event(), threading.Event()
    cancel_started, finish_cancel = threading.Event(), threading.Event()
    renewal = threading.Condition()
    renewals = 0
    tracked = []

    def renew(_request):
        nonlocal renewals
        with renewal:
            renewals += 1
            renewal.notify_all()
        return httpx.Response(200, json={'id': api_stub.isolate_id, 'expires_at': '2099-01-01T00:00:00Z'})

    def cancel(_request):
        cancel_started.set()
        if trigger == 'inflight_error':
            assert finish_cancel.wait(3)
        return httpx.Response(503)

    def poll(_request):
        if trigger == 'inflight_error':
            token.cancel()
            if not cancel_started.is_set():
                return httpx.Response(200, json=[{'id': api_stub.command_id, 'status': 'running', 'result': None}])
        if trigger == 'detached_candidate':
            assert candidate_seen.wait(2)
            token.cancel()
        return result(api_stub, stdout='preview', stdout_artifact_id=api_stub.artifact_id)

    def download(_request):
        download_started.set()
        assert finish_download.wait(3)
        return httpx.Response(200, content=api_stub.content, headers={'X-Content-SHA256': hashlib.sha256(api_stub.content).hexdigest()})

    api_stub.routes['POST', '/v1/isolates/' + api_stub.isolate_id + '/renew'] = renew
    api_stub.routes['POST', '/v1/commands/cancel'] = cancel
    api_stub.routes['POST', '/v1/commands/results'] = poll
    api_stub.routes['GET', '/v1/artifacts/' + api_stub.artifact_id + '/content'] = download
    with configured.scope as isolate, ThreadPoolExecutor(max_workers=1) as pool:
        monitor_thread = isolate._lease.thread
        original = isolate._lease.set_pending

        def observe(pending):
            if pending is not None:
                tracked.append(pending)
            original(pending)

        monkeypatch.setattr(isolate._lease, 'set_pending', observe)
        execution = pool.submit(isolate.run, 'work', token=token)
        try:
            assert download_started.wait(2)
            with renewal:
                before = renewals
                if trigger == 'token':
                    token.cancel()
                elif trigger == 'watchdog':
                    tracked[0].deadline = time.monotonic() - 1
                    tracked[0].cancel_deadline = time.monotonic() - 1
                elif trigger == 'detached_candidate':
                    finish_candidate.set()
                else:
                    finish_cancel.set()
                # Two heartbeats ensure the monitor has checked cancellation
                # after the trigger; no sleep-based assertion can miss a slow worker.
                assert renewal.wait_for(lambda: renewals >= before + 2, timeout=2)
            assert cancel_started.is_set() is (trigger == 'inflight_error')
            finish_download.set()
            output = execution.result(timeout=2)
            assert output.success
            assert output.killed_by_token is False
            assert isolate.run('next').success
        finally:
            finish_candidate.set()
            finish_cancel.set()
            finish_download.set()


def test_active_command_cancellation_error_is_not_ignored(configured, api_stub):
    """Only stale cancellation errors are irrelevant; a still-running command must fail closed."""
    configured.settings.retry_attempts = 1
    configured.settings.cancellation_timeout = 0.05
    token = SimpleToken()
    polls = 0
    def poll(_request):
        nonlocal polls
        polls += 1
        assert polls < 200, 'The active command was never monitored for cancellation.'
        token.cancel()
        return httpx.Response(200, json=[{'id': api_stub.command_id, 'status': 'running', 'result': None}])
    api_stub.routes['POST', '/v1/commands/results'] = poll
    api_stub.routes['POST', '/v1/commands/cancel'] = lambda _request: httpx.Response(503)
    with configured.scope as isolate, pytest.raises(APIError) as caught:
        isolate.run('work', token=token)
    assert caught.value.status_code == 503


@pytest.mark.parametrize(('stream', 'failure'), [
    ('stdout', 'missing'), ('stderr', 'missing'),
    ('stdout', 'unavailable'), ('stderr', 'unavailable'),
    ('stdout', 'checksum'), ('stderr', 'checksum'), ('stdout', 'malformed_result'),
])
def test_terminal_result_retrieval_error_preserves_isolate(stream, failure, configured, api_stub):
    """A confirmed terminal command does not become uncertain when its logs cannot be read."""
    configured.settings.renew_interval = 0.01
    configured.settings.retry_attempts = 1
    renewed = threading.Event()
    def renew(_request):
        renewed.set()
        return httpx.Response(200, json={'id': api_stub.isolate_id, 'expires_at': '2099-01-01T00:00:00Z'})
    api_stub.routes['POST', '/v1/isolates/' + api_stub.isolate_id + '/renew'] = renew
    values = {stream + '_artifact_id': api_stub.artifact_id}
    if failure == 'malformed_result':
        values['success'] = 'invalid'
    api_stub.routes['POST', '/v1/commands/results'] = lambda _request: result(api_stub, **values)
    code = 404 if failure == 'missing' else 503
    api_stub.routes['GET', '/v1/artifacts/' + api_stub.artifact_id + '/content'] = lambda _request: (
        httpx.Response(200, content=b'corrupt', headers={'X-Content-SHA256': 'incorrect'})
        if failure == 'checksum' else httpx.Response(code)
    )
    expected = ProtocolError if failure in ('checksum', 'malformed_result') else APIError
    with configured.scope as isolate:
        with pytest.raises(expected):
            isolate.run('finished')
        assert not isolate._lease.stop.is_set()
        assert not any(r.url.path == '/v1/commands/cancel' for r in api_stub.requests)
        renewed.clear()
        assert renewed.wait(2)
        api_stub.routes.pop(('POST', '/v1/commands/results'))
        assert isolate.run('next').success
    assert sum(r.url.path.endswith('/commands') for r in api_stub.requests) == 2


@pytest.mark.parametrize('mismatch', ['envelope', 'result'])
def test_result_identity_error_still_blocks_further_execution(mismatch, configured, api_stub):
    def poll(_request):
        payload = json.loads(result(api_stub).content)
        if mismatch == 'envelope':
            payload[0]['id'] = str(uuid4())
        else:
            payload[0]['result']['command_id'] = str(uuid4())
        return httpx.Response(200, json=payload)
    api_stub.routes['POST', '/v1/commands/results'] = poll
    with configured.scope as isolate:
        with pytest.raises(ProtocolError):
            isolate.run('work')
        before = len(api_stub.requests)
        with pytest.raises(ProtocolError):
            isolate.run('next')
        assert len(api_stub.requests) == before
        assert isolate._lease.stop.is_set()
