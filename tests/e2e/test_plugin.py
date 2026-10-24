"""The installed plugin crosses a real HTTP boundary for commands and benchmarks."""

import shlex
import shutil
import subprocess
import sys
import sysconfig
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from tarfile import open as open_tar

import pytest
from cantok import TimeoutToken
from throng import throng
from throng.errors import PreparationCommandFailedError

from liquidcomp import LiquidManager, Settings
from liquidcomp.errors import MetadataError, ServerUnavailableError


def python_command(source):
    """Quote a Python program with Linux text streams even on a Windows mock host."""
    source = (
        "import sys\n"
        "sys.stdout.reconfigure(newline='\\n')\n"
        "sys.stderr.reconfigure(newline='\\n')\n"
        + source
    )
    return shlex.quote(sys.executable) + ' -c ' + shlex.quote(source)


@pytest.mark.parametrize(('credential', 'newline'), [('valid-token', '\n'), ('valid-company', '\r\n')])
def test_installed_plugin_executes_chains_and_round_trips_state(mock_api, monkeypatch, tmp_path, credential, newline):
    """Protect single-plugin selection, state round trips and Linux output on Windows mock hosts."""
    monkeypatch.setenv('LIQUID_TOKEN', credential)
    monkeypatch.setenv('LIQUID_API_URL', mock_api.url)
    monkeypatch.setenv('LIQUID_POLL_INTERVAL', '0.01')
    startup = tmp_path / 'python-startup'
    startup.mkdir()
    (startup / 'sitecustomize.py').write_text(
        'import sys\n'
        f'sys.stdout.reconfigure(newline={newline!r})\n'
        f'sys.stderr.reconfigure(newline={newline!r})\n',
    )
    monkeypatch.setenv('PYTHONPATH', str(startup))
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'input.txt').write_text('original')
    (source / 'secret').write_text('excluded')
    manager = throng['liquid'].one(source, exclude=['secret'])
    assert isinstance(manager, LiquidManager)
    with manager.scope as isolate:
        output = isolate.run(python_command("import sys; from pathlib import Path; print(Path('input.txt').read_text()); print('diagnostic', file=sys.stderr)"))
        assert output.stdout == 'original\n'
        assert output.stderr == 'diagnostic\n'
        outputs = isolate.chain(
            python_command("from pathlib import Path; Path('output.txt').write_text('remote')"),
            python_command("from pathlib import Path; print(Path('output.txt').read_text())"),
        )
        assert all(output.success for output in outputs)
        assert outputs[-1].stdout == 'remote\n'
        state = isolate.read()
        identity = isolate.get_id()
    assert not (source / 'output.txt').exists()
    with open_tar(fileobj=BytesIO(state)) as archive:
        assert archive.extractfile('output.txt').read() == b'remote'
        assert 'secret' not in archive.getnames()
    second = manager.get(state)
    try:
        assert second.get_id() != identity
        assert second.run(python_command("from pathlib import Path; print(Path('output.txt').read_text())")).stdout == 'remote\n'
    finally:
        second.kill()
    assert all(value['status'] == 'destroyed' for value in mock_api.isolates.values())


def test_cancellation_over_http_and_large_output_artifact(mock_api, monkeypatch, tmp_path):
    """Cancellation reaches the API, and artifact output is complete rather than its preview."""
    monkeypatch.setenv('LIQUID_TOKEN', 'valid-token')
    manager = LiquidManager(tmp_path, settings=Settings(api_url=mock_api.url, poll_interval=0.01))
    with manager.scope as isolate:
        output = isolate.run(python_command('import time; time.sleep(10)'), token=TimeoutToken(0.2))
        assert not output.success
        assert output.status == 'cancelled'
        assert output.killed_by_token
        output = isolate.run(python_command("print('x' * 50000)"))
        assert output.stdout == 'x' * 50000 + '\n'
        assert output.stdout_bytes == output.stdout.encode()


def test_uncertain_command_response_executes_exactly_once(mock_api, monkeypatch, tmp_path):
    """The retry returns the original command instead of repeating its filesystem side effect."""
    monkeypatch.setenv('LIQUID_TOKEN', 'valid-token')
    mock_api.drop_command_response = True
    manager = LiquidManager(tmp_path, settings=Settings(api_url=mock_api.url, poll_interval=0.01, retry_backoff=0.0))
    with manager.scope as isolate:
        output = isolate.run(python_command("from pathlib import Path; p=Path('count'); p.write_text((p.read_text() if p.exists() else '')+'x')"))
        assert output.success
        output = isolate.run(python_command("from pathlib import Path; print(Path('count').read_text())"))
        assert output.stdout == 'x\n'
    assert len(mock_api.commands) == 2


@pytest.mark.parametrize('timestamp', [
    '2026-10-19T12:30:00Z', '2026-10-19T15:30:00+03:00', '2026-10-19T08:30:00-04:00',
])
def test_benchmark_metadata_follows_current_mock_api_measurements(timestamp, mock_api, monkeypatch, tmp_path):
    """Prevent mixing historical suites, filling missing new scores from history or losing UTC conversion."""
    monkeypatch.setenv('LIQUID_TOKEN', 'valid-token')
    manager = LiquidManager(tmp_path, settings=Settings(api_url=mock_api.url))
    with manager.scope as isolate:
        assert isolate.meta.resources.benchmarks.single_core is None
        mock_api.benchmarks[:] = [{
            'computer_id': mock_api.computer_id, 'benchmark_name': 'multi_core', 'number': 9000,
            'created_at': timestamp, 'benchmarker_version': '1.0.0',
        }]
        mock_api.benchmark_history[:] = [
            {**mock_api.benchmarks[0], 'benchmark_name': name, 'benchmarker_version': '0.0.1', 'number': score}
            for name, score in [('single_core', 111), ('multi_core', 222)]
        ]
        partial = isolate.meta
        assert partial.resources.benchmarks.single_core is None
        assert partial.resources.benchmarks.multi_core.number == 9000
        mock_api.benchmarks.append({**mock_api.benchmarks[0], 'benchmark_name': 'single_core', 'number': 1000})
        complete = isolate.meta
        assert complete.resources.shared is True
        assert complete.resources.shared_with is None
        assert complete.resources.benchmarks.single_core.number == 1000
        assert complete.resources.benchmarks.multi_core.number == 9000
        assert complete.resources.benchmarks.single_core.when.isoformat() == '2026-10-19T12:30:00+00:00'


def test_parallel_isolates_have_independent_state(mock_api, monkeypatch, tmp_path):
    """A shared manager can allocate independent remote working directories concurrently."""
    monkeypatch.setenv('LIQUID_TOKEN', 'valid-token')
    manager = LiquidManager(tmp_path, settings=Settings(api_url=mock_api.url, poll_interval=0.01))
    def work(number):
        return manager.run(python_command(f"from pathlib import Path; p=Path('own'); p.write_text('{number}'); print(p.read_text())")).stdout
    with ThreadPoolExecutor(max_workers=3) as pool:
        assert list(pool.map(work, range(3))) == ['0\n', '1\n', '2\n']


@pytest.mark.parametrize(('token', 'expected', 'word'), [(None, 1, 'missing'), ('valid-token', 0, 'valid'), ('bad', 1, 'invalid')])
@pytest.mark.parametrize('entrypoint', ['module', 'console'])
def test_cli_status_subprocess(cli_environment, token, expected, word, entrypoint):
    """Prevent missing console scripts or invalid/disconnected labels from passing CLI checks."""
    if token is not None:
        cli_environment['LIQUID_TOKEN'] = token
    if entrypoint == 'module':
        command = [sys.executable, '-m', 'liquidcomp']
    else:
        executable = shutil.which('liquid', path=sysconfig.get_path('scripts'))
        assert executable is not None
        command = [executable]
    process = subprocess.run([*command, 'status'], env=cli_environment, input='', capture_output=True, text=True, timeout=15, check=False)
    assert process.returncode == expected, process.stderr
    assert any(line.startswith('Token: ' + word) for line in process.stdout.splitlines())
    assert 'Server: connected, ready' in process.stdout.splitlines()
    if token:
        assert token not in process.stdout + process.stderr


def test_cli_init_rejects_piped_secret(cli_environment):
    """A noninteractive subprocess cannot echo a secret or pretend it was securely saved."""
    process = subprocess.run([sys.executable, '-m', 'liquidcomp', 'init'], env=cli_environment, input='new-token\n', capture_output=True, text=True, timeout=15, check=False)
    assert process.returncode == 1
    assert 'new-token' not in process.stdout + process.stderr


@pytest.mark.parametrize('failure', ['denied', 'missing', 'unavailable', 'malformed'])
def test_benchmark_http_failures_do_not_disable_commands(failure, mock_api, monkeypatch, tmp_path):
    """An API error or broken measurement differs from an empty suite over real HTTP."""
    monkeypatch.setenv('LIQUID_TOKEN', 'valid-token')
    mock_api.benchmark_status = {'denied': 403, 'missing': 404, 'unavailable': 503, 'malformed': 200}[failure]
    mock_api.benchmarks[:] = [{'computer_id': mock_api.computer_id, 'benchmark_name': 'single_core', 'number': -1,
                             'created_at': 'yesterday', 'benchmarker_version': '1.0.0'}]
    manager = LiquidManager(tmp_path, settings=Settings(api_url=mock_api.url, poll_interval=0.01, retry_backoff=0))
    with manager.scope as isolate:
        with pytest.raises(MetadataError):
            isolate.get_benchmarks()
        assert isolate.run(python_command("print('still works')")).stdout == 'still works\n'
        mock_api.benchmark_status = 200
        mock_api.benchmarks.clear()
        assert isolate.meta.resources.benchmarks.single_core is None


def test_failed_remote_prepare_releases_allocation(mock_api, monkeypatch, tmp_path):
    """A real remote process failure during preparation follows throng's cleanup contract."""
    monkeypatch.setenv('LIQUID_TOKEN', 'valid-token')
    manager = LiquidManager(tmp_path, prepare=[python_command('raise SystemExit(7)')], settings=Settings(api_url=mock_api.url, poll_interval=0.01))
    with pytest.raises(PreparationCommandFailedError):
        manager.get(manager.read())
    assert all(row['status'] == 'destroyed' for row in mock_api.isolates.values())


@pytest.mark.parametrize('timestamp', ['0001-01-01T00:00:00+01:00', '9999-12-31T23:59:59-01:00'])
def test_benchmark_utc_overflow_is_a_metadata_error(timestamp, mock_api, monkeypatch, tmp_path):
    monkeypatch.setenv('LIQUID_TOKEN', 'valid-token')
    mock_api.benchmarks[:] = [{'computer_id': mock_api.computer_id, 'benchmark_name': 'single_core', 'number': 1,
                             'created_at': timestamp, 'benchmarker_version': '1.0.0'}]
    manager = LiquidManager(tmp_path, settings=Settings(api_url=mock_api.url, poll_interval=0.01))
    with manager.scope as isolate:
        with pytest.raises(MetadataError):
            isolate.get_benchmarks()
        assert isolate.run(python_command('print(1)')).success


def test_command_directory_and_environment_cross_http(mock_api, monkeypatch, tmp_path):
    monkeypatch.setenv('LIQUID_TOKEN', 'valid-token')
    source = tmp_path / 'source'
    (source / 'nested').mkdir(parents=True)
    settings = Settings(api_url=mock_api.url, poll_interval=0.01, cwd='nested', env={'LIQUID_COMMAND_PROBE': 'works'})
    manager = LiquidManager(source, settings=settings)
    with manager.scope as isolate:
        output = isolate.run(python_command("import os; from pathlib import Path; print(Path.cwd().name + '|' + os.environ['LIQUID_COMMAND_PROBE'])"))
        assert output.stdout == 'nested|works\n'


def test_shell_redirection_crosses_http(mock_api, monkeypatch, tmp_path):
    monkeypatch.setenv('LIQUID_TOKEN', 'valid-token')
    manager = LiquidManager(tmp_path, settings=Settings(api_url=mock_api.url, poll_interval=0.01, shell=True))
    with manager.scope as isolate:
        output = isolate.run('"' + sys.executable + '" -c "print(123)" > saved.txt')
        assert output.success
        state = isolate.read()
    with open_tar(fileobj=BytesIO(state)) as archive:
        assert archive.extractfile('saved.txt').read().strip() == b'123'


def test_server_command_timeout_crosses_http(mock_api, monkeypatch, tmp_path):
    monkeypatch.setenv('LIQUID_TOKEN', 'valid-token')
    manager = LiquidManager(tmp_path, settings=Settings(api_url=mock_api.url, poll_interval=0.01, command_timeout=1))
    with manager.scope as isolate:
        output = isolate.run(python_command('import time; time.sleep(2)'))
        assert not output.success
        assert output.status == 'timed_out'
        assert output.killed_by_token is False
    assert not any(path == '/v1/commands/cancel' for _method, path, _headers, _body in mock_api.requests)


def test_provisioning_lease_is_renewed_over_http(mock_api, monkeypatch, tmp_path):
    monkeypatch.setenv('LIQUID_TOKEN', 'valid-token')
    mock_api.provisioning_renewals_required = 2
    manager = LiquidManager(tmp_path, settings=Settings(api_url=mock_api.url, poll_interval=0.01, renew_interval=0.02, operation_timeout=1))
    with manager.scope as isolate:
        assert mock_api.renewals >= 2
        assert isolate.run(python_command('print(1)')).success


@pytest.mark.parametrize('phase', ['headers', 'body'])
def test_request_timeout_interrupts_a_stalled_http_response(phase, mock_api, monkeypatch):
    """Prevent missing socket timeouts from hanging allocation before any response bytes arrive."""
    monkeypatch.setenv('LIQUID_TOKEN', 'valid-token')
    mock_api.stall_response = phase
    manager = LiquidManager(settings=Settings(api_url=mock_api.url, request_timeout=0.05, retry_attempts=1))
    with ThreadPoolExecutor(max_workers=1) as pool:
        allocation = pool.submit(manager.get, b'state')
        try:
            assert mock_api.response_started.wait(2)
            with pytest.raises(ServerUnavailableError):
                allocation.result(timeout=2)
        finally:
            mock_api.release_response.set()
    assert not mock_api.isolates
