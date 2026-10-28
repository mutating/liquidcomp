"""Transport uncertainty and public CLI behavior are testable independently."""

import hashlib
import json
import time
from types import SimpleNamespace

import httpx
import pytest

from liquidcomp import LiquidManager, api as api_module
from liquidcomp.cli import main
from liquidcomp.errors import (
    APIError,
    AuthenticationCancelledError,
    ProtocolError,
    ServerUnavailableError,
)
from liquidcomp.models import Status


@pytest.mark.parametrize('failure', ['connection', 408, 425, 429, 503])
def test_command_retry_keeps_one_idempotency_key(failure, configured, api_stub):
    """Prevent transient HTTP failures from aborting commands or replaying them with new keys."""
    requests = []
    def create(request):
        requests.append(request)
        if len(requests) == 1:
            if failure == 'connection':
                raise httpx.ReadError('lost response', request=request)
            return httpx.Response(failure)
        return httpx.Response(202, json={'id': api_stub.command_id, 'queued_deadline': '2099-01-01T00:00:00Z'})
    api_stub.routes['POST', f'/v1/isolates/{api_stub.isolate_id}/commands'] = create
    with configured.scope as isolate:
        assert isolate.run('work').success
    assert len(requests) == 2
    assert requests[0].headers['Idempotency-Key'] == requests[1].headers['Idempotency-Key']
    assert requests[0].content == requests[1].content


def test_artifact_upload_is_not_replayed_after_uncertain_response(configured, api_stub):
    """The artifact API has no idempotency key; an uncertain upload is reported once."""
    calls = []
    def upload(request):
        calls.append(request)
        raise httpx.ReadError('lost response', request=request)
    api_stub.routes['POST', '/v1/artifacts'] = upload
    with pytest.raises(ServerUnavailableError):
        configured.get(b'state')
    assert len(calls) == 1


def test_redirects_never_forward_credentials(configured, api_stub):
    """A redirected API cannot redirect the bearer token to another origin."""
    api_stub.routes['GET', '/v1/auth/check'] = lambda _request: httpx.Response(302, headers={'Location': 'https://other.invalid/steal'})
    with pytest.raises(APIError):
        configured.get(b'state')
    assert len(api_stub.requests) == 1


@pytest.mark.parametrize('mode', ['large', 'large_stream', 'bad_id', 'bad_checksum', 'bad_json'])
def test_upload_response_is_bounded_and_validated(mode, configured, api_stub, byte_stream):
    """Prevent unsafe replies or cumulative chunk sizes from bypassing control-response limits."""
    configured.settings.max_response_bytes = 128
    if mode in ('large', 'large_stream'):
        body = json.dumps({'id': api_stub.artifact_id, 'sha256': hashlib.sha256(b'state').hexdigest(), 'padding': 'x' * 128}).encode()
        response = (
            httpx.Response(201, stream=byte_stream([body[index:index + 64] for index in range(0, len(body), 64)]))
            if mode == 'large_stream' else httpx.Response(201, content=body)
        )
    elif mode == 'bad_json':
        response = httpx.Response(201, content=b'not json')
    else:
        response = httpx.Response(201, json={
            'id': '../../escape' if mode == 'bad_id' else api_stub.artifact_id,
            'sha256': hashlib.sha256(b'state').hexdigest() if mode == 'bad_id' else 'bad',
        })
    api_stub.routes['POST', '/v1/artifacts'] = lambda _request: response
    with pytest.raises(ProtocolError):
        configured.get(b'state')
    assert not any(request.url.path == '/v1/isolates' for request in api_stub.requests)
    deleted = [request.url.path for request in api_stub.requests if request.method == 'DELETE']
    assert deleted == (['/v1/artifacts/' + api_stub.artifact_id] if mode == 'bad_checksum' else [])


@pytest.mark.parametrize('command', ['init', 'status'])
def test_cli_calls_the_manager_method(command, monkeypatch, capsys):
    """CLI dispatch shares the public manager entrypoints instead of duplicating dialogs."""
    calls = []
    def init(_self):
        calls.append('init')
    def status(_self):
        calls.append('status')
        return Status(True, True, True, True, 'environment')
    monkeypatch.setattr(LiquidManager, 'init', init)
    monkeypatch.setattr(LiquidManager, 'status', status)
    assert main([command]) == 0
    assert calls == [command]
    if command == 'status':
        assert 'Token: valid (environment)' in capsys.readouterr().out.splitlines()


@pytest.mark.usefixtures('api_stub')
def test_cli_status_has_failure_exit_code_without_prompt(capsys):
    """A missing token is useful diagnostics, with a nonzero command exit status."""
    assert main(['status']) == 1
    output = capsys.readouterr().out
    assert 'Token: missing' in output.splitlines()
    assert 'Server: connected, ready' in output.splitlines()


@pytest.mark.usefixtures('api_stub')
def test_cli_init_reports_noninteractive_failure_without_traceback(capsys):
    """CLI errors are concise and never invoke an echoed fallback."""
    assert main(['init']) == 1
    assert 'Traceback' not in capsys.readouterr().err


@pytest.mark.parametrize('args', [[], ['unknown'], ['status', '--unknown']])
def test_cli_invalid_arguments_are_usage_errors(args):
    """Only documented subcommands and arguments are accepted."""
    with pytest.raises(SystemExit) as caught:
        main(args)
    assert caught.value.code == 2


@pytest.mark.parametrize('body', [
    b'{"valid":NaN}', b'{"valid":true,"scopes":null}', b'{"valid":false,"scopes":["read","execute"]}',
    b'{"valid":1,"scopes":["read","execute"]}', b'{"valid":true,"scopes":["execute"]}',
    pytest.param(b'[' * 10000 + b'0' + b']' * 10000, id='deep_json'),
])
def test_invalid_auth_wire_response_is_never_accepted(body, configured, api_stub):
    """Prevent invalid authorization or deeply nested JSON from escaping the protocol boundary."""
    api_stub.routes['GET', '/v1/auth/check'] = lambda _request: httpx.Response(200, content=body)
    with pytest.raises(ProtocolError):
        configured.get(b'state')


@pytest.mark.parametrize('dribble', [False, True])
def test_stream_response_has_a_total_read_budget(dribble, configured, api_stub, monkeypatch, byte_stream):
    """Prevent a continuous stream of timely chunks from resetting the total HTTP response budget."""
    clock = [time.monotonic()]
    monkeypatch.setattr(api_module, 'time', SimpleNamespace(monotonic=lambda: clock[0], sleep=lambda _delay: None))
    body = b'{"valid":true,"scopes":["read","execute"]}'
    chunks = [body[:12], body[12:24], body[24:]] if dribble else [body]
    def delayed():
        for chunk in chunks:
            clock[0] += 0.004 if dribble else 0.02
            yield chunk
    configured.settings.request_timeout = 0.01
    configured.settings.retry_attempts = 1
    api_stub.routes['GET', '/v1/auth/check'] = lambda _request: httpx.Response(200, stream=byte_stream(delayed()))
    with pytest.raises(ServerUnavailableError):
        configured.get(b'state')


@pytest.mark.parametrize(('status', 'expected'), [
    ((False, None, 'Could not connect.'), 'unknown validity'),
    ((False, False, None), 'invalid or insufficient permissions'),
    ((True, True, None), 'valid'),
])
def test_cli_status_reports_uncertainty_and_errors(status, expected, monkeypatch, capsys):
    """Prevent a reachable but unready server from producing a successful CLI status."""
    connected, valid, error = status
    monkeypatch.setattr(LiquidManager, 'status', lambda _self: Status(connected, False, True, valid, 'keyring', error))
    assert main(['status']) == 1
    output = capsys.readouterr().out
    assert f'Token: {expected} (keyring)' in output.splitlines()
    connection = 'connected' if connected else 'disconnected'
    assert f'Server: {connection}, not ready' in output.splitlines()
    if error:
        assert error in output


def test_cli_cancellation_is_concise(monkeypatch, capsys):
    def abort(_self):
        raise AuthenticationCancelledError('cancelled')
    monkeypatch.setattr(LiquidManager, 'init', abort)
    assert main(['init']) == 130
    assert capsys.readouterr().err == 'Initialization cancelled.\n'


def test_cli_invalid_configuration_is_concise(monkeypatch, capsys):
    monkeypatch.setenv('LIQUID_CPU', 'not-a-number')
    assert main(['status']) == 1
    assert 'configuration' in capsys.readouterr().err


def test_auth_scope_items_must_be_strings(configured, api_stub):
    api_stub.routes['GET', '/v1/auth/check'] = lambda _request: httpx.Response(200, json={'valid': True, 'scopes': ['read', 'execute', 3]})
    with pytest.raises(ProtocolError):
        configured.get(b'state')


@pytest.mark.parametrize('operation', ['allocation', 'snapshot'])
def test_control_operation_retry_keeps_one_idempotency_key(operation, configured, api_stub):
    calls = []
    def respond(request):
        calls.append(request)
        if len(calls) == 1:
            raise httpx.ReadError('lost reply', request=request)
        if operation == 'allocation':
            return httpx.Response(202, json={'id': api_stub.isolate_id, 'computer_id': api_stub.computer_id, 'status': 'ready', 'expires_at': '2099-01-01T00:00:00Z'})
        return httpx.Response(202, json={'id': api_stub.operation_id})
    path = '/v1/isolates' if operation == 'allocation' else '/v1/isolates/' + api_stub.isolate_id + '/snapshots'
    api_stub.routes['POST', path] = respond
    with configured.scope as isolate:
        if operation == 'snapshot':
            assert isolate.read() == api_stub.content
    assert len(calls) == 2
    assert calls[0].headers['Idempotency-Key'] == calls[1].headers['Idempotency-Key']
    assert calls[0].content == calls[1].content


@pytest.mark.parametrize('operation', ['allocation', 'command', 'snapshot'])
def test_uncertain_request_exposes_its_stable_idempotency_key(operation, configured, api_stub):
    calls = []
    def fail(request):
        calls.append(request)
        raise httpx.ReadError('secret upstream body', request=request)
    path = {'allocation': '/v1/isolates', 'command': '/v1/isolates/' + api_stub.isolate_id + '/commands', 'snapshot': '/v1/isolates/' + api_stub.isolate_id + '/snapshots'}[operation]
    api_stub.routes['POST', path] = fail
    def invoke():
        if operation == 'allocation':
            configured.get(b'state')
        else:
            with configured.scope as isolate:
                if operation == 'command':
                    isolate.run('work')
                else:
                    isolate.read()
    with pytest.raises(ServerUnavailableError) as caught:
        invoke()
    assert len(calls) == configured.settings.retry_attempts
    keys = {call.headers['Idempotency-Key'] for call in calls}
    assert keys == {caught.value.idempotency_key}
    assert 'secret upstream body' not in str(caught.value)


@pytest.mark.parametrize('command', ['init', 'status'])
@pytest.mark.parametrize(('filename', 'content'), [
    ('liquid.yaml', 'api_url: [\n'),
    ('liquid.json', json.dumps({'api_url': 'https://☃.invalid/v1'})),
    ('liquid.json', json.dumps({'api_url': 'https://api.invalid/\0/v1'})),
])
def test_cli_reports_invalid_configuration_without_traceback(command, filename, content, tmp_path, capsys):
    """Prevent malformed configuration or HTTPX-incompatible URLs from crashing either CLI command."""
    (tmp_path / filename).write_text(content)
    assert main([command]) == 1
    output = capsys.readouterr()
    assert 'Invalid Liquid configuration' in output.err
    assert 'Traceback' not in output.err
