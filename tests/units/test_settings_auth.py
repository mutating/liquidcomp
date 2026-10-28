"""Credential policy and settings are observable before any remote execution."""

import getpass
import json
import sys
import warnings

import httpx
import keyring
import pytest
from skelet import Storage
from throng.errors import ThrongError

from liquidcomp import LiquidManager, Settings
from liquidcomp.errors import (
    AuthenticationCancelledError,
    InvalidTokenError,
    KeyringError,
    MissingTokenError,
    NonInteractiveError,
    PermissionDeniedError,
    ProtocolError,
    ServerUnavailableError,
)


def test_settings_defaults_and_skelet_sources(tmp_path, monkeypatch):
    """Use fresh skelet sources with environment priority over ordinary files."""
    assert issubclass(Settings, Storage)
    defaults = Settings()
    assert (defaults.cpu, defaults.ram, defaults.disk, defaults.command_timeout) == (1.0, 0.5, 1.0, 3600)
    assert defaults.api_url == 'https://api.mutating.tech/v1'
    assert defaults.ask_for_token is False
    (tmp_path / 'pyproject.toml').write_text('[tool.liquid]\ncpu = 2.0\ncommand_timeout = 7200\n')
    (tmp_path / 'liquid.toml').write_text('cpu = 3.0\n')
    assert Settings().cpu == 3.0
    monkeypatch.setenv('LIQUID_CPU', '4.0')
    assert Settings().cpu == 4.0
    monkeypatch.setenv('LIQUID_CPU', '5.0')
    assert Settings().cpu == 5.0
    assert Settings(cpu=6.0).cpu == 6.0


@pytest.mark.parametrize(('field', 'value'), [
    ('cpu', 0.0), ('cpu', float('nan')), ('cpu', 1001.0), ('cpu', 0.0001),
    ('ram', -1.0), ('disk', float('inf')), ('command_timeout', 0),
    ('command_timeout', 86401), ('poll_interval', 0.0), ('operation_timeout', -1.0),
    ('request_timeout', float('inf')), ('retry_attempts', 0), ('retry_backoff', -1.0),
    ('max_artifact_bytes', 0), ('cwd', '../outside'), ('cwd', '/absolute'),
    ('env', {'bad=name': 'x'}), ('api_url', 'file:///tmp/api'),
    ('api_url', 'https://user:password@example.com/v1'), ('api_url', 'https://host/v1?token=secret'),
])
def test_settings_reject_invalid_values(field, value):
    """Reject values before they produce invalid requests or unbounded waits."""
    with pytest.raises((TypeError, ValueError)):
        Settings(**{field: value})


def test_manager_construction_and_read_do_not_resolve_credentials(clean_environment, tmp_path, api_stub):
    """Installing the plugin cannot prevent other throng managers from being constructed."""
    manager = LiquidManager(tmp_path)
    assert manager.read()
    assert not clean_environment.reads
    assert not api_stub.requests
    with pytest.raises(MissingTokenError) as caught:
        manager.get(b'state')
    assert isinstance(caught.value, ThrongError)
    assert not api_stub.requests


@pytest.mark.parametrize('case', [
    ('', False), (' ', False), ('bad token', False), ('bad\ntoken', False), ('', True),
])
@pytest.mark.parametrize('source', ['environment', 'keyring'])
def test_invalid_credential_never_falls_back(case, source, monkeypatch, clean_environment, api_stub):
    """Prevent sending malformed saved tokens or replacing an authoritative invalid credential."""
    value, ask_for_token = case
    slot = ('liquidcomp:https://api.mutating.tech/v1', 'LIQUID_TOKEN')
    clean_environment.values[slot] = value if source == 'keyring' else 'stored-valid'
    if source == 'environment':
        monkeypatch.setenv('LIQUID_TOKEN', value)
    with pytest.raises(InvalidTokenError):
        LiquidManager(settings=Settings(ask_for_token=ask_for_token)).get(b'state')
    assert clean_environment.reads == ([slot] if source == 'keyring' else [])
    assert not api_stub.requests


@pytest.mark.parametrize(('status', 'error'), [(401, InvalidTokenError), (403, PermissionDeniedError)])
def test_rejected_environment_token_is_not_replaced(status, error, monkeypatch, api_stub, clean_environment):
    """A server denial neither reads keyring nor uploads state or prompts."""
    monkeypatch.setenv('LIQUID_TOKEN', 'rejected')
    api_stub.routes['GET', '/v1/auth/check'] = lambda _request: httpx.Response(status)
    with pytest.raises(error):
        LiquidManager(settings=Settings(ask_for_token=True)).get(b'state')
    assert not clean_environment.reads
    assert len(api_stub.requests) == 1


def test_keyring_credential_is_namespaced_by_server(api_stub, clean_environment):
    """A second API endpoint cannot accidentally use the first endpoint's saved credential."""
    clean_environment.values['liquidcomp:https://api.mutating.tech/v1', 'LIQUID_TOKEN'] = 'stored'
    manager = LiquidManager()
    isolate = manager.get(b'state')
    isolate.kill()
    assert api_stub.requests[0].headers['Authorization'] == 'Bearer stored'
    with pytest.raises(MissingTokenError):
        LiquidManager(settings=Settings(api_url='http://localhost:1234/v1')).get(b'state')


@pytest.mark.parametrize('location', ['get', 'set'])
@pytest.mark.usefixtures('api_stub', 'terminal')
def test_keyring_failure_is_not_treated_as_a_missing_secret(location, monkeypatch, capsys):
    """Prevent vault failures from triggering replacement or falsely thanking for a failed save."""
    def fail(*_args):
        raise keyring.errors.KeyringError('private backend data')
    if location == 'get':
        monkeypatch.setattr(keyring, 'get_password', fail)
        action = lambda: LiquidManager(settings=Settings(ask_for_token=True)).get(b'state')
    else:
        monkeypatch.setattr(keyring, 'set_password', fail)
        monkeypatch.setattr(getpass, 'getpass', lambda _prompt: 'new-token')
        action = LiquidManager().init
    with pytest.raises(KeyringError) as caught:
        action()
    assert 'private backend data' not in str(caught.value)
    assert 'Thank you' not in capsys.readouterr().out


@pytest.mark.usefixtures('terminal')
def test_init_retries_empty_and_rejected_values_then_saves_once(api_stub, monkeypatch, clean_environment, capsys):
    """Prevent malformed CLI input from reaching HTTP or being saved before read+execute validation."""
    values = iter(['', ' ', 'bad token', 'bad\ntoken', 'rejected', 'limited', 'accepted'])
    monkeypatch.setattr(getpass, 'getpass', lambda _prompt: next(values))
    def check(request):
        assert request.headers['Authorization'] in ('Bearer rejected', 'Bearer limited', 'Bearer accepted')
        token = request.headers['Authorization'].split()[1]
        status = {'rejected': 401, 'limited': 403}.get(token, 200)
        return httpx.Response(status, json={'valid': True, 'scopes': ['read', 'execute']})
    api_stub.routes['GET', '/v1/auth/check'] = check
    LiquidManager().init()
    assert clean_environment.writes == [('liquidcomp:https://api.mutating.tech/v1', 'LIQUID_TOKEN', 'accepted')]
    assert len(api_stub.clients) == 3
    assert all(client.is_closed for client in api_stub.clients)
    text = capsys.readouterr().out
    assert text.count('Token is empty') == 2
    assert text.count('Thank you') == 1
    assert 'accepted' not in text


@pytest.mark.parametrize('source', ['environment', 'keyring'])
@pytest.mark.parametrize('interruption', [EOFError, KeyboardInterrupt])
@pytest.mark.usefixtures('terminal', 'api_stub')
def test_existing_credential_notice_and_cancellation_leave_secret_intact(source, interruption, monkeypatch, clean_environment, capsys):
    """Explicit init offers replacement while cancellation keeps the original credential."""
    slot = ('liquidcomp:https://api.mutating.tech/v1', 'LIQUID_TOKEN')
    clean_environment.values[slot] = 'stored-old'
    if source == 'environment':
        monkeypatch.setenv('LIQUID_TOKEN', 'environment-old')
    def abort(_prompt):
        raise interruption
    monkeypatch.setattr(getpass, 'getpass', abort)
    with pytest.raises(AuthenticationCancelledError):
        LiquidManager().init()
    assert clean_environment.values[slot] == 'stored-old'
    assert not clean_environment.writes
    text = capsys.readouterr().out
    assert source in text
    assert 'Ctrl+C' in text
    assert 'stored-old' not in text
    assert 'environment-old' not in text


@pytest.mark.usefixtures('terminal')
def test_init_overwrites_keyring_but_environment_stays_authoritative(api_stub, monkeypatch, clean_environment, capsys):
    """Replacing the saved token does not override the process environment."""
    monkeypatch.setenv('LIQUID_TOKEN', 'environment-token')
    monkeypatch.setattr(getpass, 'getpass', lambda _prompt: 'stored-new')
    manager = LiquidManager()
    manager.init()
    isolate = manager.get(b'state')
    isolate.kill()
    assert clean_environment.writes[-1][2] == 'stored-new'
    assert all(r.headers['Authorization'] == 'Bearer environment-token' for r in api_stub.requests[1:])
    assert 'LIQUID_TOKEN' in capsys.readouterr().out


@pytest.mark.usefixtures('terminal', 'api_stub')
def test_automatic_prompt_calls_manager_init(monkeypatch, clean_environment):
    """The automatic and explicit CLI paths share the manager's one dialog entrypoint."""
    monkeypatch.setattr(getpass, 'getpass', lambda _prompt: 'new-token')
    calls = []
    original = LiquidManager.init
    def init(self):
        calls.append(self)
        original(self)
    monkeypatch.setattr(LiquidManager, 'init', init)
    manager = LiquidManager(settings=Settings(ask_for_token=True))
    isolate = manager.get(b'state')
    isolate.kill()
    assert calls == [manager]
    assert len(clean_environment.writes) == 1


@pytest.mark.parametrize('mode', ['no_terminal', 'getpass_warning'])
@pytest.mark.usefixtures('api_stub', 'terminal')
def test_init_refuses_any_echoing_fallback(mode, monkeypatch):
    """A missing secure terminal must never fall back to an echoed password read."""
    if mode == 'no_terminal':
        monkeypatch.setattr(sys.stdin, 'isatty', lambda: False)
    else:
        def warn(_prompt):
            warnings.warn('echo', getpass.GetPassWarning, stacklevel=1)
            return 'secret'
        monkeypatch.setattr(getpass, 'getpass', warn)
    with pytest.raises(NonInteractiveError):
        LiquidManager().init()


@pytest.mark.parametrize('failure', ['network', 'unavailable', 'malformed', 'wrong_scopes', 'deep_json', 'interrupted'])
@pytest.mark.usefixtures('terminal')
def test_init_does_not_save_or_thank_on_unconfirmed_validation(failure, api_stub, monkeypatch, clean_environment, capsys):
    """Prevent saving unconfirmed tokens or leaking their HTTP client on validation failure or Ctrl+C."""
    monkeypatch.setattr(getpass, 'getpass', lambda _prompt: 'candidate')
    def check(request):
        if failure == 'interrupted':
            raise KeyboardInterrupt
        if failure == 'network':
            raise httpx.ConnectError('candidate', request=request)
        if failure == 'unavailable':
            return httpx.Response(503)
        if failure == 'wrong_scopes':
            return httpx.Response(200, json={'valid': True, 'scopes': ['read']})
        if failure == 'deep_json':
            return httpx.Response(200, content=b'[' * 10000 + b'0' + b']' * 10000)
        return httpx.Response(200, content=b'invalid json')
    api_stub.routes['GET', '/v1/auth/check'] = check
    with pytest.raises(KeyboardInterrupt if failure == 'interrupted' else (ServerUnavailableError, ProtocolError)):
        LiquidManager(settings=Settings(retry_backoff=0.0)).init()
    assert len(api_stub.clients) == 1
    assert api_stub.clients[0].is_closed
    assert not clean_environment.writes
    assert 'Thank you' not in capsys.readouterr().out


@pytest.mark.usefixtures('api_stub')
def test_token_is_never_loaded_from_ordinary_config_files(tmp_path):
    """Token acquisition remains env then keyring even when ordinary settings contain a token key."""
    (tmp_path / 'liquid.json').write_text(json.dumps({'token': 'config-secret'}))
    with pytest.raises(MissingTokenError):
        LiquidManager().get(b'state')


def test_status_reports_missing_credentials_without_prompt(api_stub):
    """Status probes the server even when authentication has not been configured."""
    status = LiquidManager(settings=Settings(ask_for_token=True)).status()
    assert status.server_connected
    assert status.server_ready
    assert not status.token_available
    assert status.token_valid is None
    assert [r.url.path for r in api_stub.requests] == ['/health/ready']
    assert len(api_stub.clients) == 1
    assert api_stub.clients[0].is_closed


@pytest.mark.parametrize(('code', 'valid'), [(200, True), (401, False), (403, False), (503, None)])
def test_status_distinguishes_invalid_credentials_from_unknown_validity(code, valid, configured, api_stub):
    """Protect validity diagnostics and close each probe before moving to the next phase."""
    def check(_request):
        assert api_stub.clients[0].is_closed
        return httpx.Response(code, json={'valid': True, 'scopes': ['read', 'execute']})
    api_stub.routes['GET', '/v1/auth/check'] = check
    status = configured.status()
    assert status.token_available
    assert status.token_source == 'environment'
    assert status.token_valid is valid
    assert status.server_connected
    assert len(api_stub.clients) == 2
    assert all(client.is_closed for client in api_stub.clients)


def test_status_distinguishes_connectivity_from_server_readiness(configured, api_stub):
    """A 503 health response proves a connection but not server readiness."""
    api_stub.routes['GET', '/health/ready'] = lambda _request: httpx.Response(503)
    status = configured.status()
    assert status.server_connected
    assert not status.server_ready
    assert status.token_valid is True


@pytest.mark.parametrize('source', ['environment', 'file', 'argument'])
def test_numeric_settings_accept_integer_and_fractional_values(source, tmp_path, monkeypatch):
    """CPU, GiB resources and intervals accept ordinary integer and fractional numbers."""
    values = {'cpu': 2, 'ram': 1, 'disk': 3, 'request_timeout': 5, 'renew_interval': 20}
    if source == 'environment':
        for name, value in values.items():
            monkeypatch.setenv('LIQUID_' + name.upper(), str(value))
        settings = Settings()
    elif source == 'file':
        (tmp_path / 'liquid.json').write_text(json.dumps(values))
        settings = Settings()
    else:
        settings = Settings(**values)
    assert {name: getattr(settings, name) for name in values} == values


@pytest.mark.parametrize('health', ['network', 'invalid_json', 'deep_json'])
@pytest.mark.parametrize('authentication', ['accepted', 'unavailable', 'absent'])
def test_status_reports_health_transport_and_protocol_errors(health, authentication, configured, api_stub, monkeypatch):
    """Protect connectivity diagnostics from transport, protocol and JSON recursion failures."""
    def failure(request):
        if health == 'network':
            raise httpx.ConnectError('private details', request=request)
        if health == 'deep_json':
            return httpx.Response(200, content=b'[' * 10000 + b'0' + b']' * 10000)
        return httpx.Response(200, content=b'bad')
    api_stub.routes['GET', '/health/ready'] = failure
    if authentication == 'unavailable':
        def unavailable(request):
            raise httpx.ConnectError('private details', request=request)
        api_stub.routes['GET', '/v1/auth/check'] = unavailable
    elif authentication == 'absent':
        monkeypatch.delenv('LIQUID_TOKEN')
    status = configured.status()
    assert status.server_ready is False
    assert status.server_connected is (health != 'network' or authentication == 'accepted')
    assert status.token_valid is (True if authentication == 'accepted' else None)
    assert status.error
    assert 'private details' not in status.error
    assert all(client.is_closed for client in api_stub.clients)


@pytest.mark.parametrize('failure', ['network', 'protocol', 'missing_endpoint', 'bad_token', 'keyring'])
def test_status_reports_unknown_auth_validity_separately(failure, configured, api_stub, monkeypatch):
    if failure == 'bad_token':
        monkeypatch.setenv('LIQUID_TOKEN', '')
    elif failure == 'keyring':
        monkeypatch.delenv('LIQUID_TOKEN')
        def locked(_service, _username):
            raise OSError('private vault details')
        monkeypatch.setattr(keyring, 'get_password', locked)
    else:
        def response(request):
            if failure == 'network':
                raise httpx.ConnectError('private network details', request=request)
            if failure == 'missing_endpoint':
                return httpx.Response(404)
            return httpx.Response(200, json={})
        api_stub.routes['GET', '/v1/auth/check'] = response
    status = configured.status()
    assert status.token_valid is (False if failure == 'bad_token' else None)
    assert status.error
    assert 'private' not in status.error
    assert all(client.is_closed for client in api_stub.clients)


@pytest.mark.parametrize(('url', 'valid'), [
    ('http://host:bad/v1', False), ('https://[bad/v1', False),
    ('https://☃.invalid/v1', False), ('https://api.invalid/\0/v1', False),
    ('https://api.invalid/v1?', False), ('https://api.invalid/v1#', False),
    ('https://api.invalid/path%3F/v1', True), ('https://api.invalid/path%23/v1', True),
])
def test_url_syntax_is_validated(url, valid):
    """Prevent InvalidURL or misplaced API routes while preserving encoded path characters."""
    if valid:
        assert Settings(api_url=url).api_url == url
    else:
        with pytest.raises(ValueError, match='api_url'):
            Settings(api_url=url)


@pytest.mark.parametrize('field', ['cpu', 'ram', 'disk', 'request_timeout', 'retry_backoff'])
def test_huge_numeric_configuration_has_a_validation_error(field):
    """Large JSON integers fail validation without leaking a numeric overflow exception."""
    with pytest.raises(ValueError, match=field):
        Settings(**{field: 10 ** 400})


@pytest.mark.parametrize(('field', 'bad', 'value'), [
    ('cpu', 'not-a-number', 2), ('command_timeout', 'bad', 2),
    ('ask_for_token', 'not-a-bool', True), ('env', 'bad-json', {'LANG': 'C'}),
])
def test_explicit_settings_override_invalid_environment(field, bad, value, monkeypatch):
    """An unused lower-priority value cannot break an explicit valid setting."""
    monkeypatch.setenv('LIQUID_' + field.upper(), bad)
    assert getattr(Settings(**{field: value}), field) == value


@pytest.mark.parametrize(('field', 'bad', 'value'), [
    ('cpu', 'wrong', 2), ('command_timeout', -1, 2), ('env', {'bad=name': 'x'}, {}),
])
def test_explicit_settings_override_invalid_file_value(field, bad, value, tmp_path):
    (tmp_path / 'liquid.json').write_text(json.dumps({field: bad}))
    assert getattr(Settings(**{field: value}), field) == value
