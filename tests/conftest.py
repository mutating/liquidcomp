"""Isolate credentials, settings and HTTP without touching the user's keychain."""

import hashlib
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import keyring
import pytest

import liquidcomp.auth
from liquidcomp import LiquidManager, Settings


@pytest.fixture
def byte_stream():
    """Keep chunk boundaries visible to exercise cumulative transport limits."""
    class Stream(httpx.SyncByteStream):
        def __init__(self, chunks):
            self.chunks = chunks

        def __iter__(self):
            yield from self.chunks
    return Stream


@pytest.fixture(autouse=True)
def clean_environment(tmp_path, monkeypatch):
    """Give each test fresh settings and a keychain which cannot reach OS secrets."""
    for name in list(os.environ):
        if name.startswith('LIQUID_'):
            monkeypatch.delenv(name)
    if os.environ.get('COVERAGE_PROCESS_START'):
        monkeypatch.setenv('COVERAGE_FILE', str(Path(__file__).resolve().parents[1] / '.coverage'))
    monkeypatch.chdir(tmp_path)
    values, reads, writes = {}, [], []

    def get(service, username):
        reads.append((service, username))
        return values.get((service, username))

    def put(service, username, value):
        writes.append((service, username, value))
        values[service, username] = value

    monkeypatch.setattr(keyring, 'get_password', get)
    monkeypatch.setattr(keyring, 'set_password', put)
    return SimpleNamespace(values=values, reads=reads, writes=writes)


@pytest.fixture
def terminal(monkeypatch, capsys):
    """Advertise terminal capability only when getpass itself is replaced by a test."""
    monkeypatch.setattr(sys.stdin, 'isatty', lambda: True)
    monkeypatch.setattr(liquidcomp.auth, 'sys', SimpleNamespace(
        stdin=sys.stdin, stderr=SimpleNamespace(isatty=lambda: True),
        stdout=SimpleNamespace(write=lambda value: sys.stdout.write(value)),
    ))


@pytest.fixture
def api_stub(monkeypatch):
    """A bounded HTTPX transport implementing the public API's default happy path."""
    state = SimpleNamespace(
        requests=[], routes={}, clients=[], isolate_id=str(uuid4()), computer_id=str(uuid4()),
        artifact_id=str(uuid4()), command_id=str(uuid4()), operation_id=str(uuid4()),
        content=b'snapshot',
    )

    def handler(request):  # noqa: PLR0911 - independent mock API route dispatch.
        state.requests.append(request)
        route = state.routes.get((request.method, request.url.path))
        if route is not None:
            return route(request)
        now = datetime.now(timezone.utc)
        path = request.url.path
        isolate = {
            'id': state.isolate_id, 'computer_id': state.computer_id, 'status': 'ready',
            'expires_at': (now + timedelta(seconds=60)).isoformat(),
        }
        if path == '/health/ready':
            return httpx.Response(200, json={'status': 'ready'})
        if path == '/v1/auth/check':
            return httpx.Response(200, json={'valid': True, 'scopes': ['read', 'execute']})
        if path == '/v1/artifacts':
            return httpx.Response(201, json={
                'id': state.artifact_id, 'sha256': hashlib.sha256(request.content).hexdigest(),
            })
        if path == '/v1/isolates' or path.endswith('/renew') or (path.endswith(state.isolate_id) and request.method == 'GET'):
            return httpx.Response(202 if request.method == 'POST' else 200, json=isolate)
        if request.method == 'DELETE':
            return httpx.Response(202, json={**isolate, 'status': 'destroying'})
        if path.endswith('/commands'):
            return httpx.Response(202, json={
                'id': state.command_id, 'queued_deadline': (now + timedelta(seconds=60)).isoformat(),
            })
        if path == '/v1/commands/results':
            return httpx.Response(200, json=[{
                'id': state.command_id, 'status': 'completed',
                'result': {'command_id': state.command_id, 'success': True, 'returncode': 0,
                           'stdout': 'ok\n', 'stderr': '', 'error': None,
                           'stdout_artifact_id': None, 'stderr_artifact_id': None},
            }])
        if path == '/v1/commands/cancel':
            return httpx.Response(202, json=[{'id': state.command_id, 'status': 'cancel_requested'}])
        if path.endswith('/snapshots'):
            return httpx.Response(202, json={'id': state.operation_id})
        if path.endswith(state.operation_id):
            return httpx.Response(200, json={
                'id': state.operation_id, 'status': 'completed', 'artifact_id': state.artifact_id,
            })
        if path.endswith('/content'):
            return httpx.Response(200, content=state.content, headers={
                'X-Content-SHA256': hashlib.sha256(state.content).hexdigest(),
            })
        if path.endswith('/benchmark-results'):
            return httpx.Response(200, json=[])
        raise AssertionError((request.method, path))

    original = httpx.Client

    def client(*args, **kwargs):
        instance = original(*args, transport=httpx.MockTransport(handler), **kwargs)
        state.clients.append(instance)
        return instance

    monkeypatch.setattr(httpx, 'Client', client)
    return state


@pytest.fixture
def configured(api_stub, monkeypatch):
    """A credentialed manager with fast polling and no real HTTP or keychain access."""
    monkeypatch.setenv('LIQUID_TOKEN', 'valid-token')
    return LiquidManager(settings=Settings(poll_interval=0.01, retry_backoff=0.0))
