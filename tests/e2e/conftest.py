"""An independent real TCP mock implementing the Liquid API wire contract."""

import hashlib
import json
import os
import shlex
import shutil
import socket
import subprocess
import tarfile
import threading
import time
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryFile
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import pytest


@pytest.fixture
def mock_api(tmp_path):
    """Restore isolated source trees and run actual subprocesses behind a mock API."""
    state = SimpleNamespace(
        requests=[], artifacts={}, isolates={}, commands={}, keys={}, benchmarks=[], benchmark_history=[],
        computer_id=str(uuid4()), drop_command_response=False, renewals=0, benchmark_status=200,
        provisioning_renewals_required=0,
        stall_response=None, response_started=threading.Event(), release_response=threading.Event(),
    )
    lock = threading.RLock()
    files = ExitStack()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *args):
            """Keep tokens, source contents and commands out of the test runner output."""

        def respond(self, code, payload=None, content=None):
            data = json.dumps(payload).encode() if content is None else content
            if state.stall_response is not None:
                self.close_connection = True
            if state.stall_response == 'headers':
                state.response_started.set()
                assert state.release_response.wait(5)
            self.send_response(code)
            self.send_header('Content-Length', str(len(data)))
            if content is not None:
                self.send_header('X-Content-SHA256', hashlib.sha256(content).hexdigest())
            try:
                self.end_headers()
                if state.stall_response == 'body':
                    state.response_started.set()
                    assert state.release_response.wait(5)
                self.wfile.write(data)
            except ConnectionError:
                pass  # The timeout tests deliberately disconnect before releasing the response.

        def do_GET(self):
            self.handle_request()

        def do_POST(self):
            self.handle_request()

        def do_DELETE(self):
            self.handle_request()

        def handle_request(self):  # noqa: PLR0911, PLR0915 - independent wire-level mock router.
            size = int(self.headers.get('Content-Length', '0'))
            body = self.rfile.read(size)
            parsed = urlsplit(self.path)
            path = parsed.path
            with lock:
                state.requests.append((self.command, path, dict(self.headers), body))
                if path == '/health/ready':
                    return self.respond(200, {'status': 'ready'})
                if self.headers.get('Authorization') not in {
                    'Bearer valid-token', 'Bearer new-token', 'Bearer valid-company',
                }:
                    return self.respond(401, {'detail': 'Invalid or expired token'})
                if path == '/v1/auth/check':
                    return self.respond(200, {'valid': True, 'scopes': ['read', 'execute']})
                if path == '/v1/artifacts':
                    identifier = str(uuid4())
                    state.artifacts[identifier] = body
                    return self.respond(201, {'id': identifier, 'sha256': hashlib.sha256(body).hexdigest()})
                if path.endswith('/content'):
                    return self.respond(200, content=state.artifacts[path.split('/')[-2]])
                if path == '/v1/isolates':
                    key = self.headers['Idempotency-Key']
                    if key not in state.keys:
                        payload = json.loads(body)
                        identifier = str(uuid4())
                        root = tmp_path / 'remote' / identifier
                        root.mkdir(parents=True)
                        with tarfile.open(fileobj=BytesIO(state.artifacts[payload['state_artifact_id']])) as archive:
                            for member in archive:
                                target = root / member.name
                                assert not member.name.startswith('/')
                                assert '..' not in Path(member.name).parts
                                if member.isdir():
                                    target.mkdir(parents=True, exist_ok=True)
                                elif member.isfile():
                                    target.parent.mkdir(parents=True, exist_ok=True)
                                    target.write_bytes(archive.extractfile(member).read())
                        state.isolates[identifier] = {'root': root, 'id': identifier, 'computer_id': state.computer_id,
                                                      'status': 'provisioning' if state.provisioning_renewals_required else 'ready', 'renewals': 0}
                        state.keys[key] = identifier
                    return self.respond(202, self.isolate(state.keys[key]))
                if path.startswith('/v1/isolates/'):
                    identifier = path.split('/')[3]
                    if path.endswith('/renew'):
                        state.renewals += 1
                        row = state.isolates[identifier]
                        row['renewals'] += 1
                        if row['renewals'] >= state.provisioning_renewals_required:
                            row['status'] = 'ready'
                        return self.respond(200, self.isolate(identifier))
                    if self.command == 'DELETE':
                        for command in state.commands.values():
                            if command['isolate_id'] == identifier and command['process'].poll() is None:
                                command['process'].kill()
                                command['process'].wait(timeout=5)
                        shutil.rmtree(state.isolates[identifier]['root'])
                        state.isolates[identifier]['status'] = 'destroyed'
                        return self.respond(202, self.isolate(identifier))
                    if path.endswith('/commands'):
                        key = self.headers['Idempotency-Key']
                        if key not in state.keys:
                            payload = json.loads(body)
                            command_id = str(uuid4())
                            stdout, stderr = files.enter_context(TemporaryFile()), files.enter_context(TemporaryFile())  # noqa: SIM115 - ExitStack owns both files.
                            process = subprocess.Popen(
                                payload['command'] if payload['shell'] else shlex.split(payload['command']),
                                shell=payload['shell'], cwd=state.isolates[identifier]['root'] / payload['cwd'],
                                env={**os.environ, **payload['env']}, stdout=stdout, stderr=stderr,
                            )
                            state.commands[command_id] = {'process': process, 'stdout': stdout, 'stderr': stderr, 'isolate_id': identifier,
                                                          'status': 'running', 'deadline': time.monotonic() + payload['timeout_seconds']}
                            state.keys[key] = command_id
                            if state.drop_command_response:
                                state.drop_command_response = False
                                self.connection.shutdown(socket.SHUT_RDWR)
                                self.connection.close()
                                self.close_connection = True
                                return None
                        return self.respond(202, {'id': state.keys[key], 'queued_deadline': (datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat()})
                    if path.endswith('/snapshots'):
                        buffer = BytesIO()
                        root = state.isolates[identifier]['root']
                        with tarfile.open(fileobj=buffer, mode='w') as archive:
                            for item in sorted(root.rglob('*')):
                                archive.add(item, arcname=item.relative_to(root).as_posix(), recursive=False)
                        artifact = str(uuid4())
                        operation = str(uuid4())
                        state.artifacts[artifact] = buffer.getvalue()
                        state.keys[operation] = artifact
                        return self.respond(202, {'id': operation})
                    return self.respond(200, self.isolate(identifier))
                if path.startswith('/v1/operations/'):
                    operation = path.split('/')[-1]
                    return self.respond(200, {'id': operation, 'status': 'completed', 'artifact_id': state.keys[operation]})
                if path == '/v1/commands/cancel':
                    for identifier in json.loads(body)['ids']:
                        command = state.commands[identifier]
                        if command['process'].poll() is None:
                            command['process'].terminate()
                            command['process'].wait(timeout=5)
                            command['status'] = 'cancelled'
                    return self.respond(202, [])
                if path == '/v1/commands/results':
                    return self.respond(200, [self.result(identifier) for identifier in json.loads(body)['ids']])
                if path.endswith('/benchmark-results'):
                    # Fixtures keep the latest suite separate from older measurements.
                    rows = state.benchmarks
                    if parse_qs(parsed.query).get('latest_only', ['true'])[0].lower() == 'false':
                        rows = state.benchmark_history + rows
                    return self.respond(state.benchmark_status, rows)
                return self.respond(404, {'detail': 'Resource not found'})

        def isolate(self, identifier):
            row = state.isolates[identifier]
            return {**{key: row[key] for key in ('id', 'computer_id', 'status')},
                'expires_at': (datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat(),
            }

        def result(self, identifier):
            command = state.commands[identifier]
            code = command['process'].poll()
            if code is None and time.monotonic() >= command['deadline']:
                command['process'].kill()
                code = command['process'].wait(timeout=5)
                command['status'] = 'timed_out'
            if code is None:
                return {'id': identifier, 'status': 'running', 'result': None}
            values = {}
            for name in ('stdout', 'stderr'):
                command[name].seek(0)
                data = command[name].read()
                artifact = None
                if len(data) > 4096 or b'\xff' in data:
                    artifact = str(uuid4())
                    state.artifacts[artifact] = data
                values[name] = data[:32].decode('utf-8', 'replace') if artifact else data.decode('utf-8')
                values[name + '_artifact_id'] = artifact
            status = command['status'] if command['status'] in ('cancelled', 'timed_out') else 'completed'
            return {'id': identifier, 'status': status, 'result': {
                'command_id': identifier, 'success': status == 'completed' and code == 0,
                'returncode': code, 'error': None, **values,
            }}

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state.url = 'http://127.0.0.1:' + str(server.server_port) + '/v1'
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        for command in state.commands.values():
            if command['process'].poll() is None:
                command['process'].kill()
                command['process'].wait(timeout=5)
        files.close()


@pytest.fixture
def cli_environment(mock_api, tmp_path):
    """Subprocesses use an explicit fake vault and only the loopback API."""
    root = Path(__file__).resolve().parents[2]
    return {
        **os.environ, 'PYTHONPATH': str(root), 'PYTHON_KEYRING_BACKEND': 'tests.keyring_backend.FileKeyring',
        'LIQUID_TEST_KEYRING': str(tmp_path / 'test-vault.json'), 'LIQUID_API_URL': mock_api.url,
        'LIQUID_POLL_INTERVAL': '0.01', 'LIQUID_RETRY_BACKOFF': '0.0',
    }
