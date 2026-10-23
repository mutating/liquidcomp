"""Exercise hidden input in an actual terminal, independently of mocked getpass."""

import json
import os
import select
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

termios = pytest.importorskip('termios')
pty = pytest.importorskip('pty')


class Terminal:
    def __init__(self, environment):
        self.master, self.slave = pty.openpty()
        self.process = subprocess.Popen(
            [sys.executable, '-m', 'liquidcomp', 'init'], env=environment,
            stdin=self.slave, stdout=self.slave, stderr=self.slave,
        )
        self.output = b''
        self.position = 0

    def expect(self, message):
        deadline = time.monotonic() + 10
        while message not in self.output[self.position:]:
            remaining = deadline - time.monotonic()
            assert remaining > 0, self.output
            ready, _, _ = select.select([self.master], [], [], remaining)
            assert ready, self.output
            data = os.read(self.master, 65536)
            assert data, self.output
            self.output += data
        self.position = self.output.index(message, self.position) + len(message)

    def send(self, value):
        assert not termios.tcgetattr(self.slave)[3] & termios.ECHO
        os.write(self.master, value)

    def finish(self):
        self.process.wait(timeout=10)
        while select.select([self.master], [], [], 0)[0]:
            self.output += os.read(self.master, 65536)
        assert termios.tcgetattr(self.slave)[3] & termios.ECHO
        return self.process.returncode

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=5)
        os.close(self.master)
        os.close(self.slave)


def test_cli_terminal_retries_and_persists_only_accepted_token(cli_environment):
    """Empty and rejected values retry with echo disabled; the accepted secret reaches keyring."""
    terminal = Terminal(cli_environment)
    try:
        for value in (b'\n', b' \n', b'rejected-secret\n', b'new-token\n'):
            terminal.expect(b'Liquid API token (Ctrl+C to cancel): ')
            terminal.send(value)
        assert terminal.finish() == 0
        assert terminal.output.count(b'Token is empty') == 2
        assert b'Token rejected' in terminal.output
        assert terminal.output.count(b'Thank you') == 1
        assert b'rejected-secret' not in terminal.output
        assert b'new-token' not in terminal.output
        saved = json.loads(Path(cli_environment['LIQUID_TEST_KEYRING']).read_text())
        assert list(saved.values()) == ['new-token']
    finally:
        terminal.close()


@pytest.mark.parametrize('interruption', ['interrupt', 'eof'])
def test_cli_terminal_cancellation_preserves_existing_token(cli_environment, interruption):
    """Both Ctrl+C and EOF restore echo, keep the old secret, and use exit status 130."""
    vault = Path(cli_environment['LIQUID_TEST_KEYRING'])
    previous = {'liquidcomp:' + cli_environment['LIQUID_API_URL'] + '\0LIQUID_TOKEN': 'old-secret'}
    vault.write_text(json.dumps(previous))
    terminal = Terminal(cli_environment)
    try:
        terminal.expect(b'Liquid API token (Ctrl+C to cancel): ')
        if interruption == 'interrupt':
            terminal.process.send_signal(signal.SIGINT)
        else:
            terminal.send(b'\x04')
        assert terminal.finish() == 130
        assert b'A token already exists in keyring.' in terminal.output
        assert b'old-secret' not in terminal.output
        assert b'Thank you' not in terminal.output
        assert json.loads(vault.read_text()) == previous
    finally:
        terminal.close()
