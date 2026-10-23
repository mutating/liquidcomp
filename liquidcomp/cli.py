"""The liquid executable delegates initialization and diagnostics to its manager."""

import argparse
import sys
from typing import Optional, Sequence, cast

from liquidcomp.errors import AuthenticationCancelledError, LiquidError
from liquidcomp.manager import LiquidManager


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog='liquid', description='Liquid Compute credentials and connectivity')
    parser.add_argument('command', choices=('init', 'status'))
    arguments = parser.parse_args(argv)
    try:
        manager = LiquidManager()
    except Exception:  # noqa: BLE001 - configuration providers expose different parser/IO errors.
        sys.stderr.write('Invalid Liquid configuration. Check LIQUID_* and liquid configuration files.\n')
        return 1
    try:
        if cast(str, arguments.command) == 'init':
            manager.init()
            return 0
        status = manager.status()
        connection = 'connected' if status.server_connected else 'disconnected'
        readiness = 'ready' if status.server_ready else 'not ready'
        if not status.token_available:
            credential = 'missing'
        elif status.token_valid is None:
            credential = 'unknown validity'
        else:
            credential = 'valid' if status.token_valid else 'invalid or insufficient permissions'
        sys.stdout.write(f'Server: {connection}, {readiness}\nToken: {credential}')
        if status.token_source is not None:
            sys.stdout.write(f' ({status.token_source})')
        sys.stdout.write('\n')
        if status.error is not None:
            sys.stdout.write(status.error + '\n')
        return 0 if status.server_ready and status.token_valid else 1
    except (AuthenticationCancelledError, KeyboardInterrupt):
        sys.stderr.write('Initialization cancelled.\n')
        return 130
    except LiquidError as error:
        sys.stderr.write(str(error) + '\n')
        return 1

