"""One hidden-input dialog for manager.init() and automatic initialization."""

import getpass
import sys
import warnings
from contextlib import closing
from typing import Optional, Tuple, cast

import keyring

from liquidcomp.api import API
from liquidcomp.errors import (
    AuthenticationCancelledError,
    InvalidTokenError,
    KeyringError,
    MissingTokenError,
    NonInteractiveError,
    PermissionDeniedError,
)
from liquidcomp.settings import Settings, environment_token


def validate_token(value: str) -> str:
    if not value or any(not 33 <= ord(char) <= 126 for char in value):
        raise InvalidTokenError('The supplied token is empty or malformed.')
    return value


class Credentials:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    @property
    def service(self) -> str:
        return 'liquidcomp:' + self.settings.api_url.rstrip('/')

    def peek(self) -> Tuple[Optional[str], Optional[str]]:
        value = environment_token()
        if value is not None:
            return value, 'environment'
        try:
            saved = keyring.get_password(self.service, 'LIQUID_TOKEN')
        except Exception:  # noqa: BLE001 - backends may raise arbitrary OS errors.
            raise KeyringError('Could not read LIQUID_TOKEN from keyring.') from None
        return saved, 'keyring' if saved is not None else None

    def get(self) -> str:
        value, _ = self.peek()
        if value is None:
            raise MissingTokenError('No Liquid token is available. Run liquid init or set LIQUID_TOKEN.')
        return validate_token(value)

    def init(self) -> None:
        old, source = self.peek()
        if old is not None:
            sys.stdout.write(f'A token already exists in {source}. Press Ctrl+C to keep it.\n')
        if source == 'environment':
            sys.stdout.write('LIQUID_TOKEN keeps priority; unset it to use the new keyring token.\n')
        if not cast(bool, sys.stdin.isatty()) or not cast(bool, sys.stderr.isatty()):
            raise NonInteractiveError('A terminal with hidden input is required for liquid init.')
        while True:
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('error', getpass.GetPassWarning)
                    candidate = getpass.getpass('Liquid API token (Ctrl+C to cancel): ')
            except getpass.GetPassWarning:
                raise NonInteractiveError('Hidden terminal input is unavailable.') from None
            except (EOFError, KeyboardInterrupt):
                raise AuthenticationCancelledError('Initialization cancelled; the saved token was not changed.') from None
            if not candidate.strip():
                sys.stdout.write('Token is empty. Please try again.\n')
                continue
            try:
                validate_token(candidate)
                with closing(API(self.settings, candidate)) as api:
                    api.check()
            except (InvalidTokenError, PermissionDeniedError):
                sys.stdout.write('Token rejected or missing required permissions. Please try again.\n')
                continue
            try:
                keyring.set_password(self.service, 'LIQUID_TOKEN', candidate)
            except Exception:  # noqa: BLE001 - preserve the old secret on backend failures.
                raise KeyringError('Could not save LIQUID_TOKEN to keyring.') from None
            sys.stdout.write('Thank you. The token was saved to keyring.\n')
            return
