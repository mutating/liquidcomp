"""A subprocess-only test vault; no test ever uses the operating-system keychain."""

import json
import os
from pathlib import Path

from keyring.backend import KeyringBackend


class FileKeyring(KeyringBackend):
    priority = 1  # type: ignore[assignment]  # keyring supports a constant or class property.

    def get_password(self, service, username):
        path = Path(os.environ['LIQUID_TEST_KEYRING'])
        values = json.loads(path.read_text()) if path.exists() else {}
        return values.get(service + '\0' + username)

    def set_password(self, service, username, password):
        path = Path(os.environ['LIQUID_TEST_KEYRING'])
        values = json.loads(path.read_text()) if path.exists() else {}
        values[service + '\0' + username] = password
        path.write_text(json.dumps(values))

    def delete_password(self, service, username):
        path = Path(os.environ['LIQUID_TEST_KEYRING'])
        values = json.loads(path.read_text())
        del values[service + '\0' + username]
        path.write_text(json.dumps(values))
