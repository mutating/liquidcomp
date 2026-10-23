"""Validated plugin settings loaded through fresh skelet sources."""

# skelet dynamically prepares Storage subclasses using Any in __init_subclass__.
# mypy: disallow-any-expr=False

from typing import Dict, Optional, Union
from urllib.parse import urlsplit

import httpx
from skelet import EnvSource, Field, MemorySource, Storage, for_tool

Number = Union[int, float]


def _url(value: str) -> bool:
    try:
        httpx.URL(value)
        parsed = urlsplit(value)
        return bool(parsed.scheme in ('http', 'https') and parsed.hostname and parsed.port != 0 and not parsed.username and not parsed.password and not any(c.isspace() or c in '?#' for c in value))
    except (ValueError, httpx.InvalidURL):
        return False


def _positive(value: Number) -> bool:
    return not isinstance(value, bool) and 0 < value <= 86400


def _cpu(value: Number) -> bool:
    return _positive(value) and value <= 1000 and round(value * 1000) >= 1


def _space(value: Number) -> bool:
    return not isinstance(value, bool) and 1 <= value * 1024 ** 3 <= 2 ** 60


def _cwd(value: str) -> bool:
    return bool(value) and not value.startswith('/') and '..' not in value.split('/') and not any(c in value for c in ('\\', ':', '\0')) and len(value) <= 1024


def _env(value: Dict[str, str]) -> bool:
    return len(value) <= 100 and all(key and len(key) <= 200 and '=' not in key and '\0' not in key and len(item) <= 8192 and '\0' not in item for key, item in value.items())


def _empty_env() -> Dict[str, str]:
    return {}


def _timeout(value: int) -> bool:
    return type(value) is int and 1 <= value <= 86400


def _image(value: str) -> bool:
    return bool(value.strip()) and len(value) <= 300


def _attempts(value: int) -> bool:
    return type(value) is int and 1 <= value <= 10


def _backoff(value: Number) -> bool:
    return not isinstance(value, bool) and 0 <= value <= 10


def _size(value: int) -> bool:
    return type(value) is int and value > 0


class Settings(Storage):
    """Environment LIQUID_* precedes liquid files and [tool.liquid]."""

    api_url: str = Field('https://api.mutating.tech/v1', validation=_url, hide=True)
    ask_for_token: bool = False
    image: str = Field('ubuntu', validation=_image)
    cpu: Number = Field(1.0, validation=_cpu)
    ram: Number = Field(0.5, validation=_space)
    disk: Number = Field(1.0, validation=_space)
    command_timeout: int = Field(3600, validation=_timeout)
    shell: bool = False
    cwd: str = Field('.', validation=_cwd)
    env: Dict[str, str] = Field(default_factory=_empty_env, validation=_env, hide=True)
    request_timeout: Number = Field(10.0, validation=_positive)
    poll_interval: Number = Field(0.2, validation=_positive)
    operation_timeout: Number = Field(300.0, validation=_positive)
    cancellation_timeout: Number = Field(40.0, validation=_positive)
    renew_interval: Number = Field(60.0, validation=_positive)
    retry_attempts: int = Field(3, validation=_attempts)
    retry_backoff: Number = Field(0.1, validation=_backoff)
    max_artifact_bytes: int = Field(100 * 1024 ** 2, validation=_size)
    max_response_bytes: int = Field(16 * 1024 ** 2, validation=_size)

    def __init__(self, **values: object) -> None:
        # skelet 0.0.21 cannot parse Union[int, float] from strings. Parse those
        # environment fields through its float source before normal resolution.
        source = EnvSource[float](prefix='LIQUID_')
        numeric = {name: source.type_awared_get(name, float) for name in (
            'cpu', 'ram', 'disk', 'request_timeout', 'poll_interval',
            'operation_timeout', 'cancellation_timeout', 'renew_interval', 'retry_backoff',
        ) if name not in values}
        overrides = MemorySource({name: value for name, value in numeric.items() if value is not None})
        super().__init__(_sources=[MemorySource(values), overrides, *for_tool('liquid')], **values)


def environment_token() -> Optional[str]:
    """Use a fresh skelet environment source, never ordinary configuration files."""
    return EnvSource[str](prefix='LIQUID_').type_awared_get('token', str)
