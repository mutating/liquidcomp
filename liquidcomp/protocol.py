"""Small strict decoders for the API's untrusted JSON boundary."""

import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, cast
from uuid import UUID

from liquidcomp.errors import ProtocolError


def object_value(value: object) -> Dict[str, object]:
    if not isinstance(value, dict):
        raise ProtocolError('Expected an API object.')
    return cast(Dict[str, object], value)


def array_value(value: object) -> List[object]:
    if not isinstance(value, list):
        raise ProtocolError('Expected an API array.')
    return cast(List[object], value)


def text(value: object) -> str:
    if not isinstance(value, str):
        raise ProtocolError('Expected an API string.')
    return value


def optional_text(value: object) -> Optional[str]:
    return None if value is None else text(value)


def identifier(value: object) -> str:
    raw = text(value)
    try:
        return str(UUID(raw))
    except ValueError:
        raise ProtocolError('Invalid API resource identifier.') from None


def when(value: object) -> datetime:
    try:
        date = datetime.fromisoformat(text(value).replace('Z', '+00:00'))
        if date.tzinfo is None or date.utcoffset() is None:
            raise ProtocolError('An API timestamp must include its timezone.')
        return date.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        raise ProtocolError('Invalid API timestamp.') from None


def number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= value <= sys.float_info.max:
        raise ProtocolError('Invalid benchmark score.')
    return float(value)
