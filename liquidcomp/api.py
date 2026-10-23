"""Bounded synchronous transport with explicit idempotency and no credential redirects."""

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Dict, List, Mapping, NoReturn, Optional, cast
from urllib.parse import urlsplit, urlunsplit

import httpx

from liquidcomp.errors import (
    APIError,
    InvalidTokenError,
    PermissionDeniedError,
    ProtocolError,
    ServerUnavailableError,
)
from liquidcomp.protocol import array_value, object_value, text
from liquidcomp.settings import Settings


def _constant(_value: str) -> NoReturn:
    raise ValueError('Non-finite JSON constant.')


@dataclass(frozen=True)
class Reply:
    content: bytes
    headers: Mapping[str, str]
    sent_at: float

    def value(self) -> object:
        try:
            return cast(object, json.loads(self.content, parse_constant=_constant))
        except (ValueError, RecursionError):
            raise ProtocolError('The API returned invalid JSON.') from None

    def mapping(self) -> Dict[str, object]:
        return object_value(self.value())

    def array(self) -> List[object]:
        return array_value(self.value())

    def deadline(self, expiry: datetime) -> float:
        date_header = self.headers.get('date')
        try:
            # HTTP Date loses fractional seconds; round conservatively so we
            # never treat a lease as live after its actual server expiry.
            server_time = parsedate_to_datetime(date_header) + timedelta(seconds=1) if date_header else datetime.now(timezone.utc)
            if server_time.tzinfo is None:
                raise ValueError('Missing timezone.')
        except (ValueError, TypeError, IndexError, OverflowError):
            raise ProtocolError('Invalid HTTP server date.') from None
        return self.sent_at + (expiry - server_time).total_seconds()


class API:
    def __init__(self, settings: Settings, credential: str) -> None:
        self.settings = settings
        self._credential = credential
        self.base_url = settings.api_url.rstrip('/')
        parsed = urlsplit(self.base_url)
        self.health_url = urlunsplit((parsed.scheme, parsed.netloc, '/health/ready', '', ''))
        self._client = httpx.Client(timeout=float(settings.request_timeout), follow_redirects=False, trust_env=False)

    def close(self) -> None:
        self._client.close()

    def request(self, method: str, path: str, *, payload: Optional[Dict[str, object]] = None, content: Optional[bytes] = None, key: Optional[str] = None, retry: bool = True, raw: bool = False, authenticated: bool = True) -> Reply:  # noqa: PLR0913
        url = self.base_url + '/' + path if authenticated else self.health_url
        headers = {'Authorization': 'Bearer ' + self._credential} if authenticated else {}
        if key is not None:
            headers['Idempotency-Key'] = key
        if content is not None:
            headers['Content-Type'] = 'application/octet-stream'
        remaining = self.settings.retry_attempts if retry else 1
        attempt = 0
        while True:
            sent_at = time.monotonic()
            try:
                with self._client.stream(method, url, headers=headers, json=payload, content=content) as response:
                    status = response.status_code
                    if status == 401:
                        raise InvalidTokenError('The API rejected the token.', status)
                    if status == 403:
                        raise PermissionDeniedError('The token lacks the required permissions.', status)
                    if status in (408, 425, 429) or status >= 500:
                        raise ServerUnavailableError('The API is temporarily unavailable.', status, key)
                    if not 200 <= status < 300:
                        raise APIError(f'Liquid API returned HTTP {status}.', status, key)
                    limit = self.settings.max_artifact_bytes if raw else self.settings.max_response_bytes
                    data = bytearray()
                    for chunk in response.iter_bytes():
                        if time.monotonic() - sent_at > self.settings.request_timeout:
                            raise ServerUnavailableError('The API response exceeded its time budget.', idempotency_key=key)
                        data.extend(chunk)
                        if len(data) > limit:
                            raise ProtocolError('The API response exceeds the configured size limit.')
                    return Reply(bytes(data), dict(response.headers), sent_at)
            except (httpx.RequestError, ServerUnavailableError) as error:
                failure = error if isinstance(error, ServerUnavailableError) else ServerUnavailableError('Could not confirm the API request.', idempotency_key=key)
                remaining -= 1
                if remaining == 0:
                    raise failure from None
                time.sleep(min(float(self.settings.retry_backoff) * 2.0 ** attempt, float(self.settings.request_timeout)))
                attempt += 1

    def check(self) -> None:
        payload = self.request('GET', 'auth/check').mapping()
        scopes = {text(item) for item in array_value(payload.get('scopes'))}
        if payload.get('valid') is not True or not {'read', 'execute'} <= scopes:
            raise ProtocolError('The API did not confirm both read and execute permissions.')

    def download(self, path: str) -> bytes:
        reply = self.request('GET', path, raw=True)
        checksum = reply.headers.get('x-content-sha256')
        if checksum != hashlib.sha256(reply.content).hexdigest():
            raise ProtocolError('Artifact checksum verification failed.')
        return reply.content
