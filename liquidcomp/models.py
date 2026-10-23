"""Public results contain execution data and diagnostics, never API credentials."""

from dataclasses import dataclass
from typing import Optional

from throng.abstracts.results import SimpleRunResult


@dataclass
class LiquidResult(SimpleRunResult):
    id: Optional[str] = None
    status: str = 'cancelled'
    killed_by_token: bool = False
    error: Optional[str] = None
    stdout_bytes: Optional[bytes] = None
    stderr_bytes: Optional[bytes] = None


@dataclass(frozen=True)
class Status:
    server_connected: bool
    server_ready: bool
    token_available: bool
    token_valid: Optional[bool]
    token_source: Optional[str] = None
    error: Optional[str] = None
