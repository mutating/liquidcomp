"""Plugin errors are compatible with throng's common exception boundary."""

from typing import Optional

from throng.errors import ThrongError


class LiquidError(ThrongError):
    """Base error for Liquid Compute."""


class APIError(LiquidError):
    """A sanitized API error, optionally identifying an uncertain operation."""

    def __init__(self, message: str, status_code: Optional[int] = None, idempotency_key: Optional[str] = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.idempotency_key = idempotency_key


class InvalidTokenError(APIError):
    """An explicitly supplied token is empty, malformed or rejected."""


class PermissionDeniedError(APIError):
    """The token lacks permission for the requested operation."""


class ServerUnavailableError(APIError):
    """The server could not confirm an operation within the retry budget."""


class MissingTokenError(LiquidError):
    """No credential is available."""


class KeyringError(LiquidError):
    """The credential vault could not be read or written."""


class AuthenticationCancelledError(LiquidError):
    """The user canceled credential replacement."""


class NonInteractiveError(LiquidError):
    """Hidden terminal input is unavailable."""


class ProtocolError(LiquidError):
    """An API response violates the wire contract."""


class StateError(LiquidError):
    """Initial state cannot be safely represented as a tar archive."""


class OperationCancelledError(LiquidError):
    """An allocation was canceled before it completed."""


class OperationTimeoutError(LiquidError):
    """A control operation exceeded its wait budget."""


class CancellationTimeoutError(LiquidError):
    """The API did not confirm a command's cancellation."""


class IsolateUnavailableError(LiquidError):
    """Provisioning or lease maintenance failed."""


class IsolateClosedError(LiquidError):
    """An operation requires a live isolate."""


class MetadataError(LiquidError):
    """Performance metadata is unavailable or malformed."""
