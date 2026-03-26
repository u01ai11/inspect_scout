"""Databricks on-behalf-of-user forwarded auth helpers."""

from __future__ import annotations

from contextvars import ContextVar

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

_forwarded_access_token: ContextVar[str | None] = ContextVar(
    "databricks_forwarded_access_token",
    default=None,
)
_forwarded_user: ContextVar[str | None] = ContextVar(
    "databricks_forwarded_user",
    default=None,
)
_forwarded_email: ContextVar[str | None] = ContextVar(
    "databricks_forwarded_email",
    default=None,
)
_forwarded_preferred_username: ContextVar[str | None] = ContextVar(
    "databricks_forwarded_preferred_username",
    default=None,
)


class DatabricksForwardedAuthMiddleware(BaseHTTPMiddleware):
    """Capture Databricks forwarded OBO headers into request-local context."""

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        token_state = _forwarded_access_token.set(
            request.headers.get("x-forwarded-access-token")
        )
        user_state = _forwarded_user.set(request.headers.get("x-forwarded-user"))
        email_state = _forwarded_email.set(request.headers.get("x-forwarded-email"))
        preferred_username_state = _forwarded_preferred_username.set(
            request.headers.get("x-forwarded-preferred-username")
        )
        try:
            return await call_next(request)
        finally:
            _forwarded_access_token.reset(token_state)
            _forwarded_user.reset(user_state)
            _forwarded_email.reset(email_state)
            _forwarded_preferred_username.reset(preferred_username_state)


def get_forwarded_access_token() -> str | None:
    """Get Databricks forwarded OBO user access token for current request."""
    return _forwarded_access_token.get()


def get_forwarded_identity() -> dict[str, str]:
    """Get forwarded identity fields for current request."""
    identity: dict[str, str] = {}
    if _forwarded_user.get():
        identity["x-forwarded-user"] = _forwarded_user.get() or ""
    if _forwarded_email.get():
        identity["x-forwarded-email"] = _forwarded_email.get() or ""
    if _forwarded_preferred_username.get():
        identity["x-forwarded-preferred-username"] = (
            _forwarded_preferred_username.get() or ""
        )
    return identity
