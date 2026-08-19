"""Request-scoped identity, readable anywhere below the route handler.

Authentication resolves the caller in a FastAPI dependency and returns a
`User`; nothing below that layer — the operation audit line, the security
event writer, a runner thread — is handed the request. So the audit trail
recorded *who* as a bare ``user_id`` and could not say which credential
acted, and the request id minted by the logging middleware never reached
the events written during that request.

Two `ContextVar`s close that gap without threading a request object through
every signature: the logging middleware binds the request id before the
route runs, and every successful authentication publishes a
:class:`RequestPrincipal`. Both survive ``await`` without leaking across
concurrent requests, follow the request into `anyio.to_thread` runner
threads (which copy the context), and are absent — ``None`` — outside a
request, which callers must treat as "unknown", never as "anonymous".

`publish_principal` also mirrors the identity onto ``request.state``
(``user_id`` / ``auth_user_id`` / ``auth_method`` / ``api_key_prefix``), the
channel the access-log middleware reads after the response, since a value
set inside the route's task is not visible back in the middleware's own
context.
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import Any

# The first 8 characters of an API key are its stored identification prefix
# (``UserAPIKey.prefix``): enough to name the credential in an incident
# without ever writing the secret.
API_KEY_PREFIX_LENGTH = 8


@dataclass(frozen=True)
class RequestPrincipal:
  user_id: str
  auth_method: str
  api_key_prefix: str | None = None

  def audit_fields(self) -> dict[str, Any]:
    fields: dict[str, Any] = {"auth_method": self.auth_method}
    if self.api_key_prefix:
      fields["api_key_prefix"] = self.api_key_prefix
    return fields


_request_id: ContextVar[str | None] = ContextVar("robosystems_request_id", default=None)
_principal: ContextVar[RequestPrincipal | None] = ContextVar(
  "robosystems_request_principal", default=None
)


def bind_request_id(request_id: str) -> Token[str | None]:
  return _request_id.set(request_id)


def reset_request_id(token: Token[str | None]) -> None:
  _request_id.reset(token)


def current_request_id() -> str | None:
  return _request_id.get()


def bind_principal(
  principal: RequestPrincipal | None,
) -> Token[RequestPrincipal | None]:
  return _principal.set(principal)


def reset_principal(token: Token[RequestPrincipal | None]) -> None:
  _principal.reset(token)


def current_principal() -> RequestPrincipal | None:
  return _principal.get()


def publish_principal(
  request: Any,
  user_id: str,
  auth_method: str,
  api_key: str | None = None,
) -> RequestPrincipal:
  """Record the authenticated caller for the rest of this request.

  Called from every authentication success branch. ``request`` is the
  Starlette request (typed loosely so this module stays import-free of the
  web layer); its ``state`` receives the same fields the ContextVar carries.
  """
  prefix = api_key[:API_KEY_PREFIX_LENGTH] if api_key else None
  principal = RequestPrincipal(
    user_id=str(user_id), auth_method=auth_method, api_key_prefix=prefix
  )
  bind_principal(principal)
  state = getattr(request, "state", None)
  if state is not None:
    state.user_id = principal.user_id
    state.auth_user_id = principal.user_id
    state.auth_method = auth_method
    if prefix is not None:
      state.api_key_prefix = prefix
  return principal


def audit_context() -> dict[str, Any]:
  """The request-scoped fields an audit or security event should carry:
  ``request_id`` when inside a request, plus the principal's credential
  attribution when one has authenticated. Empty outside a request."""
  fields: dict[str, Any] = {}
  request_id = current_request_id()
  if request_id:
    fields["request_id"] = request_id
  principal = current_principal()
  if principal is not None:
    fields.update(principal.audit_fields())
  return fields


__all__ = [
  "API_KEY_PREFIX_LENGTH",
  "RequestPrincipal",
  "audit_context",
  "bind_principal",
  "bind_request_id",
  "current_principal",
  "current_request_id",
  "publish_principal",
  "reset_principal",
  "reset_request_id",
]
