"""Structured request and security logging middleware, with query-string
credential redaction for every log surface."""

import logging
import time
import uuid
from collections.abc import Callable
from urllib.parse import parse_qsl, urlencode

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from robosystems.logger import (
  api_logger,
  log_api,
  log_app_error,
  log_auth_event,
  security_logger,
)
from robosystems.security.audit_logger import SecurityAuditLogger
from robosystems.security.request_context import bind_request_id, reset_request_id

logger = api_logger

SENSITIVE_QUERY_PARAMS = {
  "token",
  "api_key",
  "apikey",
  "api-key",
  "authorization",
  "auth",
  "password",
  "secret",
  "jwt",
  "bearer",
  "access_token",
  "refresh_token",
  "session",
  "sessionid",
  "session_id",
}


def redact_sensitive_query_params(query_string: str) -> str:
  """Redact sensitive query parameters from a query string for safe logging."""
  if not query_string:
    return ""

  try:
    qs_pairs = parse_qsl(query_string, keep_blank_values=True)
    redacted_pairs = [
      (k, "REDACTED" if k.lower() in SENSITIVE_QUERY_PARAMS else v) for k, v in qs_pairs
    ]
    return urlencode(redacted_pairs)
  except Exception:
    # Unparseable: drop it rather than expose the raw query.
    return ""


class UvicornAccessRedactionFilter(logging.Filter):
  """Redact sensitive query parameters from Uvicorn's own access log.

  Uvicorn writes that line straight from the ASGI scope, raw query string
  included, out of the app's reach; the SSE route's ``?token=`` JWT would
  otherwise land there intact.
  """

  def filter(self, record: logging.LogRecord) -> bool:
    args = record.args
    # Uvicorn's access record: the third arg is the path (+ query string).
    if isinstance(args, tuple) and len(args) >= 3 and isinstance(args[2], str):
      path = args[2]
      if "?" in path:
        base, _, query = path.partition("?")
        safe_query = redact_sensitive_query_params(query)
        record.args = (
          *args[:2],
          f"{base}?{safe_query}" if safe_query else base,
          *args[3:],
        )
    return True


def install_uvicorn_log_redaction() -> None:
  """Attach the access-log redaction filter, idempotently. Called at app
  startup so it holds however the app is served."""
  access_logger = logging.getLogger("uvicorn.access")
  if not any(
    isinstance(f, UvicornAccessRedactionFilter) for f in access_logger.filters
  ):
    access_logger.addFilter(UvicornAccessRedactionFilter())


def get_safe_url_for_logging(request: Request) -> str:
  path = request.url.path
  if request.url.query:
    safe_query = redact_sensitive_query_params(str(request.url.query))
    if safe_query:
      return f"{path}?{safe_query}"
  return path


# `/v1/graphs/<segment>` routes whose segment is not a graph id.
_GRAPH_COLLECTION_SEGMENTS = frozenset({"capacity", "extensions", "schema", "tiers"})


class StructuredLoggingMiddleware(BaseHTTPMiddleware):
  """Logs every API request with timing, caller and a request id."""

  def __init__(self, app, exclude_paths: list | None = None):
    super().__init__(app)
    self.exclude_paths = exclude_paths or [
      "/health",
      "/status",
      "/metrics",
      "/favicon.ico",
      "/docs",
      "/redoc",
      "/openapi.json",
    ]

  async def dispatch(self, request: Request, call_next: Callable) -> Response:
    if any(request.url.path.startswith(path) for path in self.exclude_paths):
      return await call_next(request)

    # Also bound to the request context so audit events below the route carry it.
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    request_id_token = bind_request_id(request_id)

    entity_id = getattr(request.state, "entity_id", None)

    path_parts = request.url.path.strip("/").split("/")
    if (
      len(path_parts) >= 3
      and path_parts[:2] == ["v1", "graphs"]
      and path_parts[2]
      and path_parts[2] not in _GRAPH_COLLECTION_SEGMENTS
    ):
      entity_id = entity_id or path_parts[2]

    start_time = time.time()

    try:
      response = await call_next(request)
      duration_ms = (time.time() - start_time) * 1000

      # Set by the auth dependency inside the route, so only readable now.
      user_id = getattr(request.state, "user_id", None)

      log_api(
        method=request.method,
        path=request.url.path,
        status_code=response.status_code,
        duration_ms=duration_ms,
        user_id=str(user_id) if user_id else None,
        entity_id=entity_id,
        request_id=request_id,
      )

      response.headers["X-Request-ID"] = request_id

      return response

    except Exception as e:
      duration_ms = (time.time() - start_time) * 1000
      user_id = getattr(request.state, "user_id", None)

      error_category = "application"
      if isinstance(e, PermissionError):
        error_category = "authorization"
      elif "database" in str(e).lower() or "connection" in str(e).lower():
        error_category = "database"
      elif "timeout" in str(e).lower():
        error_category = "timeout"
      elif "validation" in str(e).lower():
        error_category = "validation"

      log_app_error(
        error=e,
        component="api_middleware",
        action="request_processing",
        error_category=error_category,
        user_id=str(user_id) if user_id else None,
        entity_id=entity_id,
        metadata={
          "method": request.method,
          "path": request.url.path,
          "duration_ms": duration_ms,
          "request_id": request_id,
        },
      )

      raise
    finally:
      reset_request_id(request_id_token)


class SecurityLoggingMiddleware(BaseHTTPMiddleware):
  """Logs auth attempts, authorization failures, admin actions and
  suspicious requests."""

  def __init__(self, app):
    super().__init__(app)

  async def dispatch(self, request: Request, call_next: Callable) -> Response:
    client_ip = request.client.host if request.client else "unknown"
    user_agent = request.headers.get("user-agent", "unknown")

    suspicious_indicators = [
      len(request.url.path) > 500,
      "../" in request.url.path,
      "script" in request.url.path.lower(),
      "union" in str(request.url.query).lower(),
    ]

    if any(suspicious_indicators):
      safe_url = get_safe_url_for_logging(request)
      safe_query = redact_sensitive_query_params(str(request.url.query))

      security_logger.warning(
        f"Suspicious request detected from {client_ip}",
        extra={
          "component": "security",
          "action": "suspicious_request",
          "ip_address": client_ip,
          "user_agent": user_agent,
          "path": request.url.path,
          "query": safe_query,
          "method": request.method,
          "safe_url": safe_url,
          "success": False,
          "metadata": {
            "indicators": [i for i, check in enumerate(suspicious_indicators) if check],
            "indicator_count": sum(suspicious_indicators),
          },
        },
      )

    response = await call_next(request)

    if request.url.path.startswith("/v1/auth/"):
      action = request.url.path.split("/")[-1]  # login, register, etc.
      success = 200 <= response.status_code < 300

      user_id = getattr(request.state, "user_id", None)

      log_auth_event(
        event_type=f"auth_{action}",
        user_id=str(user_id) if user_id else None,
        ip_address=client_ip,
        success=success,
        metadata={
          "user_agent": user_agent,
          "status_code": response.status_code,
          "method": request.method,
          "path": request.url.path,
        },
      )

    # `admin_key_id` is set only once the admin key verifies; failures are
    # already recorded by log_admin_auth_failure.
    admin_key_id = getattr(request.state, "admin_key_id", None)
    if admin_key_id and request.url.path.startswith("/admin/v1/"):
      SecurityAuditLogger.log_admin_action(
        admin_key_id=str(admin_key_id),
        method=request.method,
        endpoint=request.url.path,
        status_code=response.status_code,
        ip_address=client_ip,
        user_agent=user_agent,
        query=redact_sensitive_query_params(str(request.url.query)) or None,
      )

    if response.status_code == 403:
      user_id = getattr(request.state, "user_id", None)

      log_auth_event(
        event_type="authorization_failed",
        user_id=str(user_id) if user_id else None,
        ip_address=client_ip,
        success=False,
        metadata={
          "method": request.method,
          "path": request.url.path,
          "user_agent": user_agent,
          "status_code": response.status_code,
        },
      )

    return response
