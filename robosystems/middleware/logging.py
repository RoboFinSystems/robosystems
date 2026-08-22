"""Logging middleware for structured API request logging.

This middleware captures all API requests and responses with structured
logging that's optimized for CloudWatch searching and cost management.
"""

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

# Sensitive query parameters that should always be redacted in logs
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
    # If parsing fails, return empty string to avoid exposing raw query
    return ""


class UvicornAccessRedactionFilter(logging.Filter):
  """Redact sensitive query parameters from Uvicorn's own access log.

  Everything else in this module redacts *application* logging, which is not
  the whole surface: the API runs with ``--access-log``, and Uvicorn writes
  its own line straight from the ASGI scope —

      '%s - "%s %s HTTP/%s" %d' % (client, method, path_with_query, ver, code)

  — where ``path_with_query`` is the raw query string. Nothing in the app can
  reach that record, so a secret in a URL lands in the access log intact no
  matter how carefully the app logs.

  That matters because the MCP connector auth deliberately accepts a
  graph-scoped key as a ``token`` query parameter: claude.ai and Claude
  Desktop custom connectors cannot send custom headers, so the URL is the only
  place the credential can ride. Redacting it here keeps the access log — the
  alternative was turning ``--access-log`` off and losing it.

  Installed on the ``uvicorn.access`` logger by ``install_uvicorn_log_redaction``.
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
  """Attach the access-log redaction filter, idempotently.

  Called from application startup rather than the entrypoint so it holds for
  every way the app is served — the container's `uvicorn` invocation, a local
  `uv run uvicorn`, and the test client alike.
  """
  access_logger = logging.getLogger("uvicorn.access")
  if not any(
    isinstance(f, UvicornAccessRedactionFilter) for f in access_logger.filters
  ):
    access_logger.addFilter(UvicornAccessRedactionFilter())


def get_safe_url_for_logging(request: Request) -> str:
  """Get a URL that's safe for logging (with sensitive query params redacted)."""
  path = request.url.path
  if request.url.query:
    safe_query = redact_sensitive_query_params(str(request.url.query))
    if safe_query:
      return f"{path}?{safe_query}"
  return path


class StructuredLoggingMiddleware(BaseHTTPMiddleware):
  """Middleware that logs all API requests with structured data for CloudWatch.

  Features:
  - Request/response timing
  - User and entity context extraction
  - Request ID generation for tracing
  - Error categorization
  - Cost-optimized log levels
  """

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
    # Skip logging for health checks and static assets
    if any(request.url.path.startswith(path) for path in self.exclude_paths):
      return await call_next(request)

    # Generate request ID for tracing. Bound to the request context as well
    # as request.state so the audit and security events written below the
    # route handler carry it (`security.request_context`).
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    request_id_token = bind_request_id(request_id)

    entity_id = getattr(request.state, "entity_id", None)

    # Extract entity from path if it's a entity-scoped endpoint
    path_parts = request.url.path.strip("/").split("/")
    if len(path_parts) >= 2 and path_parts[0] == "v1":
      # Check if second part looks like a entity ID
      potential_entity = path_parts[1]
      if potential_entity and potential_entity not in [
        "auth",
        "user",
        "status",
        "tasks",
        "create",
      ]:
        entity_id = entity_id or potential_entity

    start_time = time.time()

    try:
      response = await call_next(request)
      duration_ms = (time.time() - start_time) * 1000

      # The caller is resolved by the auth dependency inside the route, so
      # it is only knowable after the response — read it then, or every
      # access-log line records an anonymous request.
      user_id = getattr(request.state, "user_id", None)

      # Log successful requests using structured logging
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

      # Categorize errors for better searching
      error_category = "application"
      if isinstance(e, PermissionError):
        error_category = "authorization"
      elif "database" in str(e).lower() or "connection" in str(e).lower():
        error_category = "database"
      elif "timeout" in str(e).lower():
        error_category = "timeout"
      elif "validation" in str(e).lower():
        error_category = "validation"

      # Log error with context using structured logging
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

      # Re-raise the exception to be handled by FastAPI
      raise
    finally:
      reset_request_id(request_id_token)


class SecurityLoggingMiddleware(BaseHTTPMiddleware):
  """Middleware specifically for security event logging.

  Logs authentication attempts, authorization failures, and suspicious activity.
  """

  def __init__(self, app):
    super().__init__(app)

  async def dispatch(self, request: Request, call_next: Callable) -> Response:
    # Extract client information
    client_ip = request.client.host if request.client else "unknown"
    user_agent = request.headers.get("user-agent", "unknown")

    # Check for suspicious patterns
    suspicious_indicators = [
      len(request.url.path) > 500,  # Extremely long paths
      "../" in request.url.path,  # Path traversal attempts
      "script" in request.url.path.lower(),  # Script injection attempts
      "union" in str(request.url.query).lower(),  # SQL injection attempts
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

    # Log authentication events using structured logging
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

    # Record every authenticated admin-surface action. `admin_key_id` is set on
    # request.state by AdminAuthMiddleware once the key verifies, so its absence
    # means authentication never succeeded — those are already recorded (and
    # alarmed) by log_admin_auth_failure, and must not be double-logged here.
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

    # Log authorization failures (403 responses) using structured logging
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
