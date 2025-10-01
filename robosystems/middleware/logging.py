"""
Logging middleware for structured API request logging.

This middleware captures all API requests and responses with structured
logging that's optimized for CloudWatch searching and cost management.
"""

import time
import uuid
from typing import Callable, Optional
from urllib.parse import parse_qsl, urlencode

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from robosystems.logger import (
  api_logger,
  security_logger,
  log_api,
  log_app_error,
  log_auth_event,
)

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
  """
  Redact sensitive query parameters from a query string for safe logging.

  Args:
      query_string: The raw query string from a URL

  Returns:
      Query string with sensitive values replaced with REDACTED
  """
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


def get_safe_url_for_logging(request: Request) -> str:
  """
  Get a URL that's safe for logging (with sensitive query params redacted).

  Args:
      request: FastAPI request object

  Returns:
      URL string safe for logging
  """
  path = request.url.path
  if request.url.query:
    safe_query = redact_sensitive_query_params(str(request.url.query))
    if safe_query:
      return f"{path}?{safe_query}"
  return path


class StructuredLoggingMiddleware(BaseHTTPMiddleware):
  """
  Middleware that logs all API requests with structured data for CloudWatch.

  Features:
  - Request/response timing
  - User and entity context extraction
  - Request ID generation for tracing
  - Error categorization
  - Cost-optimized log levels
  """

  def __init__(self, app, exclude_paths: Optional[list] = None):
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

    # Generate request ID for tracing
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id

    # Extract user context if available
    user_id = getattr(request.state, "user_id", None)
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

      # Add request ID to response headers for tracing
      response.headers["X-Request-ID"] = request_id

      return response

    except Exception as e:
      duration_ms = (time.time() - start_time) * 1000

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


class SecurityLoggingMiddleware(BaseHTTPMiddleware):
  """
  Middleware specifically for security event logging.

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
      # Get safe URL with redacted sensitive parameters
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

      # Extract user ID if available from request state
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
