"""
Error handling that keeps internals out of client responses.

Full exception detail goes to the logs; the client gets a generic message and
a status code derived from the error's classification, so an error never
reveals database, path, or configuration internals.
"""

import re
from typing import Any, NoReturn

from fastapi import HTTPException, status

from robosystems.logger import logger


class ErrorType:
  """Standard error types for consistent handling."""

  # Client errors (4xx)
  VALIDATION_ERROR = "validation_error"
  AUTHENTICATION_ERROR = "authentication_error"
  AUTHORIZATION_ERROR = "authorization_error"
  NOT_FOUND_ERROR = "not_found_error"
  CONFLICT_ERROR = "conflict_error"
  RATE_LIMIT_ERROR = "rate_limit_error"

  # Server errors (5xx)
  INTERNAL_ERROR = "internal_error"
  SERVICE_UNAVAILABLE = "service_unavailable"
  DATABASE_ERROR = "database_error"
  EXTERNAL_SERVICE_ERROR = "external_service_error"


# Mapping of error types to HTTP status codes and generic messages
ERROR_RESPONSES = {
  ErrorType.VALIDATION_ERROR: {
    "status_code": status.HTTP_400_BAD_REQUEST,
    "detail": "Invalid request data",
  },
  ErrorType.AUTHENTICATION_ERROR: {
    "status_code": status.HTTP_401_UNAUTHORIZED,
    "detail": "Authentication required",
  },
  ErrorType.AUTHORIZATION_ERROR: {
    "status_code": status.HTTP_403_FORBIDDEN,
    "detail": "Access denied",
  },
  ErrorType.NOT_FOUND_ERROR: {
    "status_code": status.HTTP_404_NOT_FOUND,
    "detail": "Resource not found",
  },
  ErrorType.CONFLICT_ERROR: {
    "status_code": status.HTTP_409_CONFLICT,
    "detail": "Resource conflict",
  },
  ErrorType.RATE_LIMIT_ERROR: {
    "status_code": status.HTTP_429_TOO_MANY_REQUESTS,
    "detail": "Rate limit exceeded",
  },
  ErrorType.INTERNAL_ERROR: {
    "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
    "detail": "Internal server error",
  },
  ErrorType.SERVICE_UNAVAILABLE: {
    "status_code": status.HTTP_503_SERVICE_UNAVAILABLE,
    "detail": "Service temporarily unavailable",
  },
  ErrorType.DATABASE_ERROR: {
    "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
    "detail": "Database operation failed",
  },
  ErrorType.EXTERNAL_SERVICE_ERROR: {
    "status_code": status.HTTP_502_BAD_GATEWAY,
    "detail": "External service error",
  },
}


def raise_secure_error(
  error_type: str,
  original_error: Exception | None = None,
  request_id: str | None = None,
  user_id: str | None = None,
  additional_context: dict[str, Any] | None = None,
  custom_detail: str | None = None,
) -> NoReturn:
  """
  Raise an HTTPException with a generic message while logging full details.

  ``error_type`` is a constant from :class:`ErrorType`; an unknown value falls
  back to INTERNAL_ERROR. ``custom_detail`` bypasses the generic message — use
  it sparingly, and only with text that carries no sensitive data.

  Raises:
      HTTPException: With the mapped status code.
  """
  if error_type not in ERROR_RESPONSES:
    logger.warning(f"Unknown error type: {error_type}, defaulting to internal error")
    error_type = ErrorType.INTERNAL_ERROR

  error_config = ERROR_RESPONSES[error_type]

  # Log full error details for debugging
  log_context = {
    "error_type": error_type,
    "request_id": request_id,
    "user_id": user_id,
    "status_code": error_config["status_code"],
  }

  if additional_context:
    log_context.update(additional_context)

  if original_error:
    logger.error(
      f"Secure error handler - {error_type}: {original_error!s}",
      extra=log_context,
      exc_info=True,
    )
  else:
    logger.error(f"Secure error handler - {error_type}", extra=log_context)

  # Use custom detail if provided, otherwise use generic message
  detail = custom_detail if custom_detail else error_config["detail"]

  raise HTTPException(status_code=error_config["status_code"], detail=detail)


def classify_exception(exception: Exception) -> str:
  """Classify an exception into an :class:`ErrorType` constant by message and type."""
  exception_str = str(exception).lower()
  exception_type = type(exception).__name__.lower()

  # Database related errors
  if any(
    keyword in exception_str
    for keyword in ["database", "connection", "sql", "postgres", "ladybug"]
  ):
    return ErrorType.DATABASE_ERROR

  # Authentication/Authorization errors
  if any(
    keyword in exception_str
    for keyword in ["unauthorized", "authentication", "token", "login"]
  ):
    return ErrorType.AUTHENTICATION_ERROR

  if any(
    keyword in exception_str for keyword in ["forbidden", "access denied", "permission"]
  ):
    return ErrorType.AUTHORIZATION_ERROR

  # Validation errors
  if any(keyword in exception_type for keyword in ["validation", "value", "type"]):
    return ErrorType.VALIDATION_ERROR

  # Not found errors
  if any(
    keyword in exception_str for keyword in ["not found", "does not exist", "404"]
  ):
    return ErrorType.NOT_FOUND_ERROR

  # Conflict errors
  if any(
    keyword in exception_str for keyword in ["conflict", "duplicate", "already exists"]
  ):
    return ErrorType.CONFLICT_ERROR

  # Rate limiting
  if any(
    keyword in exception_str for keyword in ["rate limit", "too many", "throttle"]
  ):
    return ErrorType.RATE_LIMIT_ERROR

  # External service errors
  if any(
    keyword in exception_str
    for keyword in [
      "timeout",
      "connection refused",
      "service unavailable",
      "502",
      "503",
    ]
  ):
    return ErrorType.EXTERNAL_SERVICE_ERROR

  # Default to internal error
  return ErrorType.INTERNAL_ERROR


def handle_exception_securely(
  exception: Exception,
  request_id: str | None = None,
  user_id: str | None = None,
  additional_context: dict[str, Any] | None = None,
) -> NoReturn:
  """
  Classify an exception and raise the corresponding sanitized HTTPException.

  Raises:
      HTTPException: With a generic message and the mapped status code.
  """
  error_type = classify_exception(exception)
  raise_secure_error(
    error_type=error_type,
    original_error=exception,
    request_id=request_id,
    user_id=user_id,
    additional_context=additional_context,
  )


def is_safe_to_expose(detail_message: str) -> bool:
  """
  Check whether an error detail message is safe to expose to clients.

  Rejects any message naming credentials, infrastructure, or internals — the
  patterns below are matched as substrings, so this deliberately over-rejects.
  """
  if not detail_message:
    return True

  detail_lower = detail_message.lower()

  # Patterns that should never be exposed
  sensitive_patterns = [
    "password",
    "secret",
    "key",
    "token",
    "credential",
    "api_key",
    "database",
    "connection",
    "internal",
    "sql",
    "traceback",
    "stack trace",
    "file path",
    "directory",
    "host",
    "port",
    "ip address",
    "server",
    "config",
    "environment",
  ]

  return not any(pattern in detail_lower for pattern in sensitive_patterns)


_CONNSTR_PASSWORD_RE = re.compile(r"password=\S+")
_URL_CRED_RE = re.compile(r"(postgres(?:ql)?://[^:/@\s]+:)[^@\s]+@")


def redact_connection_secrets(text: str) -> str:
  """Redact database credentials from an error string, keeping the rest.

  The extensions materializer interpolates a libpq connstr (``password=…``)
  into the ``postgres_scan()`` SQL it ships to the graph_api; a DuckDB error
  that echoes the failing statement would otherwise carry the RDS master
  credential into a tenant-visible operation result (``result.errors``) and the
  logs. This scrubs the secret and leaves the diagnostic text intact — unlike
  :func:`sanitize_error_detail`, which would blank the whole message.
  """
  if not text:
    return text
  text = _CONNSTR_PASSWORD_RE.sub("password=***", text)
  text = _URL_CRED_RE.sub(r"\1***@", text)
  return text


def sanitize_error_detail(detail_message: str) -> str:
  """
  Return a message safe for client consumption.

  Passes the original through when :func:`is_safe_to_expose` accepts it,
  otherwise substitutes a generic message.
  """
  if not detail_message:
    return "An error occurred"

  if is_safe_to_expose(detail_message):
    return detail_message

  # Return generic message for potentially sensitive errors
  return "An error occurred while processing your request"
