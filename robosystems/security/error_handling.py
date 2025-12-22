"""
Secure error handling utilities to prevent information disclosure.

This module provides utilities for handling errors securely by:
- Preventing detailed error messages from being exposed to clients
- Logging full error details for debugging while returning generic messages
- Categorizing errors to provide appropriate status codes without revealing internals
"""

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
  Raise an HTTPException with generic error message while logging full details.

  Args:
      error_type: Type of error from ErrorType class
      original_error: The original exception that caused this error
      request_id: Request ID for tracing
      user_id: User ID associated with the request
      additional_context: Additional context for logging
      custom_detail: Custom detail message (use sparingly and ensure no sensitive data)

  Raises:
      HTTPException: With generic error message and appropriate status code
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
  """
  Classify an exception to determine the appropriate error type.

  Args:
      exception: The exception to classify

  Returns:
      Error type from ErrorType class
  """
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
  Handle an exception securely by classifying it and raising appropriate HTTPException.

  Args:
      exception: The exception to handle
      request_id: Request ID for tracing
      user_id: User ID associated with the request
      additional_context: Additional context for logging

  Raises:
      HTTPException: With generic error message and appropriate status code
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
  Check if an error detail message is safe to expose to clients.

  This function checks for common patterns that might reveal sensitive information.

  Args:
      detail_message: The error detail message to check

  Returns:
      True if safe to expose, False otherwise
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


def sanitize_error_detail(detail_message: str) -> str:
  """
  Sanitize an error detail message to remove sensitive information.

  Args:
      detail_message: The original error detail message

  Returns:
      Sanitized error message safe for client consumption
  """
  if not detail_message:
    return "An error occurred"

  if is_safe_to_expose(detail_message):
    return detail_message

  # Return generic message for potentially sensitive errors
  return "An error occurred while processing your request"
