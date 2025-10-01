"""
Kuzu API Client Exceptions.

Defines exception hierarchy for Kuzu API operations.
"""

from typing import Optional, Dict, Any


class KuzuAPIError(Exception):
  """Base exception for all Kuzu API errors."""

  def __init__(
    self,
    message: str,
    status_code: Optional[int] = None,
    response_data: Optional[Dict[str, Any]] = None,
  ):
    super().__init__(message)
    self.status_code = status_code
    self.response_data = response_data


class KuzuTransientError(KuzuAPIError):
  """
  Transient errors that can be retried.

  Examples: Network timeouts, 503 Service Unavailable, 502 Bad Gateway
  """

  pass


class KuzuTimeoutError(KuzuTransientError):
  """Request timeout errors."""

  pass


class KuzuClientError(KuzuAPIError):
  """
  Client errors that should not be retried.

  Examples: 400 Bad Request, 404 Not Found, 422 Unprocessable Entity
  """

  pass


class KuzuSyntaxError(KuzuClientError):
  """
  Query syntax/schema errors that should never be retried.

  Examples: Parser exceptions, Binder exceptions, Invalid input, Missing properties/tables
  These errors indicate fundamental issues with the query that will never succeed.
  """

  pass


class KuzuServerError(KuzuAPIError):
  """
  Server errors that might be retriable.

  Examples: 500 Internal Server Error
  """

  pass
