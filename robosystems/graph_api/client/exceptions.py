"""
Graph API Client Exceptions.

Defines exception hierarchy for Graph API operations (Kuzu, Neo4j).
"""

from typing import Optional, Dict, Any


class GraphAPIError(Exception):
  """Base exception for all Graph API errors."""

  def __init__(
    self,
    message: str,
    status_code: Optional[int] = None,
    response_data: Optional[Dict[str, Any]] = None,
  ):
    super().__init__(message)
    self.status_code = status_code
    self.response_data = response_data


class GraphTransientError(GraphAPIError):
  """
  Transient errors that can be retried.

  Examples: Network timeouts, 503 Service Unavailable, 502 Bad Gateway
  """

  pass


class GraphTimeoutError(GraphTransientError):
  """Request timeout errors."""

  pass


class GraphClientError(GraphAPIError):
  """
  Client errors that should not be retried.

  Examples: 400 Bad Request, 404 Not Found, 422 Unprocessable Entity
  """

  pass


class GraphSyntaxError(GraphClientError):
  """
  Query syntax/schema errors that should never be retried.

  Examples: Parser exceptions, Binder exceptions, Invalid input, Missing properties/tables
  These errors indicate fundamental issues with the query that will never succeed.
  """

  pass


class GraphServerError(GraphAPIError):
  """
  Server errors that might be retriable.

  Examples: 500 Internal Server Error
  """

  pass
