"""Graph API client exception hierarchy."""

from typing import Any


class GraphAPIError(Exception):
  """Base exception for all Graph API errors."""

  def __init__(
    self,
    message: str,
    status_code: int | None = None,
    response_data: dict[str, Any] | None = None,
  ):
    super().__init__(message)
    self.status_code = status_code
    self.response_data = response_data


class GraphTransientError(GraphAPIError):
  """Retryable: network timeouts, 502/503/504.

  ``cause`` is the transport error when the request never got an answer
  (connection refused, dropped mid-response), so a caller can tell a dead
  engine from an admission 503.
  """

  def __init__(
    self,
    message: str,
    status_code: int | None = None,
    response_data: dict[str, Any] | None = None,
    cause: BaseException | None = None,
  ):
    super().__init__(message, status_code, response_data)
    self.cause = cause


class GraphTimeoutError(GraphTransientError):
  """Request timeout errors."""

  pass


class GraphClientError(GraphAPIError):
  """Not retried: 4xx responses."""

  pass


class GraphSyntaxError(GraphClientError):
  """Query syntax or schema errors; never retried, even when sent as a 500."""

  pass


class GraphServerError(GraphAPIError):
  """Other 5xx responses; retried."""

  pass
