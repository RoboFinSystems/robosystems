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
  """Retryable: network timeouts, 502/503/504."""

  pass


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
