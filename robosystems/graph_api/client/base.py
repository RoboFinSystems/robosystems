"""
Base Graph API Client.

Shared functionality for sync and async clients.
"""

import random
import time
from typing import Any, TypeVar
from urllib.parse import urljoin

from robosystems.logger import logger

from .config import GraphClientConfig
from .exceptions import (
  GraphAPIError,
  GraphClientError,
  GraphServerError,
  GraphTransientError,
)

T = TypeVar("T")


class BaseGraphClient:
  """Base class for Graph API clients with shared functionality."""

  def __init__(
    self,
    base_url: str | None = None,
    config: GraphClientConfig | None = None,
    **kwargs,
  ):
    """Initialize the client; ``kwargs`` override individual config fields.

    The API key comes from ``kwargs["api_key"]`` or ``GRAPH_API_KEY`` and is
    sent as the ``X-Graph-API-Key`` header on every request.
    """
    self.config = config or GraphClientConfig.from_env()

    if base_url:
      self.config.base_url = base_url.rstrip("/")

    from robosystems.config import env

    api_key = kwargs.pop("api_key", None) or env.GRAPH_API_KEY
    if api_key:
      if "headers" not in kwargs:
        kwargs["headers"] = {}
      kwargs["headers"]["X-Graph-API-Key"] = api_key
      logger.debug("GraphClient configured with API key")
    else:
      if env.ENVIRONMENT in ("prod", "production", "staging"):
        logger.warning("GraphClient initialized without API key")
      else:
        logger.debug("GraphClient initialized without API key (development mode)")

    if kwargs:
      self.config = self.config.with_overrides(**kwargs)

    if not self.config.base_url:
      raise ValueError("base_url must be provided or set in environment")

    self._circuit_breaker_failures = 0
    self._circuit_breaker_last_failure = 0
    self._circuit_breaker_open = False

    # Default graph for methods that do not take one; set by the factory.
    self.graph_id: str | None = None

  def _build_url(self, path: str) -> str:
    """Build full URL from base and path."""
    if path.startswith("/"):
      path = path[1:]
    return urljoin(self.config.base_url + "/", path)

  def _should_retry(self, error: Exception, attempt: int) -> bool:
    """Decide whether to retry after ``error`` on 0-based ``attempt``.

    Retries transient and server errors. A malformed query fails identically
    every time, so ``GraphSyntaxError`` never retries even though it can
    arrive as a 500; client errors and unrecognized exceptions do not retry
    either.
    """
    if attempt >= self.config.max_retries:
      return False

    from .exceptions import GraphSyntaxError

    if isinstance(error, GraphSyntaxError):
      return False

    if isinstance(error, GraphTransientError):
      return True

    if isinstance(error, GraphServerError):
      return True

    if isinstance(error, GraphClientError):
      return False

    return False

  def _calculate_retry_delay(self, attempt: int) -> float:
    """Seconds to wait before the next attempt: exponential backoff plus jitter.

    The jitter (up to 10% of the delay) keeps concurrent clients from
    retrying in lockstep against an instance that just came back.
    """
    delay = self.config.retry_delay * (self.config.retry_backoff**attempt)
    jitter = random.uniform(0, delay * 0.1)
    return delay + jitter

  def _check_circuit_breaker(self) -> None:
    """Raise ``GraphTransientError`` while the breaker is open.

    Resets once ``circuit_breaker_timeout`` has elapsed since the last
    failure, so the next call probes the instance again.
    """
    if not self._circuit_breaker_open:
      return

    time_since_failure = time.time() - self._circuit_breaker_last_failure
    if time_since_failure > self.config.circuit_breaker_timeout:
      self._circuit_breaker_open = False
      self._circuit_breaker_failures = 0
      logger.info("Circuit breaker reset")
    else:
      raise GraphTransientError(
        f"Circuit breaker open. Retry after {self.config.circuit_breaker_timeout - time_since_failure:.0f}s"
      )

  def _record_failure(self) -> None:
    """Record a failure for circuit breaker."""
    self._circuit_breaker_failures += 1
    self._circuit_breaker_last_failure = time.time()

    if self._circuit_breaker_failures >= self.config.circuit_breaker_threshold:
      self._circuit_breaker_open = True
      logger.warning(
        f"Circuit breaker opened after {self._circuit_breaker_failures} failures"
      )

  def _record_success(self) -> None:
    """Record a success for circuit breaker."""
    self._circuit_breaker_failures = 0
    self._circuit_breaker_open = False

  def _handle_response_error(
    self, status_code: int, response_data: dict[str, Any] | None = None
  ) -> GraphAPIError:
    """Map an HTTP status to the matching ``GraphAPIError`` subclass.

    502/503/504 become ``GraphTransientError`` (retryable), 4xx become
    ``GraphClientError``, other 5xx become ``GraphServerError``. Query syntax
    and schema errors are detected by message pattern first, because
    LadybugDB surfaces them as 422 *or* 500 and they must never be retried.
    """
    error_message = "API request failed"
    if response_data and isinstance(response_data, dict):
      error_message = response_data.get("detail", error_message)

    if error_message and (status_code == 422 or status_code == 500):
      syntax_error_patterns = [
        "Parser exception",
        "Binder exception",
        "Invalid input",
        "Cannot find property",
        "Table does not exist",
        "does not exist",
        "Query execution failed: Parser",
        "Query execution failed: Binder",
      ]

      if any(pattern in error_message for pattern in syntax_error_patterns):
        from .exceptions import GraphSyntaxError

        return GraphSyntaxError(error_message, status_code, response_data)

    if status_code in (502, 503, 504):
      return GraphTransientError(error_message, status_code, response_data)
    elif status_code in (400, 401, 403, 404, 422):
      return GraphClientError(error_message, status_code, response_data)
    elif status_code >= 500:
      return GraphServerError(error_message, status_code, response_data)
    else:
      return GraphAPIError(error_message, status_code, response_data)
