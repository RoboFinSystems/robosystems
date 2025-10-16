"""
Base Kuzu API Client.

Shared functionality for sync and async clients.
"""

import time
import random
from typing import Optional, Dict, Any, TypeVar
from urllib.parse import urljoin

from robosystems.logger import logger
from .config import KuzuClientConfig
from .exceptions import (
  KuzuAPIError,
  KuzuTransientError,
  KuzuClientError,
  KuzuServerError,
)

T = TypeVar("T")


class BaseKuzuClient:
  """Base class for Kuzu API clients with shared functionality."""

  def __init__(
    self,
    base_url: Optional[str] = None,
    config: Optional[KuzuClientConfig] = None,
    **kwargs,
  ):
    """
    Initialize the client.

    Args:
        base_url: Base URL for the API
        config: Client configuration
        **kwargs: Additional config overrides
    """
    # Use provided config or create from environment
    self.config = config or KuzuClientConfig.from_env()

    # Override with provided values
    if base_url:
      self.config.base_url = base_url.rstrip("/")

    # Handle API key authentication
    # Use centralized config to get from Secrets Manager
    from robosystems.config import env

    api_key = kwargs.pop("api_key", None) or env.GRAPH_API_KEY
    if api_key:
      if "headers" not in kwargs:
        kwargs["headers"] = {}
      kwargs["headers"]["X-Kuzu-API-Key"] = api_key
      logger.debug("KuzuClient configured with API key")
    else:
      # Only warn about missing API key in production environments
      if env.ENVIRONMENT in ("prod", "production", "staging"):
        logger.warning("KuzuClient initialized without API key")
      else:
        logger.debug("KuzuClient initialized without API key (development mode)")

    if kwargs:
      self.config = self.config.with_overrides(**kwargs)

    # Validate configuration
    if not self.config.base_url:
      raise ValueError("base_url must be provided or set in environment")

    # Circuit breaker state
    self._circuit_breaker_failures = 0
    self._circuit_breaker_last_failure = 0
    self._circuit_breaker_open = False

    # Graph ID for operations (can be set for compatibility)
    self.graph_id: Optional[str] = None

  def _build_url(self, path: str) -> str:
    """Build full URL from base and path."""
    if path.startswith("/"):
      path = path[1:]
    return urljoin(self.config.base_url + "/", path)

  def _should_retry(self, error: Exception, attempt: int) -> bool:
    """
    Determine if request should be retried.

    Args:
        error: The exception that occurred
        attempt: Current attempt number (0-based)

    Returns:
        True if should retry, False otherwise
    """
    if attempt >= self.config.max_retries:
      return False

    # Import here to avoid circular imports
    from .exceptions import KuzuSyntaxError

    # Syntax errors should NEVER be retried - fail fast
    if isinstance(error, KuzuSyntaxError):
      return False

    # Check if error is retriable
    if isinstance(error, KuzuTransientError):
      return True

    if isinstance(error, KuzuServerError):
      # 500 errors might be retriable
      return True

    if isinstance(error, KuzuClientError):
      # Client errors are not retriable
      return False

    # Unknown errors - don't retry
    return False

  def _calculate_retry_delay(self, attempt: int) -> float:
    """
    Calculate delay before retry using exponential backoff with jitter.

    Args:
        attempt: Current attempt number (0-based)

    Returns:
        Delay in seconds
    """
    delay = self.config.retry_delay * (self.config.retry_backoff**attempt)
    # Add jitter to prevent thundering herd
    jitter = random.uniform(0, delay * 0.1)
    return delay + jitter

  def _check_circuit_breaker(self) -> None:
    """
    Check if circuit breaker is open.

    Raises:
        KuzuTransientError: If circuit breaker is open
    """
    if not self._circuit_breaker_open:
      return

    # Check if timeout has passed
    time_since_failure = time.time() - self._circuit_breaker_last_failure
    if time_since_failure > self.config.circuit_breaker_timeout:
      # Reset circuit breaker
      self._circuit_breaker_open = False
      self._circuit_breaker_failures = 0
      logger.info("Circuit breaker reset")
    else:
      raise KuzuTransientError(
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
    self, status_code: int, response_data: Optional[Dict[str, Any]] = None
  ) -> KuzuAPIError:
    """
    Convert HTTP status code to appropriate exception.

    Args:
        status_code: HTTP status code
        response_data: Response body data

    Returns:
        Appropriate KuzuAPIError subclass
    """
    error_message = "API request failed"
    if response_data and isinstance(response_data, dict):
      error_message = response_data.get("detail", error_message)

    # Check for specific syntax/schema errors that should never be retried
    # These can come as 422 (validation) or 500 (execution) errors
    if error_message and (status_code == 422 or status_code == 500):
      # These are permanent errors that will never succeed on retry
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
        from .exceptions import KuzuSyntaxError

        return KuzuSyntaxError(error_message, status_code, response_data)

    if status_code in (502, 503, 504):
      return KuzuTransientError(error_message, status_code, response_data)
    elif status_code in (400, 401, 403, 404, 422):
      return KuzuClientError(error_message, status_code, response_data)
    elif status_code >= 500:
      return KuzuServerError(error_message, status_code, response_data)
    else:
      return KuzuAPIError(error_message, status_code, response_data)
