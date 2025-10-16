"""Tests for base Kuzu API client."""

import time
from unittest.mock import patch
import pytest

from robosystems.graph_api.client.base import BaseKuzuClient
from robosystems.graph_api.client.config import KuzuClientConfig
from robosystems.graph_api.client.exceptions import (
  KuzuAPIError,
  KuzuTransientError,
  KuzuClientError,
  KuzuServerError,
  KuzuSyntaxError,
)


class TestBaseKuzuClient:
  """Test cases for BaseKuzuClient."""

  def test_initialization_with_base_url(self):
    """Test client initialization with base URL."""
    with patch("robosystems.config.env") as mock_env:
      mock_env.GRAPH_API_KEY = None
      mock_env.ENVIRONMENT = "dev"

      client = BaseKuzuClient(base_url="http://localhost:8001")

      assert client.config.base_url == "http://localhost:8001"
      assert client.graph_id is None
      assert client._circuit_breaker_open is False

  def test_initialization_with_config(self):
    """Test client initialization with custom config."""
    config = KuzuClientConfig(
      base_url="http://custom.example.com", timeout=60, max_retries=5
    )

    with patch("robosystems.config.env") as mock_env:
      mock_env.GRAPH_API_KEY = None
      mock_env.ENVIRONMENT = "dev"

      client = BaseKuzuClient(config=config)

      assert client.config.base_url == "http://custom.example.com"
      assert client.config.timeout == 60
      assert client.config.max_retries == 5

  def test_initialization_with_api_key(self):
    """Test client initialization with API key."""
    with patch("robosystems.config.env") as mock_env:
      mock_env.GRAPH_API_KEY = None
      mock_env.ENVIRONMENT = "dev"

      client = BaseKuzuClient(
        base_url="http://localhost:8001", api_key="test-api-key-123"
      )

      assert "X-Kuzu-API-Key" in client.config.headers
      assert client.config.headers["X-Kuzu-API-Key"] == "test-api-key-123"

  def test_initialization_with_env_api_key(self):
    """Test client initialization with API key from environment."""
    with patch("robosystems.config.env") as mock_env:
      mock_env.GRAPH_API_KEY = "env-api-key-456"
      mock_env.ENVIRONMENT = "prod"

      client = BaseKuzuClient(base_url="http://localhost:8001")

      assert "X-Kuzu-API-Key" in client.config.headers
      assert client.config.headers["X-Kuzu-API-Key"] == "env-api-key-456"

  def test_initialization_no_base_url_error(self):
    """Test that initialization fails without base URL."""
    config = KuzuClientConfig()  # No base_url

    with patch("robosystems.config.env") as mock_env:
      mock_env.GRAPH_API_KEY = None
      mock_env.ENVIRONMENT = "dev"

      with pytest.raises(ValueError, match="base_url must be provided"):
        BaseKuzuClient(config=config)

  def test_initialization_strips_trailing_slash(self):
    """Test that base URL trailing slash is stripped."""
    with patch("robosystems.config.env") as mock_env:
      mock_env.GRAPH_API_KEY = None
      mock_env.ENVIRONMENT = "dev"

      client = BaseKuzuClient(base_url="http://localhost:8001/")

      assert client.config.base_url == "http://localhost:8001"

  def test_build_url(self):
    """Test URL building."""
    with patch("robosystems.config.env") as mock_env:
      mock_env.GRAPH_API_KEY = None
      mock_env.ENVIRONMENT = "dev"

      client = BaseKuzuClient(base_url="http://localhost:8001")

      # Test with leading slash
      assert client._build_url("/databases") == "http://localhost:8001/databases"

      # Test without leading slash
      assert client._build_url("databases") == "http://localhost:8001/databases"

      # Test with nested path
      assert (
        client._build_url("/databases/test/query")
        == "http://localhost:8001/databases/test/query"
      )

  def test_should_retry_transient_errors(self):
    """Test retry logic for transient errors."""
    with patch("robosystems.config.env") as mock_env:
      mock_env.GRAPH_API_KEY = None
      mock_env.ENVIRONMENT = "dev"

      client = BaseKuzuClient(base_url="http://localhost:8001")

      # Transient errors should be retried
      transient_error = KuzuTransientError("Network issue")
      assert client._should_retry(transient_error, 0) is True
      assert client._should_retry(transient_error, 1) is True
      assert client._should_retry(transient_error, 2) is True
      # But not after max retries
      assert client._should_retry(transient_error, 3) is False

  def test_should_not_retry_syntax_errors(self):
    """Test that syntax errors are never retried."""
    with patch("robosystems.config.env") as mock_env:
      mock_env.GRAPH_API_KEY = None
      mock_env.ENVIRONMENT = "dev"

      client = BaseKuzuClient(base_url="http://localhost:8001")

      syntax_error = KuzuSyntaxError("Invalid query")
      # Should never retry, even on first attempt
      assert client._should_retry(syntax_error, 0) is False
      assert client._should_retry(syntax_error, 1) is False

  def test_should_not_retry_client_errors(self):
    """Test that client errors are not retried."""
    with patch("robosystems.config.env") as mock_env:
      mock_env.GRAPH_API_KEY = None
      mock_env.ENVIRONMENT = "dev"

      client = BaseKuzuClient(base_url="http://localhost:8001")

      client_error = KuzuClientError("Bad request")
      assert client._should_retry(client_error, 0) is False

  def test_should_retry_server_errors(self):
    """Test that server errors are retried."""
    with patch("robosystems.config.env") as mock_env:
      mock_env.GRAPH_API_KEY = None
      mock_env.ENVIRONMENT = "dev"

      client = BaseKuzuClient(base_url="http://localhost:8001")

      server_error = KuzuServerError("Internal error")
      assert client._should_retry(server_error, 0) is True
      assert client._should_retry(server_error, 1) is True

  def test_calculate_retry_delay(self):
    """Test retry delay calculation with exponential backoff."""
    config = KuzuClientConfig(
      base_url="http://localhost:8001", retry_delay=1.0, retry_backoff=2.0
    )

    with patch("robosystems.config.env") as mock_env:
      mock_env.GRAPH_API_KEY = None
      mock_env.ENVIRONMENT = "dev"

      client = BaseKuzuClient(config=config)

      # Test exponential backoff
      delay0 = client._calculate_retry_delay(0)
      assert 1.0 <= delay0 <= 1.1  # Base delay + jitter

      delay1 = client._calculate_retry_delay(1)
      assert 2.0 <= delay1 <= 2.2  # 1 * 2^1 + jitter

      delay2 = client._calculate_retry_delay(2)
      assert 4.0 <= delay2 <= 4.4  # 1 * 2^2 + jitter

  def test_circuit_breaker_opens_after_threshold(self):
    """Test that circuit breaker opens after failure threshold."""
    config = KuzuClientConfig(
      base_url="http://localhost:8001", circuit_breaker_threshold=3
    )

    with patch("robosystems.config.env") as mock_env:
      mock_env.GRAPH_API_KEY = None
      mock_env.ENVIRONMENT = "dev"

      client = BaseKuzuClient(config=config)

      # Record failures
      client._record_failure()
      assert client._circuit_breaker_open is False

      client._record_failure()
      assert client._circuit_breaker_open is False

      client._record_failure()  # Third failure
      assert client._circuit_breaker_open is True

  def test_circuit_breaker_check_when_open(self):
    """Test circuit breaker check when open."""
    config = KuzuClientConfig(
      base_url="http://localhost:8001", circuit_breaker_timeout=60
    )

    with patch("robosystems.config.env") as mock_env:
      mock_env.GRAPH_API_KEY = None
      mock_env.ENVIRONMENT = "dev"

      client = BaseKuzuClient(config=config)

      # Open circuit breaker
      client._circuit_breaker_open = True
      client._circuit_breaker_last_failure = time.time()

      # Should raise error when open
      with pytest.raises(KuzuTransientError, match="Circuit breaker open"):
        client._check_circuit_breaker()

  def test_circuit_breaker_resets_after_timeout(self):
    """Test that circuit breaker resets after timeout."""
    config = KuzuClientConfig(
      base_url="http://localhost:8001",
      circuit_breaker_timeout=0.1,  # 100ms for testing
    )

    with patch("robosystems.config.env") as mock_env:
      mock_env.GRAPH_API_KEY = None
      mock_env.ENVIRONMENT = "dev"

      client = BaseKuzuClient(config=config)

      # Open circuit breaker
      client._circuit_breaker_open = True
      client._circuit_breaker_failures = 5
      client._circuit_breaker_last_failure = time.time() - 0.2  # 200ms ago

      # Should reset after timeout
      client._check_circuit_breaker()
      assert client._circuit_breaker_open is False
      assert client._circuit_breaker_failures == 0

  def test_record_success_resets_circuit_breaker(self):
    """Test that success resets circuit breaker."""
    with patch("robosystems.config.env") as mock_env:
      mock_env.GRAPH_API_KEY = None
      mock_env.ENVIRONMENT = "dev"

      client = BaseKuzuClient(base_url="http://localhost:8001")

      # Set some failures
      client._circuit_breaker_failures = 2
      client._circuit_breaker_open = True

      # Record success should reset
      client._record_success()
      assert client._circuit_breaker_failures == 0
      assert client._circuit_breaker_open is False

  def test_handle_response_error_client_errors(self):
    """Test handling of client error responses."""
    with patch("robosystems.config.env") as mock_env:
      mock_env.GRAPH_API_KEY = None
      mock_env.ENVIRONMENT = "dev"

      client = BaseKuzuClient(base_url="http://localhost:8001")

      # 400 Bad Request
      error = client._handle_response_error(400, {"detail": "Invalid request"})
      assert isinstance(error, KuzuClientError)
      assert error.status_code == 400
      assert str(error) == "Invalid request"

      # 404 Not Found
      error = client._handle_response_error(404, {"detail": "Not found"})
      assert isinstance(error, KuzuClientError)
      assert error.status_code == 404

  def test_handle_response_error_server_errors(self):
    """Test handling of server error responses."""
    with patch("robosystems.config.env") as mock_env:
      mock_env.GRAPH_API_KEY = None
      mock_env.ENVIRONMENT = "dev"

      client = BaseKuzuClient(base_url="http://localhost:8001")

      # 500 Internal Server Error
      error = client._handle_response_error(500, {"detail": "Server error"})
      # Note: The actual implementation may classify 500 errors differently
      # based on the error message
      assert isinstance(error, KuzuAPIError)
      assert error.status_code == 500

      # 503 Service Unavailable
      error = client._handle_response_error(503, {"detail": "Service unavailable"})
      assert isinstance(error, KuzuTransientError)
      assert error.status_code == 503

  def test_initialization_warns_missing_api_key_in_prod(self):
    """Test that missing API key warns in production."""
    with patch("robosystems.config.env") as mock_env:
      mock_env.GRAPH_API_KEY = None
      mock_env.ENVIRONMENT = "prod"

      with patch("robosystems.graph_api.client.base.logger") as mock_logger:
        BaseKuzuClient(base_url="http://localhost:8001")
        mock_logger.warning.assert_called_with("KuzuClient initialized without API key")

  def test_initialization_debug_missing_api_key_in_dev(self):
    """Test that missing API key only debugs in development."""
    with patch("robosystems.config.env") as mock_env:
      mock_env.GRAPH_API_KEY = None
      mock_env.ENVIRONMENT = "dev"

      with patch("robosystems.graph_api.client.base.logger") as mock_logger:
        BaseKuzuClient(base_url="http://localhost:8001")
        mock_logger.debug.assert_called_with(
          "KuzuClient initialized without API key (development mode)"
        )
        mock_logger.warning.assert_not_called()
