"""Tests for async Graph API client."""

import time
from unittest.mock import Mock, patch
import pytest
import httpx

from robosystems.graph_api.client.client import GraphClient
from robosystems.graph_api.client.config import GraphClientConfig
from robosystems.graph_api.client.exceptions import (
  GraphTimeoutError,
  GraphTransientError,
  GraphSyntaxError,
)


class TestLadybugClient:
  """Test cases for async GraphClient."""

  @pytest.fixture
  def mock_env(self):
    """Mock environment variables."""
    with patch("robosystems.config.env") as mock:
      mock.GRAPH_API_KEY = None
      mock.ENVIRONMENT = "dev"
      yield mock

  @pytest.mark.asyncio
  async def test_client_initialization(self, mock_env):
    """Test client initialization."""
    client = GraphClient(base_url="http://localhost:8001")

    assert client.config.base_url == "http://localhost:8001"
    assert isinstance(client.client, httpx.AsyncClient)
    assert client._route_target is None
    assert client._graph_id is None

    await client.close()

  @pytest.mark.asyncio
  async def test_client_context_manager(self, mock_env):
    """Test client as async context manager."""
    async with GraphClient(base_url="http://localhost:8001") as client:
      assert isinstance(client, GraphClient)
      assert isinstance(client.client, httpx.AsyncClient)

    # Client should be closed after context
    assert client.client.is_closed

  @pytest.mark.asyncio
  async def test_client_with_custom_config(self, mock_env):
    """Test client with custom configuration."""
    config = GraphClientConfig(
      base_url="http://custom.example.com",
      timeout=60,
      max_connections=200,
      headers={"X-Custom": "value"},
    )

    client = GraphClient(config=config)

    assert client.config.timeout == 60
    assert client.config.max_connections == 200
    assert "X-Custom" in client.config.headers

    await client.close()

  @pytest.mark.asyncio
  async def test_execute_with_retry_success(self, mock_env):
    """Test successful execution without retry."""
    client = GraphClient(base_url="http://localhost:8001")

    async def mock_func():
      return {"success": True}

    result = await client._execute_with_retry(mock_func)
    assert result == {"success": True}

    await client.close()

  @pytest.mark.asyncio
  async def test_execute_with_retry_transient_error(self, mock_env):
    """Test retry on transient errors."""
    client = GraphClient(base_url="http://localhost:8001")
    client.config.retry_delay = 0.01  # Fast retry for testing

    attempt_count = 0

    async def mock_func():
      nonlocal attempt_count
      attempt_count += 1
      if attempt_count < 3:
        raise GraphTransientError("Temporary failure")
      return {"success": True}

    result = await client._execute_with_retry(mock_func)
    assert result == {"success": True}
    assert attempt_count == 3

    await client.close()

  @pytest.mark.asyncio
  async def test_execute_with_retry_syntax_error_no_retry(self, mock_env):
    """Test that syntax errors are not retried."""
    client = GraphClient(base_url="http://localhost:8001")

    attempt_count = 0

    async def mock_func():
      nonlocal attempt_count
      attempt_count += 1
      raise GraphSyntaxError("Invalid query syntax")

    with pytest.raises(GraphSyntaxError):
      await client._execute_with_retry(mock_func)

    # Should only try once
    assert attempt_count == 1

    await client.close()

  @pytest.mark.asyncio
  async def test_execute_with_retry_timeout_conversion(self, mock_env):
    """Test conversion of httpx timeout to GraphTimeoutError."""
    client = GraphClient(base_url="http://localhost:8001")

    async def mock_func():
      raise httpx.TimeoutException("Request timed out")

    with pytest.raises(GraphTimeoutError):
      await client._execute_with_retry(mock_func)

    await client.close()

  @pytest.mark.asyncio
  async def test_execute_with_retry_connection_error_conversion(self, mock_env):
    """Test conversion of httpx connection errors."""
    client = GraphClient(base_url="http://localhost:8001")
    client.config.retry_delay = 0.01

    attempt_count = 0

    async def mock_func():
      nonlocal attempt_count
      attempt_count += 1
      if attempt_count == 1:
        raise httpx.ConnectError("Connection refused")
      return {"success": True}

    result = await client._execute_with_retry(mock_func)
    assert result == {"success": True}
    assert attempt_count == 2

    await client.close()

  @pytest.mark.asyncio
  async def test_execute_with_retry_max_attempts(self, mock_env):
    """Test that retry stops after max attempts."""
    config = GraphClientConfig(
      base_url="http://localhost:8001", max_retries=2, retry_delay=0.01
    )
    client = GraphClient(config=config)

    attempt_count = 0

    async def mock_func():
      nonlocal attempt_count
      attempt_count += 1
      raise GraphTransientError("Always fails")

    with pytest.raises(GraphTransientError):
      await client._execute_with_retry(mock_func)

    # Should try max_retries + 1 times
    assert attempt_count == 3

    await client.close()

  @pytest.mark.asyncio
  async def test_execute_with_retry_circuit_breaker(self, mock_env):
    """Test circuit breaker integration."""
    config = GraphClientConfig(
      base_url="http://localhost:8001",
      circuit_breaker_threshold=2,
      circuit_breaker_timeout=60,
    )
    client = GraphClient(config=config)

    # Open circuit breaker
    client._circuit_breaker_open = True
    client._circuit_breaker_last_failure = time.time()  # Use time.time() instead

    async def mock_func():
      return {"success": True}

    with pytest.raises(GraphTransientError, match="Circuit breaker open"):
      await client._execute_with_retry(mock_func)

    await client.close()

  @pytest.mark.asyncio
  async def test_request_method(self, mock_env):
    """Test _request method."""
    client = GraphClient(base_url="http://localhost:8001")

    # Mock the httpx request
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"result": "success"}
    mock_response.raise_for_status = Mock()

    with patch.object(
      client.client, "request", return_value=mock_response
    ) as mock_request:
      response = await client._request(
        "POST", "/test", json_data={"key": "value"}, params={"param": "test"}
      )

      assert response == mock_response
      mock_request.assert_called_once()

    await client.close()

  @pytest.mark.asyncio
  async def test_client_with_metadata(self, mock_env):
    """Test client with routing metadata."""
    client = GraphClient(base_url="http://localhost:8001")

    # Set metadata (usually done by factory)
    client._route_target = "writer-01"
    client._graph_id = "kg123456"
    client._database_name = "test_db"
    client._instance_id = "i-abc123"
    client._purpose = "testing"

    assert client._route_target == "writer-01"
    assert client._graph_id == "kg123456"
    assert client._database_name == "test_db"
    assert client._instance_id == "i-abc123"
    assert client._purpose == "testing"

    await client.close()

  @pytest.mark.asyncio
  async def test_close_client(self, mock_env):
    """Test client closure."""
    client = GraphClient(base_url="http://localhost:8001")

    assert not client.client.is_closed

    await client.close()

    assert client.client.is_closed
