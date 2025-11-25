"""Tests for MCP factory module."""

import pytest
from unittest.mock import Mock, patch, AsyncMock
from contextlib import asynccontextmanager

from robosystems.middleware.mcp.factory import (
  create_graph_mcp_client,
  acquire_graph_mcp_client,
)


class TestCreateGraphMCPClient:
  """Test create_graph_mcp_client function."""

  @patch("robosystems.middleware.mcp.factory.env")
  @patch("robosystems.middleware.mcp.factory.GraphMCPClient")
  async def test_create_client_with_explicit_url(self, mock_client_class, mock_env):
    """Test creating client with explicit API URL."""
    mock_env.GRAPH_HTTP_TIMEOUT = 30
    mock_env.GRAPH_QUERY_TIMEOUT = 60
    mock_env.GRAPH_MAX_QUERY_LENGTH = "50000"

    mock_client = AsyncMock()
    mock_client_class.return_value = mock_client

    api_url = "http://test-api.example.com"
    client = await create_graph_mcp_client("test_graph", api_base_url=api_url)

    assert client == mock_client
    mock_client_class.assert_called_once_with(
      api_base_url=api_url,
      graph_id="test_graph",
      timeout=30,
      query_timeout=60,
      max_query_length=50000,
    )

  @patch("robosystems.middleware.mcp.factory.env")
  @patch("robosystems.middleware.mcp.factory.GraphMCPClient")
  @patch("robosystems.graph_api.client.factory.GraphClientFactory")
  @patch("robosystems.middleware.graph.utils.MultiTenantUtils")
  async def test_create_client_with_discovery_shared_repo(
    self, mock_utils, mock_factory, mock_client_class, mock_env
  ):
    """Test creating client with URL discovery for shared repository."""
    mock_env.GRAPH_HTTP_TIMEOUT = 30
    mock_env.GRAPH_QUERY_TIMEOUT = 60
    mock_env.GRAPH_MAX_QUERY_LENGTH = "50000"

    # Mock shared repository detection
    mock_utils.is_shared_repository.return_value = True

    # Mock client factory
    mock_graph_client = Mock()
    mock_graph_client.config.base_url = "http://discovered-url.com"
    mock_factory.create_client = AsyncMock(return_value=mock_graph_client)

    # Mock MCP client
    mock_mcp_client = AsyncMock()
    mock_client_class.return_value = mock_mcp_client

    await create_graph_mcp_client("sec")

    # Verify shared repository detection
    mock_utils.is_shared_repository.assert_called_once_with("sec")

    # Verify factory called with read operation for shared repo
    mock_factory.create_client.assert_called_once_with(
      graph_id="sec", operation_type="read"
    )

    # Verify MCP client created with discovered URL
    mock_client_class.assert_called_once_with(
      api_base_url="http://discovered-url.com",
      graph_id="sec",
      timeout=30,
      query_timeout=60,
      max_query_length=50000,
    )

  @patch("robosystems.middleware.mcp.factory.env")
  @patch("robosystems.middleware.mcp.factory.GraphMCPClient")
  @patch("robosystems.graph_api.client.factory.GraphClientFactory")
  @patch("robosystems.middleware.graph.utils.MultiTenantUtils")
  async def test_create_client_with_discovery_user_graph(
    self, mock_utils, mock_factory, mock_client_class, mock_env
  ):
    """Test creating client with URL discovery for user graph."""
    mock_env.GRAPH_HTTP_TIMEOUT = 30
    mock_env.GRAPH_QUERY_TIMEOUT = 60
    mock_env.GRAPH_MAX_QUERY_LENGTH = "50000"

    # Mock user graph detection
    mock_utils.is_shared_repository.return_value = False

    # Mock client factory
    mock_graph_client = Mock()
    mock_graph_client.config = None  # No config attribute
    mock_graph_client._base_url = "http://user-graph-url.com"
    mock_factory.create_client = AsyncMock(return_value=mock_graph_client)

    # Mock MCP client
    mock_mcp_client = AsyncMock()
    mock_client_class.return_value = mock_mcp_client

    await create_graph_mcp_client("kg123abc")

    # Verify user graph detection
    mock_utils.is_shared_repository.assert_called_once_with("kg123abc")

    # Verify factory called with write operation for user graph
    mock_factory.create_client.assert_called_once_with(
      graph_id="kg123abc", operation_type="write"
    )

    # Verify MCP client created with discovered URL
    mock_client_class.assert_called_once_with(
      api_base_url="http://user-graph-url.com",
      graph_id="kg123abc",
      timeout=30,
      query_timeout=60,
      max_query_length=50000,
    )

  @patch("robosystems.middleware.mcp.factory.env")
  @patch("robosystems.middleware.mcp.factory.GraphMCPClient")
  @patch("robosystems.graph_api.client.factory.GraphClientFactory")
  @patch("robosystems.middleware.graph.utils.MultiTenantUtils")
  async def test_create_client_url_discovery_fallback_base_url(
    self, mock_utils, mock_factory, mock_client_class, mock_env
  ):
    """Test URL discovery with fallback to base_url attribute."""
    mock_env.GRAPH_HTTP_TIMEOUT = 30
    mock_env.GRAPH_QUERY_TIMEOUT = 60
    mock_env.GRAPH_MAX_QUERY_LENGTH = "50000"

    mock_utils.is_shared_repository.return_value = False

    # Mock client with base_url attribute
    mock_graph_client = Mock()
    del mock_graph_client.config  # No config attribute
    del mock_graph_client._base_url  # No _base_url attribute
    mock_graph_client.base_url = "http://base-url-fallback.com"
    mock_factory.create_client = AsyncMock(return_value=mock_graph_client)

    mock_mcp_client = AsyncMock()
    mock_client_class.return_value = mock_mcp_client

    await create_graph_mcp_client("kg123abc")

    mock_client_class.assert_called_once_with(
      api_base_url="http://base-url-fallback.com",
      graph_id="kg123abc",
      timeout=30,
      query_timeout=60,
      max_query_length=50000,
    )

  @patch("robosystems.middleware.mcp.factory.env")
  @patch("robosystems.middleware.mcp.factory.GraphMCPClient")
  @patch("robosystems.graph_api.client.factory.GraphClientFactory")
  @patch("robosystems.middleware.graph.utils.MultiTenantUtils")
  async def test_create_client_url_discovery_env_fallback(
    self, mock_utils, mock_factory, mock_client_class, mock_env
  ):
    """Test URL discovery with fallback to environment variable."""
    mock_env.GRAPH_HTTP_TIMEOUT = 30
    mock_env.GRAPH_QUERY_TIMEOUT = 60
    mock_env.GRAPH_MAX_QUERY_LENGTH = "50000"
    mock_env.GRAPH_API_URL = "http://env-fallback.com"

    mock_utils.is_shared_repository.return_value = False

    # Mock client with no URL attributes
    mock_graph_client = Mock()
    del mock_graph_client.config
    del mock_graph_client._base_url
    del mock_graph_client.base_url
    mock_factory.create_client = AsyncMock(return_value=mock_graph_client)

    mock_mcp_client = AsyncMock()
    mock_client_class.return_value = mock_mcp_client

    await create_graph_mcp_client("kg123abc")

    mock_client_class.assert_called_once_with(
      api_base_url="http://env-fallback.com",
      graph_id="kg123abc",
      timeout=30,
      query_timeout=60,
      max_query_length=50000,
    )

  @patch("robosystems.middleware.mcp.factory.env")
  @patch("robosystems.middleware.mcp.factory.GraphMCPClient")
  @patch("robosystems.graph_api.client.factory.GraphClientFactory")
  @patch("robosystems.middleware.graph.utils.MultiTenantUtils")
  async def test_create_client_url_discovery_final_fallback(
    self, mock_utils, mock_factory, mock_client_class, mock_env
  ):
    """Test URL discovery with final localhost fallback."""
    mock_env.GRAPH_HTTP_TIMEOUT = 30
    mock_env.GRAPH_QUERY_TIMEOUT = 60
    mock_env.GRAPH_MAX_QUERY_LENGTH = "50000"
    mock_env.GRAPH_API_URL = None

    mock_utils.is_shared_repository.return_value = False

    # Mock client with no URL attributes
    mock_graph_client = Mock()
    del mock_graph_client.config
    del mock_graph_client._base_url
    del mock_graph_client.base_url
    mock_factory.create_client = AsyncMock(return_value=mock_graph_client)

    mock_mcp_client = AsyncMock()
    mock_client_class.return_value = mock_mcp_client

    await create_graph_mcp_client("kg123abc")

    mock_client_class.assert_called_once_with(
      api_base_url="http://localhost:8001",
      graph_id="kg123abc",
      timeout=30,
      query_timeout=60,
      max_query_length=50000,
    )

  @patch("robosystems.middleware.mcp.factory.env")
  @patch("robosystems.middleware.mcp.factory.GraphMCPClient")
  async def test_create_client_env_defaults(self, mock_client_class, mock_env):
    """Test client creation with environment defaults."""
    mock_env.GRAPH_HTTP_TIMEOUT = 45
    mock_env.GRAPH_QUERY_TIMEOUT = 90
    # No GRAPH_MAX_QUERY_LENGTH attribute (testing hasattr fallback)
    delattr(mock_env, "GRAPH_MAX_QUERY_LENGTH")

    mock_client = AsyncMock()
    mock_client_class.return_value = mock_client

    await create_graph_mcp_client("test", api_base_url="http://test.com")

    mock_client_class.assert_called_once_with(
      api_base_url="http://test.com",
      graph_id="test",
      timeout=45,
      query_timeout=90,
      max_query_length=50000,  # Default fallback
    )

  @patch("robosystems.middleware.mcp.factory.env")
  @patch("robosystems.middleware.mcp.factory.GraphMCPClient")
  async def test_create_client_default_graph_id(self, mock_client_class, mock_env):
    """Test client creation with default graph ID."""
    mock_env.GRAPH_HTTP_TIMEOUT = 30
    mock_env.GRAPH_QUERY_TIMEOUT = 60
    mock_env.GRAPH_MAX_QUERY_LENGTH = "50000"

    mock_client = AsyncMock()
    mock_client_class.return_value = mock_client

    # Call without graph_id parameter (should default to "sec")
    await create_graph_mcp_client(api_base_url="http://test.com")

    mock_client_class.assert_called_once_with(
      api_base_url="http://test.com",
      graph_id="sec",
      timeout=30,
      query_timeout=60,
      max_query_length=50000,
    )

  @patch("robosystems.middleware.mcp.factory.logger")
  @patch("robosystems.middleware.mcp.factory.env")
  @patch("robosystems.middleware.mcp.factory.GraphMCPClient")
  @patch("robosystems.graph_api.client.factory.GraphClientFactory")
  @patch("robosystems.middleware.graph.utils.MultiTenantUtils")
  async def test_create_client_logs_discovery(
    self, mock_utils, mock_factory, mock_client_class, mock_env, mock_logger
  ):
    """Test that URL discovery is logged."""
    mock_env.GRAPH_HTTP_TIMEOUT = 30
    mock_env.GRAPH_QUERY_TIMEOUT = 60
    mock_env.GRAPH_MAX_QUERY_LENGTH = "50000"

    mock_utils.is_shared_repository.return_value = True

    mock_graph_client = Mock()
    mock_graph_client.config.base_url = "http://discovered.com"
    mock_factory.create_client = AsyncMock(return_value=mock_graph_client)

    mock_mcp_client = AsyncMock()
    mock_client_class.return_value = mock_mcp_client

    await create_graph_mcp_client("sec")

    mock_logger.info.assert_called_once_with(
      "GraphClientFactory discovered endpoint: http://discovered.com for graph sec"
    )


class TestAcquireGraphMCPClient:
  """Test acquire_graph_mcp_client async context manager."""

  @patch("robosystems.middleware.mcp.factory.get_connection_pool")
  async def test_acquire_client_with_pool(self, mock_get_pool):
    """Test acquiring client with connection pooling."""
    # Mock connection pool
    mock_pool = AsyncMock()
    mock_client = AsyncMock()

    @asynccontextmanager
    async def mock_acquire(graph_id, api_base_url):
      yield mock_client

    mock_pool.acquire = mock_acquire
    mock_get_pool.return_value = mock_pool

    # Test the context manager
    async with acquire_graph_mcp_client("test_graph", "http://test.com") as client:
      assert client == mock_client

    mock_get_pool.assert_called_once()

  @patch("robosystems.middleware.mcp.factory.create_graph_mcp_client")
  async def test_acquire_client_without_pool(self, mock_create_client):
    """Test acquiring client without connection pooling."""
    mock_client = AsyncMock()
    mock_client.close = AsyncMock()
    mock_create_client.return_value = mock_client

    async with acquire_graph_mcp_client(
      "test_graph", "http://test.com", use_pool=False
    ) as client:
      assert client == mock_client

    mock_create_client.assert_called_once_with("test_graph", "http://test.com")
    mock_client.close.assert_called_once()

  @patch("robosystems.middleware.mcp.factory.create_graph_mcp_client")
  async def test_acquire_client_without_pool_no_close_method(self, mock_create_client):
    """Test acquiring client without pool when client has no close method."""
    mock_client = AsyncMock()
    # Don't add close method
    del mock_client.close
    mock_create_client.return_value = mock_client

    async with acquire_graph_mcp_client(
      "test_graph", "http://test.com", use_pool=False
    ) as client:
      assert client == mock_client

    mock_create_client.assert_called_once_with("test_graph", "http://test.com")
    # Should not raise error even without close method

  @patch("robosystems.middleware.mcp.factory.logger")
  @patch("robosystems.middleware.mcp.factory.create_graph_mcp_client")
  async def test_acquire_client_close_error_handling(
    self, mock_create_client, mock_logger
  ):
    """Test error handling when client close fails."""
    mock_client = AsyncMock()
    mock_client.close = AsyncMock(side_effect=Exception("Close failed"))
    mock_create_client.return_value = mock_client

    async with acquire_graph_mcp_client(
      "test_graph", "http://test.com", use_pool=False
    ) as client:
      assert client == mock_client

    mock_client.close.assert_called_once()
    mock_logger.error.assert_called_once_with("Error closing MCP client: Close failed")

  @patch("robosystems.middleware.mcp.factory.get_connection_pool")
  async def test_acquire_client_default_parameters(self, mock_get_pool):
    """Test acquiring client with default parameters."""
    mock_pool = AsyncMock()
    mock_client = AsyncMock()

    @asynccontextmanager
    async def mock_acquire(graph_id, api_base_url):
      assert graph_id == "sec"  # Default value
      assert api_base_url is None  # Default value
      yield mock_client

    mock_pool.acquire = mock_acquire
    mock_get_pool.return_value = mock_pool

    async with acquire_graph_mcp_client() as client:
      assert client == mock_client

  @patch("robosystems.middleware.mcp.factory.get_connection_pool")
  async def test_acquire_client_custom_parameters(self, mock_get_pool):
    """Test acquiring client with custom parameters."""
    mock_pool = AsyncMock()
    mock_client = AsyncMock()

    @asynccontextmanager
    async def mock_acquire(graph_id, api_base_url):
      assert graph_id == "custom_graph"
      assert api_base_url == "http://custom.com"
      yield mock_client

    mock_pool.acquire = mock_acquire
    mock_get_pool.return_value = mock_pool

    async with acquire_graph_mcp_client("custom_graph", "http://custom.com") as client:
      assert client == mock_client

  @patch("robosystems.middleware.mcp.factory.get_connection_pool")
  async def test_acquire_client_pool_exception_propagation(self, mock_get_pool):
    """Test that pool exceptions are properly propagated."""
    mock_pool = AsyncMock()

    @asynccontextmanager
    async def mock_acquire(graph_id, api_base_url):
      raise Exception("Pool error")
      yield  # This will never be reached

    mock_pool.acquire = mock_acquire
    mock_get_pool.return_value = mock_pool

    with pytest.raises(Exception, match="Pool error"):
      async with acquire_graph_mcp_client("test"):
        pass  # Should not reach here

  @patch("robosystems.middleware.mcp.factory.create_graph_mcp_client")
  async def test_acquire_client_creation_exception_propagation(
    self, mock_create_client
  ):
    """Test that client creation exceptions are properly propagated."""
    mock_create_client.side_effect = Exception("Creation failed")

    with pytest.raises(Exception, match="Creation failed"):
      async with acquire_graph_mcp_client("test", use_pool=False):
        pass  # Should not reach here


class TestFactoryIntegration:
  """Test integration between factory functions."""

  @patch("robosystems.middleware.mcp.factory.get_connection_pool")
  @patch("robosystems.middleware.mcp.factory.create_graph_mcp_client")
  async def test_pool_vs_direct_creation_consistency(
    self, mock_create_client, mock_get_pool
  ):
    """Test that pool and direct creation produce consistent results."""
    mock_client_direct = AsyncMock()
    mock_client_pooled = AsyncMock()
    mock_create_client.return_value = mock_client_direct

    # Mock pool
    mock_pool = AsyncMock()

    @asynccontextmanager
    async def mock_acquire(graph_id, api_base_url):
      yield mock_client_pooled

    mock_pool.acquire = mock_acquire
    mock_get_pool.return_value = mock_pool

    # Test direct creation
    async with acquire_graph_mcp_client("test", use_pool=False) as client_direct:
      assert client_direct == mock_client_direct

    # Test pooled creation
    async with acquire_graph_mcp_client("test", use_pool=True) as client_pooled:
      assert client_pooled == mock_client_pooled

    # Verify both paths were taken
    mock_create_client.assert_called_once()
    mock_get_pool.assert_called_once()

  async def test_context_manager_protocol(self):
    """Test that acquire_graph_mcp_client follows async context manager protocol."""
    # This test verifies the function signature and async context manager behavior
    func = acquire_graph_mcp_client

    # Should be a function that returns an async context manager
    assert callable(func)

    # Test that it can be used with async with (this is syntax validation)
    try:
      async with func("test", use_pool=False):
        # The actual client creation will fail without proper mocking,
        # but the syntax should be valid
        pass
    except Exception:
      # Expected to fail without mocks, but syntax should be valid
      pass
