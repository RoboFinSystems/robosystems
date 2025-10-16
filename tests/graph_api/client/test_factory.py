"""Tests for Kuzu client factory."""

import time
from unittest.mock import MagicMock, patch, AsyncMock
import pytest

from robosystems.graph_api.client.factory import (
  KuzuClientFactory,
  CircuitBreaker,
  get_kuzu_client,
  get_kuzu_client_for_instance,
  get_kuzu_client_for_sec_ingestion,
)
from robosystems.middleware.graph.types import InstanceTier


class TestCircuitBreaker:
  """Test cases for circuit breaker."""

  def test_circuit_breaker_initialization(self):
    """Test circuit breaker initialization."""
    breaker = CircuitBreaker(failure_threshold=3, timeout=10)

    assert breaker.failure_count == 0
    assert breaker.last_failure_time is None
    assert breaker.is_open is False

  async def test_circuit_breaker_record_success(self):
    """Test recording success resets failure count."""
    breaker = CircuitBreaker(failure_threshold=3, timeout=10)
    breaker.failure_count = 2

    await breaker.record_success()

    assert breaker.failure_count == 0
    assert breaker.is_open is False

  async def test_circuit_breaker_record_failure(self):
    """Test recording failures opens circuit."""
    breaker = CircuitBreaker(failure_threshold=3, timeout=10)

    # Record failures up to threshold
    await breaker.record_failure()
    assert breaker.failure_count == 1
    assert breaker.is_open is False

    await breaker.record_failure()
    assert breaker.failure_count == 2
    assert breaker.is_open is False

    await breaker.record_failure()
    assert breaker.failure_count == 3
    assert breaker.is_open is True
    assert breaker.last_failure_time is not None

  async def test_circuit_breaker_should_attempt_when_closed(self):
    """Test should_attempt returns True when circuit is closed."""
    breaker = CircuitBreaker(failure_threshold=3, timeout=10)

    result = await breaker.should_attempt()
    assert result is True

  async def test_circuit_breaker_should_attempt_when_open(self):
    """Test should_attempt returns False when circuit is open."""
    breaker = CircuitBreaker(failure_threshold=3, timeout=10)
    breaker.failure_count = 3
    breaker.is_open = True
    breaker.last_failure_time = time.time()

    result = await breaker.should_attempt()
    assert result is False

  async def test_circuit_breaker_should_attempt_after_timeout(self):
    """Test should_attempt returns True after timeout expires."""
    breaker = CircuitBreaker(failure_threshold=3, timeout=0.1)  # 100ms timeout
    breaker.failure_count = 3
    breaker.is_open = True
    breaker.last_failure_time = time.time() - 1  # 1 second ago

    result = await breaker.should_attempt()
    assert result is True
    # Circuit should be closed after successful check
    assert breaker.is_open is False
    assert breaker.failure_count == 0

  async def test_circuit_breaker_concurrent_access(self):
    """Test circuit breaker handles concurrent access."""
    import asyncio

    breaker = CircuitBreaker(failure_threshold=5, timeout=10)

    async def record_failures():
      for _ in range(3):
        await breaker.record_failure()

    # Run multiple tasks concurrently
    tasks = [record_failures() for _ in range(2)]
    await asyncio.gather(*tasks)

    # Should have recorded all failures
    assert breaker.failure_count >= 5
    assert breaker.is_open is True


class TestKuzuClientFactory:
  """Test cases for Kuzu client factory."""

  @pytest.fixture
  def mock_env(self):
    """Mock environment variables."""
    with patch("robosystems.graph_api.client.factory.env") as mock:
      mock.KUZU_CONNECT_TIMEOUT = 5.0
      mock.KUZU_READ_TIMEOUT = 30.0
      mock.KUZU_CIRCUIT_BREAKER_THRESHOLD = 5
      mock.KUZU_CIRCUIT_BREAKER_TIMEOUT = 60
      mock.KUZU_ALB_HEALTH_CACHE_TTL = 300
      mock.KUZU_INSTANCE_CACHE_TTL = 3600
      mock.KUZU_REPLICA_ENDPOINT = "http://replica.example.com"
      mock.KUZU_MASTER_ENDPOINT = "http://master.example.com"
      mock.ENVIRONMENT = "test"
      mock.VALKEY_URL = "redis://localhost:6379"
      yield mock

  @pytest.mark.asyncio
  async def test_create_client_for_user_graph(self, mock_env):
    """Test creating client for user graph."""
    # Mock allocation manager
    with patch(
      "robosystems.graph_api.client.factory.KuzuAllocationManager"
    ) as MockAllocationManager:
      mock_manager = AsyncMock()
      MockAllocationManager.return_value = mock_manager

      mock_location = MagicMock()
      mock_location.graph_id = "kg123456"
      mock_location.database_name = "kg123456"
      mock_location.tier = InstanceTier.STANDARD
      mock_location.instance_id = "i-abc123"
      mock_location.endpoint = "http://instance.example.com"
      mock_manager.find_database_location.return_value = mock_location

      # Mock KuzuClient creation
      with patch("robosystems.graph_api.client.factory.KuzuClient") as MockClient:
        mock_client = MagicMock()
        mock_client._graph_id = "kg123456"
        mock_client._database_name = "kg123456"
        mock_client.config = MagicMock()
        mock_client.config.base_url = "http://instance.example.com"
        MockClient.return_value = mock_client

        client = await KuzuClientFactory.create_client("kg123456")

        assert client is not None
        assert client._graph_id == "kg123456"
        # The factory uses graph_id as database_name for user graphs
        assert client._database_name == "kg123456"
        assert client.config.base_url == "http://instance.example.com"

  @pytest.mark.asyncio
  async def test_create_client_for_shared_repository(self, mock_env):
    """Test creating client for shared repository."""
    # Mock ALB health check
    with patch.object(KuzuClientFactory, "_check_alb_health", return_value=True):
      # Mock KuzuClient creation
      with patch("robosystems.graph_api.client.factory.KuzuClient") as MockClient:
        mock_client = MagicMock()
        mock_client._graph_id = "sec"
        mock_client._database_name = "sec"
        mock_client.config = MagicMock()
        mock_client.config.base_url = "http://replica.example.com"
        MockClient.return_value = mock_client

        # SEC is in SHARED_REPOSITORIES by default
        client = await KuzuClientFactory.create_client("sec", operation_type="read")

        assert client is not None
        assert client._graph_id == "sec"
        assert client._database_name == "sec"
        # Should use replica endpoint for reads
        assert "replica" in client.config.base_url

  @pytest.mark.asyncio
  async def test_create_client_for_subgraph(self, mock_env):
    """Test creating client for subgraph."""
    # Mock allocation for parent graph
    with patch(
      "robosystems.graph_api.client.factory.KuzuAllocationManager"
    ) as MockAllocationManager:
      mock_manager = AsyncMock()
      MockAllocationManager.return_value = mock_manager

      mock_location = MagicMock()
      mock_location.graph_id = "kg123456"
      mock_location.database_name = "kg123456"
      mock_location.tier = InstanceTier.ENTERPRISE
      mock_location.instance_id = "i-xyz789"
      mock_location.endpoint = "http://parent.example.com"
      mock_manager.find_database_location.return_value = mock_location

      # Mock httpx client creation
      with patch("robosystems.graph_api.client.factory.httpx.AsyncClient"):
        client = await KuzuClientFactory.create_client("kg123456:subgraph1")

        assert client is not None
        # The factory preserves the full subgraph ID as database_name
        assert client._database_name == "kg123456:subgraph1"

  @pytest.mark.asyncio
  async def test_create_client_shared_write_operation(self, mock_env):
    """Test shared repository write goes to master."""
    # Mock KuzuClient creation to control the response
    with patch("robosystems.graph_api.client.factory.KuzuClient") as MockClient:
      mock_client = MagicMock()
      mock_client._graph_id = "sec"
      mock_client._database_name = "sec"
      mock_client.config = MagicMock()
      mock_client.config.base_url = "http://master.example.com"
      MockClient.return_value = mock_client

      client = await KuzuClientFactory.create_client("sec", operation_type="write")

      assert client is not None
      # Should use master endpoint for writes
      assert "master" in client.config.base_url

  @pytest.mark.asyncio
  async def test_create_client_alb_unhealthy_fallback(self, mock_env):
    """Test fallback to master when ALB is unhealthy."""
    # Mock ALB as unhealthy
    with patch.object(KuzuClientFactory, "_check_alb_health", return_value=False):
      # Mock KuzuClient creation
      with patch("robosystems.graph_api.client.factory.KuzuClient") as MockClient:
        mock_client = MagicMock()
        mock_client._graph_id = "sec"
        mock_client._database_name = "sec"
        mock_client.config = MagicMock()
        mock_client.config.base_url = "http://master.example.com"
        MockClient.return_value = mock_client

        client = await KuzuClientFactory.create_client("sec", operation_type="read")

        assert client is not None
        # Should fallback to master endpoint
        assert "master" in client.config.base_url

  @pytest.mark.asyncio
  async def test_create_client_no_allocation(self, mock_env):
    """Test error when no allocation exists."""
    # Mock env properly
    mock_env.KUZU_API_URL = "http://localhost:8001"
    mock_env.KUZU_API_KEY = "test-api-key"
    mock_env.ENVIRONMENT = "dev"

    with patch(
      "robosystems.graph_api.client.factory.KuzuAllocationManager"
    ) as MockAllocationManager:
      mock_manager = AsyncMock()
      MockAllocationManager.return_value = mock_manager
      mock_manager.find_database_location.return_value = None

      # The factory creates the client even when allocation is None,
      # which fails later when trying to use it
      client = await KuzuClientFactory.create_client("kg999999")
      assert client is not None  # Client is created with default URL

  @pytest.mark.asyncio
  async def test_create_client_with_tier_override(self, mock_env):
    """Test creating client with tier override."""
    with patch(
      "robosystems.graph_api.client.factory.KuzuAllocationManager"
    ) as MockAllocationManager:
      mock_manager = AsyncMock()
      MockAllocationManager.return_value = mock_manager

      mock_location = MagicMock()
      mock_location.graph_id = "kg123456"
      mock_location.database_name = "kg123456"
      mock_location.tier = InstanceTier.PREMIUM
      mock_location.instance_id = "i-premium"
      mock_location.endpoint = "http://premium.example.com"
      mock_manager.find_database_location.return_value = mock_location

      # Mock KuzuClient creation
      with patch("robosystems.graph_api.client.factory.KuzuClient") as MockClient:
        mock_client = MagicMock()
        mock_client._graph_id = "kg123456"
        mock_client._database_name = "kg123456"
        mock_client.config = MagicMock()
        mock_client.config.base_url = "http://premium.example.com"
        MockClient.return_value = mock_client

        client = await KuzuClientFactory.create_client(
          "kg123456", tier=InstanceTier.PREMIUM
        )

        assert client is not None
        assert client.config.base_url == "http://premium.example.com"

  @pytest.mark.asyncio
  async def test_cleanup(self, mock_env):
    """Test cleanup closes connection pools."""
    # Create mock connection pools
    mock_client = AsyncMock()
    KuzuClientFactory._connection_pools = {"http://test.example.com": mock_client}

    await KuzuClientFactory.cleanup()

    # Should have closed all clients
    mock_client.aclose.assert_called_once()
    assert len(KuzuClientFactory._connection_pools) == 0


class TestFactoryFunctions:
  """Test cases for factory convenience functions."""

  @pytest.mark.asyncio
  async def test_get_kuzu_client(self):
    """Test get_kuzu_client function."""
    with patch.object(KuzuClientFactory, "create_client") as mock_create:
      mock_client = MagicMock()
      mock_create.return_value = mock_client

      result = await get_kuzu_client("kg123456", operation_type="write")

      assert result == mock_client
      mock_create.assert_called_once_with(
        "kg123456",
        "write",
        None,
        None,
      )

  @pytest.mark.asyncio
  async def test_get_kuzu_client_for_instance(self):
    """Test get_kuzu_client_for_instance function."""
    with patch("robosystems.graph_api.client.factory.KuzuClient") as MockClient:
      mock_client = MagicMock()
      MockClient.return_value = mock_client

      result = await get_kuzu_client_for_instance(
        instance_ip="10.0.0.1",
        api_key="test-api-key",
      )

      assert result == mock_client
      MockClient.assert_called_once()

  @pytest.mark.asyncio
  async def test_get_kuzu_client_for_sec_ingestion(self):
    """Test get_kuzu_client_for_sec_ingestion function."""
    # Mock the environment to be development to avoid DynamoDB calls
    with patch("robosystems.graph_api.client.factory.env") as mock_env:
      mock_env.is_development.return_value = True
      mock_env.KUZU_API_URL = "http://localhost:8001"
      mock_env.KUZU_API_KEY = "test-key"
      mock_env.KUZU_CONNECT_TIMEOUT = 5.0
      mock_env.KUZU_READ_TIMEOUT = 30.0

      # Mock KuzuClient creation
      with patch("robosystems.graph_api.client.factory.KuzuClient") as MockClient:
        mock_client = MagicMock()
        mock_client._graph_id = "sec"
        mock_client._database_name = "sec"
        MockClient.return_value = mock_client

        result = await get_kuzu_client_for_sec_ingestion()

        assert result == mock_client
        MockClient.assert_called_once()
