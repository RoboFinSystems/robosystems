"""Comprehensive unit tests for GraphClientFactory.

Covers CircuitBreaker state transitions, with_retry decorator, cache key generation,
_determine_read_target, shared repository vs user graph routing, subgraph parsing,
create_client_sync, pool statistics, and cleanup.
"""

import asyncio
import json
import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from robosystems.config.graph_tier import GraphTier
from robosystems.graph_api.client.factory import (
  CircuitBreaker,
  ConfigurationError,
  GraphClientFactory,
  RouteError,
  RouteTarget,
  ServiceUnavailableError,
  get_graph_client,
  get_graph_client_for_instance,
  get_graph_client_for_sec_ingestion,
  with_retry,
)

FACTORY_MODULE = "robosystems.graph_api.client.factory"


# ---------------------------------------------------------------------------
# CircuitBreaker
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCircuitBreakerStateTransitions:
  """Full coverage of CircuitBreaker closed -> open -> half-open -> closed."""

  @pytest.mark.asyncio
  async def test_initial_state_is_closed(self):
    """Circuit breaker starts in closed state."""
    cb = CircuitBreaker(failure_threshold=3, timeout=60)
    assert cb.is_open is False
    assert cb.failure_count == 0
    assert cb.last_failure_time is None

  @pytest.mark.asyncio
  async def test_record_success_resets_state(self):
    """Success resets failure count, closes circuit."""
    cb = CircuitBreaker(failure_threshold=3, timeout=60)
    cb.failure_count = 2
    cb.is_open = True
    cb.last_failure_time = time.time()

    await cb.record_success()

    assert cb.failure_count == 0
    assert cb.is_open is False
    assert cb.last_failure_time is None

  @pytest.mark.asyncio
  async def test_record_failure_increments_count(self):
    """Each failure increments the counter."""
    cb = CircuitBreaker(failure_threshold=5, timeout=60)

    await cb.record_failure()
    assert cb.failure_count == 1
    assert cb.is_open is False

    await cb.record_failure()
    assert cb.failure_count == 2
    assert cb.is_open is False

  @pytest.mark.asyncio
  async def test_record_failure_opens_at_threshold(self):
    """Circuit opens when failure count reaches threshold."""
    cb = CircuitBreaker(failure_threshold=3, timeout=60)

    for _ in range(3):
      await cb.record_failure()

    assert cb.failure_count == 3
    assert cb.is_open is True
    assert cb.last_failure_time is not None

  @pytest.mark.asyncio
  async def test_should_attempt_when_closed(self):
    """Returns True when circuit is closed."""
    cb = CircuitBreaker(failure_threshold=3, timeout=60)
    assert await cb.should_attempt() is True

  @pytest.mark.asyncio
  async def test_should_attempt_when_open_before_timeout(self):
    """Returns False when circuit is open and timeout not elapsed."""
    cb = CircuitBreaker(failure_threshold=3, timeout=60)
    cb.is_open = True
    cb.failure_count = 3
    cb.last_failure_time = time.time()

    assert await cb.should_attempt() is False

  @pytest.mark.asyncio
  async def test_should_attempt_transitions_to_half_open_after_timeout(self):
    """After timeout, circuit moves to half-open (is_open=False, count=0)."""
    cb = CircuitBreaker(failure_threshold=3, timeout=1)
    cb.is_open = True
    cb.failure_count = 3
    cb.last_failure_time = time.time() - 2  # 2 seconds ago, timeout is 1

    result = await cb.should_attempt()

    assert result is True
    # Half-open: resets state, allows attempt
    assert cb.is_open is False
    assert cb.failure_count == 0

  @pytest.mark.asyncio
  async def test_full_cycle_closed_to_open_to_closed(self):
    """Full lifecycle: closed -> failures -> open -> timeout -> closed."""
    cb = CircuitBreaker(failure_threshold=2, timeout=0)

    # Closed -> Open
    await cb.record_failure()
    await cb.record_failure()
    assert cb.is_open is True

    # Open -> Half-open (timeout is 0, so immediate)
    result = await cb.should_attempt()
    assert result is True
    assert cb.is_open is False

    # Half-open -> Closed (success)
    await cb.record_success()
    assert cb.failure_count == 0
    assert cb.is_open is False

  @pytest.mark.asyncio
  async def test_concurrent_access_is_safe(self):
    """Concurrent record_failure calls are handled safely."""
    cb = CircuitBreaker(failure_threshold=10, timeout=60)

    async def record_many():
      for _ in range(5):
        await cb.record_failure()

    await asyncio.gather(record_many(), record_many())

    assert cb.failure_count == 10
    assert cb.is_open is True

  @pytest.mark.asyncio
  async def test_should_attempt_with_none_last_failure(self):
    """Open circuit with None last_failure_time returns False."""
    cb = CircuitBreaker(failure_threshold=3, timeout=60)
    cb.is_open = True
    cb.last_failure_time = None

    result = await cb.should_attempt()
    assert result is False


# ---------------------------------------------------------------------------
# with_retry decorator
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestWithRetryDecorator:
  """Tests for the with_retry decorator with exponential backoff."""

  @pytest.mark.asyncio
  async def test_retry_disabled_when_feature_flag_off(self):
    """When GRAPH_RETRY_LOGIC_ENABLED is False, function executes once."""
    call_count = 0

    @with_retry(max_attempts=3, base_delay=0.001)
    async def my_func():
      nonlocal call_count
      call_count += 1
      return "ok"

    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.GRAPH_RETRY_LOGIC_ENABLED = False
      result = await my_func()

    assert result == "ok"
    assert call_count == 1

  @pytest.mark.asyncio
  async def test_retry_on_timeout_exception(self):
    """Retries on httpx.TimeoutException."""
    call_count = 0

    @with_retry(max_attempts=3, base_delay=0.001)
    async def my_func():
      nonlocal call_count
      call_count += 1
      if call_count < 3:
        raise httpx.TimeoutException("timeout")
      return "recovered"

    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.GRAPH_RETRY_LOGIC_ENABLED = True
      result = await my_func()

    assert result == "recovered"
    assert call_count == 3

  @pytest.mark.asyncio
  async def test_retry_on_connect_error(self):
    """Retries on httpx.ConnectError."""
    call_count = 0

    @with_retry(max_attempts=2, base_delay=0.001)
    async def my_func():
      nonlocal call_count
      call_count += 1
      if call_count < 2:
        raise httpx.ConnectError("refused")
      return "connected"

    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.GRAPH_RETRY_LOGIC_ENABLED = True
      result = await my_func()

    assert result == "connected"
    assert call_count == 2

  @pytest.mark.asyncio
  async def test_retry_on_service_unavailable_error(self):
    """Retries on ServiceUnavailableError."""
    call_count = 0

    @with_retry(max_attempts=2, base_delay=0.001)
    async def my_func():
      nonlocal call_count
      call_count += 1
      if call_count < 2:
        raise ServiceUnavailableError("unavailable")
      return "available"

    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.GRAPH_RETRY_LOGIC_ENABLED = True
      result = await my_func()

    assert result == "available"
    assert call_count == 2

  @pytest.mark.asyncio
  async def test_no_retry_on_non_retriable_exception(self):
    """Non-retriable exceptions are raised immediately without retry."""
    call_count = 0

    @with_retry(max_attempts=3, base_delay=0.001)
    async def my_func():
      nonlocal call_count
      call_count += 1
      raise ValueError("bad value")

    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.GRAPH_RETRY_LOGIC_ENABLED = True
      with pytest.raises(ValueError, match="bad value"):
        await my_func()

    assert call_count == 1

  @pytest.mark.asyncio
  async def test_raises_last_exception_after_max_attempts(self):
    """After max_attempts, the last exception is raised."""

    @with_retry(max_attempts=2, base_delay=0.001)
    async def my_func():
      raise httpx.TimeoutException("always times out")

    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.GRAPH_RETRY_LOGIC_ENABLED = True
      with pytest.raises(httpx.TimeoutException, match="always times out"):
        await my_func()

  @pytest.mark.asyncio
  async def test_exponential_backoff_with_jitter(self):
    """Delay increases exponentially with jitter."""
    delays = []

    async def mock_sleep(delay):
      delays.append(delay)

    @with_retry(max_attempts=4, base_delay=1.0, exponential_base=2.0, jitter=True)
    async def my_func():
      raise httpx.TimeoutException("timeout")

    with (
      patch(f"{FACTORY_MODULE}.env") as mock_env,
      patch("asyncio.sleep", side_effect=mock_sleep),
    ):
      mock_env.GRAPH_RETRY_LOGIC_ENABLED = True
      with pytest.raises(httpx.TimeoutException):
        await my_func()

    # Should have 3 sleeps (attempts 0, 1, 2 - last attempt doesn't sleep)
    assert len(delays) == 3
    # With jitter, delay = base * (exponential_base ** attempt) * (0.5 + random())
    # delay0 should be around 1.0 * (0.5 to 1.5)
    assert 0.4 < delays[0] < 1.6
    # delay1 should be around 2.0 * (0.5 to 1.5)
    assert 0.9 < delays[1] < 3.1

  @pytest.mark.asyncio
  async def test_max_delay_caps_backoff(self):
    """Delay does not exceed max_delay."""
    delays = []

    async def mock_sleep(delay):
      delays.append(delay)

    @with_retry(
      max_attempts=4,
      base_delay=10.0,
      max_delay=5.0,
      exponential_base=10.0,
      jitter=False,
    )
    async def my_func():
      raise httpx.TimeoutException("timeout")

    with (
      patch(f"{FACTORY_MODULE}.env") as mock_env,
      patch("asyncio.sleep", side_effect=mock_sleep),
    ):
      mock_env.GRAPH_RETRY_LOGIC_ENABLED = True
      with pytest.raises(httpx.TimeoutException):
        await my_func()

    # All delays should be capped at max_delay (5.0)
    for delay in delays:
      assert delay <= 5.0

  @pytest.mark.asyncio
  async def test_no_jitter_when_disabled(self):
    """With jitter=False, delay is exact exponential backoff."""
    delays = []

    async def mock_sleep(delay):
      delays.append(delay)

    @with_retry(
      max_attempts=3,
      base_delay=1.0,
      max_delay=100.0,
      exponential_base=2.0,
      jitter=False,
    )
    async def my_func():
      raise httpx.TimeoutException("timeout")

    with (
      patch(f"{FACTORY_MODULE}.env") as mock_env,
      patch("asyncio.sleep", side_effect=mock_sleep),
    ):
      mock_env.GRAPH_RETRY_LOGIC_ENABLED = True
      with pytest.raises(httpx.TimeoutException):
        await my_func()

    assert len(delays) == 2
    assert delays[0] == 1.0  # 1.0 * (2.0 ** 0)
    assert delays[1] == 2.0  # 1.0 * (2.0 ** 1)


# ---------------------------------------------------------------------------
# GraphClientFactory cache key generation
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGraphClientFactoryCacheKey:
  """Tests for _get_cache_key method."""

  def test_cache_key_with_identifier(self):
    """Cache key includes env prefix, type, and identifier."""
    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.ENVIRONMENT = "prod"
      key = GraphClientFactory._get_cache_key("location", "kg123")
    assert key == "graph:prod:location:kg123"

  def test_cache_key_without_identifier(self):
    """Cache key without identifier omits trailing component."""
    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.ENVIRONMENT = "staging"
      key = GraphClientFactory._get_cache_key("master")
    assert key == "graph:staging:master"

  def test_cache_key_fallback_env(self):
    """Falls back to 'dev' when ENVIRONMENT is None."""
    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.ENVIRONMENT = None
      key = GraphClientFactory._get_cache_key("alb", "sec")
    assert key == "graph:dev:alb:sec"


# ---------------------------------------------------------------------------
# _determine_read_target
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDetermineReadTarget:
  """Tests for _determine_read_target with ALB vs master logic."""

  @pytest.mark.asyncio
  async def test_routes_to_replica_alb_when_configured(self):
    """Reads go to replica ALB when SHARED_REPLICA_ALB_URL is set."""
    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.SHARED_REPLICA_ALB_URL = "http://alb.example.com"
      mock_env.GRAPH_API_KEY = "test-key"

      target, url, api_key = await GraphClientFactory._determine_read_target("sec")

    assert target == RouteTarget.SHARED_REPLICA
    assert url == "http://alb.example.com"

  @pytest.mark.asyncio
  async def test_falls_back_to_master_when_no_alb(self):
    """Falls back to master when no ALB and master reads enabled."""
    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.SHARED_REPLICA_ALB_URL = None
      mock_env.SHARED_MASTER_READS_ENABLED = True
      mock_env.GRAPH_API_KEY = "test-key"

      with patch.object(
        GraphClientFactory,
        "_get_shared_master_url",
        return_value="http://master.example.com",
      ):
        target, url, api_key = await GraphClientFactory._determine_read_target("sec")

    assert target == RouteTarget.SHARED_MASTER
    assert url == "http://master.example.com"

  @pytest.mark.asyncio
  async def test_raises_when_no_read_endpoint(self):
    """Raises ServiceUnavailableError when no read endpoint available."""
    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.SHARED_REPLICA_ALB_URL = None
      mock_env.SHARED_MASTER_READS_ENABLED = False

      with pytest.raises(ServiceUnavailableError, match="No read endpoint available"):
        await GraphClientFactory._determine_read_target("sec")


# ---------------------------------------------------------------------------
# Shared repository vs user graph client creation
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCreateSharedRepositoryClient:
  """Tests for _create_shared_repository_client routing."""

  @pytest.mark.asyncio
  async def test_dev_routes_to_local_graph(self):
    """In dev, shared repos route to local graph instance."""
    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.is_development.return_value = True
      mock_env.is_test.return_value = False
      mock_env.GRAPH_API_URL = "http://localhost:8001"
      mock_env.GRAPH_API_KEY = "dev-key"
      mock_env.ENVIRONMENT = "dev"

      with patch(f"{FACTORY_MODULE}.GraphClient") as MockClient:
        mock_client = MagicMock()
        MockClient.return_value = mock_client

        await GraphClientFactory._create_shared_repository_client("sec", "read")

        MockClient.assert_called_once_with(
          base_url="http://localhost:8001", api_key="dev-key"
        )
        assert mock_client._route_target == RouteTarget.SHARED_MASTER.value
        assert mock_client._graph_id == "sec"

  @pytest.mark.asyncio
  async def test_prod_write_routes_to_master(self):
    """In prod, writes always go to shared master."""
    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.is_development.return_value = False
      mock_env.is_test.return_value = False
      mock_env.GRAPH_API_KEY = "prod-key"
      mock_env.ENVIRONMENT = "prod"

      with (
        patch.object(
          GraphClientFactory,
          "_get_shared_master_url",
          return_value="http://master.prod.example.com",
        ),
        patch(f"{FACTORY_MODULE}.GraphClient") as MockClient,
      ):
        mock_client = MagicMock()
        MockClient.return_value = mock_client

        await GraphClientFactory._create_shared_repository_client("sec", "write")

        MockClient.assert_called_once_with(
          base_url="http://master.prod.example.com", api_key="prod-key"
        )
        assert mock_client._route_target == RouteTarget.SHARED_MASTER.value

  @pytest.mark.asyncio
  async def test_prod_read_uses_determine_read_target(self):
    """In prod, reads go through _determine_read_target."""
    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.is_development.return_value = False
      mock_env.is_test.return_value = False
      mock_env.GRAPH_API_KEY = "prod-key"
      mock_env.ENVIRONMENT = "prod"

      with (
        patch.object(
          GraphClientFactory,
          "_determine_read_target",
          return_value=(
            RouteTarget.SHARED_REPLICA,
            "http://alb.example.com",
            "prod-key",
          ),
        ),
        patch(f"{FACTORY_MODULE}.GraphClient") as MockClient,
      ):
        mock_client = MagicMock()
        MockClient.return_value = mock_client

        await GraphClientFactory._create_shared_repository_client("sec", "read")

        MockClient.assert_called_once_with(
          base_url="http://alb.example.com", api_key="prod-key"
        )
        assert mock_client._route_target == RouteTarget.SHARED_REPLICA.value

  @pytest.mark.asyncio
  async def test_raises_configuration_error_when_no_url_in_prod(self):
    """Raises ConfigurationError when _determine_read_target returns empty URL."""
    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.is_development.return_value = False
      mock_env.is_test.return_value = False
      mock_env.GRAPH_API_KEY = "test-key"
      mock_env.ENVIRONMENT = "prod"

      with patch.object(
        GraphClientFactory,
        "_determine_read_target",
        return_value=(RouteTarget.SHARED_REPLICA, "", "test-key"),
      ):
        with pytest.raises(ConfigurationError, match="No endpoint configured"):
          await GraphClientFactory._create_shared_repository_client("sec", "read")

  @pytest.mark.asyncio
  async def test_warns_when_no_api_key_in_prod(self):
    """Warns when no API key in production but still creates client."""
    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.is_development.return_value = False
      mock_env.is_test.return_value = False
      mock_env.GRAPH_API_KEY = None
      mock_env.ENVIRONMENT = "prod"

      with (
        patch.object(
          GraphClientFactory,
          "_get_shared_master_url",
          return_value="http://master.example.com",
        ),
        patch(f"{FACTORY_MODULE}.GraphClient") as MockClient,
      ):
        mock_client = MagicMock()
        MockClient.return_value = mock_client

        result = await GraphClientFactory._create_shared_repository_client(
          "sec", "write"
        )

        # Client should still be created with fallback api_key (None)
        assert result is mock_client


# ---------------------------------------------------------------------------
# create_client top-level routing
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCreateClient:
  """Tests for the top-level create_client routing."""

  @pytest.mark.asyncio
  async def test_shared_repo_routes_to_shared_client(self):
    """Shared repository IDs route to _create_shared_repository_client."""
    with (
      patch(f"{FACTORY_MODULE}.env") as mock_env,
      patch(f"{FACTORY_MODULE}.is_shared_repository", return_value=True),
      patch.object(
        GraphClientFactory,
        "_create_shared_repository_client",
        new_callable=AsyncMock,
      ) as mock_create,
    ):
      mock_env.ENVIRONMENT = "dev"
      mock_client = MagicMock()
      mock_create.return_value = mock_client

      result = await GraphClientFactory.create_client("sec", "read")

      mock_create.assert_called_once_with("sec", "read")
      assert result is mock_client

  @pytest.mark.asyncio
  async def test_shared_repo_subgraph_routes_to_shared_client(self):
    """Subgraphs of shared repos route to _create_shared_repository_client."""
    mock_subgraph_info = MagicMock()
    mock_subgraph_info.parent_graph_id = "sec"

    with (
      patch(f"{FACTORY_MODULE}.env") as mock_env,
      patch(f"{FACTORY_MODULE}.is_shared_repository", side_effect=[False, True]),
      patch(f"{FACTORY_MODULE}.parse_subgraph_id", return_value=mock_subgraph_info),
      patch.object(
        GraphClientFactory,
        "_create_shared_repository_client",
        new_callable=AsyncMock,
      ) as mock_create,
    ):
      mock_env.ENVIRONMENT = "dev"
      mock_client = MagicMock()
      mock_create.return_value = mock_client

      await GraphClientFactory.create_client("sec_historical", "read")

      mock_create.assert_called_once_with("sec_historical", "read")

  @pytest.mark.asyncio
  async def test_user_graph_routes_to_user_client(self):
    """Non-shared graphs route to _create_user_graph_client."""
    with (
      patch(f"{FACTORY_MODULE}.env") as mock_env,
      patch(f"{FACTORY_MODULE}.is_shared_repository", return_value=False),
      patch(f"{FACTORY_MODULE}.parse_subgraph_id", return_value=None),
      patch.object(
        GraphClientFactory,
        "_create_user_graph_client",
        new_callable=AsyncMock,
      ) as mock_create,
    ):
      mock_env.ENVIRONMENT = "dev"
      mock_client = MagicMock()
      mock_create.return_value = mock_client

      await GraphClientFactory.create_client("kg123456", "write")

      mock_create.assert_called_once_with("kg123456", "dev", None)

  @pytest.mark.asyncio
  async def test_service_unavailable_wraps_with_context(self):
    """ServiceUnavailableError is re-raised with context."""
    with (
      patch(f"{FACTORY_MODULE}.env") as mock_env,
      patch(f"{FACTORY_MODULE}.is_shared_repository", return_value=False),
      patch(f"{FACTORY_MODULE}.parse_subgraph_id", return_value=None),
      patch.object(
        GraphClientFactory,
        "_create_user_graph_client",
        side_effect=ServiceUnavailableError("no instance"),
      ),
    ):
      mock_env.ENVIRONMENT = "dev"

      with pytest.raises(ServiceUnavailableError, match="kg123456"):
        await GraphClientFactory.create_client("kg123456", "read")


# ---------------------------------------------------------------------------
# User graph client creation
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCreateUserGraphClient:
  """Tests for _create_user_graph_client routing."""

  @pytest.mark.asyncio
  async def test_dev_routes_to_local(self):
    """In dev, user graphs route to local graph instance."""
    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.is_development.return_value = True
      mock_env.GRAPH_API_URL = "http://localhost:8001"
      mock_env.GRAPH_API_KEY = "dev-key"

      with (
        patch(f"{FACTORY_MODULE}.parse_subgraph_id", return_value=None),
        patch(f"{FACTORY_MODULE}.GraphClient") as MockClient,
      ):
        mock_client = MagicMock()
        MockClient.return_value = mock_client

        await GraphClientFactory._create_user_graph_client("kg123", "dev", None)

        MockClient.assert_called_once_with(
          base_url="http://localhost:8001", api_key="dev-key"
        )
        assert mock_client._route_target == RouteTarget.USER_GRAPH.value
        assert mock_client._graph_id == "kg123"
        assert mock_client._database_name == "kg123"

  @pytest.mark.asyncio
  async def test_dev_subgraph_uses_parent_routing(self):
    """In dev, subgraph routes to local but uses subgraph database name."""
    mock_subgraph_info = MagicMock()
    mock_subgraph_info.parent_graph_id = "kg123"
    mock_subgraph_info.database_name = "kg123_dev"

    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.is_development.return_value = True
      mock_env.GRAPH_API_URL = "http://localhost:8001"
      mock_env.GRAPH_API_KEY = "dev-key"

      with (
        patch(
          f"{FACTORY_MODULE}.parse_subgraph_id",
          return_value=mock_subgraph_info,
        ),
        patch(f"{FACTORY_MODULE}.GraphClient") as MockClient,
      ):
        mock_client = MagicMock()
        MockClient.return_value = mock_client

        await GraphClientFactory._create_user_graph_client("kg123_dev", "dev", None)

        assert mock_client._graph_id == "kg123_dev"
        assert mock_client._database_name == "kg123_dev"

  @pytest.mark.asyncio
  async def test_prod_raises_route_error_when_no_location(self):
    """In prod, RouteError when allocation manager returns None."""
    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.is_development.return_value = False
      mock_env.ENVIRONMENT = "prod"
      mock_env.GRAPH_API_KEY = "prod-key"
      mock_env.GRAPH_REDIS_CACHE_ENABLED = False

      with (
        patch(f"{FACTORY_MODULE}.parse_subgraph_id", return_value=None),
        patch(f"{FACTORY_MODULE}.LadybugAllocationManager") as MockAllocManager,
      ):
        mock_manager = AsyncMock()
        MockAllocManager.return_value = mock_manager
        mock_manager.find_database_location.return_value = None

        with pytest.raises(RouteError, match="not found"):
          await GraphClientFactory._create_user_graph_client("kg999", "prod", None)

  @pytest.mark.asyncio
  async def test_prod_subgraph_error_message_references_parent(self):
    """RouteError for subgraph mentions parent graph."""
    mock_subgraph_info = MagicMock()
    mock_subgraph_info.parent_graph_id = "kg123"
    mock_subgraph_info.database_name = "kg123_dev"

    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.is_development.return_value = False
      mock_env.ENVIRONMENT = "prod"
      mock_env.GRAPH_API_KEY = "prod-key"
      mock_env.GRAPH_REDIS_CACHE_ENABLED = False

      with (
        patch(
          f"{FACTORY_MODULE}.parse_subgraph_id",
          return_value=mock_subgraph_info,
        ),
        patch(f"{FACTORY_MODULE}.LadybugAllocationManager") as MockAllocManager,
      ):
        mock_manager = AsyncMock()
        MockAllocManager.return_value = mock_manager
        mock_manager.find_database_location.return_value = None

        with pytest.raises(RouteError, match="Parent graph kg123 not found"):
          await GraphClientFactory._create_user_graph_client("kg123_dev", "prod", None)


class _FakeRedis:
  """Minimal stateful async Redis stand-in for routing-cache round-trips."""

  def __init__(self):
    self.store: dict[str, str] = {}

  async def get(self, key):
    return self.store.get(key)

  async def setex(self, key, _ttl, value):
    self.store[key] = value


@pytest.mark.unit
class TestUserGraphLocationCache:
  """Regression coverage for the prod/staging routing location cache (#758).

  The write path stored {instance_id, private_ip, database_name} scalars while
  the read path tried to reconstruct a DatabaseLocation dataclass — which has
  required fields the payload never carried, so every read raised and the cache
  was effectively dead. Routing only needs private_ip + instance_id, so both
  paths now exchange those scalars directly.
  """

  @pytest.mark.asyncio
  async def test_location_cache_round_trips(self):
    """Write then read of the routing cache must round-trip (no second lookup)."""
    fake_redis = _FakeRedis()

    mock_location = MagicMock()
    mock_location.private_ip = "10.0.7.7"
    mock_location.instance_id = "i-roundtrip"

    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.is_development.return_value = False
      mock_env.ENVIRONMENT = "prod"
      mock_env.GRAPH_API_KEY = "prod-key"

      with (
        patch(f"{FACTORY_MODULE}.parse_subgraph_id", return_value=None),
        patch.object(
          GraphClientFactory,
          "_get_redis",
          new_callable=AsyncMock,
          return_value=fake_redis,
        ),
        patch(f"{FACTORY_MODULE}.LadybugAllocationManager") as MockAllocManager,
        patch(f"{FACTORY_MODULE}.GraphClient") as MockClient,
      ):
        mock_manager = AsyncMock()
        MockAllocManager.return_value = mock_manager
        mock_manager.find_database_location.return_value = mock_location
        MockClient.return_value = MagicMock()

        # First call misses → allocation lookup + cache write.
        await GraphClientFactory._create_user_graph_client("kg123", "prod", None)
        # Second call hits the cache → no second allocation lookup.
        await GraphClientFactory._create_user_graph_client("kg123", "prod", None)

      assert mock_manager.find_database_location.call_count == 1
      # Both calls routed to the cached instance IP.
      assert MockClient.call_args_list[-1].kwargs["base_url"] == "http://10.0.7.7:8001"

  @pytest.mark.asyncio
  async def test_cache_write_stores_routing_scalars(self):
    """Cached payload carries only the scalars the read path consumes."""
    mock_redis = AsyncMock()
    mock_redis.get.return_value = None  # force a miss

    mock_location = MagicMock()
    mock_location.private_ip = "10.0.9.9"
    mock_location.instance_id = "i-fresh"

    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.is_development.return_value = False
      mock_env.ENVIRONMENT = "prod"
      mock_env.GRAPH_API_KEY = "prod-key"

      with (
        patch(f"{FACTORY_MODULE}.parse_subgraph_id", return_value=None),
        patch.object(
          GraphClientFactory,
          "_get_redis",
          new_callable=AsyncMock,
          return_value=mock_redis,
        ),
        patch(f"{FACTORY_MODULE}.LadybugAllocationManager") as MockAllocManager,
        patch(f"{FACTORY_MODULE}.GraphClient") as MockClient,
      ):
        mock_manager = AsyncMock()
        MockAllocManager.return_value = mock_manager
        mock_manager.find_database_location.return_value = mock_location
        MockClient.return_value = MagicMock()

        await GraphClientFactory._create_user_graph_client("kg123", "prod", None)

    mock_redis.setex.assert_called_once()
    cached = json.loads(mock_redis.setex.call_args.args[2])
    assert cached["location"] == {"instance_id": "i-fresh", "private_ip": "10.0.9.9"}
    assert "database_name" not in cached["location"]

  @pytest.mark.asyncio
  async def test_cache_hit_skips_allocation_lookup(self):
    """A fresh cached location routes without touching the allocation manager."""
    mock_redis = AsyncMock()
    mock_redis.get.return_value = json.dumps(
      {
        "location": {"instance_id": "i-cached", "private_ip": "10.0.5.5"},
        "timestamp": time.time(),
      }
    )

    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.is_development.return_value = False
      mock_env.ENVIRONMENT = "prod"
      mock_env.GRAPH_API_KEY = "prod-key"

      with (
        patch(f"{FACTORY_MODULE}.parse_subgraph_id", return_value=None),
        patch.object(
          GraphClientFactory,
          "_get_redis",
          new_callable=AsyncMock,
          return_value=mock_redis,
        ),
        patch(f"{FACTORY_MODULE}.LadybugAllocationManager") as MockAllocManager,
        patch(f"{FACTORY_MODULE}.GraphClient") as MockClient,
      ):
        mock_client = MagicMock()
        MockClient.return_value = mock_client

        await GraphClientFactory._create_user_graph_client("kg123", "prod", None)

      MockAllocManager.assert_not_called()
      MockClient.assert_called_once_with(
        base_url="http://10.0.5.5:8001", api_key="prod-key"
      )
      assert mock_client._instance_id == "i-cached"


# ---------------------------------------------------------------------------
# create_client_sync
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCreateClientSync:
  """Tests for synchronous client creation."""

  @pytest.mark.asyncio
  async def test_raises_in_async_context(self):
    """Raises RuntimeError when called from async context."""
    with pytest.raises(RuntimeError, match="cannot be called from an async context"):
      GraphClientFactory.create_client_sync("kg123")

  def test_works_without_running_loop(self):
    """Works correctly when no event loop is running."""
    with (
      patch.object(
        GraphClientFactory, "create_client", new_callable=AsyncMock
      ) as mock_create,
    ):
      mock_client = MagicMock()
      mock_create.return_value = mock_client

      result = GraphClientFactory.create_client_sync("kg123", "read", "dev")

      assert result is mock_client
      mock_create.assert_called_once_with("kg123", "read", "dev", None)


# ---------------------------------------------------------------------------
# Pool statistics
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPoolStatistics:
  """Tests for get_pool_statistics."""

  def test_returns_circuit_breaker_info(self):
    """Pool stats include circuit breaker state."""
    # Reset factory state
    original_pools = GraphClientFactory._connection_pools.copy()
    original_stats = GraphClientFactory._pool_stats.copy()
    try:
      GraphClientFactory._connection_pools = {}
      GraphClientFactory._pool_stats = {}

      stats = GraphClientFactory.get_pool_statistics()

      assert "circuit_breakers" in stats
      assert "master" in stats["circuit_breakers"]
      assert "total_pools" in stats
      assert stats["total_pools"] == 0
    finally:
      GraphClientFactory._connection_pools = original_pools
      GraphClientFactory._pool_stats = original_stats

  def test_includes_pool_specific_stats(self):
    """Pool stats include per-pool failure rates."""
    original_pools = GraphClientFactory._connection_pools.copy()
    original_stats = GraphClientFactory._pool_stats.copy()
    try:
      mock_client = MagicMock()
      GraphClientFactory._connection_pools = {"http://test:8001": mock_client}
      GraphClientFactory._pool_stats = {
        "http://test:8001": {
          "created_at": time.time(),
          "requests": 100,
          "failures": 5,
        }
      }

      stats = GraphClientFactory.get_pool_statistics()

      assert stats["total_pools"] == 1
      pool_stat = stats["pools"]["http://test:8001"]
      assert pool_stat["requests"] == 100
      assert pool_stat["failures"] == 5
      assert pool_stat["failure_rate"] == 0.05
      assert pool_stat["is_active"] is True
    finally:
      GraphClientFactory._connection_pools = original_pools
      GraphClientFactory._pool_stats = original_stats

  def test_zero_requests_gives_zero_failure_rate(self):
    """Zero requests results in zero failure rate."""
    original_stats = GraphClientFactory._pool_stats.copy()
    try:
      GraphClientFactory._pool_stats = {
        "http://test:8001": {"requests": 0, "failures": 0}
      }
      stats = GraphClientFactory.get_pool_statistics()
      pool_stat = stats["pools"]["http://test:8001"]
      assert pool_stat["failure_rate"] == 0
    finally:
      GraphClientFactory._pool_stats = original_stats


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFactoryCleanup:
  """Tests for GraphClientFactory.cleanup."""

  @pytest.mark.asyncio
  async def test_cleanup_closes_all_pools(self):
    """Cleanup closes all HTTP connection pools."""
    original_pools = GraphClientFactory._connection_pools.copy()
    original_stats = GraphClientFactory._pool_stats.copy()
    try:
      mock_client1 = AsyncMock()
      mock_client2 = AsyncMock()
      GraphClientFactory._connection_pools = {
        "http://a:8001": mock_client1,
        "http://b:8001": mock_client2,
      }
      GraphClientFactory._pool_stats = {}

      await GraphClientFactory.cleanup()

      mock_client1.aclose.assert_called_once()
      mock_client2.aclose.assert_called_once()
      assert len(GraphClientFactory._connection_pools) == 0
    finally:
      GraphClientFactory._connection_pools = original_pools
      GraphClientFactory._pool_stats = original_stats

  @pytest.mark.asyncio
  async def test_cleanup_handles_pool_close_error(self):
    """Cleanup continues even if a pool close fails."""
    original_pools = GraphClientFactory._connection_pools.copy()
    original_stats = GraphClientFactory._pool_stats.copy()
    try:
      mock_client = AsyncMock()
      mock_client.aclose.side_effect = RuntimeError("close failed")
      GraphClientFactory._connection_pools = {"http://a:8001": mock_client}
      GraphClientFactory._pool_stats = {}

      await GraphClientFactory.cleanup()  # Should not raise

      assert len(GraphClientFactory._connection_pools) == 0
    finally:
      GraphClientFactory._connection_pools = original_pools
      GraphClientFactory._pool_stats = original_stats

  @pytest.mark.asyncio
  async def test_cleanup_clears_stats(self):
    """Cleanup clears pool statistics."""
    original_pools = GraphClientFactory._connection_pools.copy()
    original_stats = GraphClientFactory._pool_stats.copy()
    try:
      GraphClientFactory._connection_pools = {}
      GraphClientFactory._pool_stats = {"http://a:8001": {"requests": 100}}

      await GraphClientFactory.cleanup()

      assert len(GraphClientFactory._pool_stats) == 0
    finally:
      GraphClientFactory._connection_pools = original_pools
      GraphClientFactory._pool_stats = original_stats

  @pytest.mark.asyncio
  async def test_cleanup_resets_circuit_breaker(self):
    """Cleanup resets the master circuit breaker."""
    original_pools = GraphClientFactory._connection_pools.copy()
    original_stats = GraphClientFactory._pool_stats.copy()
    try:
      GraphClientFactory._master_circuit_breaker.failure_count = 10
      GraphClientFactory._master_circuit_breaker.is_open = True

      GraphClientFactory._connection_pools = {}
      GraphClientFactory._pool_stats = {}

      await GraphClientFactory.cleanup()

      assert GraphClientFactory._master_circuit_breaker.failure_count == 0
      assert GraphClientFactory._master_circuit_breaker.is_open is False
    finally:
      GraphClientFactory._connection_pools = original_pools
      GraphClientFactory._pool_stats = original_stats


# ---------------------------------------------------------------------------
# Convenience functions
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestConvenienceFunctions:
  """Tests for module-level convenience functions."""

  @pytest.mark.asyncio
  async def test_get_graph_client_delegates_to_factory(self):
    """get_graph_client calls GraphClientFactory.create_client."""
    with patch.object(
      GraphClientFactory, "create_client", new_callable=AsyncMock
    ) as mock_create:
      mock_client = MagicMock()
      mock_create.return_value = mock_client

      result = await get_graph_client("sec", "write", "dev", GraphTier.LADYBUG_STANDARD)

      mock_create.assert_called_once_with(
        "sec", "write", "dev", GraphTier.LADYBUG_STANDARD
      )
      assert result is mock_client

  @pytest.mark.asyncio
  async def test_get_graph_client_for_instance_creates_direct_client(self):
    """get_graph_client_for_instance creates client for direct IP access."""
    with patch(f"{FACTORY_MODULE}.GraphClient") as MockClient:
      mock_client = MagicMock()
      MockClient.return_value = mock_client

      result = await get_graph_client_for_instance("10.0.1.123", "my-key")

      MockClient.assert_called_once_with(
        base_url="http://10.0.1.123:8001", api_key="my-key"
      )
      assert result is mock_client

  @pytest.mark.asyncio
  async def test_get_graph_client_for_instance_default_key(self):
    """get_graph_client_for_instance uses env key when none provided."""
    with (
      patch(f"{FACTORY_MODULE}.env") as mock_env,
      patch(f"{FACTORY_MODULE}.GraphClient") as MockClient,
    ):
      mock_env.GRAPH_API_KEY = "env-key"
      mock_client = MagicMock()
      MockClient.return_value = mock_client

      await get_graph_client_for_instance("10.0.1.123")

      MockClient.assert_called_once_with(
        base_url="http://10.0.1.123:8001", api_key="env-key"
      )

  @pytest.mark.asyncio
  async def test_get_graph_client_for_sec_ingestion_dev(self):
    """SEC ingestion in dev routes to local graph."""
    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.is_development.return_value = True
      mock_env.GRAPH_API_URL = "http://localhost:8001"
      mock_env.GRAPH_API_KEY = "dev-key"

      with patch(f"{FACTORY_MODULE}.GraphClient") as MockClient:
        mock_client = MagicMock()
        MockClient.return_value = mock_client

        await get_graph_client_for_sec_ingestion()

        MockClient.assert_called_once_with(
          base_url="http://localhost:8001", api_key="dev-key"
        )
        assert mock_client._route_target == RouteTarget.SHARED_MASTER.value
        assert mock_client._purpose == "sec_ingestion"

  @pytest.mark.asyncio
  async def test_get_graph_client_for_sec_ingestion_prod(self):
    """SEC ingestion in prod discovers shared master."""
    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.is_development.return_value = False
      mock_env.GRAPH_API_KEY = "prod-key"

      with (
        patch.object(
          GraphClientFactory,
          "_get_shared_master_url",
          return_value="http://master.prod.com",
        ),
        patch(f"{FACTORY_MODULE}.GraphClient") as MockClient,
      ):
        mock_client = MagicMock()
        MockClient.return_value = mock_client

        await get_graph_client_for_sec_ingestion()

        MockClient.assert_called_once_with(
          base_url="http://master.prod.com", api_key="prod-key"
        )

  @pytest.mark.asyncio
  async def test_sec_ingestion_prod_fallback_on_discovery_failure(self):
    """SEC ingestion falls back to GRAPH_API_URL on discovery failure."""
    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.is_development.return_value = False
      mock_env.GRAPH_API_URL = "http://fallback:8001"
      mock_env.GRAPH_API_KEY = "key"

      with (
        patch.object(
          GraphClientFactory,
          "_get_shared_master_url",
          side_effect=Exception("discovery failed"),
        ),
        patch(f"{FACTORY_MODULE}.GraphClient") as MockClient,
      ):
        mock_client = MagicMock()
        MockClient.return_value = mock_client

        await get_graph_client_for_sec_ingestion()

        MockClient.assert_called_once_with(
          base_url="http://fallback:8001", api_key="key"
        )

  @pytest.mark.asyncio
  async def test_sec_ingestion_prod_raises_when_no_fallback(self):
    """SEC ingestion raises when no master and no fallback URL."""
    with patch(f"{FACTORY_MODULE}.env") as mock_env:
      mock_env.is_development.return_value = False
      mock_env.GRAPH_API_URL = None
      mock_env.GRAPH_API_KEY = "key"

      with patch.object(
        GraphClientFactory,
        "_get_shared_master_url",
        side_effect=Exception("discovery failed"),
      ):
        with pytest.raises(ServiceUnavailableError, match="Cannot find shared master"):
          await get_graph_client_for_sec_ingestion()


# ---------------------------------------------------------------------------
# Exception classes
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFactoryExceptions:
  """Tests for factory exception hierarchy."""

  def test_graph_client_error_is_base(self):
    """GraphClientError is the base exception."""
    from robosystems.graph_api.client.factory import GraphClientError

    error = GraphClientError("test")
    assert isinstance(error, Exception)

  def test_service_unavailable_is_graph_client_error(self):
    """ServiceUnavailableError inherits from GraphClientError."""
    error = ServiceUnavailableError("unavailable")
    from robosystems.graph_api.client.factory import GraphClientError

    assert isinstance(error, GraphClientError)

  def test_configuration_error_is_graph_client_error(self):
    """ConfigurationError inherits from GraphClientError."""
    error = ConfigurationError("bad config")
    from robosystems.graph_api.client.factory import GraphClientError

    assert isinstance(error, GraphClientError)

  def test_route_error_is_graph_client_error(self):
    """RouteError inherits from GraphClientError."""
    error = RouteError("no route")
    from robosystems.graph_api.client.factory import GraphClientError

    assert isinstance(error, GraphClientError)


# ---------------------------------------------------------------------------
# RouteTarget enum
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRouteTarget:
  """Tests for RouteTarget enum."""

  def test_user_graph_value(self):
    assert RouteTarget.USER_GRAPH.value == "user_graph"

  def test_shared_master_value(self):
    assert RouteTarget.SHARED_MASTER.value == "shared_master"

  def test_shared_replica_value(self):
    assert RouteTarget.SHARED_REPLICA.value == "shared_replica"
