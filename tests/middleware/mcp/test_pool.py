"""Comprehensive unit tests for MCP connection pool module."""

import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.middleware.mcp.pool import (
  MCPConnectionPool,
  get_connection_pool,
  initialize_pool,
  shutdown_pool,
)


@pytest.mark.unit
class TestMCPConnectionPoolInit:
  """Tests for MCPConnectionPool initialization."""

  def test_init_with_defaults(self):
    """Test pool initializes with TuningConfig defaults."""
    with (
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_idle_timeout",
        return_value=300,
      ),
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_max_lifetime",
        return_value=3600,
      ),
    ):
      pool = MCPConnectionPool()

    assert pool.max_connections_per_graph == 10
    assert pool.max_idle_time == 300
    assert pool.max_lifetime == 3600
    assert pool._pools == {}
    assert pool._running is False

  def test_init_with_custom_values(self):
    """Test pool initializes with custom values overriding defaults."""
    with (
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_idle_timeout",
        return_value=300,
      ),
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_max_lifetime",
        return_value=3600,
      ),
    ):
      pool = MCPConnectionPool(
        max_connections_per_graph=5,
        max_idle_time=120,
        max_lifetime=600,
      )

    assert pool.max_connections_per_graph == 5
    assert pool.max_idle_time == 120
    assert pool.max_lifetime == 600


@pytest.mark.unit
class TestMCPConnectionPoolLifecycle:
  """Tests for pool start/stop lifecycle."""

  @pytest.fixture
  def pool(self):
    with (
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_idle_timeout",
        return_value=300,
      ),
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_max_lifetime",
        return_value=3600,
      ),
    ):
      return MCPConnectionPool(max_connections_per_graph=3)

  @pytest.mark.asyncio
  async def test_start_sets_running(self, pool):
    """Test that start() sets _running to True."""
    await pool.start()
    assert pool._running is True
    assert pool._cleanup_task is not None
    await pool.stop()

  @pytest.mark.asyncio
  async def test_start_idempotent(self, pool):
    """Test that calling start() twice does not create duplicate tasks."""
    await pool.start()
    first_task = pool._cleanup_task
    await pool.start()
    assert pool._cleanup_task is first_task
    await pool.stop()

  @pytest.mark.asyncio
  async def test_stop_sets_not_running(self, pool):
    """Test that stop() sets _running to False and cancels cleanup."""
    await pool.start()
    await pool.stop()
    assert pool._running is False

  @pytest.mark.asyncio
  async def test_stop_closes_all_connections(self, pool):
    """Test that stop() closes all pooled connections."""
    mock_client = AsyncMock()
    mock_client.close = AsyncMock()

    # Manually add a connection to the pool
    pool._pools["graph1"] = [(mock_client, datetime.now(), datetime.now())]
    pool._locks["graph1"] = asyncio.Lock()

    await pool.stop()

    mock_client.close.assert_called_once()
    assert "graph1" not in pool._pools


@pytest.mark.unit
class TestMCPConnectionPoolAcquire:
  """Tests for acquire() context manager."""

  @pytest.fixture
  def pool(self):
    with (
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_idle_timeout",
        return_value=300,
      ),
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_max_lifetime",
        return_value=3600,
      ),
    ):
      return MCPConnectionPool(max_connections_per_graph=3)

  @pytest.mark.asyncio
  async def test_acquire_creates_new_client_when_pool_empty(self, pool):
    """Test that acquire creates new client when pool has no connections."""
    mock_client = AsyncMock()

    with patch(
      "robosystems.middleware.mcp.factory.create_graph_mcp_client",
      new_callable=lambda: AsyncMock(return_value=mock_client),
    ):
      async with pool.acquire("graph1") as client:
        assert client is mock_client

  @pytest.mark.asyncio
  async def test_acquire_reuses_pooled_connection(self, pool):
    """Test that acquire returns an existing connection from the pool."""
    mock_client = MagicMock()
    now = datetime.now()
    pool._pools["graph1"] = [(mock_client, now, now)]
    pool._locks["graph1"] = asyncio.Lock()

    async with pool.acquire("graph1") as client:
      assert client is mock_client

  @pytest.mark.asyncio
  async def test_acquire_returns_client_to_pool(self, pool):
    """Test that acquire returns client to pool after use."""
    mock_client = AsyncMock()

    with patch(
      "robosystems.middleware.mcp.factory.create_graph_mcp_client",
      new_callable=lambda: AsyncMock(return_value=mock_client),
    ):
      async with pool.acquire("graph1"):
        pass

    assert len(pool._pools.get("graph1", [])) == 1

  @pytest.mark.asyncio
  async def test_acquire_closes_client_when_pool_full(self, pool):
    """Test that excess clients are closed when pool is full on return."""
    now = datetime.now()
    # Fill pool to max (3) - acquire will pop one, reducing to 2
    popped_client = AsyncMock()
    popped_client.close = AsyncMock()
    pool._pools["graph1"] = [
      (popped_client, now, now),
      (MagicMock(), now, now),
      (MagicMock(), now, now),
    ]
    pool._locks["graph1"] = asyncio.Lock()

    async with pool.acquire("graph1") as client:
      # Client popped from pool (pool now at 2)
      assert client is popped_client
      # Simulate pool being refilled to max while we hold this client
      pool._pools["graph1"].append((MagicMock(), now, now))
      assert len(pool._pools["graph1"]) == pool.max_connections_per_graph

    # Client should be closed because pool was full when returning
    popped_client.close.assert_called_once()

  @pytest.mark.asyncio
  async def test_acquire_with_api_base_url(self, pool):
    """Test that acquire passes api_base_url to factory."""
    mock_client = AsyncMock()

    with patch(
      "robosystems.middleware.mcp.factory.create_graph_mcp_client",
      return_value=mock_client,
    ) as mock_factory:
      async with pool.acquire("graph1", api_base_url="http://custom:8001"):
        pass

      mock_factory.assert_called_once_with("graph1", "http://custom:8001")


@pytest.mark.unit
class TestMCPConnectionPoolCleanup:
  """Tests for idle connection cleanup."""

  @pytest.fixture
  def pool(self):
    with (
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_idle_timeout",
        return_value=300,
      ),
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_max_lifetime",
        return_value=3600,
      ),
    ):
      return MCPConnectionPool(
        max_connections_per_graph=5,
        max_idle_time=60,
        max_lifetime=120,
      )

  @pytest.mark.asyncio
  async def test_cleanup_removes_idle_connections(self, pool):
    """Test that cleanup removes connections exceeding idle time."""
    mock_client = AsyncMock()
    mock_client.close = AsyncMock()

    # Add a connection that has been idle for longer than max_idle_time
    old_time = datetime.now() - timedelta(seconds=120)
    pool._pools["graph1"] = [(mock_client, old_time, datetime.now())]
    pool._locks["graph1"] = asyncio.Lock()

    await pool._cleanup_idle_connections()

    mock_client.close.assert_called_once()
    assert "graph1" not in pool._pools

  @pytest.mark.asyncio
  async def test_cleanup_removes_expired_lifetime(self, pool):
    """Test that cleanup removes connections exceeding max lifetime."""
    mock_client = AsyncMock()
    mock_client.close = AsyncMock()

    # Add a connection that has exceeded max_lifetime
    old_creation = datetime.now() - timedelta(seconds=200)
    pool._pools["graph1"] = [(mock_client, datetime.now(), old_creation)]
    pool._locks["graph1"] = asyncio.Lock()

    await pool._cleanup_idle_connections()

    mock_client.close.assert_called_once()
    assert "graph1" not in pool._pools

  @pytest.mark.asyncio
  async def test_cleanup_keeps_active_connections(self, pool):
    """Test that cleanup keeps connections within time limits."""
    mock_client = MagicMock()
    now = datetime.now()

    pool._pools["graph1"] = [(mock_client, now, now)]
    pool._locks["graph1"] = asyncio.Lock()

    await pool._cleanup_idle_connections()

    assert "graph1" in pool._pools
    assert len(pool._pools["graph1"]) == 1

  @pytest.mark.asyncio
  async def test_cleanup_handles_close_error(self, pool):
    """Test that cleanup handles errors during client close."""
    mock_client = AsyncMock()
    mock_client.close = AsyncMock(side_effect=Exception("close error"))

    old_time = datetime.now() - timedelta(seconds=120)
    pool._pools["graph1"] = [(mock_client, old_time, datetime.now())]
    pool._locks["graph1"] = asyncio.Lock()

    # Should not raise
    await pool._cleanup_idle_connections()

    mock_client.close.assert_called_once()
    assert "graph1" not in pool._pools

  @pytest.mark.asyncio
  async def test_cleanup_removes_empty_pool_entries(self, pool):
    """Test that cleanup removes graph_id entries when pool is empty."""
    mock_client = AsyncMock()
    mock_client.close = AsyncMock()

    old_time = datetime.now() - timedelta(seconds=120)
    pool._pools["graph1"] = [(mock_client, old_time, datetime.now())]
    pool._locks["graph1"] = asyncio.Lock()

    await pool._cleanup_idle_connections()

    assert "graph1" not in pool._pools
    assert "graph1" not in pool._locks

  @pytest.mark.asyncio
  async def test_cleanup_handles_client_without_close(self, pool):
    """Test cleanup handles clients without close method."""
    mock_client = MagicMock(spec=[])  # No close method

    old_time = datetime.now() - timedelta(seconds=120)
    pool._pools["graph1"] = [(mock_client, old_time, datetime.now())]
    pool._locks["graph1"] = asyncio.Lock()

    # Should not raise even without close method
    await pool._cleanup_idle_connections()
    assert "graph1" not in pool._pools


@pytest.mark.unit
class TestMCPConnectionPoolStats:
  """Tests for get_stats()."""

  @pytest.fixture
  def pool(self):
    with (
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_idle_timeout",
        return_value=300,
      ),
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_max_lifetime",
        return_value=3600,
      ),
    ):
      return MCPConnectionPool(max_connections_per_graph=5)

  @pytest.mark.asyncio
  async def test_get_stats_empty_pool(self, pool):
    """Test stats for empty pool."""
    stats = await pool.get_stats()
    assert stats == {}

  @pytest.mark.asyncio
  async def test_get_stats_with_connections(self, pool):
    """Test stats with active connections in pool."""
    mock_client = MagicMock()
    now = datetime.now()
    pool._pools["graph1"] = [(mock_client, now, now)]

    stats = await pool.get_stats()

    assert "graph1" in stats
    assert stats["graph1"]["total_connections"] == 1
    assert stats["graph1"]["max_connections"] == 5
    assert len(stats["graph1"]["connections"]) == 1
    conn_stat = stats["graph1"]["connections"][0]
    assert "idle_seconds" in conn_stat
    assert "lifetime_seconds" in conn_stat
    assert "will_expire_in" in conn_stat

  @pytest.mark.asyncio
  async def test_get_stats_multiple_graphs(self, pool):
    """Test stats with multiple graphs in pool."""
    now = datetime.now()
    pool._pools["graph1"] = [(MagicMock(), now, now)]
    pool._pools["graph2"] = [
      (MagicMock(), now, now),
      (MagicMock(), now, now),
    ]

    stats = await pool.get_stats()

    assert "graph1" in stats
    assert "graph2" in stats
    assert stats["graph1"]["total_connections"] == 1
    assert stats["graph2"]["total_connections"] == 2


@pytest.mark.unit
class TestMCPConnectionPoolSingleton:
  """Tests for singleton pattern functions."""

  @pytest.fixture(autouse=True)
  def reset_singleton(self):
    import robosystems.middleware.mcp.pool as pool_mod

    pool_mod._global_pool = None
    yield
    pool_mod._global_pool = None

  def test_get_connection_pool_returns_instance(self):
    """Test that get_connection_pool returns an MCPConnectionPool."""
    with (
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_idle_timeout",
        return_value=300,
      ),
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_max_lifetime",
        return_value=3600,
      ),
    ):
      pool = get_connection_pool()
    assert isinstance(pool, MCPConnectionPool)

  def test_get_connection_pool_returns_same_instance(self):
    """Test that get_connection_pool returns the same singleton."""
    with (
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_idle_timeout",
        return_value=300,
      ),
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_max_lifetime",
        return_value=3600,
      ),
    ):
      pool1 = get_connection_pool()
      pool2 = get_connection_pool()
    assert pool1 is pool2

  @pytest.mark.asyncio
  async def test_initialize_pool_starts_pool(self):
    """Test that initialize_pool starts the global pool."""
    with (
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_idle_timeout",
        return_value=300,
      ),
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_max_lifetime",
        return_value=3600,
      ),
    ):
      await initialize_pool()
      pool = get_connection_pool()
    assert pool._running is True
    await pool.stop()

  @pytest.mark.asyncio
  async def test_shutdown_pool_stops_pool(self):
    """Test that shutdown_pool stops the global pool."""
    with (
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_idle_timeout",
        return_value=300,
      ),
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_max_lifetime",
        return_value=3600,
      ),
    ):
      await initialize_pool()
      await shutdown_pool()
      pool = get_connection_pool()
    assert pool._running is False


@pytest.mark.unit
class TestMCPConnectionPoolClosePool:
  """Tests for _close_pool method."""

  @pytest.fixture
  def pool(self):
    with (
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_idle_timeout",
        return_value=300,
      ),
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_max_lifetime",
        return_value=3600,
      ),
    ):
      return MCPConnectionPool(max_connections_per_graph=5)

  @pytest.mark.asyncio
  async def test_close_pool_closes_all_clients(self, pool):
    """Test that _close_pool closes all clients in a graph's pool."""
    mock_client1 = AsyncMock()
    mock_client2 = AsyncMock()
    now = datetime.now()

    pool._pools["graph1"] = [
      (mock_client1, now, now),
      (mock_client2, now, now),
    ]
    pool._locks["graph1"] = asyncio.Lock()

    await pool._close_pool("graph1")

    mock_client1.close.assert_called_once()
    mock_client2.close.assert_called_once()
    assert "graph1" not in pool._pools

  @pytest.mark.asyncio
  async def test_close_pool_handles_close_error(self, pool):
    """Test that _close_pool handles close errors gracefully."""
    mock_client = AsyncMock()
    mock_client.close = AsyncMock(side_effect=Exception("close failed"))
    now = datetime.now()

    pool._pools["graph1"] = [(mock_client, now, now)]
    pool._locks["graph1"] = asyncio.Lock()

    # Should not raise
    await pool._close_pool("graph1")
    assert "graph1" not in pool._pools

  @pytest.mark.asyncio
  async def test_close_pool_empty(self, pool):
    """Test that _close_pool on non-existent graph is a no-op."""
    pool._locks["graph1"] = asyncio.Lock()
    await pool._close_pool("graph1")

  @pytest.mark.asyncio
  async def test_close_pool_client_without_close(self, pool):
    """Test _close_pool with clients that lack close method."""
    mock_client = MagicMock(spec=[])  # No close method
    now = datetime.now()
    pool._pools["graph1"] = [(mock_client, now, now)]
    pool._locks["graph1"] = asyncio.Lock()

    # Should not raise
    await pool._close_pool("graph1")
    assert "graph1" not in pool._pools


@pytest.mark.unit
class TestMCPConnectionPoolGetLock:
  """Tests for _get_lock method."""

  @pytest.fixture
  def pool(self):
    with (
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_idle_timeout",
        return_value=300,
      ),
      patch(
        "robosystems.middleware.mcp.pool.TuningConfig.get_mcp_pool_max_lifetime",
        return_value=3600,
      ),
    ):
      return MCPConnectionPool()

  def test_get_lock_creates_new_lock(self, pool):
    """Test that _get_lock creates a new lock for unknown graph_id."""
    lock = pool._get_lock("new_graph")
    assert isinstance(lock, asyncio.Lock)

  def test_get_lock_returns_same_lock(self, pool):
    """Test that _get_lock returns the same lock for same graph_id."""
    lock1 = pool._get_lock("graph1")
    lock2 = pool._get_lock("graph1")
    assert lock1 is lock2
