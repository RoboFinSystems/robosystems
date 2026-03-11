"""Comprehensive tests for graph memory management utilities.

Covers DuckDB/LadybugDB memory boost context managers, ensure/restore idempotency,
release helpers, subgraph eviction logic, double-boosting prevention, and tracking
set management.
"""

from unittest.mock import MagicMock, patch

import pytest

MODULE = "robosystems.graph_api.core.memory_manager"


@pytest.fixture(autouse=True)
def clear_boost_state():
  """Clear module-level boost tracking between tests."""
  from robosystems.graph_api.core.memory_manager import (
    _active_duckdb_boosts,
    _active_ladybug_boosts,
  )

  _active_duckdb_boosts.clear()
  _active_ladybug_boosts.clear()
  yield
  _active_duckdb_boosts.clear()
  _active_ladybug_boosts.clear()


# ---------------------------------------------------------------------------
# is_*_memory_boosted status checks
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestIsBoosted:
  """Tests for is_duckdb_memory_boosted and is_ladybug_memory_boosted."""

  def test_is_duckdb_memory_boosted_false_by_default(self):
    from robosystems.graph_api.core.memory_manager import is_duckdb_memory_boosted

    assert is_duckdb_memory_boosted("test") is False

  def test_is_duckdb_memory_boosted_true_when_tracked(self):
    from robosystems.graph_api.core.memory_manager import (
      _active_duckdb_boosts,
      is_duckdb_memory_boosted,
    )

    _active_duckdb_boosts.add("test_graph")
    assert is_duckdb_memory_boosted("test_graph") is True

  def test_is_duckdb_memory_boosted_false_for_different_graph(self):
    from robosystems.graph_api.core.memory_manager import (
      _active_duckdb_boosts,
      is_duckdb_memory_boosted,
    )

    _active_duckdb_boosts.add("other_graph")
    assert is_duckdb_memory_boosted("test_graph") is False

  def test_is_ladybug_memory_boosted_false_by_default(self):
    from robosystems.graph_api.core.memory_manager import (
      is_ladybug_memory_boosted,
    )

    assert is_ladybug_memory_boosted("test") is False

  def test_is_ladybug_memory_boosted_true_when_tracked(self):
    from robosystems.graph_api.core.memory_manager import (
      _active_ladybug_boosts,
      is_ladybug_memory_boosted,
    )

    _active_ladybug_boosts.add("test_graph")
    assert is_ladybug_memory_boosted("test_graph") is True


# ---------------------------------------------------------------------------
# _evict_idle_subgraph_databases
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestEvictIdleSubgraphDatabases:
  """Tests for _evict_idle_subgraph_databases."""

  def test_evicts_idle_subgraphs(self):
    """Idle subgraphs are evicted; primary databases are not."""
    from robosystems.graph_api.core.memory_manager import (
      _evict_idle_subgraph_databases,
    )

    mock_pool = MagicMock()
    mock_pool.list_databases.return_value = [
      "sec",
      "sec_historical",
      "sec_quarterly",
    ]
    mock_pool.has_active_connections.return_value = False

    evicted = _evict_idle_subgraph_databases(mock_pool, target_graph_id="sec")

    assert "sec_historical" in evicted
    assert "sec_quarterly" in evicted
    assert "sec" not in evicted

  def test_skips_target_graph(self):
    """The target graph is never evicted even if it looks like a subgraph."""
    from robosystems.graph_api.core.memory_manager import (
      _evict_idle_subgraph_databases,
    )

    mock_pool = MagicMock()
    mock_pool.list_databases.return_value = [
      "sec",
      "sec_historical",
      "sec_quarterly",
    ]
    mock_pool.has_active_connections.return_value = False

    evicted = _evict_idle_subgraph_databases(
      mock_pool, target_graph_id="sec_historical"
    )

    assert "sec_historical" not in evicted
    assert "sec_quarterly" in evicted

  def test_skips_active_connections(self):
    """Subgraphs with active connections are not evicted."""
    from robosystems.graph_api.core.memory_manager import (
      _evict_idle_subgraph_databases,
    )

    mock_pool = MagicMock()
    mock_pool.list_databases.return_value = [
      "sec",
      "sec_historical",
      "sec_quarterly",
    ]
    mock_pool.has_active_connections.side_effect = lambda db: db == "sec_historical"

    evicted = _evict_idle_subgraph_databases(mock_pool, target_graph_id="sec")

    assert "sec_historical" not in evicted
    assert "sec_quarterly" in evicted

  def test_skips_non_subgraph_databases(self):
    """Databases without underscores are not evicted."""
    from robosystems.graph_api.core.memory_manager import (
      _evict_idle_subgraph_databases,
    )

    mock_pool = MagicMock()
    mock_pool.list_databases.return_value = ["sec", "kg123", "industry"]
    mock_pool.has_active_connections.return_value = False

    evicted = _evict_idle_subgraph_databases(mock_pool, target_graph_id="sec")

    assert evicted == []

  def test_handles_close_error_gracefully(self):
    """Errors during close_database_connections are handled."""
    from robosystems.graph_api.core.memory_manager import (
      _evict_idle_subgraph_databases,
    )

    mock_pool = MagicMock()
    mock_pool.list_databases.return_value = ["sec_historical"]
    mock_pool.has_active_connections.return_value = False
    mock_pool.close_database_connections.side_effect = RuntimeError("close failed")

    evicted = _evict_idle_subgraph_databases(mock_pool, target_graph_id="sec")

    assert evicted == []

  def test_database_still_active_after_eviction_attempt(self):
    """Database with connections after eviction attempt is not counted."""
    from robosystems.graph_api.core.memory_manager import (
      _evict_idle_subgraph_databases,
    )

    mock_pool = MagicMock()
    mock_pool.list_databases.return_value = ["sec_historical"]
    # First call returns False (no active), second returns True (still active after close)
    mock_pool.has_active_connections.side_effect = [False, True]

    evicted = _evict_idle_subgraph_databases(mock_pool, target_graph_id="sec")

    assert evicted == []

  def test_empty_database_list(self):
    """No databases to evict returns empty list."""
    from robosystems.graph_api.core.memory_manager import (
      _evict_idle_subgraph_databases,
    )

    mock_pool = MagicMock()
    mock_pool.list_databases.return_value = []

    evicted = _evict_idle_subgraph_databases(mock_pool, target_graph_id="sec")

    assert evicted == []


# ---------------------------------------------------------------------------
# release_duckdb_memory
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestReleaseDuckdbMemory:
  """Tests for release_duckdb_memory."""

  @patch(f"{MODULE}.get_duckdb_pool", create=True)
  def test_release_success(self, _mock_import):
    """Closing pool connections returns correct count."""
    from robosystems.graph_api.core.memory_manager import release_duckdb_memory

    mock_pool = MagicMock()
    mock_pool._pools = {"test_graph": [1, 2, 3]}

    with patch(
      "robosystems.graph_api.core.duckdb.pool.get_duckdb_pool",
      return_value=mock_pool,
    ):
      result = release_duckdb_memory("test_graph")

    assert result["connections_closed"] == 3
    assert result["success"] is True

  def test_release_zero_connections(self):
    """Graph not in pool returns zero connections."""
    from robosystems.graph_api.core.memory_manager import release_duckdb_memory

    mock_pool = MagicMock()
    mock_pool._pools = {}

    with patch(
      "robosystems.graph_api.core.duckdb.pool.get_duckdb_pool",
      return_value=mock_pool,
    ):
      result = release_duckdb_memory("test_graph")

    assert result["connections_closed"] == 0
    assert result["success"] is True

  def test_release_error(self):
    """Exception during release is caught and returns success=False."""
    from robosystems.graph_api.core.memory_manager import release_duckdb_memory

    with patch(
      "robosystems.graph_api.core.duckdb.pool.get_duckdb_pool",
      side_effect=Exception("pool error"),
    ):
      result = release_duckdb_memory("test_graph")

    assert result["success"] is False
    assert "error" in result


# ---------------------------------------------------------------------------
# release_ladybug_memory
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestReleaseLadybugMemory:
  """Tests for release_ladybug_memory."""

  def test_release_success(self):
    """Successful release returns success=True."""
    from robosystems.graph_api.core.memory_manager import release_ladybug_memory

    mock_pool = MagicMock()

    with patch(
      "robosystems.graph_api.core.ladybug.pool.get_connection_pool",
      return_value=mock_pool,
    ):
      result = release_ladybug_memory("test_graph")

    assert result["success"] is True
    assert result["aggressive"] is True

  def test_release_non_aggressive(self):
    """Non-aggressive release passes aggressive=False."""
    from robosystems.graph_api.core.memory_manager import release_ladybug_memory

    mock_pool = MagicMock()

    with patch(
      "robosystems.graph_api.core.ladybug.pool.get_connection_pool",
      return_value=mock_pool,
    ):
      result = release_ladybug_memory("test_graph", aggressive=False)

    assert result["aggressive"] is False
    mock_pool.force_database_cleanup.assert_called_once_with(
      "test_graph", aggressive=False
    )

  def test_release_error(self):
    """Exception during release returns success=False."""
    from robosystems.graph_api.core.memory_manager import release_ladybug_memory

    with patch(
      "robosystems.graph_api.core.ladybug.pool.get_connection_pool",
      side_effect=Exception("pool error"),
    ):
      result = release_ladybug_memory("test_graph")

    assert result["success"] is False
    assert "error" in result


# ---------------------------------------------------------------------------
# boost_duckdb_memory context manager
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestBoostDuckdbMemoryContextManager:
  """Tests for the boost_duckdb_memory context manager."""

  def test_boost_with_tier(self):
    """When tier is configured, boost is applied and restored."""
    from robosystems.graph_api.core.memory_manager import boost_duckdb_memory

    mock_pool = MagicMock()

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
      patch(
        "robosystems.graph_api.core.duckdb.pool.set_duckdb_memory_override",
        return_value=None,
      ) as mock_set_override,
      patch(
        "robosystems.graph_api.core.duckdb.pool.get_duckdb_pool",
        return_value=mock_pool,
      ),
    ):
      mock_env.CLUSTER_TIER = "ladybug-shared"
      mock_tier_config.get_duckdb_memory_boost.return_value = "55GB"
      mock_tier_config.get_duckdb_memory_limit.return_value = "10GB"

      with boost_duckdb_memory("sec") as boost:
        assert boost == "55GB"
        mock_set_override.assert_called_with("55GB", "sec")

      # After context exit, override is restored
      assert mock_set_override.call_count == 2
      mock_set_override.assert_called_with(None, "sec")

  def test_no_boost_when_no_tier(self):
    """When CLUSTER_TIER is None, context manager yields None."""
    from robosystems.graph_api.core.memory_manager import boost_duckdb_memory

    with patch(f"{MODULE}.env") as mock_env:
      mock_env.CLUSTER_TIER = None

      with boost_duckdb_memory("test") as boost:
        assert boost is None

  def test_no_boost_when_boost_limit_is_none(self):
    """When boost limit is None, context manager yields None."""
    from robosystems.graph_api.core.memory_manager import boost_duckdb_memory

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
    ):
      mock_env.CLUSTER_TIER = "ladybug-standard"
      mock_tier_config.get_duckdb_memory_boost.return_value = None

      with boost_duckdb_memory("test") as boost:
        assert boost is None

  def test_boost_reconfigures_pool_on_error(self):
    """Pool reconfigure error is caught and logged."""
    from robosystems.graph_api.core.memory_manager import boost_duckdb_memory

    mock_pool = MagicMock()
    mock_pool.reconfigure_memory_limit.side_effect = RuntimeError("pool error")

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
      patch(
        "robosystems.graph_api.core.duckdb.pool.set_duckdb_memory_override",
        return_value=None,
      ),
      patch(
        "robosystems.graph_api.core.duckdb.pool.get_duckdb_pool",
        return_value=mock_pool,
      ),
    ):
      mock_env.CLUSTER_TIER = "ladybug-shared"
      mock_tier_config.get_duckdb_memory_boost.return_value = "55GB"
      mock_tier_config.get_duckdb_memory_limit.return_value = "10GB"

      # Should not raise despite pool error
      with boost_duckdb_memory("sec") as boost:
        assert boost == "55GB"


# ---------------------------------------------------------------------------
# boost_ladybug_memory context manager
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestBoostLadybugMemoryContextManager:
  """Tests for the boost_ladybug_memory context manager."""

  def test_boost_with_tier(self):
    """When tier is configured, LadybugDB boost is applied and restored."""
    from robosystems.graph_api.core.memory_manager import boost_ladybug_memory

    mock_pool = MagicMock()
    mock_pool.list_databases.return_value = []

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
      patch(
        "robosystems.graph_api.core.ladybug.config.set_ladybug_memory_override",
        return_value=None,
      ) as mock_set_override,
      patch(
        "robosystems.graph_api.core.ladybug.pool.get_connection_pool",
        return_value=mock_pool,
      ),
      patch(f"{MODULE}.release_duckdb_memory"),
    ):
      mock_env.CLUSTER_TIER = "ladybug-shared"
      mock_tier_config.get_ladybug_memory_boost_mb.return_value = 50000

      with boost_ladybug_memory("sec") as boost:
        assert boost == 50000
        mock_set_override.assert_called_with(50000, graph_id="sec")

      # Restored after exit
      assert mock_set_override.call_count == 2
      mock_set_override.assert_called_with(None, graph_id="sec")
      # Database should be recreated twice (boost + restore)
      assert mock_pool.recreate_database.call_count == 2

  def test_no_boost_when_no_tier(self):
    """When CLUSTER_TIER is None, yields None."""
    from robosystems.graph_api.core.memory_manager import boost_ladybug_memory

    with patch(f"{MODULE}.env") as mock_env:
      mock_env.CLUSTER_TIER = None

      with boost_ladybug_memory("test") as boost:
        assert boost is None

  def test_no_boost_when_boost_mb_is_none(self):
    """When boost MB is None, yields None."""
    from robosystems.graph_api.core.memory_manager import boost_ladybug_memory

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
    ):
      mock_env.CLUSTER_TIER = "ladybug-standard"
      mock_tier_config.get_ladybug_memory_boost_mb.return_value = None

      with boost_ladybug_memory("test") as boost:
        assert boost is None

  def test_boost_evicts_idle_subgraphs(self):
    """Boost evicts idle subgraph databases before boosting."""
    from robosystems.graph_api.core.memory_manager import boost_ladybug_memory

    mock_pool = MagicMock()
    mock_pool.list_databases.return_value = ["sec_historical", "sec_quarterly"]
    mock_pool.has_active_connections.return_value = False

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
      patch(
        "robosystems.graph_api.core.ladybug.config.set_ladybug_memory_override",
        return_value=None,
      ),
      patch(
        "robosystems.graph_api.core.ladybug.pool.get_connection_pool",
        return_value=mock_pool,
      ),
      patch(f"{MODULE}.release_duckdb_memory"),
    ):
      mock_env.CLUSTER_TIER = "ladybug-shared"
      mock_tier_config.get_ladybug_memory_boost_mb.return_value = 50000

      with boost_ladybug_memory("sec") as boost:
        assert boost == 50000

      # Should have called close_database_connections for subgraphs
      assert mock_pool.close_database_connections.call_count >= 1

  def test_duckdb_release_error_is_caught(self):
    """Error releasing DuckDB memory before boost does not block."""
    from robosystems.graph_api.core.memory_manager import boost_ladybug_memory

    mock_pool = MagicMock()
    mock_pool.list_databases.return_value = []

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
      patch(
        "robosystems.graph_api.core.ladybug.config.set_ladybug_memory_override",
        return_value=None,
      ),
      patch(
        "robosystems.graph_api.core.ladybug.pool.get_connection_pool",
        return_value=mock_pool,
      ),
      patch(
        f"{MODULE}.release_duckdb_memory",
        side_effect=RuntimeError("release error"),
      ),
    ):
      mock_env.CLUSTER_TIER = "ladybug-shared"
      mock_tier_config.get_ladybug_memory_boost_mb.return_value = 50000

      with boost_ladybug_memory("sec") as boost:
        assert boost == 50000


# ---------------------------------------------------------------------------
# Async wrapper context managers
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestAsyncBoostWrappers:
  """Tests for async versions of boost context managers."""

  @pytest.mark.asyncio
  async def test_boost_duckdb_memory_async(self):
    """Async wrapper delegates to sync context manager."""
    from robosystems.graph_api.core.memory_manager import (
      boost_duckdb_memory_async,
    )

    with patch(f"{MODULE}.env") as mock_env:
      mock_env.CLUSTER_TIER = None

      async with boost_duckdb_memory_async("test") as boost:
        assert boost is None

  @pytest.mark.asyncio
  async def test_boost_ladybug_memory_async(self):
    """Async wrapper delegates to sync context manager."""
    from robosystems.graph_api.core.memory_manager import (
      boost_ladybug_memory_async,
    )

    with patch(f"{MODULE}.env") as mock_env:
      mock_env.CLUSTER_TIER = None

      async with boost_ladybug_memory_async("test") as boost:
        assert boost is None


# ---------------------------------------------------------------------------
# ensure_duckdb_memory_boosted (idempotent)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestEnsureDuckdbMemoryBoosted:
  """Tests for ensure_duckdb_memory_boosted (idempotent boost)."""

  def test_boost_on_first_call(self):
    """First call with a valid tier applies the boost and returns the limit."""
    from robosystems.graph_api.core.memory_manager import (
      ensure_duckdb_memory_boosted,
    )

    mock_pool = MagicMock()

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
      patch(
        "robosystems.graph_api.core.duckdb.pool.set_duckdb_memory_override",
      ) as mock_set_override,
      patch(
        "robosystems.graph_api.core.duckdb.pool.get_duckdb_memory_override",
        return_value=None,
      ),
      patch(
        "robosystems.graph_api.core.duckdb.pool.get_duckdb_pool",
        return_value=mock_pool,
      ),
    ):
      mock_env.CLUSTER_TIER = "ladybug-shared"
      mock_tier_config.get_duckdb_memory_boost.return_value = "55GB"

      result = ensure_duckdb_memory_boosted("test_graph")

    assert result == "55GB"
    mock_set_override.assert_called_once_with("55GB", "test_graph")

  def test_already_boosted_returns_none(self):
    """Returns None when override is already active for this graph."""
    from robosystems.graph_api.core.memory_manager import (
      _active_duckdb_boosts,
      ensure_duckdb_memory_boosted,
    )

    _active_duckdb_boosts.discard("test_graph")

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
      patch(
        "robosystems.graph_api.core.duckdb.pool.get_duckdb_memory_override",
        return_value="55GB",
      ),
    ):
      mock_env.CLUSTER_TIER = "ladybug-shared"
      mock_tier_config.get_duckdb_memory_boost.return_value = "55GB"

      result = ensure_duckdb_memory_boosted("test_graph")

    assert result is None
    assert "test_graph" in _active_duckdb_boosts

  def test_no_tier_returns_none(self):
    """Returns None when CLUSTER_TIER is not set."""
    from robosystems.graph_api.core.memory_manager import (
      ensure_duckdb_memory_boosted,
    )

    with patch(f"{MODULE}.env") as mock_env:
      mock_env.CLUSTER_TIER = None

      result = ensure_duckdb_memory_boosted("test_graph")

    assert result is None

  def test_no_boost_limit_returns_none(self):
    """Returns None when no boost limit configured for tier."""
    from robosystems.graph_api.core.memory_manager import (
      ensure_duckdb_memory_boosted,
    )

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
    ):
      mock_env.CLUSTER_TIER = "ladybug-standard"
      mock_tier_config.get_duckdb_memory_boost.return_value = None

      result = ensure_duckdb_memory_boosted("test_graph")

    assert result is None

  def test_override_already_active_adds_to_tracking(self):
    """When override is already active externally, adds to tracking set."""
    from robosystems.graph_api.core.memory_manager import (
      _active_duckdb_boosts,
      ensure_duckdb_memory_boosted,
    )

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
      patch(
        "robosystems.graph_api.core.duckdb.pool.get_duckdb_memory_override",
        return_value="55GB",  # Already active
      ),
    ):
      mock_env.CLUSTER_TIER = "ladybug-shared"
      mock_tier_config.get_duckdb_memory_boost.return_value = "55GB"

      result = ensure_duckdb_memory_boosted("test_graph")

    assert result is None
    assert "test_graph" in _active_duckdb_boosts

  def test_pool_reconfigure_error_caught(self):
    """Pool reconfigure error does not prevent boost."""
    from robosystems.graph_api.core.memory_manager import (
      ensure_duckdb_memory_boosted,
    )

    mock_pool = MagicMock()
    mock_pool.reconfigure_memory_limit.side_effect = RuntimeError("pool error")

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
      patch(
        "robosystems.graph_api.core.duckdb.pool.set_duckdb_memory_override",
      ),
      patch(
        "robosystems.graph_api.core.duckdb.pool.get_duckdb_memory_override",
        return_value=None,
      ),
      patch(
        "robosystems.graph_api.core.duckdb.pool.get_duckdb_pool",
        return_value=mock_pool,
      ),
    ):
      mock_env.CLUSTER_TIER = "ladybug-shared"
      mock_tier_config.get_duckdb_memory_boost.return_value = "55GB"

      result = ensure_duckdb_memory_boosted("test_graph")

    assert result == "55GB"


# ---------------------------------------------------------------------------
# restore_duckdb_memory
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRestoreDuckdbMemory:
  """Tests for restore_duckdb_memory."""

  def test_restore_success(self):
    """Restoring a boosted graph removes it from active set and returns True."""
    from robosystems.graph_api.core.memory_manager import (
      _active_duckdb_boosts,
      restore_duckdb_memory,
    )

    _active_duckdb_boosts.add("test_graph")

    mock_pool = MagicMock()

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
      patch(
        "robosystems.graph_api.core.duckdb.pool.set_duckdb_memory_override",
      ) as mock_set_override,
      patch(
        "robosystems.graph_api.core.duckdb.pool.get_duckdb_pool",
        return_value=mock_pool,
      ),
    ):
      mock_env.CLUSTER_TIER = "ladybug-shared"
      mock_tier_config.get_duckdb_memory_limit.return_value = "10GB"

      result = restore_duckdb_memory("test_graph")

    assert result is True
    assert "test_graph" not in _active_duckdb_boosts
    mock_set_override.assert_called_once_with(None, "test_graph")

  def test_not_boosted_returns_false(self):
    """Returns False when the graph was not boosted."""
    from robosystems.graph_api.core.memory_manager import restore_duckdb_memory

    result = restore_duckdb_memory("test_graph")
    assert result is False

  def test_restore_pool_error_returns_false(self):
    """Pool error during restore returns False."""
    from robosystems.graph_api.core.memory_manager import (
      _active_duckdb_boosts,
      restore_duckdb_memory,
    )

    _active_duckdb_boosts.add("test_graph")

    mock_pool = MagicMock()
    mock_pool.reconfigure_memory_limit.side_effect = RuntimeError("pool error")

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
      patch(
        "robosystems.graph_api.core.duckdb.pool.set_duckdb_memory_override",
      ),
      patch(
        "robosystems.graph_api.core.duckdb.pool.get_duckdb_pool",
        return_value=mock_pool,
      ),
    ):
      mock_env.CLUSTER_TIER = "ladybug-shared"
      mock_tier_config.get_duckdb_memory_limit.return_value = "10GB"

      result = restore_duckdb_memory("test_graph")

    assert result is False

  def test_restore_no_tier_uses_default(self):
    """When CLUSTER_TIER is None, uses 2GB default."""
    from robosystems.graph_api.core.memory_manager import (
      _active_duckdb_boosts,
      restore_duckdb_memory,
    )

    _active_duckdb_boosts.add("test_graph")

    mock_pool = MagicMock()

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(
        "robosystems.graph_api.core.duckdb.pool.set_duckdb_memory_override",
      ),
      patch(
        "robosystems.graph_api.core.duckdb.pool.get_duckdb_pool",
        return_value=mock_pool,
      ),
    ):
      mock_env.CLUSTER_TIER = None

      result = restore_duckdb_memory("test_graph")

    assert result is True
    mock_pool.reconfigure_memory_limit.assert_called_once_with("test_graph", "2GB")


# ---------------------------------------------------------------------------
# ensure_ladybug_memory_boosted (idempotent)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestEnsureLadybugMemoryBoosted:
  """Tests for ensure_ladybug_memory_boosted (idempotent boost)."""

  def test_boost_on_first_call(self):
    """First call with valid tier applies boost and returns MB."""
    from robosystems.graph_api.core.memory_manager import (
      _active_ladybug_boosts,
      ensure_ladybug_memory_boosted,
    )

    mock_pool = MagicMock()
    mock_pool.list_databases.return_value = []

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
      patch(
        "robosystems.graph_api.core.ladybug.config.set_ladybug_memory_override",
      ) as mock_set_override,
      patch(
        "robosystems.graph_api.core.ladybug.config.get_ladybug_memory_override",
        return_value=None,
      ),
      patch(
        "robosystems.graph_api.core.ladybug.pool.get_connection_pool",
        return_value=mock_pool,
      ),
      patch(f"{MODULE}.release_duckdb_memory", return_value={"connections_closed": 0}),
    ):
      mock_env.CLUSTER_TIER = "ladybug-shared"
      mock_tier_config.get_ladybug_memory_boost_mb.return_value = 50000

      result = ensure_ladybug_memory_boosted("sec")

    assert result == 50000
    assert "sec" in _active_ladybug_boosts
    mock_set_override.assert_called_once_with(50000, graph_id="sec")

  def test_already_boosted_returns_none(self):
    """Returns None when override is already active for this graph."""
    from robosystems.graph_api.core.memory_manager import (
      _active_ladybug_boosts,
      ensure_ladybug_memory_boosted,
    )

    _active_ladybug_boosts.discard("sec")

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
      patch(
        "robosystems.graph_api.core.ladybug.config.get_ladybug_memory_override",
        return_value=50000,
      ),
    ):
      mock_env.CLUSTER_TIER = "ladybug-shared"
      mock_tier_config.get_ladybug_memory_boost_mb.return_value = 50000

      result = ensure_ladybug_memory_boosted("sec")

    assert result is None
    assert "sec" in _active_ladybug_boosts

  def test_no_tier_returns_none(self):
    """Returns None when CLUSTER_TIER is not set."""
    from robosystems.graph_api.core.memory_manager import (
      ensure_ladybug_memory_boosted,
    )

    with patch(f"{MODULE}.env") as mock_env:
      mock_env.CLUSTER_TIER = None

      result = ensure_ladybug_memory_boosted("sec")

    assert result is None

  def test_no_boost_mb_returns_none(self):
    """Returns None when no boost configured for tier."""
    from robosystems.graph_api.core.memory_manager import (
      ensure_ladybug_memory_boosted,
    )

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
    ):
      mock_env.CLUSTER_TIER = "ladybug-standard"
      mock_tier_config.get_ladybug_memory_boost_mb.return_value = None

      result = ensure_ladybug_memory_boosted("sec")

    assert result is None

  def test_override_already_active_adds_tracking(self):
    """When override is externally active, adds to tracking set."""
    from robosystems.graph_api.core.memory_manager import (
      _active_ladybug_boosts,
      ensure_ladybug_memory_boosted,
    )

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
      patch(
        "robosystems.graph_api.core.ladybug.config.get_ladybug_memory_override",
        return_value=50000,
      ),
    ):
      mock_env.CLUSTER_TIER = "ladybug-shared"
      mock_tier_config.get_ladybug_memory_boost_mb.return_value = 50000

      result = ensure_ladybug_memory_boosted("sec")

    assert result is None
    assert "sec" in _active_ladybug_boosts

  def test_duckdb_release_error_does_not_block_boost(self):
    """DuckDB release error does not prevent LadybugDB boost."""
    from robosystems.graph_api.core.memory_manager import (
      ensure_ladybug_memory_boosted,
    )

    mock_pool = MagicMock()
    mock_pool.list_databases.return_value = []

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
      patch(
        "robosystems.graph_api.core.ladybug.config.set_ladybug_memory_override",
      ),
      patch(
        "robosystems.graph_api.core.ladybug.config.get_ladybug_memory_override",
        return_value=None,
      ),
      patch(
        "robosystems.graph_api.core.ladybug.pool.get_connection_pool",
        return_value=mock_pool,
      ),
      patch(
        f"{MODULE}.release_duckdb_memory",
        side_effect=RuntimeError("release error"),
      ),
    ):
      mock_env.CLUSTER_TIER = "ladybug-shared"
      mock_tier_config.get_ladybug_memory_boost_mb.return_value = 50000

      result = ensure_ladybug_memory_boosted("sec")

    assert result == 50000

  def test_recreate_database_error_does_not_block(self):
    """Pool recreate error does not prevent boost."""
    from robosystems.graph_api.core.memory_manager import (
      ensure_ladybug_memory_boosted,
    )

    mock_pool = MagicMock()
    mock_pool.list_databases.return_value = []
    mock_pool.recreate_database.side_effect = RuntimeError("recreate error")

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
      patch(
        "robosystems.graph_api.core.ladybug.config.set_ladybug_memory_override",
      ),
      patch(
        "robosystems.graph_api.core.ladybug.config.get_ladybug_memory_override",
        return_value=None,
      ),
      patch(
        "robosystems.graph_api.core.ladybug.pool.get_connection_pool",
        return_value=mock_pool,
      ),
      patch(f"{MODULE}.release_duckdb_memory", return_value={"connections_closed": 0}),
    ):
      mock_env.CLUSTER_TIER = "ladybug-shared"
      mock_tier_config.get_ladybug_memory_boost_mb.return_value = 50000

      result = ensure_ladybug_memory_boosted("sec")

    assert result == 50000


# ---------------------------------------------------------------------------
# restore_ladybug_memory
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRestoreLadybugMemory:
  """Tests for restore_ladybug_memory."""

  def test_restore_success(self):
    """Restoring a boosted graph removes it from tracking and returns True."""
    from robosystems.graph_api.core.memory_manager import (
      _active_ladybug_boosts,
      restore_ladybug_memory,
    )

    _active_ladybug_boosts.add("sec")

    mock_pool = MagicMock()

    with (
      patch(
        "robosystems.graph_api.core.ladybug.config.set_ladybug_memory_override",
      ) as mock_set_override,
      patch(
        "robosystems.graph_api.core.ladybug.pool.get_connection_pool",
        return_value=mock_pool,
      ),
    ):
      result = restore_ladybug_memory("sec")

    assert result is True
    assert "sec" not in _active_ladybug_boosts
    mock_set_override.assert_called_once_with(None, graph_id="sec")
    mock_pool.recreate_database.assert_called_once_with("sec")

  def test_not_boosted_returns_false(self):
    """Returns False when graph was not boosted."""
    from robosystems.graph_api.core.memory_manager import restore_ladybug_memory

    result = restore_ladybug_memory("sec")
    assert result is False

  def test_recreate_error_returns_false(self):
    """Pool recreate error returns False."""
    from robosystems.graph_api.core.memory_manager import (
      _active_ladybug_boosts,
      restore_ladybug_memory,
    )

    _active_ladybug_boosts.add("sec")

    mock_pool = MagicMock()
    mock_pool.recreate_database.side_effect = RuntimeError("recreate error")

    with (
      patch(
        "robosystems.graph_api.core.ladybug.config.set_ladybug_memory_override",
      ),
      patch(
        "robosystems.graph_api.core.ladybug.pool.get_connection_pool",
        return_value=mock_pool,
      ),
    ):
      result = restore_ladybug_memory("sec")

    assert result is False
    assert "sec" not in _active_ladybug_boosts


# ---------------------------------------------------------------------------
# Double-boosting prevention
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDoubleBoostPrevention:
  """Tests ensuring double-boost is prevented for both DuckDB and LadybugDB."""

  def test_duckdb_double_boost_prevented(self):
    """Second ensure_duckdb_memory_boosted call does not re-apply boost."""
    from robosystems.graph_api.core.memory_manager import (
      ensure_duckdb_memory_boosted,
    )

    mock_pool = MagicMock()

    # Simulate override state: None initially, "55GB" after first call sets it
    override_state = {"value": None}

    def mock_get_override(graph_id="sec"):
      return override_state["value"]

    def mock_set_override(limit, graph_id="sec"):
      old = override_state["value"]
      override_state["value"] = limit
      return old

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
      patch(
        "robosystems.graph_api.core.duckdb.pool.set_duckdb_memory_override",
        side_effect=mock_set_override,
      ),
      patch(
        "robosystems.graph_api.core.duckdb.pool.get_duckdb_memory_override",
        side_effect=mock_get_override,
      ),
      patch(
        "robosystems.graph_api.core.duckdb.pool.get_duckdb_pool",
        return_value=mock_pool,
      ),
    ):
      mock_env.CLUSTER_TIER = "ladybug-shared"
      mock_tier_config.get_duckdb_memory_boost.return_value = "55GB"

      # First call applies boost
      first = ensure_duckdb_memory_boosted("test")
      assert first == "55GB"

      # Second call sees override already active, returns None
      second = ensure_duckdb_memory_boosted("test")
      assert second is None

  def test_ladybug_double_boost_prevented(self):
    """Second ensure_ladybug_memory_boosted call does not re-apply boost."""
    from robosystems.graph_api.core.memory_manager import (
      ensure_ladybug_memory_boosted,
    )

    mock_pool = MagicMock()
    mock_pool.list_databases.return_value = []

    # Simulate override state: None initially, 50000 after first call sets it
    override_state: dict[str, int | None] = {"value": None}

    def mock_get_override(graph_id=None):
      return override_state["value"]

    def mock_set_override(limit, graph_id=None):
      old = override_state["value"]
      override_state["value"] = limit
      return old

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
      patch(
        "robosystems.graph_api.core.ladybug.config.set_ladybug_memory_override",
        side_effect=mock_set_override,
      ),
      patch(
        "robosystems.graph_api.core.ladybug.config.get_ladybug_memory_override",
        side_effect=mock_get_override,
      ),
      patch(
        "robosystems.graph_api.core.ladybug.pool.get_connection_pool",
        return_value=mock_pool,
      ),
      patch(f"{MODULE}.release_duckdb_memory", return_value={"connections_closed": 0}),
    ):
      mock_env.CLUSTER_TIER = "ladybug-shared"
      mock_tier_config.get_ladybug_memory_boost_mb.return_value = 50000

      first = ensure_ladybug_memory_boosted("sec")
      assert first == 50000

      # Second call sees override already active, returns None
      second = ensure_ladybug_memory_boosted("sec")
      assert second is None


# ---------------------------------------------------------------------------
# Stale tracking set regression tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestStaleTrackingSetRegression:
  """Regression tests for stale tracking set bug.

  When a Dagster run is cancelled before release_graph_memory cleans up,
  the tracking set retains the graph_id but the actual override is cleared.
  The next run must re-apply the boost instead of short-circuiting.
  """

  def setup_method(self):
    from robosystems.graph_api.core.memory_manager import (
      _active_duckdb_boosts,
      _active_ladybug_boosts,
    )

    _active_duckdb_boosts.discard("sec")
    _active_ladybug_boosts.discard("sec")

  def test_duckdb_stale_tracking_set_reapplies_boost(self):
    """DuckDB boost is re-applied when tracking set is stale (override cleared)."""
    from robosystems.graph_api.core.memory_manager import (
      _active_duckdb_boosts,
      ensure_duckdb_memory_boosted,
    )

    # Simulate stale state: tracking set has entry but override was cleared
    _active_duckdb_boosts.add("sec")

    mock_pool = MagicMock()

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
      patch(
        "robosystems.graph_api.core.duckdb.pool.set_duckdb_memory_override",
      ) as mock_set,
      patch(
        "robosystems.graph_api.core.duckdb.pool.get_duckdb_memory_override",
        return_value=None,
      ),
      patch(
        "robosystems.graph_api.core.duckdb.pool.get_duckdb_pool",
        return_value=mock_pool,
      ),
    ):
      mock_env.CLUSTER_TIER = "ladybug-shared"
      mock_tier_config.get_duckdb_memory_boost.return_value = "55GB"

      result = ensure_duckdb_memory_boosted("sec")

    # Boost must be re-applied, not skipped
    assert result == "55GB"
    mock_set.assert_called_once_with("55GB", "sec")
    mock_pool.reconfigure_memory_limit.assert_called_once_with("sec", "55GB")

  def test_ladybug_stale_tracking_set_reapplies_boost(self):
    """LadybugDB boost is re-applied when tracking set is stale (override cleared)."""
    from robosystems.graph_api.core.memory_manager import (
      _active_ladybug_boosts,
      ensure_ladybug_memory_boosted,
    )

    # Simulate stale state: tracking set has entry but override was cleared
    _active_ladybug_boosts.add("sec")

    mock_pool = MagicMock()
    mock_pool.list_databases.return_value = []

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
      patch(
        "robosystems.graph_api.core.ladybug.config.set_ladybug_memory_override",
      ) as mock_set,
      patch(
        "robosystems.graph_api.core.ladybug.config.get_ladybug_memory_override",
        return_value=None,
      ),
      patch(
        "robosystems.graph_api.core.ladybug.pool.get_connection_pool",
        return_value=mock_pool,
      ),
      patch(f"{MODULE}.release_duckdb_memory", return_value={"connections_closed": 0}),
    ):
      mock_env.CLUSTER_TIER = "ladybug-shared"
      mock_tier_config.get_ladybug_memory_boost_mb.return_value = 50000

      result = ensure_ladybug_memory_boosted("sec")

    # Boost must be re-applied, not skipped
    assert result == 50000
    mock_set.assert_called_once_with(50000, graph_id="sec")
    mock_pool.recreate_database.assert_called_once_with("sec")


# ---------------------------------------------------------------------------
# Tracking set management
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestTrackingSetManagement:
  """Tests for correct tracking set add/discard behavior."""

  def test_duckdb_boost_adds_to_set(self):
    """ensure_duckdb_memory_boosted adds graph_id to tracking set."""
    from robosystems.graph_api.core.memory_manager import (
      _active_duckdb_boosts,
      ensure_duckdb_memory_boosted,
    )

    mock_pool = MagicMock()

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
      patch(
        "robosystems.graph_api.core.duckdb.pool.set_duckdb_memory_override",
      ),
      patch(
        "robosystems.graph_api.core.duckdb.pool.get_duckdb_memory_override",
        return_value=None,
      ),
      patch(
        "robosystems.graph_api.core.duckdb.pool.get_duckdb_pool",
        return_value=mock_pool,
      ),
    ):
      mock_env.CLUSTER_TIER = "ladybug-shared"
      mock_tier_config.get_duckdb_memory_boost.return_value = "55GB"

      ensure_duckdb_memory_boosted("graph_a")
      ensure_duckdb_memory_boosted("graph_b")

    assert "graph_a" in _active_duckdb_boosts
    assert "graph_b" in _active_duckdb_boosts

  def test_duckdb_restore_removes_from_set(self):
    """restore_duckdb_memory removes graph_id from tracking set."""
    from robosystems.graph_api.core.memory_manager import (
      _active_duckdb_boosts,
      restore_duckdb_memory,
    )

    _active_duckdb_boosts.add("graph_a")
    _active_duckdb_boosts.add("graph_b")

    mock_pool = MagicMock()

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(f"{MODULE}.GraphTierConfig") as mock_tier_config,
      patch(
        "robosystems.graph_api.core.duckdb.pool.set_duckdb_memory_override",
      ),
      patch(
        "robosystems.graph_api.core.duckdb.pool.get_duckdb_pool",
        return_value=mock_pool,
      ),
    ):
      mock_env.CLUSTER_TIER = "ladybug-shared"
      mock_tier_config.get_duckdb_memory_limit.return_value = "10GB"

      restore_duckdb_memory("graph_a")

    assert "graph_a" not in _active_duckdb_boosts
    assert "graph_b" in _active_duckdb_boosts

  def test_ladybug_restore_removes_from_set(self):
    """restore_ladybug_memory removes graph_id from tracking set."""
    from robosystems.graph_api.core.memory_manager import (
      _active_ladybug_boosts,
      restore_ladybug_memory,
    )

    _active_ladybug_boosts.add("sec")

    mock_pool = MagicMock()

    with (
      patch(
        "robosystems.graph_api.core.ladybug.config.set_ladybug_memory_override",
      ),
      patch(
        "robosystems.graph_api.core.ladybug.pool.get_connection_pool",
        return_value=mock_pool,
      ),
    ):
      restore_ladybug_memory("sec")

    assert "sec" not in _active_ladybug_boosts
