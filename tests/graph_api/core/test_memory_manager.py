"""Tests for graph memory management utilities.

Covers DuckDB/LadybugDB memory boost tracking, release helpers,
subgraph eviction, and the ensure/restore lifecycle.
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


class TestIsBoosted:
  """Tests for is_duckdb_memory_boosted and is_ladybug_memory_boosted."""

  @pytest.mark.unit
  def test_is_duckdb_memory_boosted_false_by_default(self):
    from robosystems.graph_api.core.memory_manager import is_duckdb_memory_boosted

    assert is_duckdb_memory_boosted("test") is False

  @pytest.mark.unit
  def test_is_ladybug_memory_boosted_false_by_default(self):
    from robosystems.graph_api.core.memory_manager import (
      is_ladybug_memory_boosted,
    )

    assert is_ladybug_memory_boosted("test") is False


class TestEvictIdleSubgraphDatabases:
  """Tests for _evict_idle_subgraph_databases."""

  @pytest.mark.unit
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
    # Primary graph "sec" has no underscore, so it should not be evicted
    assert "sec" not in evicted

  @pytest.mark.unit
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

  @pytest.mark.unit
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
    # sec_historical has active connections, sec_quarterly does not
    mock_pool.has_active_connections.side_effect = lambda db: db == "sec_historical"

    evicted = _evict_idle_subgraph_databases(mock_pool, target_graph_id="sec")

    assert "sec_historical" not in evicted
    assert "sec_quarterly" in evicted


class TestReleaseDuckdbMemory:
  """Tests for release_duckdb_memory."""

  @pytest.mark.unit
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

  @pytest.mark.unit
  def test_release_error(self):
    """Exception during release is caught and returns success=False."""
    from robosystems.graph_api.core.memory_manager import release_duckdb_memory

    with patch(
      "robosystems.graph_api.core.duckdb.pool.get_duckdb_pool",
      side_effect=Exception("pool error"),
    ):
      result = release_duckdb_memory("test_graph")

    assert result["success"] is False


class TestReleaseLadybugMemory:
  """Tests for release_ladybug_memory."""

  @pytest.mark.unit
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

  @pytest.mark.unit
  def test_release_error(self):
    """Exception during release returns success=False."""
    from robosystems.graph_api.core.memory_manager import release_ladybug_memory

    with patch(
      "robosystems.graph_api.core.ladybug.pool.get_connection_pool",
      side_effect=Exception("pool error"),
    ):
      result = release_ladybug_memory("test_graph")

    assert result["success"] is False


class TestBoostDuckdbMemoryContextManager:
  """Tests for the boost_duckdb_memory context manager."""

  @pytest.mark.unit
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
        # Boost was applied
        mock_set_override.assert_called_with("55GB", "sec")

      # After context exit, override is restored
      assert mock_set_override.call_count == 2
      mock_set_override.assert_called_with(None, "sec")

  @pytest.mark.unit
  def test_no_boost_when_no_tier(self):
    """When CLUSTER_TIER is None, context manager yields None."""
    from robosystems.graph_api.core.memory_manager import boost_duckdb_memory

    with patch(f"{MODULE}.env") as mock_env:
      mock_env.CLUSTER_TIER = None

      with boost_duckdb_memory("test") as boost:
        assert boost is None


class TestEnsureDuckdbMemoryBoosted:
  """Tests for ensure_duckdb_memory_boosted (idempotent boost)."""

  @pytest.mark.unit
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

  @pytest.mark.unit
  def test_already_boosted_returns_none(self):
    """Second call for the same graph_id returns None (already boosted)."""
    from robosystems.graph_api.core.memory_manager import (
      _active_duckdb_boosts,
      ensure_duckdb_memory_boosted,
    )

    _active_duckdb_boosts.add("test_graph")

    result = ensure_duckdb_memory_boosted("test_graph")
    assert result is None

  @pytest.mark.unit
  def test_no_tier_returns_none(self):
    """Returns None when CLUSTER_TIER is not set."""
    from robosystems.graph_api.core.memory_manager import (
      ensure_duckdb_memory_boosted,
    )

    with patch(f"{MODULE}.env") as mock_env:
      mock_env.CLUSTER_TIER = None

      result = ensure_duckdb_memory_boosted("test_graph")

    assert result is None


class TestRestoreDuckdbMemory:
  """Tests for restore_duckdb_memory."""

  @pytest.mark.unit
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

  @pytest.mark.unit
  def test_not_boosted_returns_false(self):
    """Returns False when the graph was not boosted."""
    from robosystems.graph_api.core.memory_manager import restore_duckdb_memory

    result = restore_duckdb_memory("test_graph")
    assert result is False
