"""Tests for IngestionLimitChecker."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.middleware.graph.ingestion_limits import IngestionLimitChecker


class TestCheckMaterializationLimits:
  """Test materialization limit checking (per-operation safety limits only)."""

  @pytest.mark.asyncio
  async def test_within_limits_allowed(self):
    """Test that materialization is allowed when within row limits."""
    mock_db = MagicMock()
    with patch.object(
      IngestionLimitChecker,
      "_get_pending_row_counts",
      return_value={"Entity": 1000, "Fact": 2000, "ENTITY_HAS_FACT": 500},
    ):
      result = await IngestionLimitChecker.check_materialization_limits(
        db=mock_db,
        graph_id="kg_test",
        tier="ladybug-standard",
      )

    assert result["allowed"] is True
    assert result["errors"] == []
    assert result["tier"] == "ladybug-standard"
    assert result["current_usage"]["total_pending_rows"] == 3500

  @pytest.mark.asyncio
  async def test_exceeds_max_rows_per_copy(self):
    """Test that exceeding max_rows_per_copy blocks materialization."""
    mock_db = MagicMock()
    with (
      patch.object(
        IngestionLimitChecker,
        "_get_pending_row_counts",
        return_value={"Entity": 3_000_000},
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.GraphTierConfig.get_graph_limits",
        return_value={
          "instance_storage_limit_gb": 20,
          "max_rows_per_copy": 2_000_000,
          "max_single_table_rows": 5_000_000,
          "chunk_size_rows": 1_000_000,
          "warn_at_percentage": 80,
        },
      ),
    ):
      result = await IngestionLimitChecker.check_materialization_limits(
        db=mock_db,
        graph_id="kg_test",
        tier="ladybug-standard",
      )

    assert result["allowed"] is False
    assert any("max_rows_per_copy" in e for e in result["errors"])

  @pytest.mark.asyncio
  async def test_exceeds_single_table_rows(self):
    """Test that exceeding max_single_table_rows blocks materialization."""
    mock_db = MagicMock()
    with (
      patch.object(
        IngestionLimitChecker,
        "_get_pending_row_counts",
        return_value={"Entity": 6_000_000},
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.GraphTierConfig.get_graph_limits",
        return_value={
          "instance_storage_limit_gb": 20,
          "max_rows_per_copy": 10_000_000,
          "max_single_table_rows": 5_000_000,
          "chunk_size_rows": 1_000_000,
          "warn_at_percentage": 80,
        },
      ),
    ):
      result = await IngestionLimitChecker.check_materialization_limits(
        db=mock_db,
        graph_id="kg_test",
        tier="ladybug-standard",
      )

    assert result["allowed"] is False
    assert any("max_single_table_rows" in e for e in result["errors"])

  @pytest.mark.asyncio
  async def test_storage_over_limit_blocks_materialization(self):
    """§3.7 Phase 1: aggregate instance storage over cap blocks materialization."""
    mock_db = MagicMock()
    with (
      patch.object(
        IngestionLimitChecker,
        "_get_pending_row_counts",
        return_value={"Entity": 1000},
      ),
      patch.object(
        IngestionLimitChecker,
        "_get_database_size_bytes",
        new_callable=AsyncMock,
        return_value=25 * 1024**3,  # 25 GB on a 20 GB tier
      ),
      patch(
        "robosystems.models.core.graph.Graph.get_subgraphs",
        return_value=[],
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.GraphTierConfig.get_instance_storage_limit_gb",
        return_value=20.0,
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.GraphTierConfig.get_graph_limits",
        return_value={
          "instance_storage_limit_gb": 20,
          "max_rows_per_copy": 2_000_000,
          "max_single_table_rows": 5_000_000,
          "chunk_size_rows": 1_000_000,
          "warn_at_percentage": 80,
        },
      ),
    ):
      result = await IngestionLimitChecker.check_materialization_limits(
        db=mock_db,
        graph_id="kg_test",
        tier="ladybug-standard",
      )

    assert result["allowed"] is False
    assert any("instance storage" in e for e in result["errors"])
    assert result["current_usage"]["total_storage_gb"] == 25.0
    assert result["current_usage"]["storage_usage_percentage"] == 125.0
    assert result["limits"]["instance_storage_limit_gb"] == 20.0

  @pytest.mark.asyncio
  async def test_no_node_or_relationship_blocking(self):
    """Test that materialization is never blocked by node/relationship counts.

    The old behavior blocked at 413 when projected nodes exceeded limits.
    The new behavior only blocks on per-operation row limits.
    """
    mock_db = MagicMock()
    # Even with large row counts, as long as they're within per-operation limits
    with patch.object(
      IngestionLimitChecker,
      "_get_pending_row_counts",
      return_value={"Entity": 1_000_000, "ENTITY_HAS_FACT": 500_000},
    ):
      result = await IngestionLimitChecker.check_materialization_limits(
        db=mock_db,
        graph_id="kg_test",
        tier="ladybug-standard",
      )

    assert result["allowed"] is True
    # No node/relationship counts in the response
    assert "current_nodes" not in result["current_usage"]
    assert "current_relationships" not in result["current_usage"]


class TestCheckInstanceStorage:
  """Test instance storage usage checking."""

  @pytest.mark.asyncio
  async def test_healthy_status(self):
    """Test that status is healthy when under 80% of storage limit."""
    mock_db = MagicMock()
    with (
      patch.object(
        IngestionLimitChecker,
        "_get_database_size_bytes",
        new_callable=AsyncMock,
        return_value=5 * 1024**3,  # 5 GB
      ),
      patch(
        "robosystems.models.core.graph.Graph.get_subgraphs",
        return_value=[],
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.GraphTierConfig.get_instance_storage_limit_gb",
        return_value=20.0,
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.GraphTierConfig.get_graph_limits",
        return_value={"warn_at_percentage": 80},
      ),
    ):
      result = await IngestionLimitChecker.check_instance_storage(
        db=mock_db,
        graph_id="kg_test",
        tier="ladybug-standard",
      )

    assert result["status"] == "healthy"
    assert result["total_storage_gb"] == 5.0
    assert result["limit_gb"] == 20.0
    assert result["usage_percentage"] == 25.0
    assert len(result["databases"]) == 1
    assert result["databases"][0]["is_parent"] is True

  @pytest.mark.asyncio
  async def test_approaching_status(self):
    """Test that status is approaching when between 80-100%."""
    mock_db = MagicMock()
    with (
      patch.object(
        IngestionLimitChecker,
        "_get_database_size_bytes",
        new_callable=AsyncMock,
        return_value=17 * 1024**3,  # 17 GB
      ),
      patch(
        "robosystems.models.core.graph.Graph.get_subgraphs",
        return_value=[],
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.GraphTierConfig.get_instance_storage_limit_gb",
        return_value=20.0,
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.GraphTierConfig.get_graph_limits",
        return_value={"warn_at_percentage": 80},
      ),
    ):
      result = await IngestionLimitChecker.check_instance_storage(
        db=mock_db,
        graph_id="kg_test",
        tier="ladybug-standard",
      )

    assert result["status"] == "approaching"
    assert result["usage_percentage"] == 85.0

  @pytest.mark.asyncio
  async def test_over_limit_status(self):
    """Test that status is over_limit when exceeding 100%."""
    mock_db = MagicMock()
    with (
      patch.object(
        IngestionLimitChecker,
        "_get_database_size_bytes",
        new_callable=AsyncMock,
        return_value=21 * 1024**3,  # 21 GB
      ),
      patch(
        "robosystems.models.core.graph.Graph.get_subgraphs",
        return_value=[],
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.GraphTierConfig.get_instance_storage_limit_gb",
        return_value=20.0,
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.GraphTierConfig.get_graph_limits",
        return_value={"warn_at_percentage": 80},
      ),
    ):
      result = await IngestionLimitChecker.check_instance_storage(
        db=mock_db,
        graph_id="kg_test",
        tier="ladybug-standard",
      )

    assert result["status"] == "over_limit"
    assert result["usage_percentage"] == 105.0
    # §3.7 Phase 1: over_limit populates blocking errors
    assert result["allowed"] is False
    assert len(result["errors"]) == 1
    assert "exceeds" in result["errors"][0]

  @pytest.mark.asyncio
  async def test_healthy_status_is_allowed(self):
    """§3.7 Phase 1: under-cap status returns allowed=True with empty errors."""
    mock_db = MagicMock()
    with (
      patch.object(
        IngestionLimitChecker,
        "_get_database_size_bytes",
        new_callable=AsyncMock,
        return_value=5 * 1024**3,
      ),
      patch(
        "robosystems.models.core.graph.Graph.get_subgraphs",
        return_value=[],
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.GraphTierConfig.get_instance_storage_limit_gb",
        return_value=20.0,
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.GraphTierConfig.get_graph_limits",
        return_value={"warn_at_percentage": 80},
      ),
    ):
      result = await IngestionLimitChecker.check_instance_storage(
        db=mock_db,
        graph_id="kg_test",
        tier="ladybug-standard",
      )

    assert result["allowed"] is True
    assert result["errors"] == []

  @pytest.mark.asyncio
  async def test_aggregates_subgraphs(self):
    """Test that storage is summed across parent and subgraphs."""
    mock_db = MagicMock()

    # Create mock subgraph
    mock_subgraph = MagicMock()
    mock_subgraph.graph_id = "kg_test_dev"

    # Return different sizes for parent vs subgraph
    async def mock_get_size(graph_id):
      if graph_id == "kg_test":
        return 10 * 1024**3  # 10 GB parent
      return 5 * 1024**3  # 5 GB subgraph

    with (
      patch.object(
        IngestionLimitChecker,
        "_get_database_size_bytes",
        side_effect=mock_get_size,
      ),
      patch(
        "robosystems.models.core.graph.Graph.get_subgraphs",
        return_value=[mock_subgraph],
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.GraphTierConfig.get_instance_storage_limit_gb",
        return_value=20.0,
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.GraphTierConfig.get_graph_limits",
        return_value={"warn_at_percentage": 80},
      ),
    ):
      result = await IngestionLimitChecker.check_instance_storage(
        db=mock_db,
        graph_id="kg_test",
        tier="ladybug-standard",
      )

    assert result["total_storage_gb"] == 15.0
    assert result["usage_percentage"] == 75.0
    assert result["status"] == "healthy"
    assert len(result["databases"]) == 2
    assert result["databases"][0]["is_parent"] is True
    assert result["databases"][1]["is_parent"] is False
    assert result["databases"][1]["graph_id"] == "kg_test_dev"

  @pytest.mark.asyncio
  async def test_handles_unavailable_subgraph(self):
    """Test graceful degradation when a subgraph's size can't be fetched."""
    mock_db = MagicMock()

    mock_subgraph = MagicMock()
    mock_subgraph.graph_id = "kg_test_dev"

    async def mock_get_size(graph_id):
      if graph_id == "kg_test":
        return 10 * 1024**3
      return None  # Subgraph unavailable

    with (
      patch.object(
        IngestionLimitChecker,
        "_get_database_size_bytes",
        side_effect=mock_get_size,
      ),
      patch(
        "robosystems.models.core.graph.Graph.get_subgraphs",
        return_value=[mock_subgraph],
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.GraphTierConfig.get_instance_storage_limit_gb",
        return_value=20.0,
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.GraphTierConfig.get_graph_limits",
        return_value={"warn_at_percentage": 80},
      ),
    ):
      result = await IngestionLimitChecker.check_instance_storage(
        db=mock_db,
        graph_id="kg_test",
        tier="ladybug-standard",
      )

    # Only parent size counted
    assert result["total_storage_gb"] == 10.0
    assert len(result["databases"]) == 2
    assert result["databases"][1]["size_mb"] is None
