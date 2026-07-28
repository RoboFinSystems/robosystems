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
    """Aggregate instance storage over cap blocks materialization."""
    mock_db = MagicMock()
    with (
      patch.object(
        IngestionLimitChecker,
        "_get_pending_row_counts",
        return_value={"Entity": 1000},
      ),
      patch.object(
        IngestionLimitChecker,
        "_get_storage_breakdown",
        new_callable=AsyncMock,
        return_value={
          "total_bytes": 25 * 1024**3,
          "items": [{"type": "graph", "id": "kg_test", "bytes": 25 * 1024**3}],
        },  # 25 GB on a 20 GB tier
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
      return_value={"Entity": 500_000, "ENTITY_HAS_FACT": 250_000},
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
        "_get_storage_breakdown",
        new_callable=AsyncMock,
        return_value={
          "total_bytes": 5 * 1024**3,
          "items": [{"type": "graph", "id": "kg_test", "bytes": 5 * 1024**3}],
        },  # 5 GB
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
        "_get_storage_breakdown",
        new_callable=AsyncMock,
        return_value={
          "total_bytes": 17 * 1024**3,
          "items": [{"type": "graph", "id": "kg_test", "bytes": 17 * 1024**3}],
        },  # 17 GB
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
        "_get_storage_breakdown",
        new_callable=AsyncMock,
        return_value={
          "total_bytes": 21 * 1024**3,
          "items": [{"type": "graph", "id": "kg_test", "bytes": 21 * 1024**3}],
        },  # 21 GB
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
    # over_limit populates blocking errors
    assert result["allowed"] is False
    assert len(result["errors"]) == 1
    assert "exceeds" in result["errors"][0]

  @pytest.mark.asyncio
  async def test_healthy_status_is_allowed(self):
    """Under-cap status returns allowed=True with empty errors."""
    mock_db = MagicMock()
    with (
      patch.object(
        IngestionLimitChecker,
        "_get_storage_breakdown",
        new_callable=AsyncMock,
        return_value={
          "total_bytes": 5 * 1024**3,
          "items": [{"type": "graph", "id": "kg_test", "bytes": 5 * 1024**3}],
        },
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
    """Storage covers parent and subgraphs from one instance-wide read.

    Subgraphs always live on their parent's instance, so a single breakdown
    already itemizes them — no per-subgraph call, and no dependence on the
    graph registry being in step with what is actually on disk.
    """
    mock_db = MagicMock()

    breakdown = {
      "graph_id": "kg_test",
      "total_bytes": 15 * 1024**3,
      "items": [
        {"type": "graph", "id": "kg_test", "bytes": 10 * 1024**3},
        {"type": "subgraph", "id": "kg_test_dev", "bytes": 5 * 1024**3},
      ],
    }

    with (
      patch.object(
        IngestionLimitChecker,
        "_get_storage_breakdown",
        new_callable=AsyncMock,
        return_value=breakdown,
      ) as mock_breakdown,
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
    assert result["items"] == breakdown["items"]

    # One call for the whole instance, not one per database.
    mock_breakdown.assert_awaited_once_with("kg_test")

  @pytest.mark.asyncio
  async def test_rolls_types_up_per_database(self):
    """A database's graph, vector and staging bytes report as one line."""
    mock_db = MagicMock()

    breakdown = {
      "graph_id": "kg_test",
      "total_bytes": 6 * 1024**3,
      "items": [
        {"type": "graph", "id": "kg_test", "bytes": 3 * 1024**3},
        {"type": "vectors", "id": "kg_test", "bytes": 1 * 1024**3},
        {"type": "staging", "id": "kg_test", "bytes": 2 * 1024**3},
      ],
    }

    with (
      patch.object(
        IngestionLimitChecker,
        "_get_storage_breakdown",
        new_callable=AsyncMock,
        return_value=breakdown,
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

    assert len(result["databases"]) == 1
    assert result["databases"][0]["size_mb"] == 6 * 1024
    assert result["total_storage_gb"] == 6.0

  @pytest.mark.asyncio
  async def test_unavailable_breakdown_degrades_to_zero(self):
    """An unreachable node must not fabricate usage or block on nothing."""
    mock_db = MagicMock()

    with (
      patch.object(
        IngestionLimitChecker,
        "_get_storage_breakdown",
        new_callable=AsyncMock,
        return_value=None,
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
    assert result["total_storage_gb"] == 0.0
    assert result["databases"] == []
    assert result["items"] == []
