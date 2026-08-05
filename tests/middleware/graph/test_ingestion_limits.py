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
    with (
      patch.object(
        IngestionLimitChecker,
        "_get_pending_row_counts",
        return_value={"Entity": 1000, "Fact": 2000, "ENTITY_HAS_FACT": 500},
      ),
      # A measured, in-bounds breakdown. Without this mock the test only
      # passed via the old fail-open path (unmeasured -> 0 GB -> allowed).
      patch.object(
        IngestionLimitChecker,
        "_get_storage_breakdown",
        new_callable=AsyncMock,
        return_value={"total_bytes": 1024**3, "items": []},
      ),
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
    with (
      patch.object(
        IngestionLimitChecker,
        "_get_pending_row_counts",
        return_value={"Entity": 500_000, "ENTITY_HAS_FACT": 250_000},
      ),
      patch.object(
        IngestionLimitChecker,
        "_get_storage_breakdown",
        new_callable=AsyncMock,
        return_value={"total_bytes": 1024**3, "items": []},
      ),
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

  @pytest.mark.asyncio
  async def test_subgraph_storage_check_measures_parent_scope(self):
    """A subgraph id must widen to its parent for the storage check.

    The breakdown's prefix scan for `kg123_dev` covers only `kg123_dev*`,
    excluding the parent and sibling databases from the denominator — a
    tenant at cap could keep promoting data via subgraphs. Row-count
    checks stay on the subgraph's own staging.
    """
    mock_db = MagicMock()
    mock_graph = MagicMock()
    mock_graph.parent_graph_id = "kg123"

    storage_ok = {
      "allowed": True,
      "retryable": False,
      "errors": [],
      "total_storage_gb": 1.0,
      "limit_gb": 20.0,
      "usage_percentage": 5.0,
      "status": "healthy",
      "databases": [],
      "items": [],
    }

    with (
      patch.object(
        IngestionLimitChecker,
        "_get_pending_row_counts",
        return_value={"Entity": 1000},
      ) as mock_rows,
      patch(
        "robosystems.models.core.Graph.get_by_id",
        return_value=mock_graph,
      ),
      patch.object(
        IngestionLimitChecker,
        "check_instance_storage",
        new_callable=AsyncMock,
        return_value=storage_ok,
      ) as mock_storage,
    ):
      result = await IngestionLimitChecker.check_materialization_limits(
        db=mock_db,
        graph_id="kg123_dev",
        tier="ladybug-standard",
      )

    assert result["allowed"] is True
    mock_storage.assert_awaited_once_with(mock_db, "kg123", "ladybug-standard")
    mock_rows.assert_called_once_with(mock_db, "kg123_dev")

  @pytest.mark.asyncio
  async def test_parent_graph_storage_check_keeps_own_scope(self):
    """A parent (or unregistered) graph id passes through unchanged."""
    mock_db = MagicMock()
    mock_graph = MagicMock()
    mock_graph.parent_graph_id = None

    storage_ok = {
      "allowed": True,
      "retryable": False,
      "errors": [],
      "total_storage_gb": 1.0,
      "limit_gb": 20.0,
      "usage_percentage": 5.0,
      "status": "healthy",
      "databases": [],
      "items": [],
    }

    with (
      patch.object(
        IngestionLimitChecker,
        "_get_pending_row_counts",
        return_value={},
      ),
      patch(
        "robosystems.models.core.Graph.get_by_id",
        return_value=mock_graph,
      ),
      patch.object(
        IngestionLimitChecker,
        "check_instance_storage",
        new_callable=AsyncMock,
        return_value=storage_ok,
      ) as mock_storage,
    ):
      await IngestionLimitChecker.check_materialization_limits(
        db=mock_db,
        graph_id="kg123",
        tier="ladybug-standard",
      )

    mock_storage.assert_awaited_once_with(mock_db, "kg123", "ladybug-standard")


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


class TestUnmeasurableStorageFailsClosed:
  """A Graph API failure used to read as 0 GB / healthy / allowed —
  silently disabling the storage cap exactly when the instance was
  struggling, which is when it matters most."""

  @pytest.mark.asyncio
  async def test_check_instance_storage_returns_unknown_and_blocks(self):
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
        db=mock_db, graph_id="kg_test", tier="ladybug-standard"
      )

    assert result["allowed"] is False
    assert result["retryable"] is True
    assert result["status"] == "unknown"
    assert result["total_storage_gb"] is None
    assert result["usage_percentage"] is None

  @pytest.mark.asyncio
  async def test_materialization_check_marks_the_block_retryable(self):
    """Callers must be able to distinguish 'cannot verify, retry' from
    'over your cap' — a 503 versus a 413."""
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
        return_value=None,
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.GraphTierConfig.get_instance_storage_limit_gb",
        return_value=20.0,
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.GraphTierConfig.get_graph_limits",
        return_value={
          "instance_storage_limit_gb": 20,
          "max_rows_per_copy": 1_000_000,
          "max_single_table_rows": 2_500_000,
          "chunk_size_rows": 250_000,
          "warn_at_percentage": 80,
        },
      ),
    ):
      result = await IngestionLimitChecker.check_materialization_limits(
        db=mock_db, graph_id="kg_test", tier="ladybug-standard"
      )

    assert result["allowed"] is False
    assert result["retryable"] is True
    assert any("could not be verified" in e for e in result["errors"])

  @pytest.mark.asyncio
  async def test_measured_over_limit_is_not_retryable(self):
    """A genuine over-cap block must stay a hard 413, not a retry hint."""
    mock_db = MagicMock()
    with (
      patch.object(
        IngestionLimitChecker,
        "_get_storage_breakdown",
        new_callable=AsyncMock,
        return_value={
          "total_bytes": 25 * 1024**3,
          "items": [{"type": "graph", "id": "kg_test", "bytes": 25 * 1024**3}],
        },
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
        db=mock_db, graph_id="kg_test", tier="ladybug-standard"
      )

    assert result["allowed"] is False
    assert result["retryable"] is False
    assert result["status"] == "over_limit"
