"""Tests for chunked materialization of user-graph staging tables."""

from unittest.mock import AsyncMock, patch

import pytest

from robosystems.operations.graph.engine.chunked_materialization import (
  CHUNK_TIMEOUT,
  materialize_table_chunked,
)


class TestMaterializeTableChunked:
  """Test materialize_table_chunked function."""

  @pytest.mark.asyncio
  async def test_small_table_single_pass(self):
    """Tables below chunk_size use a single materialize_table call."""
    client = AsyncMock()
    client.query_table.return_value = {"rows": [[500]]}
    client.materialize_table.return_value = {"rows_ingested": 500}

    result = await materialize_table_chunked(
      client=client,
      graph_id="kg123",
      table_name="Entity",
      tier="ladybug-standard",
    )

    assert result["rows_ingested"] == 500
    assert result["chunked"] is False
    assert result["batches"] == 1

    # Single call, no batch params
    client.materialize_table.assert_called_once_with(
      graph_id="kg123",
      table_name="Entity",
      ignore_errors=True,
      materialize_embeddings=False,
      file_ids=None,
      timeout=CHUNK_TIMEOUT,
    )

  @pytest.mark.asyncio
  async def test_large_table_batched(self):
    """Tables above chunk_size are split into hash-based batches."""
    client = AsyncMock()
    client.query_table.return_value = {"rows": [[2_500_000]]}
    client.materialize_table.side_effect = [
      {"rows_ingested": 850_000},
      {"rows_ingested": 830_000},
      {"rows_ingested": 820_000},
    ]

    with patch(
      "robosystems.operations.graph.engine.chunked_materialization.GraphTierConfig"
    ) as mock_config:
      mock_config.get_graph_limits.return_value = {"chunk_size_rows": 1_000_000}

      result = await materialize_table_chunked(
        client=client,
        graph_id="kg123",
        table_name="Entity",
        tier="ladybug-standard",
      )

    assert result["rows_ingested"] == 2_500_000
    assert result["chunked"] is True
    assert result["batches"] == 3

    # Verify batch_num/num_batches were passed
    calls = client.materialize_table.call_args_list
    assert len(calls) == 3
    for i, call in enumerate(calls):
      assert call.kwargs["batch_num"] == i
      assert call.kwargs["num_batches"] == 3

  @pytest.mark.asyncio
  async def test_row_count_query_failure_falls_back_to_single_pass(self):
    """When row count query fails, fall back to single-pass materialization."""
    client = AsyncMock()
    client.query_table.side_effect = Exception("DuckDB unavailable")
    client.materialize_table.return_value = {"rows_ingested": 100}

    result = await materialize_table_chunked(
      client=client,
      graph_id="kg123",
      table_name="Entity",
      tier="ladybug-standard",
    )

    assert result["rows_ingested"] == 100
    assert result["chunked"] is False
    assert result["batches"] == 1
    # Should have called materialize without batch params
    client.materialize_table.assert_called_once()
    assert "batch_num" not in client.materialize_table.call_args.kwargs

  @pytest.mark.asyncio
  async def test_chunk_failure_with_ignore_errors_continues(self):
    """With ignore_errors=True, batch failures are logged and skipped."""
    client = AsyncMock()
    client.query_table.return_value = {"rows": [[2_000_000]]}
    client.materialize_table.side_effect = [
      {"rows_ingested": 900_000},
      Exception("OOM on batch 2"),
      {"rows_ingested": 100_000},  # never reached for 2 batches
    ]

    with patch(
      "robosystems.operations.graph.engine.chunked_materialization.GraphTierConfig"
    ) as mock_config:
      mock_config.get_graph_limits.return_value = {"chunk_size_rows": 1_000_000}

      result = await materialize_table_chunked(
        client=client,
        graph_id="kg123",
        table_name="Entity",
        tier="ladybug-standard",
        ignore_errors=True,
      )

    # Only first batch's rows counted; second failed
    assert result["rows_ingested"] == 900_000
    assert result["chunked"] is True
    assert result["batches"] == 2

  @pytest.mark.asyncio
  async def test_chunk_failure_with_ignore_errors_false_raises(self):
    """With ignore_errors=False, batch failures propagate immediately."""
    client = AsyncMock()
    client.query_table.return_value = {"rows": [[2_000_000]]}
    client.materialize_table.side_effect = [
      {"rows_ingested": 900_000},
      Exception("OOM on batch 2"),
    ]

    with (
      patch(
        "robosystems.operations.graph.engine.chunked_materialization.GraphTierConfig"
      ) as mock_config,
      pytest.raises(Exception, match="OOM on batch 2"),
    ):
      mock_config.get_graph_limits.return_value = {"chunk_size_rows": 1_000_000}

      await materialize_table_chunked(
        client=client,
        graph_id="kg123",
        table_name="Entity",
        tier="ladybug-standard",
        ignore_errors=False,
      )

  @pytest.mark.asyncio
  async def test_different_tiers_produce_different_chunk_sizes(self):
    """Verify tier config is used to determine chunk size."""
    client = AsyncMock()
    client.query_table.return_value = {"rows": [[3_000_000]]}
    client.materialize_table.return_value = {"rows_ingested": 1_000_000}

    with patch(
      "robosystems.operations.graph.engine.chunked_materialization.GraphTierConfig"
    ) as mock_config:
      # Large tier: 2M chunk -> 2 batches
      mock_config.get_graph_limits.return_value = {"chunk_size_rows": 2_000_000}

      result = await materialize_table_chunked(
        client=client,
        graph_id="kg123",
        table_name="Entity",
        tier="ladybug-large",
      )
      assert result["batches"] == 2
      assert result["chunked"] is True

    client.reset_mock()
    client.query_table.return_value = {"rows": [[3_000_000]]}
    client.materialize_table.return_value = {"rows_ingested": 600_000}

    with patch(
      "robosystems.operations.graph.engine.chunked_materialization.GraphTierConfig"
    ) as mock_config:
      # Standard tier: 1M chunk -> 3 batches
      mock_config.get_graph_limits.return_value = {"chunk_size_rows": 1_000_000}

      result = await materialize_table_chunked(
        client=client,
        graph_id="kg123",
        table_name="Entity",
        tier="ladybug-standard",
      )
      assert result["batches"] == 3
      assert result["chunked"] is True

  @pytest.mark.asyncio
  async def test_tier_config_failure_uses_default_chunk_size(self):
    """When tier config lookup fails, use DEFAULT_CHUNK_SIZE_ROWS."""
    client = AsyncMock()
    # Row count > DEFAULT_CHUNK_SIZE_ROWS (1M) -> should chunk
    client.query_table.return_value = {"rows": [[1_500_000]]}
    client.materialize_table.return_value = {"rows_ingested": 750_000}

    with patch(
      "robosystems.operations.graph.engine.chunked_materialization.GraphTierConfig"
    ) as mock_config:
      mock_config.get_graph_limits.side_effect = Exception("Config unavailable")

      result = await materialize_table_chunked(
        client=client,
        graph_id="kg123",
        table_name="Entity",
        tier="unknown-tier",
      )

    assert result["chunked"] is True
    assert result["batches"] == 2

  @pytest.mark.asyncio
  async def test_file_ids_passed_through(self):
    """file_ids parameter is forwarded to materialize_table."""
    client = AsyncMock()
    client.query_table.return_value = {"rows": [[100]]}
    client.materialize_table.return_value = {"rows_ingested": 100}

    await materialize_table_chunked(
      client=client,
      graph_id="kg123",
      table_name="Entity",
      tier="ladybug-standard",
      file_ids=["f1", "f2"],
    )

    call_kwargs = client.materialize_table.call_args.kwargs
    assert call_kwargs["file_ids"] == ["f1", "f2"]

  @pytest.mark.asyncio
  async def test_exact_chunk_boundary(self):
    """Row count exactly equal to chunk_size -> single pass (not chunked)."""
    client = AsyncMock()
    client.query_table.return_value = {"rows": [[1_000_000]]}
    client.materialize_table.return_value = {"rows_ingested": 1_000_000}

    with patch(
      "robosystems.operations.graph.engine.chunked_materialization.GraphTierConfig"
    ) as mock_config:
      mock_config.get_graph_limits.return_value = {"chunk_size_rows": 1_000_000}

      result = await materialize_table_chunked(
        client=client,
        graph_id="kg123",
        table_name="Entity",
        tier="ladybug-standard",
      )

    assert result["chunked"] is False
    assert result["batches"] == 1

  @pytest.mark.asyncio
  async def test_empty_row_count_response(self):
    """Empty row count response falls back to single pass."""
    client = AsyncMock()
    client.query_table.return_value = {"rows": []}
    client.materialize_table.return_value = {"rows_ingested": 0}

    result = await materialize_table_chunked(
      client=client,
      graph_id="kg123",
      table_name="Entity",
      tier="ladybug-standard",
    )

    assert result["chunked"] is False
    assert result["batches"] == 1
