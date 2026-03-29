"""Tests for LadybugDB materialization operations.

Tests LadybugMaterializer initialization, materialize_from_duckdb,
and copy_incremental_to_ladybug.
"""

from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.unit
class TestLadybugMaterializerInit:
  """Tests for LadybugMaterializer initialization."""

  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  def test_default_init(self, mock_env, mock_s3_client):
    """Default initialization sets expected attributes."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()
    assert mat.graph_id == "sec"
    assert mat.source_prefix == "sec/processed"
    assert mat.bucket == "test-bucket"

  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  def test_custom_init(self, mock_env, mock_s3_client):
    """Custom initialization overrides defaults."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer(graph_id="custom", source_prefix="custom/prefix")
    assert mat.graph_id == "custom"
    assert mat.source_prefix == "custom/prefix"


@pytest.mark.unit
class TestMaterializeFromDuckDBErrors:
  """Tests for materialize_from_duckdb error handling."""

  @pytest.mark.asyncio
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.get_graph_client"
  )
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.RoboLedgerContext"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_graph_client_init_failure(
    self, mock_env, mock_s3_client, mock_context, mock_get_client
  ):
    """Returns error when graph client fails to initialize."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    mock_env.ENVIRONMENT = "dev"

    mock_context.get_all_table_names_for_context.return_value = {"Entity": "nodes"}
    mock_context.SEC_REPOSITORY = "sec"

    mock_get_client.side_effect = RuntimeError("Connection refused")

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()
    result = await mat.materialize_from_duckdb()

    assert result.status == "error"
    assert "Graph client initialization failed" in result.error

  @pytest.mark.asyncio
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.RoboLedgerContext"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_empty_table_names(self, mock_env, mock_s3_client, mock_context):
    """Returns no_data when no tables are found in schema."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    mock_env.ENVIRONMENT = "dev"

    mock_context.get_all_table_names_for_context.return_value = {}
    mock_context.SEC_REPOSITORY = "sec"

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()
    result = await mat.materialize_from_duckdb()

    assert result.status == "no_data"

  @pytest.mark.asyncio
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.get_graph_client"
  )
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.RoboLedgerContext"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_unexpected_exception(
    self, mock_env, mock_s3_client, mock_context, mock_get_client
  ):
    """Unexpected exceptions are caught and returned as error."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    mock_env.ENVIRONMENT = "dev"

    mock_context.get_all_table_names_for_context.side_effect = RuntimeError("Boom")
    mock_context.SEC_REPOSITORY = "sec"

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()
    result = await mat.materialize_from_duckdb()

    assert result.status == "error"
    assert "Boom" in result.error


@pytest.mark.unit
class TestMaterializeFromDuckDBSuccess:
  """Tests for successful materialize_from_duckdb."""

  @pytest.mark.asyncio
  @patch(
    "robosystems.operations.graph.shared_repository_service.ensure_shared_repository_exists",
    new_callable=AsyncMock,
  )
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.get_graph_client"
  )
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.RoboLedgerContext"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_specific_table_names(
    self,
    mock_env,
    mock_s3_client,
    mock_context,
    mock_get_client,
    mock_ensure_repo,
  ):
    """Can materialize specific table names instead of all from schema."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    mock_env.ENVIRONMENT = "dev"

    mock_ensure_repo.return_value = {"status": "exists"}

    mock_client = AsyncMock()
    mock_get_client.return_value = mock_client
    mock_client.materialize_table.return_value = {
      "rows_ingested": 100,
      "execution_time_ms": 500,
      "status": "success",
    }

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()
    result = await mat.materialize_from_duckdb(table_names=["Entity"])

    assert result.status == "success"
    assert result.total_rows_ingested == 100
    mock_client.materialize_table.assert_called_once()


@pytest.mark.unit
class TestTriggerIngestion:
  """Tests for _trigger_ingestion batched materialization."""

  @pytest.mark.asyncio
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_single_pass_for_small_table(self, mock_env, mock_s3_client):
    """Small tables use single-pass materialization."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()

    mock_client = AsyncMock()
    mock_client.materialize_table.return_value = {
      "rows_ingested": 500,
      "execution_time_ms": 100,
      "status": "success",
    }

    result = await mat._trigger_ingestion(
      table_names=["Entity"],
      graph_client=mock_client,
      batch_materialization=True,
      batch_size=20_000_000,
    )

    assert result["total_rows_ingested"] == 500
    # Small table should not trigger row count query
    mock_client.query_table.assert_not_called()

  @pytest.mark.asyncio
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_batched_for_large_table(self, mock_env, mock_s3_client):
    """Large tables (Fact) use batched materialization when row count exceeds threshold."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()

    mock_client = AsyncMock()
    mock_client.query_table.return_value = {
      "rows": [[50_000_000]]  # 50M rows
    }
    mock_client.materialize_table.return_value = {
      "rows_ingested": 16_666_666,
      "execution_time_ms": 30000,
    }

    await mat._trigger_ingestion(
      table_names=["Fact"],
      graph_client=mock_client,
      batch_materialization=True,
      batch_size=20_000_000,
    )

    # 50M / 20M = 3 batches
    assert mock_client.materialize_table.call_count == 3
    # Check that batch_num and num_batches were passed
    first_call = mock_client.materialize_table.call_args_list[0]
    assert first_call.kwargs["batch_num"] == 0
    assert first_call.kwargs["num_batches"] == 3

  @pytest.mark.asyncio
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_materialization_error_tracked(self, mock_env, mock_s3_client):
    """Materialization errors are captured in results."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()

    mock_client = AsyncMock()
    mock_client.materialize_table.side_effect = RuntimeError("OOM")

    result = await mat._trigger_ingestion(
      table_names=["Entity"],
      graph_client=mock_client,
    )

    assert len(result["tables"]) == 1
    assert result["tables"][0]["status"] == "error"
    assert "OOM" in result["tables"][0]["error"]
