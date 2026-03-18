"""Tests for DuckDB staging operations.

Tests DuckDBStager initialization, stage_to_duckdb, stage_incremental_to_duckdb,
retry logic, and error handling.
"""

from unittest.mock import AsyncMock, patch

import pytest

from robosystems.adapters.sec.processors.ingestion.models import (
  STAGING_MAX_RETRIES,
  TableInfo,
)


def _make_stager():
  """Create a DuckDBStager with mocked dependencies."""
  with (
    patch("robosystems.adapters.sec.processors.ingestion.staging.S3Client"),
    patch("robosystems.adapters.sec.processors.ingestion.staging.env") as mock_env,
  ):
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    from robosystems.adapters.sec.processors.ingestion.staging import DuckDBStager

    stager = DuckDBStager(graph_id="sec", source_prefix="sec/processed")
  return stager


@pytest.mark.unit
class TestDuckDBStagerInit:
  """Tests for DuckDBStager initialization."""

  @patch("robosystems.adapters.sec.processors.ingestion.staging.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.env")
  def test_default_init(self, mock_env, mock_s3_client):
    """Default initialization sets expected attributes."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    from robosystems.adapters.sec.processors.ingestion.staging import DuckDBStager

    stager = DuckDBStager()
    assert stager.graph_id == "sec"
    assert stager.source_prefix == "sec/processed"
    assert stager.bucket == "test-bucket"

  @patch("robosystems.adapters.sec.processors.ingestion.staging.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.env")
  def test_custom_init(self, mock_env, mock_s3_client):
    """Custom initialization overrides defaults."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    from robosystems.adapters.sec.processors.ingestion.staging import DuckDBStager

    stager = DuckDBStager(graph_id="custom_db", source_prefix="custom/prefix")
    assert stager.graph_id == "custom_db"
    assert stager.source_prefix == "custom/prefix"


@pytest.mark.unit
class TestStageToDuckDBGraphClientError:
  """Tests for stage_to_duckdb when graph client fails."""

  @pytest.mark.asyncio
  @patch("robosystems.adapters.sec.processors.ingestion.staging.get_graph_client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.env")
  async def test_graph_client_init_failure(
    self, mock_env, mock_s3_client, mock_get_client
  ):
    """Returns error when graph client fails to initialize."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    mock_get_client.side_effect = RuntimeError("Connection refused")

    from robosystems.adapters.sec.processors.ingestion.staging import DuckDBStager

    stager = DuckDBStager()
    result = await stager.stage_to_duckdb()

    assert result.status == "error"
    assert "Graph client initialization failed" in result.error


@pytest.mark.unit
class TestStageToDuckDBGlobMode:
  """Tests for stage_to_duckdb in glob mode."""

  @pytest.mark.asyncio
  @patch(
    "robosystems.adapters.sec.processors.ingestion.staging.get_staging_duckdb_path"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.staging.get_graph_client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.RoboLedgerContext")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.env")
  async def test_successful_glob_staging(
    self,
    mock_env,
    mock_s3_client,
    mock_context,
    mock_get_client,
    mock_duckdb_path,
  ):
    """Successful glob staging returns success with table info."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    mock_duckdb_path.return_value = "/data/staging/sec.duckdb"

    # Schema returns 2 tables
    mock_context.get_all_table_names_for_context.return_value = {
      "Entity": "nodes",
      "Report": "nodes",
    }
    mock_context.SEC_REPOSITORY = "sec"

    # Graph client mock
    mock_client = AsyncMock()
    mock_get_client.return_value = mock_client

    # create_table returns success for each table
    mock_client.create_table.return_value = {
      "status": "success",
      "result": {"row_count": 100, "duration_seconds": 1.5},
      "duration_seconds": 1.5,
    }

    from robosystems.adapters.sec.processors.ingestion.staging import DuckDBStager

    stager = DuckDBStager()
    result = await stager.stage_to_duckdb()

    assert result.status == "success"
    assert len(result.table_names) == 2
    assert "Entity" in result.table_names
    assert "Report" in result.table_names
    assert result.total_rows == 200  # 100 per table

  @pytest.mark.asyncio
  @patch(
    "robosystems.adapters.sec.processors.ingestion.staging.get_staging_duckdb_path"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.staging.get_graph_client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.RoboLedgerContext")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.env")
  async def test_no_files_found_skips_table(
    self,
    mock_env,
    mock_s3_client,
    mock_context,
    mock_get_client,
    mock_duckdb_path,
  ):
    """Tables with no files are skipped successfully."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    mock_duckdb_path.return_value = "/data/staging/sec.duckdb"

    mock_context.get_all_table_names_for_context.return_value = {
      "Entity": "nodes",
    }
    mock_context.SEC_REPOSITORY = "sec"

    mock_client = AsyncMock()
    mock_get_client.return_value = mock_client
    mock_client.create_table.return_value = {
      "status": "failed",
      "error": "No files found matching pattern",
    }

    from robosystems.adapters.sec.processors.ingestion.staging import DuckDBStager

    stager = DuckDBStager()
    result = await stager.stage_to_duckdb()

    assert result.status == "success"
    assert "Entity" in result.table_names

  @pytest.mark.asyncio
  @patch(
    "robosystems.adapters.sec.processors.ingestion.staging.get_staging_duckdb_path"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.staging.get_graph_client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.RoboLedgerContext")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.env")
  async def test_year_filter(
    self,
    mock_env,
    mock_s3_client,
    mock_context,
    mock_get_client,
    mock_duckdb_path,
  ):
    """Year parameter filters S3 patterns correctly."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    mock_duckdb_path.return_value = "/data/staging/sec.duckdb"

    mock_context.get_all_table_names_for_context.return_value = {
      "Entity": "nodes",
    }
    mock_context.SEC_REPOSITORY = "sec"

    mock_client = AsyncMock()
    mock_get_client.return_value = mock_client
    mock_client.create_table.return_value = {
      "status": "success",
      "result": {"row_count": 50},
      "duration_seconds": 1.0,
    }

    from robosystems.adapters.sec.processors.ingestion.staging import DuckDBStager

    stager = DuckDBStager()
    result = await stager.stage_to_duckdb(year=2024)

    assert result.status == "success"
    # Verify the S3 pattern uses the year filter
    create_call = mock_client.create_table.call_args
    s3_pattern = create_call.kwargs["s3_pattern"]
    assert "filed=2024-Q*" in s3_pattern


@pytest.mark.unit
class TestStageTableWithRetry:
  """Tests for _stage_table_with_retry retry logic."""

  @pytest.mark.asyncio
  @patch("robosystems.adapters.sec.processors.ingestion.staging.asyncio.sleep")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.env")
  async def test_retry_on_failure(self, mock_env, mock_s3_client, mock_sleep):
    """Retries with exponential backoff on failure."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"

    from robosystems.adapters.sec.processors.ingestion.staging import DuckDBStager

    stager = DuckDBStager()
    mock_client = AsyncMock()

    call_count = 0

    async def stage_fn():
      nonlocal call_count
      call_count += 1
      if call_count < 3:
        return False, None, f"Error attempt {call_count}"
      return (
        True,
        TableInfo(name="Entity", row_count=100, file_count=1, staged_at="now"),
        None,
      )

    success, info, error = await stager._stage_table_with_retry(
      table_name="Entity",
      stage_fn=stage_fn,
      graph_client=mock_client,
      log_progress=lambda msg: None,
      table_index=1,
      total_tables=1,
    )

    assert success is True
    assert info.row_count == 100
    assert call_count == 3

  @pytest.mark.asyncio
  @patch("robosystems.adapters.sec.processors.ingestion.staging.asyncio.sleep")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.env")
  async def test_all_retries_exhausted(self, mock_env, mock_s3_client, mock_sleep):
    """Returns failure after all retries are exhausted."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"

    from robosystems.adapters.sec.processors.ingestion.staging import DuckDBStager

    stager = DuckDBStager()
    mock_client = AsyncMock()

    async def always_fail():
      return False, None, "Persistent error"

    success, info, error = await stager._stage_table_with_retry(
      table_name="Entity",
      stage_fn=always_fail,
      graph_client=mock_client,
      log_progress=lambda msg: None,
      table_index=1,
      total_tables=1,
    )

    assert success is False
    assert info is None
    assert f"Failed after {STAGING_MAX_RETRIES} attempts" in error

  @pytest.mark.asyncio
  @patch("robosystems.adapters.sec.processors.ingestion.staging.asyncio.sleep")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.env")
  async def test_retry_drops_partial_table(self, mock_env, mock_s3_client, mock_sleep):
    """Drops partial table before each retry attempt."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"

    from robosystems.adapters.sec.processors.ingestion.staging import DuckDBStager

    stager = DuckDBStager()
    mock_client = AsyncMock()

    call_count = 0

    async def fail_then_succeed():
      nonlocal call_count
      call_count += 1
      if call_count == 1:
        return False, None, "First attempt failed"
      return (
        True,
        TableInfo(name="Entity", row_count=50, file_count=1, staged_at="now"),
        None,
      )

    await stager._stage_table_with_retry(
      table_name="Entity",
      stage_fn=fail_then_succeed,
      graph_client=mock_client,
      log_progress=lambda msg: None,
      table_index=1,
      total_tables=1,
    )

    # delete_table should be called once before the retry
    mock_client.delete_table.assert_called_once_with("sec", "Entity")

  @pytest.mark.asyncio
  @patch("robosystems.adapters.sec.processors.ingestion.staging.asyncio.sleep")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.env")
  async def test_first_attempt_success_no_retry(
    self, mock_env, mock_s3_client, mock_sleep
  ):
    """No retries needed when first attempt succeeds."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"

    from robosystems.adapters.sec.processors.ingestion.staging import DuckDBStager

    stager = DuckDBStager()
    mock_client = AsyncMock()

    async def succeed_immediately():
      return (
        True,
        TableInfo(name="Entity", row_count=100, file_count=1, staged_at="now"),
        None,
      )

    success, info, error = await stager._stage_table_with_retry(
      table_name="Entity",
      stage_fn=succeed_immediately,
      graph_client=mock_client,
      log_progress=lambda msg: None,
      table_index=1,
      total_tables=1,
    )

    assert success is True
    mock_sleep.assert_not_called()


@pytest.mark.unit
class TestStageIncrementalToDuckDB:
  """Tests for stage_incremental_to_duckdb."""

  @pytest.mark.asyncio
  @patch("robosystems.adapters.sec.processors.ingestion.staging.get_graph_client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.env")
  async def test_no_existing_tables_returns_error(
    self, mock_env, mock_s3_client, mock_get_client
  ):
    """Returns error when no DuckDB tables exist for incremental staging."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"

    mock_client = AsyncMock()
    mock_client.list_tables.return_value = []
    mock_get_client.return_value = mock_client

    from robosystems.adapters.sec.processors.ingestion.staging import DuckDBStager

    stager = DuckDBStager()
    result = await stager.stage_incremental_to_duckdb(year=2024, quarter=1)

    assert result.status == "error"
    assert "No existing DuckDB tables" in result.error

  @pytest.mark.asyncio
  @patch("robosystems.adapters.sec.processors.ingestion.staging.asyncio.sleep")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.s3_get_table_patterns")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.RoboLedgerContext")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.get_graph_client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.env")
  async def test_incremental_insert_success(
    self,
    mock_env,
    mock_s3_client,
    mock_get_client,
    mock_context,
    mock_patterns,
    mock_sleep,
  ):
    """Successful incremental insert returns updated row counts."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"

    mock_context.get_all_table_names_for_context.return_value = {
      "Entity": "nodes",
    }
    mock_context.SEC_REPOSITORY = "sec"

    mock_client = AsyncMock()
    mock_client.list_tables.return_value = [{"table_name": "Entity"}]
    mock_client.insert_into_table.return_value = {
      "status": "success",
      "result": {"row_count": 50},
      "duration_seconds": 2.0,
    }
    mock_get_client.return_value = mock_client

    mock_patterns.return_value = [
      "s3://test-bucket/sec/processed/filed=2024-Q1/nodes/Entity/*.parquet"
    ]

    from robosystems.adapters.sec.processors.ingestion.staging import DuckDBStager

    stager = DuckDBStager()
    result = await stager.stage_incremental_to_duckdb(year=2024, quarter=1)

    assert result.status == "success"
    assert "Entity" in result.table_names
    assert result.total_rows == 50

  @pytest.mark.asyncio
  @patch("robosystems.adapters.sec.processors.ingestion.staging.asyncio.sleep")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.s3_get_table_patterns")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.RoboLedgerContext")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.get_graph_client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.env")
  async def test_incremental_no_files_for_table(
    self,
    mock_env,
    mock_s3_client,
    mock_get_client,
    mock_context,
    mock_patterns,
    mock_sleep,
  ):
    """Table with no S3 files is skipped successfully."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"

    mock_context.get_all_table_names_for_context.return_value = {
      "Entity": "nodes",
    }
    mock_context.SEC_REPOSITORY = "sec"

    mock_client = AsyncMock()
    mock_client.list_tables.return_value = [{"table_name": "Entity"}]
    mock_get_client.return_value = mock_client

    # No S3 patterns found
    mock_patterns.return_value = []

    from robosystems.adapters.sec.processors.ingestion.staging import DuckDBStager

    stager = DuckDBStager()
    result = await stager.stage_incremental_to_duckdb(year=2024, quarter=1)

    assert result.status == "success"
    assert "Entity" in result.table_names
    assert result.tables["Entity"].skipped is True

  @pytest.mark.asyncio
  @patch("robosystems.adapters.sec.processors.ingestion.staging.asyncio.sleep")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.s3_get_table_patterns")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.RoboLedgerContext")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.get_graph_client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.env")
  async def test_incremental_skips_nonexistent_tables(
    self,
    mock_env,
    mock_s3_client,
    mock_get_client,
    mock_context,
    mock_patterns,
    mock_sleep,
  ):
    """Tables not in DuckDB are skipped with zero rows."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"

    mock_context.get_all_table_names_for_context.return_value = {
      "Entity": "nodes",
      "NewTable": "nodes",
    }
    mock_context.SEC_REPOSITORY = "sec"

    mock_client = AsyncMock()
    # Only Entity exists in DuckDB
    mock_client.list_tables.return_value = [{"table_name": "Entity"}]
    mock_client.insert_into_table.return_value = {
      "status": "success",
      "result": {"row_count": 10},
      "duration_seconds": 0.5,
    }
    mock_get_client.return_value = mock_client

    mock_patterns.return_value = [
      "s3://test-bucket/sec/processed/filed=2024-Q1/nodes/Entity/*.parquet"
    ]

    from robosystems.adapters.sec.processors.ingestion.staging import DuckDBStager

    stager = DuckDBStager()
    result = await stager.stage_incremental_to_duckdb(year=2024, quarter=1)

    assert result.status == "success"
    assert "Entity" in result.table_names
    assert "NewTable" in result.table_names
    assert result.tables["NewTable"].skipped is True


@pytest.mark.unit
class TestStageToDuckDBResetStaging:
  """Tests for stage_to_duckdb with reset_staging flag."""

  @pytest.mark.asyncio
  @patch(
    "robosystems.adapters.sec.processors.ingestion.staging.get_staging_duckdb_path"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.staging.get_graph_client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.RoboLedgerContext")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.env")
  async def test_reset_staging_deletes_database(
    self,
    mock_env,
    mock_s3_client,
    mock_context,
    mock_get_client,
    mock_duckdb_path,
  ):
    """reset_staging=True triggers delete_database with staging_only=True."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    mock_duckdb_path.return_value = "/data/staging/sec.duckdb"

    mock_context.get_all_table_names_for_context.return_value = {}
    mock_context.SEC_REPOSITORY = "sec"

    mock_client = AsyncMock()
    mock_get_client.return_value = mock_client

    from robosystems.adapters.sec.processors.ingestion.staging import DuckDBStager

    stager = DuckDBStager()
    await stager.stage_to_duckdb(reset_staging=True)

    mock_client.delete_database.assert_called_once_with("sec", staging_only=True)

  @pytest.mark.asyncio
  @patch(
    "robosystems.adapters.sec.processors.ingestion.staging.get_staging_duckdb_path"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.staging.get_graph_client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.RoboLedgerContext")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.env")
  async def test_reset_staging_non_fatal_on_failure(
    self,
    mock_env,
    mock_s3_client,
    mock_context,
    mock_get_client,
    mock_duckdb_path,
  ):
    """Reset failure is non-fatal (database might not exist yet)."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    mock_duckdb_path.return_value = "/data/staging/sec.duckdb"

    mock_context.get_all_table_names_for_context.return_value = {}
    mock_context.SEC_REPOSITORY = "sec"

    mock_client = AsyncMock()
    mock_client.delete_database.side_effect = RuntimeError("DB not found")
    mock_get_client.return_value = mock_client

    from robosystems.adapters.sec.processors.ingestion.staging import DuckDBStager

    stager = DuckDBStager()
    # Should NOT raise despite reset failure
    result = await stager.stage_to_duckdb(reset_staging=True)
    assert result.status == "success"


@pytest.mark.unit
class TestStageToDuckDBExceptionHandling:
  """Tests for exception handling in stage_to_duckdb."""

  @pytest.mark.asyncio
  @patch(
    "robosystems.adapters.sec.processors.ingestion.staging.get_staging_duckdb_path"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.staging.get_graph_client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.RoboLedgerContext")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.staging.env")
  async def test_unexpected_exception_returns_error(
    self,
    mock_env,
    mock_s3_client,
    mock_context,
    mock_get_client,
    mock_duckdb_path,
  ):
    """Unexpected exceptions are caught and returned as error result."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    mock_duckdb_path.return_value = "/data/staging/sec.duckdb"

    mock_context.get_all_table_names_for_context.side_effect = RuntimeError(
      "Schema error"
    )
    mock_context.SEC_REPOSITORY = "sec"

    mock_client = AsyncMock()
    mock_get_client.return_value = mock_client

    from robosystems.adapters.sec.processors.ingestion.staging import DuckDBStager

    stager = DuckDBStager()
    result = await stager.stage_to_duckdb()

    assert result.status == "error"
    assert "Schema error" in result.error
