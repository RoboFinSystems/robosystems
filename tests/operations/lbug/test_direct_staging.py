"""
Tests for direct file staging to DuckDB without Dagster orchestration.

These tests cover the fast path for staging small files directly to DuckDB,
bypassing Dagster job overhead while still reporting AssetMaterializations
for observability in the Dagster UI.
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from robosystems.operations.lbug.direct_staging import (
  DAGSTER_REPORT_TIMEOUT,
  _report_staging_materialization,
  _report_staging_materialization_sync,
  stage_file_directly,
)

MODULE = "robosystems.operations.lbug.direct_staging"

# Patch targets for lazy imports (patched at their source modules)
FACTORY_PATH = "robosystems.graph_api.client.factory.GraphClientFactory"
GRAPH_FILE_PATH = "robosystems.models.iam.GraphFile"
GRAPH_TABLE_PATH = "robosystems.models.iam.GraphTable"


class TestStageFileDirectly:
  """Test stage_file_directly function."""

  @pytest.fixture
  def mock_db_session(self):
    """Create a mock database session."""
    return Mock()

  @pytest.fixture
  def mock_graph_file(self):
    """Create a mock GraphFile object."""
    gf = Mock()
    gf.id = "gf_file1"
    gf.s3_key = "graphs/kg1234/tables/entity/file1.parquet"
    gf.upload_status = "uploaded"
    gf.mark_duckdb_staged = Mock()
    return gf

  @pytest.fixture
  def mock_graph_file_pending(self):
    """Create a mock GraphFile with pending upload status."""
    gf = Mock()
    gf.id = "gf_file2"
    gf.s3_key = "graphs/kg1234/tables/entity/file2.parquet"
    gf.upload_status = "pending"
    gf.mark_duckdb_staged = Mock()
    return gf

  @pytest.fixture
  def mock_graph_table(self):
    """Create a mock GraphTable object."""
    table = Mock()
    table.id = "gt_table1"
    table.table_name = "Entity"
    return table

  @pytest.fixture
  def mock_graph_client(self):
    """Create a mock async Graph API client."""
    client = AsyncMock()
    client.list_tables = AsyncMock(return_value=[])
    client.create_table = AsyncMock(return_value={"status": "created", "rows": 50})
    client.insert_into_table = AsyncMock(
      return_value={"status": "inserted", "rows": 10}
    )
    client.close = AsyncMock()
    return client

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_successful_staging_new_table(
    self, mock_db_session, mock_graph_file, mock_graph_table, mock_graph_client
  ):
    """Test successful staging when the DuckDB table does not yet exist."""
    with (
      patch(FACTORY_PATH) as mock_factory,
      patch(GRAPH_FILE_PATH) as mock_file_class,
      patch(GRAPH_TABLE_PATH) as mock_table_class,
      patch(f"{MODULE}._report_staging_materialization", new_callable=AsyncMock),
      patch(f"{MODULE}.env") as mock_env,
    ):
      mock_env.USER_DATA_BUCKET = "robosystems-local"
      mock_file_class.get_by_id.return_value = mock_graph_file
      mock_table_class.get_by_id.return_value = mock_graph_table
      mock_file_class.get_all_for_table.return_value = [mock_graph_file]
      mock_factory.create_client = AsyncMock(return_value=mock_graph_client)
      mock_graph_client.list_tables.return_value = []

      result = await stage_file_directly(
        db=mock_db_session,
        file_id="gf_file1",
        graph_id="kg1234567890abcdef",
        table_id="gt_table1",
        s3_key="graphs/kg1234/tables/entity/file1.parquet",
        file_size_bytes=1024,
        row_count=50,
      )

      assert result["status"] == "success"
      assert result["file_id"] == "gf_file1"
      assert result["graph_id"] == "kg1234567890abcdef"
      assert result["table_name"] == "Entity"
      assert result["files_staged"] == 1
      assert result["duckdb_status"] == "staged"
      assert result["method"] == "direct"
      assert "duration_ms" in result
      mock_graph_client.create_table.assert_called_once()
      mock_graph_client.insert_into_table.assert_not_called()
      mock_graph_file.mark_duckdb_staged.assert_called_once_with(
        session=mock_db_session, row_count=50
      )
      mock_graph_client.close.assert_called_once()

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_successful_staging_existing_table_incremental(
    self, mock_db_session, mock_graph_file, mock_graph_table, mock_graph_client
  ):
    """Test incremental INSERT INTO when the DuckDB table already exists."""
    with (
      patch(FACTORY_PATH) as mock_factory,
      patch(GRAPH_FILE_PATH) as mock_file_class,
      patch(GRAPH_TABLE_PATH) as mock_table_class,
      patch(f"{MODULE}._report_staging_materialization", new_callable=AsyncMock),
      patch(f"{MODULE}.env") as mock_env,
    ):
      mock_env.USER_DATA_BUCKET = "robosystems-local"
      mock_file_class.get_by_id.return_value = mock_graph_file
      mock_table_class.get_by_id.return_value = mock_graph_table
      mock_file_class.get_all_for_table.return_value = [mock_graph_file]
      mock_factory.create_client = AsyncMock(return_value=mock_graph_client)
      # Table already exists
      mock_graph_client.list_tables.return_value = [
        {"table_name": "Entity", "row_count": 100}
      ]

      result = await stage_file_directly(
        db=mock_db_session,
        file_id="gf_file1",
        graph_id="kg1234567890abcdef",
        table_id="gt_table1",
        s3_key="graphs/kg1234/tables/entity/file1.parquet",
        file_size_bytes=1024,
        row_count=10,
      )

      assert result["status"] == "success"
      mock_graph_client.insert_into_table.assert_called_once_with(
        graph_id="kg1234567890abcdef",
        table_name="Entity",
        s3_pattern=["s3://robosystems-local/graphs/kg1234/tables/entity/file1.parquet"],
        deduplicate=True,
      )
      mock_graph_client.create_table.assert_not_called()
      mock_graph_client.close.assert_called_once()

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_file_not_found(self, mock_db_session):
    """Test staging when the file record is not found in the database."""
    with (
      patch(GRAPH_FILE_PATH) as mock_file_class,
      patch(GRAPH_TABLE_PATH),
    ):
      mock_file_class.get_by_id.return_value = None

      result = await stage_file_directly(
        db=mock_db_session,
        file_id="gf_nonexistent",
        graph_id="kg1234567890abcdef",
        table_id="gt_table1",
        s3_key="some/key.parquet",
        file_size_bytes=1024,
      )

      assert result["status"] == "error"
      assert "not found" in result["message"]
      assert result["file_id"] == "gf_nonexistent"

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_table_not_found(self, mock_db_session, mock_graph_file):
    """Test staging when the table record is not found in the database."""
    with (
      patch(GRAPH_FILE_PATH) as mock_file_class,
      patch(GRAPH_TABLE_PATH) as mock_table_class,
    ):
      mock_file_class.get_by_id.return_value = mock_graph_file
      mock_table_class.get_by_id.return_value = None

      result = await stage_file_directly(
        db=mock_db_session,
        file_id="gf_file1",
        graph_id="kg1234567890abcdef",
        table_id="gt_nonexistent",
        s3_key="some/key.parquet",
        file_size_bytes=1024,
      )

      assert result["status"] == "error"
      assert "not found" in result["message"]
      assert result["file_id"] == "gf_file1"

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_no_uploaded_files(
    self, mock_db_session, mock_graph_file_pending, mock_graph_table
  ):
    """Test staging when no files have upload_status of 'uploaded'."""
    with (
      patch(GRAPH_FILE_PATH) as mock_file_class,
      patch(GRAPH_TABLE_PATH) as mock_table_class,
    ):
      mock_file_class.get_by_id.return_value = mock_graph_file_pending
      mock_table_class.get_by_id.return_value = mock_graph_table
      # All files are pending, none uploaded
      mock_file_class.get_all_for_table.return_value = [mock_graph_file_pending]

      result = await stage_file_directly(
        db=mock_db_session,
        file_id="gf_file2",
        graph_id="kg1234567890abcdef",
        table_id="gt_table1",
        s3_key="some/key.parquet",
        file_size_bytes=1024,
      )

      assert result["status"] == "skipped"
      assert "No uploaded files" in result["message"]

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_multiple_uploaded_files_first_time(
    self, mock_db_session, mock_graph_table, mock_graph_client
  ):
    """Test staging with multiple uploaded files when table does not exist yet."""
    file1 = Mock()
    file1.id = "gf_f1"
    file1.s3_key = "graphs/kg1234/tables/entity/file1.parquet"
    file1.upload_status = "uploaded"
    file1.mark_duckdb_staged = Mock()

    file2 = Mock()
    file2.id = "gf_f2"
    file2.s3_key = "graphs/kg1234/tables/entity/file2.parquet"
    file2.upload_status = "uploaded"

    with (
      patch(FACTORY_PATH) as mock_factory,
      patch(GRAPH_FILE_PATH) as mock_file_class,
      patch(GRAPH_TABLE_PATH) as mock_table_class,
      patch(f"{MODULE}._report_staging_materialization", new_callable=AsyncMock),
      patch(f"{MODULE}.env") as mock_env,
    ):
      mock_env.USER_DATA_BUCKET = "robosystems-local"
      mock_file_class.get_by_id.return_value = file1
      mock_table_class.get_by_id.return_value = mock_graph_table
      mock_file_class.get_all_for_table.return_value = [file1, file2]
      mock_factory.create_client = AsyncMock(return_value=mock_graph_client)
      mock_graph_client.list_tables.return_value = []

      result = await stage_file_directly(
        db=mock_db_session,
        file_id="gf_f1",
        graph_id="kg1234567890abcdef",
        table_id="gt_table1",
        s3_key="graphs/kg1234/tables/entity/file1.parquet",
        file_size_bytes=2048,
        row_count=100,
      )

      assert result["status"] == "success"
      assert result["files_staged"] == 2
      # create_table should be called with both files
      mock_graph_client.create_table.assert_called_once()
      call_kwargs = mock_graph_client.create_table.call_args
      assert len(call_kwargs.kwargs["s3_pattern"]) == 2

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_graph_client_error(
    self, mock_db_session, mock_graph_file, mock_graph_table
  ):
    """Test handling of Graph API client errors during staging."""
    mock_client = AsyncMock()
    mock_client.list_tables = AsyncMock(
      side_effect=Exception("Graph API connection failed")
    )
    mock_client.close = AsyncMock()

    with (
      patch(FACTORY_PATH) as mock_factory,
      patch(GRAPH_FILE_PATH) as mock_file_class,
      patch(GRAPH_TABLE_PATH) as mock_table_class,
      patch(f"{MODULE}.env") as mock_env,
    ):
      mock_env.USER_DATA_BUCKET = "robosystems-local"
      mock_file_class.get_by_id.return_value = mock_graph_file
      mock_table_class.get_by_id.return_value = mock_graph_table
      mock_file_class.get_all_for_table.return_value = [mock_graph_file]
      mock_factory.create_client = AsyncMock(return_value=mock_client)

      result = await stage_file_directly(
        db=mock_db_session,
        file_id="gf_file1",
        graph_id="kg1234567890abcdef",
        table_id="gt_table1",
        s3_key="some/key.parquet",
        file_size_bytes=1024,
      )

      assert result["status"] == "error"
      assert "Graph API connection failed" in result["message"]
      assert result["method"] == "direct"
      assert "duration_ms" in result
      # Client close should still be called in the finally block
      mock_client.close.assert_called_once()

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_create_table_error(
    self, mock_db_session, mock_graph_file, mock_graph_table
  ):
    """Test handling of errors during table creation."""
    mock_client = AsyncMock()
    mock_client.list_tables = AsyncMock(return_value=[])
    mock_client.create_table = AsyncMock(
      side_effect=Exception("DuckDB create table failed")
    )
    mock_client.close = AsyncMock()

    with (
      patch(FACTORY_PATH) as mock_factory,
      patch(GRAPH_FILE_PATH) as mock_file_class,
      patch(GRAPH_TABLE_PATH) as mock_table_class,
      patch(f"{MODULE}.env") as mock_env,
    ):
      mock_env.USER_DATA_BUCKET = "robosystems-local"
      mock_file_class.get_by_id.return_value = mock_graph_file
      mock_table_class.get_by_id.return_value = mock_graph_table
      mock_file_class.get_all_for_table.return_value = [mock_graph_file]
      mock_factory.create_client = AsyncMock(return_value=mock_client)

      result = await stage_file_directly(
        db=mock_db_session,
        file_id="gf_file1",
        graph_id="kg1234567890abcdef",
        table_id="gt_table1",
        s3_key="some/key.parquet",
        file_size_bytes=1024,
      )

      assert result["status"] == "error"
      assert "DuckDB create table failed" in result["message"]
      mock_client.close.assert_called_once()

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_insert_into_table_error(
    self, mock_db_session, mock_graph_file, mock_graph_table
  ):
    """Test handling of errors during incremental INSERT INTO."""
    mock_client = AsyncMock()
    mock_client.list_tables = AsyncMock(return_value=[{"table_name": "Entity"}])
    mock_client.insert_into_table = AsyncMock(
      side_effect=Exception("DuckDB insert failed")
    )
    mock_client.close = AsyncMock()

    with (
      patch(FACTORY_PATH) as mock_factory,
      patch(GRAPH_FILE_PATH) as mock_file_class,
      patch(GRAPH_TABLE_PATH) as mock_table_class,
      patch(f"{MODULE}.env") as mock_env,
    ):
      mock_env.USER_DATA_BUCKET = "robosystems-local"
      mock_file_class.get_by_id.return_value = mock_graph_file
      mock_table_class.get_by_id.return_value = mock_graph_table
      mock_file_class.get_all_for_table.return_value = [mock_graph_file]
      mock_factory.create_client = AsyncMock(return_value=mock_client)

      result = await stage_file_directly(
        db=mock_db_session,
        file_id="gf_file1",
        graph_id="kg1234567890abcdef",
        table_id="gt_table1",
        s3_key="some/key.parquet",
        file_size_bytes=1024,
      )

      assert result["status"] == "error"
      assert "DuckDB insert failed" in result["message"]
      mock_client.close.assert_called_once()

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_row_count_none_defaults_to_zero(
    self, mock_db_session, mock_graph_file, mock_graph_table, mock_graph_client
  ):
    """Test that row_count=None is passed as 0 to mark_duckdb_staged."""
    with (
      patch(FACTORY_PATH) as mock_factory,
      patch(GRAPH_FILE_PATH) as mock_file_class,
      patch(GRAPH_TABLE_PATH) as mock_table_class,
      patch(f"{MODULE}._report_staging_materialization", new_callable=AsyncMock),
      patch(f"{MODULE}.env") as mock_env,
    ):
      mock_env.USER_DATA_BUCKET = "robosystems-local"
      mock_file_class.get_by_id.return_value = mock_graph_file
      mock_table_class.get_by_id.return_value = mock_graph_table
      mock_file_class.get_all_for_table.return_value = [mock_graph_file]
      mock_factory.create_client = AsyncMock(return_value=mock_graph_client)

      result = await stage_file_directly(
        db=mock_db_session,
        file_id="gf_file1",
        graph_id="kg1234567890abcdef",
        table_id="gt_table1",
        s3_key="some/key.parquet",
        file_size_bytes=1024,
        row_count=None,
      )

      assert result["status"] == "success"
      mock_graph_file.mark_duckdb_staged.assert_called_once_with(
        session=mock_db_session, row_count=0
      )

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_s3_uri_construction(
    self, mock_db_session, mock_graph_file, mock_graph_table, mock_graph_client
  ):
    """Test that S3 URIs are properly constructed with the bucket name."""
    with (
      patch(FACTORY_PATH) as mock_factory,
      patch(GRAPH_FILE_PATH) as mock_file_class,
      patch(GRAPH_TABLE_PATH) as mock_table_class,
      patch(f"{MODULE}._report_staging_materialization", new_callable=AsyncMock),
      patch(f"{MODULE}.env") as mock_env,
    ):
      mock_env.USER_DATA_BUCKET = "my-custom-bucket"
      mock_file_class.get_by_id.return_value = mock_graph_file
      mock_table_class.get_by_id.return_value = mock_graph_table
      mock_file_class.get_all_for_table.return_value = [mock_graph_file]
      mock_factory.create_client = AsyncMock(return_value=mock_graph_client)
      mock_graph_client.list_tables.return_value = []

      await stage_file_directly(
        db=mock_db_session,
        file_id="gf_file1",
        graph_id="kg1234567890abcdef",
        table_id="gt_table1",
        s3_key="graphs/kg1234/tables/entity/file1.parquet",
        file_size_bytes=1024,
      )

      call_kwargs = mock_graph_client.create_table.call_args.kwargs
      assert call_kwargs["s3_pattern"] == [
        "s3://my-custom-bucket/graphs/kg1234/tables/entity/file1.parquet"
      ]
      # file_id_map should also use the correct bucket
      assert (
        "s3://my-custom-bucket/graphs/kg1234/tables/entity/file1.parquet"
        in call_kwargs["file_id_map"]
      )

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_file_id_map_construction(
    self, mock_db_session, mock_graph_table, mock_graph_client
  ):
    """Test that the file_id_map maps S3 URIs to file IDs correctly."""
    file1 = Mock()
    file1.id = "gf_aaa"
    file1.s3_key = "path/to/file_a.parquet"
    file1.upload_status = "uploaded"
    file1.mark_duckdb_staged = Mock()

    file2 = Mock()
    file2.id = "gf_bbb"
    file2.s3_key = "path/to/file_b.parquet"
    file2.upload_status = "uploaded"

    with (
      patch(FACTORY_PATH) as mock_factory,
      patch(GRAPH_FILE_PATH) as mock_file_class,
      patch(GRAPH_TABLE_PATH) as mock_table_class,
      patch(f"{MODULE}._report_staging_materialization", new_callable=AsyncMock),
      patch(f"{MODULE}.env") as mock_env,
    ):
      mock_env.USER_DATA_BUCKET = "test-bucket"
      mock_file_class.get_by_id.return_value = file1
      mock_table_class.get_by_id.return_value = mock_graph_table
      mock_file_class.get_all_for_table.return_value = [file1, file2]
      mock_factory.create_client = AsyncMock(return_value=mock_graph_client)
      mock_graph_client.list_tables.return_value = []

      await stage_file_directly(
        db=mock_db_session,
        file_id="gf_aaa",
        graph_id="kg1234567890abcdef",
        table_id="gt_table1",
        s3_key="path/to/file_a.parquet",
        file_size_bytes=1024,
      )

      call_kwargs = mock_graph_client.create_table.call_args.kwargs
      file_id_map = call_kwargs["file_id_map"]
      assert file_id_map["s3://test-bucket/path/to/file_a.parquet"] == "gf_aaa"
      assert file_id_map["s3://test-bucket/path/to/file_b.parquet"] == "gf_bbb"

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_client_closed_on_success(
    self, mock_db_session, mock_graph_file, mock_graph_table, mock_graph_client
  ):
    """Test that the graph client is always closed after successful staging."""
    with (
      patch(FACTORY_PATH) as mock_factory,
      patch(GRAPH_FILE_PATH) as mock_file_class,
      patch(GRAPH_TABLE_PATH) as mock_table_class,
      patch(f"{MODULE}._report_staging_materialization", new_callable=AsyncMock),
      patch(f"{MODULE}.env") as mock_env,
    ):
      mock_env.USER_DATA_BUCKET = "robosystems-local"
      mock_file_class.get_by_id.return_value = mock_graph_file
      mock_table_class.get_by_id.return_value = mock_graph_table
      mock_file_class.get_all_for_table.return_value = [mock_graph_file]
      mock_factory.create_client = AsyncMock(return_value=mock_graph_client)

      await stage_file_directly(
        db=mock_db_session,
        file_id="gf_file1",
        graph_id="kg1234567890abcdef",
        table_id="gt_table1",
        s3_key="some/key.parquet",
        file_size_bytes=512,
      )

      mock_graph_client.close.assert_called_once()

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_client_closed_on_error(
    self, mock_db_session, mock_graph_file, mock_graph_table
  ):
    """Test that the graph client is closed even when staging fails."""
    mock_client = AsyncMock()
    mock_client.list_tables = AsyncMock(return_value=[])
    mock_client.create_table = AsyncMock(side_effect=RuntimeError("Boom"))
    mock_client.close = AsyncMock()

    with (
      patch(FACTORY_PATH) as mock_factory,
      patch(GRAPH_FILE_PATH) as mock_file_class,
      patch(GRAPH_TABLE_PATH) as mock_table_class,
      patch(f"{MODULE}.env") as mock_env,
    ):
      mock_env.USER_DATA_BUCKET = "robosystems-local"
      mock_file_class.get_by_id.return_value = mock_graph_file
      mock_table_class.get_by_id.return_value = mock_graph_table
      mock_file_class.get_all_for_table.return_value = [mock_graph_file]
      mock_factory.create_client = AsyncMock(return_value=mock_client)

      result = await stage_file_directly(
        db=mock_db_session,
        file_id="gf_file1",
        graph_id="kg1234567890abcdef",
        table_id="gt_table1",
        s3_key="some/key.parquet",
        file_size_bytes=512,
      )

      assert result["status"] == "error"
      # close is in the finally block, so it should always be called
      mock_client.close.assert_called_once()

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_mixed_upload_status_filters_correctly(
    self, mock_db_session, mock_graph_table, mock_graph_client
  ):
    """Test that only files with upload_status='uploaded' are included."""
    uploaded = Mock()
    uploaded.id = "gf_up"
    uploaded.s3_key = "path/uploaded.parquet"
    uploaded.upload_status = "uploaded"
    uploaded.mark_duckdb_staged = Mock()

    pending = Mock()
    pending.id = "gf_pend"
    pending.s3_key = "path/pending.parquet"
    pending.upload_status = "pending"

    failed = Mock()
    failed.id = "gf_fail"
    failed.s3_key = "path/failed.parquet"
    failed.upload_status = "failed"

    with (
      patch(FACTORY_PATH) as mock_factory,
      patch(GRAPH_FILE_PATH) as mock_file_class,
      patch(GRAPH_TABLE_PATH) as mock_table_class,
      patch(f"{MODULE}._report_staging_materialization", new_callable=AsyncMock),
      patch(f"{MODULE}.env") as mock_env,
    ):
      mock_env.USER_DATA_BUCKET = "robosystems-local"
      mock_file_class.get_by_id.return_value = uploaded
      mock_table_class.get_by_id.return_value = mock_graph_table
      mock_file_class.get_all_for_table.return_value = [
        uploaded,
        pending,
        failed,
      ]
      mock_factory.create_client = AsyncMock(return_value=mock_graph_client)
      mock_graph_client.list_tables.return_value = []

      result = await stage_file_directly(
        db=mock_db_session,
        file_id="gf_up",
        graph_id="kg1234567890abcdef",
        table_id="gt_table1",
        s3_key="path/uploaded.parquet",
        file_size_bytes=512,
      )

      assert result["status"] == "success"
      assert result["files_staged"] == 1
      call_kwargs = mock_graph_client.create_table.call_args.kwargs
      assert len(call_kwargs["s3_pattern"]) == 1
      assert "uploaded.parquet" in call_kwargs["s3_pattern"][0]

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_duration_ms_is_measured(
    self, mock_db_session, mock_graph_file, mock_graph_table, mock_graph_client
  ):
    """Test that duration_ms is included in successful and error results."""
    with (
      patch(FACTORY_PATH) as mock_factory,
      patch(GRAPH_FILE_PATH) as mock_file_class,
      patch(GRAPH_TABLE_PATH) as mock_table_class,
      patch(f"{MODULE}._report_staging_materialization", new_callable=AsyncMock),
      patch(f"{MODULE}.env") as mock_env,
    ):
      mock_env.USER_DATA_BUCKET = "robosystems-local"
      mock_file_class.get_by_id.return_value = mock_graph_file
      mock_table_class.get_by_id.return_value = mock_graph_table
      mock_file_class.get_all_for_table.return_value = [mock_graph_file]
      mock_factory.create_client = AsyncMock(return_value=mock_graph_client)

      result = await stage_file_directly(
        db=mock_db_session,
        file_id="gf_file1",
        graph_id="kg1234567890abcdef",
        table_id="gt_table1",
        s3_key="some/key.parquet",
        file_size_bytes=1024,
      )

      assert "duration_ms" in result
      assert isinstance(result["duration_ms"], float)
      assert result["duration_ms"] >= 0

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_report_materialization_called_on_success(
    self, mock_db_session, mock_graph_file, mock_graph_table, mock_graph_client
  ):
    """Test that _report_staging_materialization is called after successful staging."""
    with (
      patch(FACTORY_PATH) as mock_factory,
      patch(GRAPH_FILE_PATH) as mock_file_class,
      patch(GRAPH_TABLE_PATH) as mock_table_class,
      patch(
        f"{MODULE}._report_staging_materialization", new_callable=AsyncMock
      ) as mock_report,
      patch(f"{MODULE}.env") as mock_env,
    ):
      mock_env.USER_DATA_BUCKET = "robosystems-local"
      mock_file_class.get_by_id.return_value = mock_graph_file
      mock_table_class.get_by_id.return_value = mock_graph_table
      mock_file_class.get_all_for_table.return_value = [mock_graph_file]
      mock_factory.create_client = AsyncMock(return_value=mock_graph_client)

      await stage_file_directly(
        db=mock_db_session,
        file_id="gf_file1",
        graph_id="kg1234567890abcdef",
        table_id="gt_table1",
        s3_key="some/key.parquet",
        file_size_bytes=2048,
        row_count=75,
      )

      mock_report.assert_called_once()
      call_kwargs = mock_report.call_args.kwargs
      assert call_kwargs["file_id"] == "gf_file1"
      assert call_kwargs["graph_id"] == "kg1234567890abcdef"
      assert call_kwargs["table_name"] == "Entity"
      assert call_kwargs["file_size_bytes"] == 2048
      assert call_kwargs["row_count"] == 75
      assert call_kwargs["files_staged"] == 1


class TestStageFileDirectlyDeduplicate:
  """Test that insert_into_table always deduplicates."""

  @pytest.fixture
  def mock_db_session(self):
    return Mock()

  @pytest.fixture
  def mock_graph_file(self):
    gf = Mock()
    gf.id = "gf_file1"
    gf.s3_key = "graphs/kg1234/tables/entity/file1.parquet"
    gf.upload_status = "uploaded"
    gf.mark_duckdb_staged = Mock()
    return gf

  @pytest.fixture
  def mock_graph_table(self):
    table = Mock()
    table.id = "gt_table1"
    table.table_name = "Entity"
    return table

  @pytest.fixture
  def mock_graph_client(self):
    client = AsyncMock()
    client.list_tables = AsyncMock(
      return_value=[{"table_name": "Entity", "row_count": 100}]
    )
    client.insert_into_table = AsyncMock(
      return_value={"status": "inserted", "rows": 10}
    )
    client.close = AsyncMock()
    return client

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_insert_always_deduplicates(
    self, mock_db_session, mock_graph_file, mock_graph_table, mock_graph_client
  ):
    """Test that insert_into_table is called with deduplicate=True."""
    with (
      patch(FACTORY_PATH) as mock_factory,
      patch(GRAPH_FILE_PATH) as mock_file_class,
      patch(GRAPH_TABLE_PATH) as mock_table_class,
      patch(f"{MODULE}._report_staging_materialization", new_callable=AsyncMock),
      patch(f"{MODULE}.env") as mock_env,
    ):
      mock_env.USER_DATA_BUCKET = "robosystems-local"
      mock_file_class.get_by_id.return_value = mock_graph_file
      mock_table_class.get_by_id.return_value = mock_graph_table
      mock_file_class.get_all_for_table.return_value = [mock_graph_file]
      mock_factory.create_client = AsyncMock(return_value=mock_graph_client)

      result = await stage_file_directly(
        db=mock_db_session,
        file_id="gf_file1",
        graph_id="kg1234567890abcdef",
        table_id="gt_table1",
        s3_key="graphs/kg1234/tables/entity/file1.parquet",
        file_size_bytes=1024,
      )

      assert result["status"] == "success"
      mock_graph_client.insert_into_table.assert_called_once_with(
        graph_id="kg1234567890abcdef",
        table_name="Entity",
        s3_pattern=["s3://robosystems-local/graphs/kg1234/tables/entity/file1.parquet"],
        deduplicate=True,
      )


class TestReportStagingMaterialization:
  """Test _report_staging_materialization async function."""

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_successful_reporting(self):
    """Test successful Dagster materialization reporting."""
    with patch(f"{MODULE}._report_staging_materialization_sync") as mock_sync:
      await _report_staging_materialization(
        file_id="gf_file1",
        graph_id="kg1234567890abcdef",
        table_name="Entity",
        file_size_bytes=1024,
        row_count=50,
        duration_ms=100.5,
        files_staged=1,
      )

      mock_sync.assert_called_once_with(
        "gf_file1",
        "kg1234567890abcdef",
        "Entity",
        1024,
        50,
        100.5,
        1,
      )

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_timeout_does_not_raise(self):
    """Test that a Dagster reporting timeout does not raise an exception."""
    with patch(
      f"{MODULE}._report_staging_materialization_sync",
      side_effect=lambda *args: __import__("time").sleep(10),
    ):
      with patch(f"{MODULE}.DAGSTER_REPORT_TIMEOUT", 0.01):
        # Should not raise, just log a warning
        await _report_staging_materialization(
          file_id="gf_file1",
          graph_id="kg1234567890abcdef",
          table_name="Entity",
          file_size_bytes=1024,
          row_count=50,
          duration_ms=100.5,
          files_staged=1,
        )

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_exception_does_not_raise(self):
    """Test that a Dagster reporting error does not raise an exception."""
    with patch(
      f"{MODULE}._report_staging_materialization_sync",
      side_effect=RuntimeError("Dagster instance unavailable"),
    ):
      # Should not raise, just log a warning
      await _report_staging_materialization(
        file_id="gf_file1",
        graph_id="kg1234567890abcdef",
        table_name="Entity",
        file_size_bytes=1024,
        row_count=50,
        duration_ms=100.5,
        files_staged=1,
      )


class TestReportStagingMaterializationSync:
  """Test _report_staging_materialization_sync function."""

  @pytest.mark.unit
  def test_creates_and_reports_materialization(self):
    """Test that materialization is created with correct metadata and reported."""
    with (
      patch("dagster.DagsterInstance") as mock_instance_class,
      patch("dagster.AssetMaterialization") as mock_mat_class,
      patch("dagster.AssetKey") as mock_key_class,
      patch("dagster.MetadataValue") as mock_meta_class,
    ):
      mock_instance = Mock()
      mock_instance_class.get.return_value = mock_instance

      mock_meta_class.text.side_effect = lambda x: f"text:{x}"
      mock_meta_class.int.side_effect = lambda x: f"int:{x}"
      mock_meta_class.float.side_effect = lambda x: f"float:{x}"

      _report_staging_materialization_sync(
        file_id="gf_file1",
        graph_id="kg1234567890abcdef",
        table_name="Entity",
        file_size_bytes=2048,
        row_count=100,
        duration_ms=50.5,
        files_staged=3,
      )

      # Verify DagsterInstance was obtained
      mock_instance_class.get.assert_called_once()

      # Verify AssetKey was created
      mock_key_class.assert_called_once_with("user_graph_file_staging")

      # Verify AssetMaterialization was created
      mock_mat_class.assert_called_once()
      mat_kwargs = mock_mat_class.call_args.kwargs
      assert "description" in mat_kwargs
      assert "Entity" in mat_kwargs["description"]
      assert "3" in mat_kwargs["description"]

      # Verify metadata values
      metadata = mat_kwargs["metadata"]
      assert metadata["file_id"] == "text:gf_file1"
      assert metadata["graph_id"] == "text:kg1234567890abcdef"
      assert metadata["table_name"] == "text:Entity"
      assert metadata["file_size_bytes"] == "int:2048"
      assert metadata["row_count"] == "int:100"
      assert metadata["duration_ms"] == "float:50.5"
      assert metadata["files_staged"] == "int:3"
      assert metadata["staging_method"] == "text:direct"

      # Verify materialization was reported
      mock_instance.report_runless_asset_event.assert_called_once()

  @pytest.mark.unit
  def test_row_count_none_defaults_to_zero(self):
    """Test that row_count=None is reported as 0 in metadata."""
    with (
      patch("dagster.DagsterInstance") as mock_instance_class,
      patch("dagster.AssetMaterialization"),
      patch("dagster.AssetKey"),
      patch("dagster.MetadataValue") as mock_meta_class,
    ):
      mock_instance = Mock()
      mock_instance_class.get.return_value = mock_instance

      mock_meta_class.text.side_effect = lambda x: f"text:{x}"
      mock_meta_class.int.side_effect = lambda x: f"int:{x}"
      mock_meta_class.float.side_effect = lambda x: f"float:{x}"

      _report_staging_materialization_sync(
        file_id="gf_file1",
        graph_id="kg1234567890abcdef",
        table_name="Entity",
        file_size_bytes=1024,
        row_count=None,
        duration_ms=10.0,
        files_staged=1,
      )

      # row_count should be 0 when None
      mock_meta_class.int.assert_any_call(0)


class TestDagsterReportTimeout:
  """Test the DAGSTER_REPORT_TIMEOUT constant and timeout behavior."""

  @pytest.mark.unit
  def test_timeout_constant_is_positive(self):
    """Test that the Dagster report timeout is a positive number."""
    assert DAGSTER_REPORT_TIMEOUT > 0

  @pytest.mark.unit
  def test_timeout_constant_is_reasonable(self):
    """Test that the timeout is within a reasonable range (1-30 seconds)."""
    assert 1.0 <= DAGSTER_REPORT_TIMEOUT <= 30.0


class TestStageFileDirectlyEdgeCases:
  """Test edge cases for stage_file_directly."""

  @pytest.fixture
  def mock_db_session(self):
    """Create a mock database session."""
    return Mock()

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_empty_table_with_all_files_non_uploaded(self, mock_db_session):
    """Test when get_all_for_table returns files but none are uploaded."""
    file1 = Mock()
    file1.upload_status = "failed"
    file2 = Mock()
    file2.upload_status = "processing"

    graph_file = Mock()
    graph_file.id = "gf_test"

    with (
      patch(GRAPH_FILE_PATH) as mock_file_class,
      patch(GRAPH_TABLE_PATH) as mock_table_class,
    ):
      mock_file_class.get_by_id.return_value = graph_file
      mock_table_class.get_by_id.return_value = Mock(table_name="Entity")
      mock_file_class.get_all_for_table.return_value = [file1, file2]

      result = await stage_file_directly(
        db=mock_db_session,
        file_id="gf_test",
        graph_id="kg1234567890abcdef",
        table_id="gt_table1",
        s3_key="some/key.parquet",
        file_size_bytes=1024,
      )

      assert result["status"] == "skipped"
      assert "No uploaded files" in result["message"]

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_graph_client_factory_error(self, mock_db_session):
    """Test when GraphClientFactory.create_client raises an error."""
    mock_graph_file = Mock()
    mock_graph_file.id = "gf_test"
    mock_graph_file.upload_status = "uploaded"
    mock_graph_file.s3_key = "path/file.parquet"

    with (
      patch(FACTORY_PATH) as mock_factory,
      patch(GRAPH_FILE_PATH) as mock_file_class,
      patch(GRAPH_TABLE_PATH) as mock_table_class,
      patch(f"{MODULE}.env") as mock_env,
    ):
      mock_env.USER_DATA_BUCKET = "test-bucket"
      mock_file_class.get_by_id.return_value = mock_graph_file
      mock_table_class.get_by_id.return_value = Mock(table_name="Entity")
      mock_file_class.get_all_for_table.return_value = [mock_graph_file]
      mock_factory.create_client = AsyncMock(
        side_effect=ConnectionError("Cannot reach Graph API")
      )

      result = await stage_file_directly(
        db=mock_db_session,
        file_id="gf_test",
        graph_id="kg1234567890abcdef",
        table_id="gt_table1",
        s3_key="path/file.parquet",
        file_size_bytes=1024,
      )

      assert result["status"] == "error"
      assert "Cannot reach Graph API" in result["message"]

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_large_file_size_bytes(self, mock_db_session):
    """Test staging with a large file size value."""
    mock_graph_file = Mock()
    mock_graph_file.id = "gf_large"
    mock_graph_file.s3_key = "path/large.parquet"
    mock_graph_file.upload_status = "uploaded"
    mock_graph_file.mark_duckdb_staged = Mock()

    mock_client = AsyncMock()
    mock_client.list_tables = AsyncMock(return_value=[])
    mock_client.create_table = AsyncMock(return_value={"status": "ok"})
    mock_client.close = AsyncMock()

    with (
      patch(FACTORY_PATH) as mock_factory,
      patch(GRAPH_FILE_PATH) as mock_file_class,
      patch(GRAPH_TABLE_PATH) as mock_table_class,
      patch(f"{MODULE}._report_staging_materialization", new_callable=AsyncMock),
      patch(f"{MODULE}.env") as mock_env,
    ):
      mock_env.USER_DATA_BUCKET = "test-bucket"
      mock_file_class.get_by_id.return_value = mock_graph_file
      mock_table_class.get_by_id.return_value = Mock(table_name="BigTable")
      mock_file_class.get_all_for_table.return_value = [mock_graph_file]
      mock_factory.create_client = AsyncMock(return_value=mock_client)

      large_size = 500 * 1024 * 1024  # 500 MB
      result = await stage_file_directly(
        db=mock_db_session,
        file_id="gf_large",
        graph_id="kg1234567890abcdef",
        table_id="gt_table1",
        s3_key="path/large.parquet",
        file_size_bytes=large_size,
        row_count=1_000_000,
      )

      assert result["status"] == "success"

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_graph_client_create_called_with_write_operation(self, mock_db_session):
    """Test that GraphClientFactory.create_client is called with operation_type='write'."""
    mock_graph_file = Mock()
    mock_graph_file.id = "gf_file1"
    mock_graph_file.s3_key = "path/file.parquet"
    mock_graph_file.upload_status = "uploaded"
    mock_graph_file.mark_duckdb_staged = Mock()

    mock_client = AsyncMock()
    mock_client.list_tables = AsyncMock(return_value=[])
    mock_client.create_table = AsyncMock(return_value={"status": "ok"})
    mock_client.close = AsyncMock()

    with (
      patch(FACTORY_PATH) as mock_factory,
      patch(GRAPH_FILE_PATH) as mock_file_class,
      patch(GRAPH_TABLE_PATH) as mock_table_class,
      patch(f"{MODULE}._report_staging_materialization", new_callable=AsyncMock),
      patch(f"{MODULE}.env") as mock_env,
    ):
      mock_env.USER_DATA_BUCKET = "test-bucket"
      mock_file_class.get_by_id.return_value = mock_graph_file
      mock_table_class.get_by_id.return_value = Mock(table_name="Entity")
      mock_file_class.get_all_for_table.return_value = [mock_graph_file]
      mock_factory.create_client = AsyncMock(return_value=mock_client)

      await stage_file_directly(
        db=mock_db_session,
        file_id="gf_file1",
        graph_id="kg1234567890abcdef",
        table_id="gt_table1",
        s3_key="path/file.parquet",
        file_size_bytes=1024,
      )

      mock_factory.create_client.assert_called_once_with(
        graph_id="kg1234567890abcdef", operation_type="write"
      )
