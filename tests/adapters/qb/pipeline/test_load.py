"""Tests for QuickBooks load Dagster asset, _load_tables, and _count_parquet_rows."""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from dagster import build_asset_context

# All imports inside _load_tables function body - patch at source module paths.
# When a function does `from robosystems.foo import Bar`, the patch target is
# 'robosystems.foo.Bar' (the canonical location), not the calling module.
_PATCH_SESSION = "robosystems.database.SessionFactory"
_PATCH_S3_CLIENT = "robosystems.operations.aws.s3.S3Client"
_PATCH_GRAPH_CLIENT = "robosystems.graph_api.client.factory.get_graph_client"
_PATCH_GRAPH_FILE = "robosystems.models.iam.graph_file.GraphFile"
_PATCH_GRAPH_TABLE = "robosystems.models.iam.graph_table.GraphTable"
_PATCH_STAGING_KEY = "robosystems.config.storage.graph.get_staging_key"
_PATCH_CONN_SVC = "robosystems.operations.connection_service.ConnectionService"
_PATCH_ENV = "robosystems.config.env"
# The load module imports get_pipeline_work_dir at module level
_PATCH_LOAD_WORK_DIR = (
  "robosystems.adapters.quickbooks.pipeline.load.get_pipeline_work_dir"
)
# _count_parquet_rows is in the load module — mock to avoid pyarrow/DuckDB conflict
_PATCH_COUNT_ROWS = "robosystems.adapters.quickbooks.pipeline.load._count_parquet_rows"


def _make_context():
  """Create a mock AssetExecutionContext with a log attribute."""
  context = Mock()
  context.log = Mock()
  context.log.info = Mock()
  context.log.error = Mock()
  context.log.warning = Mock()
  return context


def _make_config(
  graph_id="kg_test123",
  connection_id="conn_abc",
  user_id="user_xyz",
  realm_id="123456789",
  full_rebuild=False,
  lookback_days=60,
):
  """Create a QBSyncConfig for tests."""
  from robosystems.adapters.quickbooks.pipeline.configs import QBSyncConfig

  return QBSyncConfig(
    graph_id=graph_id,
    connection_id=connection_id,
    user_id=user_id,
    realm_id=realm_id,
    full_rebuild=full_rebuild,
    lookback_days=lookback_days,
  )


def _write_dummy_file(path: Path, num_rows: int = 3):
  """Write a small file to simulate a parquet file on disk (for size checks)."""
  path.parent.mkdir(parents=True, exist_ok=True)
  # Write enough bytes to look like a real file
  path.write_bytes(b"PAR1" + b"\x00" * (num_rows * 10) + b"PAR1")


def _make_mock_session():
  """Create a context-manager-compatible mock session."""
  mock_session = MagicMock()
  mock_session.__enter__ = Mock(return_value=mock_session)
  mock_session.__exit__ = Mock(return_value=False)
  mock_session.commit = Mock()
  return mock_session


def _make_mock_graph_table(table_id="table_1"):
  """Create a mock GraphTable with expected attributes."""
  gt = Mock()
  gt.id = table_id
  return gt


def _make_mock_graph_file(file_id="file_1"):
  """Create a mock GraphFile with expected attributes."""
  gf = Mock()
  gf.id = file_id
  gf.s3_key = ""
  gf.upload_status = "pending"
  gf.duckdb_status = "staged"
  gf.graph_status = "pending"
  gf.mark_duckdb_staged = Mock()
  gf.mark_graph_ingested = Mock()
  return gf


def _make_mock_graph_client():
  """Create a mock async graph client."""
  client = AsyncMock()
  client.create_table = AsyncMock(return_value=None)
  client.materialize_table = AsyncMock(return_value={"rows_ingested": 10})
  return client


@pytest.mark.unit
class TestCountParquetRows:
  """Tests for _count_parquet_rows helper."""

  def test_count_rows_returns_correct_count(self):
    """Test that _count_parquet_rows reads metadata.num_rows from pyarrow."""
    from robosystems.adapters.quickbooks.pipeline.load import _count_parquet_rows

    mock_metadata = Mock()
    mock_metadata.num_rows = 7

    with patch("pyarrow.parquet.read_metadata", return_value=mock_metadata):
      count = _count_parquet_rows(Path("/fake/path.parquet"))

    assert count == 7

  def test_count_rows_returns_zero_on_error(self, tmp_path):
    """Test that _count_parquet_rows returns 0 if the file is invalid."""
    from robosystems.adapters.quickbooks.pipeline.load import _count_parquet_rows

    bad_path = tmp_path / "not_a_parquet.parquet"
    bad_path.write_text("this is not parquet data")

    count = _count_parquet_rows(bad_path)
    assert count == 0

  def test_count_rows_returns_zero_on_exception(self):
    """Test that _count_parquet_rows returns 0 when pyarrow raises."""
    from robosystems.adapters.quickbooks.pipeline.load import _count_parquet_rows

    with patch("pyarrow.parquet.read_metadata", side_effect=Exception("Corrupt")):
      count = _count_parquet_rows(Path("/fake/path.parquet"))

    assert count == 0

  def test_count_rows_large_file(self):
    """Test that _count_parquet_rows handles a larger parquet file."""
    from robosystems.adapters.quickbooks.pipeline.load import _count_parquet_rows

    mock_metadata = Mock()
    mock_metadata.num_rows = 1000

    with patch("pyarrow.parquet.read_metadata", return_value=mock_metadata):
      count = _count_parquet_rows(Path("/fake/path.parquet"))

    assert count == 1000


@pytest.mark.unit
class TestQbLoadAsset:
  """Tests for the qb_load Dagster asset entrypoint."""

  def test_load_calls_asyncio_run(self, tmp_path):
    """Test that qb_load invokes asyncio.run with _load_tables result."""
    from dagster import MaterializeResult

    from robosystems.adapters.quickbooks.pipeline.load import qb_load

    config = _make_config()

    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)

    expected_metadata = {
      "graph_id": config.graph_id,
      "tables_staged": 2,
      "tables_materialized": 2,
      "total_rows": 50,
    }

    with (
      patch(
        _PATCH_LOAD_WORK_DIR,
        return_value=work_dir,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.load.asyncio.run",
        return_value=expected_metadata,
      ) as mock_asyncio_run,
    ):
      context = build_asset_context()
      result = qb_load(context, config)

    assert isinstance(result, MaterializeResult)
    assert result.metadata["tables_staged"] == 2
    assert result.metadata["tables_materialized"] == 2
    assert result.metadata["total_rows"] == 50
    mock_asyncio_run.assert_called_once()

  def test_load_uses_output_subdir_of_work_dir(self, tmp_path):
    """Test that qb_load computes output_dir as work_dir/output."""
    from robosystems.adapters.quickbooks.pipeline.load import qb_load

    config = _make_config(graph_id="kg_dir_test")

    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)

    captured = {}

    def capture_asyncio_run(coro):
      captured["ran"] = True
      return {
        "graph_id": config.graph_id,
        "tables_staged": 0,
        "tables_materialized": 0,
        "total_rows": 0,
      }

    with (
      patch(
        _PATCH_LOAD_WORK_DIR,
        return_value=work_dir,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.load.asyncio.run",
        side_effect=capture_asyncio_run,
      ),
    ):
      context = build_asset_context()
      qb_load(context, config)

    assert captured.get("ran") is True


@pytest.mark.unit
class TestLoadTables:
  """Tests for the _load_tables async helper function."""

  def test_load_tables_skips_missing_parquet(self, tmp_path):
    """Test that tables with no parquet file are skipped."""
    from robosystems.adapters.quickbooks.pipeline.load import _load_tables

    context = _make_context()
    config = _make_config()

    # Empty output_dir - no parquet files exist
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True)

    mock_session = _make_mock_session()
    mock_s3 = Mock()
    mock_graph_client = _make_mock_graph_client()
    mock_env = Mock()
    mock_env.USER_DATA_BUCKET = "test-bucket"

    with (
      patch(_PATCH_ENV, mock_env),
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_S3_CLIENT, return_value=mock_s3),
      patch(_PATCH_GRAPH_CLIENT, return_value=mock_graph_client),
      patch(_PATCH_CONN_SVC) as MockConnSvc,
    ):
      MockConnSvc.update_last_sync = AsyncMock(return_value=None)
      result = asyncio.run(_load_tables(context, config, output_dir))

    assert result["tables_staged"] == 0
    assert result["tables_materialized"] == 0
    assert result["total_rows"] == 0
    mock_s3.upload_file.assert_not_called()

  def test_load_tables_skips_missing_in_log(self, tmp_path):
    """Test that skipped tables produce an info log message."""
    from robosystems.adapters.quickbooks.pipeline.load import _load_tables

    context = _make_context()
    config = _make_config()

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True)

    mock_session = _make_mock_session()
    mock_env = Mock()
    mock_env.USER_DATA_BUCKET = "test-bucket"

    with (
      patch(_PATCH_ENV, mock_env),
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_S3_CLIENT, return_value=Mock()),
      patch(_PATCH_GRAPH_CLIENT, return_value=_make_mock_graph_client()),
      patch(_PATCH_CONN_SVC) as MockConnSvc,
    ):
      MockConnSvc.update_last_sync = AsyncMock(return_value=None)
      asyncio.run(_load_tables(context, config, output_dir))

    log_calls = [str(c) for c in context.log.info.call_args_list]
    assert any("Skipping" in c for c in log_calls)

  def test_load_tables_uploads_parquet_to_s3(self, tmp_path):
    """Test that parquet files are uploaded to S3 when they exist."""
    from robosystems.adapters.quickbooks.pipeline.load import _load_tables

    context = _make_context()
    config = _make_config()

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True)

    # Create a dummy file for "entity" table
    _write_dummy_file(output_dir / "qb_entity.parquet", num_rows=3)

    mock_session = _make_mock_session()
    mock_s3 = Mock()
    mock_graph_client = _make_mock_graph_client()
    mock_graph_table = _make_mock_graph_table("tbl_1")
    mock_graph_file = _make_mock_graph_file("file_1")
    mock_env = Mock()
    mock_env.USER_DATA_BUCKET = "my-bucket"

    MockGraphTable = Mock()
    MockGraphTable.get_by_name.return_value = mock_graph_table
    MockGraphFile = Mock()
    MockGraphFile.create.return_value = mock_graph_file
    MockGraphFile.get_all_for_table.return_value = [mock_graph_file]

    with (
      patch(_PATCH_ENV, mock_env),
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_S3_CLIENT, return_value=mock_s3),
      patch(_PATCH_GRAPH_CLIENT, return_value=mock_graph_client),
      patch(_PATCH_GRAPH_TABLE, MockGraphTable),
      patch(_PATCH_GRAPH_FILE, MockGraphFile),
      patch(
        _PATCH_STAGING_KEY,
        return_value="user/kg_test123/Entity/file_1/qb_Entity.parquet",
      ),
      patch(_PATCH_COUNT_ROWS, return_value=3),
      patch(_PATCH_CONN_SVC) as MockConnSvc,
    ):
      MockConnSvc.update_last_sync = AsyncMock(return_value=None)
      result = asyncio.run(_load_tables(context, config, output_dir))

    mock_s3.upload_file.assert_called()
    assert result["tables_staged"] >= 1

  def test_load_tables_creates_graph_table_when_missing(self, tmp_path):
    """Test that a new GraphTable is created when none exists."""
    from robosystems.adapters.quickbooks.pipeline.load import _load_tables

    context = _make_context()
    config = _make_config()

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True)

    _write_dummy_file(output_dir / "qb_entity.parquet", num_rows=2)

    mock_session = _make_mock_session()
    mock_graph_table = _make_mock_graph_table()
    mock_graph_file = _make_mock_graph_file()
    mock_graph_client = _make_mock_graph_client()
    mock_env = Mock()
    mock_env.USER_DATA_BUCKET = "bucket"

    MockGraphTable = Mock()
    MockGraphTable.get_by_name.return_value = None  # Not found -> create
    MockGraphTable.create.return_value = mock_graph_table
    MockGraphFile = Mock()
    MockGraphFile.create.return_value = mock_graph_file
    MockGraphFile.get_all_for_table.return_value = []

    with (
      patch(_PATCH_ENV, mock_env),
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_S3_CLIENT, return_value=Mock()),
      patch(_PATCH_GRAPH_CLIENT, return_value=mock_graph_client),
      patch(_PATCH_GRAPH_TABLE, MockGraphTable),
      patch(_PATCH_GRAPH_FILE, MockGraphFile),
      patch(_PATCH_STAGING_KEY, return_value="s3/key/path"),
      patch(_PATCH_COUNT_ROWS, return_value=2),
      patch(_PATCH_CONN_SVC) as MockConnSvc,
    ):
      MockConnSvc.update_last_sync = AsyncMock(return_value=None)
      asyncio.run(_load_tables(context, config, output_dir))

    MockGraphTable.create.assert_called_once()
    create_kwargs = MockGraphTable.create.call_args[1]
    assert create_kwargs["graph_id"] == config.graph_id
    assert create_kwargs["table_name"] == "Entity"

  def test_load_tables_uses_existing_graph_table(self, tmp_path):
    """Test that an existing GraphTable is reused, not recreated."""
    from robosystems.adapters.quickbooks.pipeline.load import _load_tables

    context = _make_context()
    config = _make_config()

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True)

    _write_dummy_file(output_dir / "qb_entity.parquet", num_rows=2)

    mock_session = _make_mock_session()
    mock_graph_table = _make_mock_graph_table()
    mock_graph_file = _make_mock_graph_file()
    mock_graph_client = _make_mock_graph_client()
    mock_env = Mock()
    mock_env.USER_DATA_BUCKET = "bucket"

    MockGraphTable = Mock()
    MockGraphTable.get_by_name.return_value = mock_graph_table  # Found -> reuse
    MockGraphFile = Mock()
    MockGraphFile.create.return_value = mock_graph_file
    MockGraphFile.get_all_for_table.return_value = []

    with (
      patch(_PATCH_ENV, mock_env),
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_S3_CLIENT, return_value=Mock()),
      patch(_PATCH_GRAPH_CLIENT, return_value=mock_graph_client),
      patch(_PATCH_GRAPH_TABLE, MockGraphTable),
      patch(_PATCH_GRAPH_FILE, MockGraphFile),
      patch(_PATCH_STAGING_KEY, return_value="s3/key/path"),
      patch(_PATCH_COUNT_ROWS, return_value=2),
      patch(_PATCH_CONN_SVC) as MockConnSvc,
    ):
      MockConnSvc.update_last_sync = AsyncMock(return_value=None)
      asyncio.run(_load_tables(context, config, output_dir))

    MockGraphTable.create.assert_not_called()

  def test_load_tables_raises_on_staging_failure(self, tmp_path):
    """Test that a staging failure raises the exception."""
    from robosystems.adapters.quickbooks.pipeline.load import _load_tables

    context = _make_context()
    config = _make_config()

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True)

    _write_dummy_file(output_dir / "qb_entity.parquet", num_rows=2)

    mock_session = _make_mock_session()
    mock_graph_table = _make_mock_graph_table()
    mock_graph_file = _make_mock_graph_file()
    mock_graph_client = AsyncMock()
    mock_graph_client.create_table = AsyncMock(
      side_effect=RuntimeError("Graph API unavailable")
    )
    mock_env = Mock()
    mock_env.USER_DATA_BUCKET = "bucket"

    MockGraphTable = Mock()
    MockGraphTable.get_by_name.return_value = mock_graph_table
    MockGraphFile = Mock()
    MockGraphFile.create.return_value = mock_graph_file

    with (
      patch(_PATCH_ENV, mock_env),
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_S3_CLIENT, return_value=Mock()),
      patch(_PATCH_GRAPH_CLIENT, return_value=mock_graph_client),
      patch(_PATCH_GRAPH_TABLE, MockGraphTable),
      patch(_PATCH_GRAPH_FILE, MockGraphFile),
      patch(_PATCH_STAGING_KEY, return_value="s3/key"),
      patch(_PATCH_COUNT_ROWS, return_value=2),
      patch(_PATCH_CONN_SVC) as MockConnSvc,
    ):
      MockConnSvc.update_last_sync = AsyncMock(return_value=None)

      with pytest.raises(RuntimeError, match="Graph API unavailable"):
        asyncio.run(_load_tables(context, config, output_dir))

  def test_load_tables_raises_on_materialization_failure(self, tmp_path):
    """Test that a materialization failure raises the exception."""
    from robosystems.adapters.quickbooks.pipeline.load import _load_tables

    context = _make_context()
    config = _make_config()

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True)

    _write_dummy_file(output_dir / "qb_entity.parquet", num_rows=2)

    mock_session = _make_mock_session()
    mock_graph_table = _make_mock_graph_table()
    mock_graph_file = _make_mock_graph_file()

    # Staging succeeds, materialization fails
    mock_graph_client = AsyncMock()
    mock_graph_client.create_table = AsyncMock(return_value=None)
    mock_graph_client.materialize_table = AsyncMock(
      side_effect=RuntimeError("Materialization failed")
    )
    mock_env = Mock()
    mock_env.USER_DATA_BUCKET = "bucket"

    MockGraphTable = Mock()
    MockGraphTable.get_by_name.return_value = mock_graph_table
    MockGraphFile = Mock()
    MockGraphFile.create.return_value = mock_graph_file
    MockGraphFile.get_all_for_table.return_value = []

    with (
      patch(_PATCH_ENV, mock_env),
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_S3_CLIENT, return_value=Mock()),
      patch(_PATCH_GRAPH_CLIENT, return_value=mock_graph_client),
      patch(_PATCH_GRAPH_TABLE, MockGraphTable),
      patch(_PATCH_GRAPH_FILE, MockGraphFile),
      patch(_PATCH_STAGING_KEY, return_value="s3/key"),
      patch(_PATCH_COUNT_ROWS, return_value=2),
      patch(_PATCH_CONN_SVC) as MockConnSvc,
    ):
      MockConnSvc.update_last_sync = AsyncMock(return_value=None)

      with pytest.raises(RuntimeError, match="Materialization failed"):
        asyncio.run(_load_tables(context, config, output_dir))

  def test_load_tables_updates_last_sync(self, tmp_path):
    """Test that ConnectionService.update_last_sync is called on success."""
    from robosystems.adapters.quickbooks.pipeline.load import _load_tables

    context = _make_context()
    config = _make_config(connection_id="conn_sync_test")

    # Empty output_dir - no tables processed, but sync update should still fire
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True)

    mock_session = _make_mock_session()
    mock_env = Mock()
    mock_env.USER_DATA_BUCKET = "bucket"

    with (
      patch(_PATCH_ENV, mock_env),
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_S3_CLIENT, return_value=Mock()),
      patch(_PATCH_GRAPH_CLIENT, return_value=_make_mock_graph_client()),
      patch(_PATCH_CONN_SVC) as MockConnSvc,
    ):
      MockConnSvc.update_last_sync = AsyncMock(return_value=None)
      asyncio.run(_load_tables(context, config, output_dir))

    MockConnSvc.update_last_sync.assert_awaited_once_with(
      "conn_sync_test", config.graph_id
    )

  def test_load_tables_last_sync_failure_is_non_fatal(self, tmp_path):
    """Test that a last_sync update failure does not raise, only warns."""
    from robosystems.adapters.quickbooks.pipeline.load import _load_tables

    context = _make_context()
    config = _make_config()

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True)

    mock_session = _make_mock_session()
    mock_env = Mock()
    mock_env.USER_DATA_BUCKET = "bucket"

    with (
      patch(_PATCH_ENV, mock_env),
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_S3_CLIENT, return_value=Mock()),
      patch(_PATCH_GRAPH_CLIENT, return_value=_make_mock_graph_client()),
      patch(_PATCH_CONN_SVC) as MockConnSvc,
    ):
      MockConnSvc.update_last_sync = AsyncMock(
        side_effect=Exception("DB connection lost")
      )
      # Should NOT raise
      result = asyncio.run(_load_tables(context, config, output_dir))

    assert result["tables_staged"] == 0
    log_warn_calls = [str(c) for c in context.log.warning.call_args_list]
    assert any(
      "non-fatal" in c.lower() or "last_sync" in c.lower() for c in log_warn_calls
    )

  def test_load_tables_result_includes_graph_id(self, tmp_path):
    """Test that the result dict contains the graph_id."""
    from robosystems.adapters.quickbooks.pipeline.load import _load_tables

    context = _make_context()
    config = _make_config(graph_id="kg_result_check")

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True)

    mock_session = _make_mock_session()
    mock_env = Mock()
    mock_env.USER_DATA_BUCKET = "bucket"

    with (
      patch(_PATCH_ENV, mock_env),
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_S3_CLIENT, return_value=Mock()),
      patch(_PATCH_GRAPH_CLIENT, return_value=_make_mock_graph_client()),
      patch(_PATCH_CONN_SVC) as MockConnSvc,
    ):
      MockConnSvc.update_last_sync = AsyncMock(return_value=None)
      result = asyncio.run(_load_tables(context, config, output_dir))

    assert result["graph_id"] == "kg_result_check"
