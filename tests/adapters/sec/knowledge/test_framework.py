"""Tests for DuckDBAnalyticsContext framework.

Tests the context manager lifecycle, connection management, query execution,
parquet writing, output uploads, and dev vs prod behavior.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from robosystems.adapters.sec.knowledge.framework import DuckDBAnalyticsContext

pytestmark = pytest.mark.unit


class TestDuckDBAnalyticsContextInit:
  """Tests for DuckDBAnalyticsContext initialization."""

  def test_default_parameters(self):
    """Default parameters are set correctly."""
    ctx = DuckDBAnalyticsContext()
    assert ctx._duckdb_source == "sec"
    assert ctx._memory_limit == "8GB"
    assert ctx._threads is None
    assert ctx._local_dir is None
    assert ctx._conn is None
    assert ctx._tmpdir is None
    assert ctx._output_dir is None
    assert ctx._db_path is None

  def test_custom_parameters(self):
    """Custom parameters are stored correctly."""
    ctx = DuckDBAnalyticsContext(
      duckdb_source="custom",
      memory_limit="4GB",
      threads=2,
      local_dir="/tmp/test",
    )
    assert ctx._duckdb_source == "custom"
    assert ctx._memory_limit == "4GB"
    assert ctx._threads == 2
    assert ctx._local_dir == Path("/tmp/test")

  def test_local_dir_converted_to_path(self):
    """String local_dir is converted to Path."""
    ctx = DuckDBAnalyticsContext(local_dir="/some/path")
    assert isinstance(ctx._local_dir, Path)

  def test_none_local_dir_stays_none(self):
    """None local_dir stays None."""
    ctx = DuckDBAnalyticsContext(local_dir=None)
    assert ctx._local_dir is None


class TestDuckDBAnalyticsContextProperties:
  """Tests for conn and db_path properties when not in context."""

  def test_conn_raises_when_not_open(self):
    """Accessing conn raises RuntimeError when not in context."""
    ctx = DuckDBAnalyticsContext()
    with pytest.raises(RuntimeError, match="not open"):
      _ = ctx.conn

  def test_db_path_raises_when_not_open(self):
    """Accessing db_path raises RuntimeError when not in context."""
    ctx = DuckDBAnalyticsContext()
    with pytest.raises(RuntimeError, match="not open"):
      _ = ctx.db_path


class TestDuckDBAnalyticsContextManagerDev:
  """Tests for context manager lifecycle in development mode."""

  def test_enter_opens_connection(self, tmp_path):
    """__enter__ opens a DuckDB connection in dev mode."""
    db_path = tmp_path / "sec.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE test (id INT)")
    conn.close()

    with (
      patch("robosystems.adapters.sec.knowledge.framework.env") as mock_env,
      patch(
        "robosystems.adapters.sec.knowledge.framework.get_staging_duckdb_path",
        return_value=str(db_path),
      ),
    ):
      mock_env.is_development.return_value = True

      ctx = DuckDBAnalyticsContext(duckdb_source="sec")
      result = ctx.__enter__()

      assert result is ctx
      assert ctx._conn is not None
      assert ctx._db_path == db_path
      ctx.__exit__(None, None, None)

  def test_exit_closes_connection(self, tmp_path):
    """__exit__ closes the DuckDB connection."""
    db_path = tmp_path / "sec.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.close()

    with (
      patch("robosystems.adapters.sec.knowledge.framework.env") as mock_env,
      patch(
        "robosystems.adapters.sec.knowledge.framework.get_staging_duckdb_path",
        return_value=str(db_path),
      ),
    ):
      mock_env.is_development.return_value = True

      ctx = DuckDBAnalyticsContext(duckdb_source="sec")
      ctx.__enter__()
      assert ctx._conn is not None
      ctx.__exit__(None, None, None)
      assert ctx._conn is None

  def test_dev_file_not_found_raises(self, tmp_path):
    """Raises FileNotFoundError when dev staging file is missing."""
    missing_path = tmp_path / "missing.duckdb"

    with (
      patch("robosystems.adapters.sec.knowledge.framework.env") as mock_env,
      patch(
        "robosystems.adapters.sec.knowledge.framework.get_staging_duckdb_path",
        return_value=str(missing_path),
      ),
    ):
      mock_env.is_development.return_value = True

      ctx = DuckDBAnalyticsContext(duckdb_source="sec")
      with pytest.raises(FileNotFoundError, match="not found"):
        ctx.__enter__()

  def test_output_dir_created(self, tmp_path):
    """Output directory is created next to the database file."""
    db_path = tmp_path / "sec.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.close()

    with (
      patch("robosystems.adapters.sec.knowledge.framework.env") as mock_env,
      patch(
        "robosystems.adapters.sec.knowledge.framework.get_staging_duckdb_path",
        return_value=str(db_path),
      ),
    ):
      mock_env.is_development.return_value = True

      with DuckDBAnalyticsContext(duckdb_source="sec") as ctx:
        assert ctx._output_dir is not None
        assert ctx._output_dir.exists()
        assert ctx._output_dir.name == "output"

  def test_custom_local_dir_output(self, tmp_path):
    """Custom local_dir places output in local_dir/output."""
    db_path = tmp_path / "sec.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.close()

    local_dir = tmp_path / "custom"
    local_dir.mkdir()

    with (
      patch("robosystems.adapters.sec.knowledge.framework.env") as mock_env,
      patch(
        "robosystems.adapters.sec.knowledge.framework.get_staging_duckdb_path",
        return_value=str(db_path),
      ),
    ):
      mock_env.is_development.return_value = True

      with DuckDBAnalyticsContext(duckdb_source="sec", local_dir=str(local_dir)) as ctx:
        assert ctx._output_dir == local_dir / "output"

  def test_threads_setting(self, tmp_path):
    """Threads setting is applied when specified."""
    db_path = tmp_path / "sec.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.close()

    with (
      patch("robosystems.adapters.sec.knowledge.framework.env") as mock_env,
      patch(
        "robosystems.adapters.sec.knowledge.framework.get_staging_duckdb_path",
        return_value=str(db_path),
      ),
    ):
      mock_env.is_development.return_value = True

      with DuckDBAnalyticsContext(duckdb_source="sec", threads=2) as ctx:
        # Verify connection is valid and threads were set
        result = ctx.conn.execute("SELECT current_setting('threads')").fetchone()
        assert result is not None
        assert int(result[0]) == 2


class TestDuckDBAnalyticsContextQuery:
  """Tests for the query method."""

  def test_query_executes_sql(self, tmp_path):
    """query() executes SQL and returns a DuckDB relation."""
    db_path = tmp_path / "sec.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE numbers (val INT)")
    conn.execute("INSERT INTO numbers VALUES (1), (2), (3)")
    conn.close()

    with (
      patch("robosystems.adapters.sec.knowledge.framework.env") as mock_env,
      patch(
        "robosystems.adapters.sec.knowledge.framework.get_staging_duckdb_path",
        return_value=str(db_path),
      ),
    ):
      mock_env.is_development.return_value = True

      with DuckDBAnalyticsContext(duckdb_source="sec") as ctx:
        result = ctx.query("SELECT SUM(val) AS total FROM numbers")
        rows = result.fetchall()
        assert rows == [(6,)]

  def test_query_raises_when_not_open(self):
    """query() raises RuntimeError when context is not open."""
    ctx = DuckDBAnalyticsContext()
    with pytest.raises(RuntimeError, match="not open"):
      ctx.query("SELECT 1")


class TestDuckDBAnalyticsContextWriteParquet:
  """Tests for the write_parquet method."""

  def test_write_parquet_creates_file(self, tmp_path):
    """write_parquet writes a parquet file to the output directory."""
    db_path = tmp_path / "sec.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE data (id INT, name VARCHAR)")
    conn.execute("INSERT INTO data VALUES (1, 'test')")
    conn.close()

    with (
      patch("robosystems.adapters.sec.knowledge.framework.env") as mock_env,
      patch(
        "robosystems.adapters.sec.knowledge.framework.get_staging_duckdb_path",
        return_value=str(db_path),
      ),
    ):
      mock_env.is_development.return_value = True

      with DuckDBAnalyticsContext(duckdb_source="sec") as ctx:
        relation = ctx.query("SELECT * FROM data")
        path = ctx.write_parquet(relation, "test_output")

        assert path.exists()
        assert path.suffix == ".parquet"
        assert path.stem == "test_output"

  def test_write_parquet_nodes_subdirectory(self, tmp_path):
    """Default table_type 'nodes' creates nodes subdirectory."""
    db_path = tmp_path / "sec.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE data (id INT)")
    conn.execute("INSERT INTO data VALUES (1)")
    conn.close()

    with (
      patch("robosystems.adapters.sec.knowledge.framework.env") as mock_env,
      patch(
        "robosystems.adapters.sec.knowledge.framework.get_staging_duckdb_path",
        return_value=str(db_path),
      ),
    ):
      mock_env.is_development.return_value = True

      with DuckDBAnalyticsContext(duckdb_source="sec") as ctx:
        relation = ctx.query("SELECT * FROM data")
        path = ctx.write_parquet(relation, "test_nodes")
        assert "nodes" in str(path)

  def test_write_parquet_relationships_subdirectory(self, tmp_path):
    """table_type 'relationships' creates relationships subdirectory."""
    db_path = tmp_path / "sec.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE data (src INT, dst INT)")
    conn.execute("INSERT INTO data VALUES (1, 2)")
    conn.close()

    with (
      patch("robosystems.adapters.sec.knowledge.framework.env") as mock_env,
      patch(
        "robosystems.adapters.sec.knowledge.framework.get_staging_duckdb_path",
        return_value=str(db_path),
      ),
    ):
      mock_env.is_development.return_value = True

      with DuckDBAnalyticsContext(duckdb_source="sec") as ctx:
        relation = ctx.query("SELECT * FROM data")
        path = ctx.write_parquet(relation, "test_rels", table_type="relationships")
        assert "relationships" in str(path)

  def test_write_parquet_raises_when_not_open(self):
    """write_parquet raises RuntimeError when context is not open."""
    ctx = DuckDBAnalyticsContext()
    with pytest.raises(RuntimeError, match="Output directory not set"):
      ctx.write_parquet(MagicMock(), "test")


class TestDuckDBAnalyticsContextUploadOutputs:
  """Tests for upload_outputs method."""

  def test_dev_returns_local_paths(self, tmp_path):
    """In dev mode, returns local paths without uploading to S3."""
    db_path = tmp_path / "sec.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE data (id INT)")
    conn.execute("INSERT INTO data VALUES (1)")
    conn.close()

    with (
      patch("robosystems.adapters.sec.knowledge.framework.env") as mock_env,
      patch(
        "robosystems.adapters.sec.knowledge.framework.get_staging_duckdb_path",
        return_value=str(db_path),
      ),
    ):
      mock_env.is_development.return_value = True

      with DuckDBAnalyticsContext(duckdb_source="sec") as ctx:
        relation = ctx.query("SELECT * FROM data")
        ctx.write_parquet(relation, "test_table")

        results = ctx.upload_outputs("test_analysis")

        assert isinstance(results, dict)
        assert "test_table" in results
        # In dev mode, values are local file paths
        assert Path(results["test_table"]).exists()

  def test_no_parquet_files_returns_empty(self, tmp_path):
    """Returns empty dict when no parquet files exist."""
    db_path = tmp_path / "sec.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.close()

    with (
      patch("robosystems.adapters.sec.knowledge.framework.env") as mock_env,
      patch(
        "robosystems.adapters.sec.knowledge.framework.get_staging_duckdb_path",
        return_value=str(db_path),
      ),
    ):
      mock_env.is_development.return_value = True

      with DuckDBAnalyticsContext(duckdb_source="sec") as ctx:
        results = ctx.upload_outputs("test_analysis")
        assert results == {}

  def test_upload_raises_when_not_open(self):
    """upload_outputs raises RuntimeError when context is not open."""
    ctx = DuckDBAnalyticsContext()
    with pytest.raises(RuntimeError, match="Output directory not set"):
      ctx.upload_outputs("test")


class TestDuckDBAnalyticsContextProd:
  """Tests for production (non-dev) behavior."""

  def test_prod_downloads_from_s3(self, tmp_path):
    """In prod mode, downloads from S3 and creates temp directory."""
    with (
      patch("robosystems.adapters.sec.knowledge.framework.env") as mock_env,
      patch.object(DuckDBAnalyticsContext, "_download_from_s3") as mock_download,
    ):
      mock_env.is_development.return_value = False

      # Create a fake DB at the download destination
      def fake_download(dest_path):
        conn = duckdb.connect(str(dest_path))
        conn.close()

      mock_download.side_effect = fake_download

      ctx = DuckDBAnalyticsContext(duckdb_source="sec")
      ctx.__enter__()

      assert ctx._tmpdir is not None
      assert ctx._conn is not None
      mock_download.assert_called_once()

      ctx.__exit__(None, None, None)
      assert ctx._conn is None
      assert ctx._tmpdir is None

  def test_prod_cleanup_on_exit(self, tmp_path):
    """Prod mode cleans up temp directory on exit."""
    with (
      patch("robosystems.adapters.sec.knowledge.framework.env") as mock_env,
      patch.object(DuckDBAnalyticsContext, "_download_from_s3") as mock_download,
    ):
      mock_env.is_development.return_value = False

      tmpdir_name = None

      def fake_download(dest_path):
        nonlocal tmpdir_name
        tmpdir_name = dest_path.parent
        conn = duckdb.connect(str(dest_path))
        conn.close()

      mock_download.side_effect = fake_download

      with DuckDBAnalyticsContext(duckdb_source="sec") as ctx:
        pass

      # After exit, temp dir should be cleaned up
      assert ctx._tmpdir is None

  def test_prod_upload_to_s3(self, tmp_path):
    """In prod mode, upload_outputs uploads to S3."""
    import sys

    mock_s3_client = MagicMock()

    # Build a mock boto3 package hierarchy that supports
    # "import boto3" and "from boto3.s3.transfer import TransferConfig"
    mock_transfer = MagicMock()
    mock_transfer.TransferConfig = MagicMock()

    mock_s3_mod = MagicMock()
    mock_s3_mod.transfer = mock_transfer

    mock_boto3 = MagicMock()
    mock_boto3.client.return_value = mock_s3_client
    mock_boto3.s3 = mock_s3_mod

    saved = {}
    modules_to_mock = {
      "boto3": mock_boto3,
      "boto3.s3": mock_s3_mod,
      "boto3.s3.transfer": mock_transfer,
    }
    for mod_name, mock_obj in modules_to_mock.items():
      saved[mod_name] = sys.modules.get(mod_name)
      sys.modules[mod_name] = mock_obj

    try:
      with (
        patch("robosystems.adapters.sec.knowledge.framework.env") as mock_env,
        patch.object(DuckDBAnalyticsContext, "_download_from_s3") as mock_download,
      ):
        mock_env.is_development.return_value = False
        mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
        mock_env.AWS_REGION = "us-east-1"

        def fake_download(dest_path):
          conn = duckdb.connect(str(dest_path))
          conn.execute("CREATE TABLE data (id INT)")
          conn.execute("INSERT INTO data VALUES (1)")
          conn.close()

        mock_download.side_effect = fake_download

        with DuckDBAnalyticsContext(duckdb_source="sec") as ctx:
          relation = ctx.query("SELECT * FROM data")
          ctx.write_parquet(relation, "test_table")

          results = ctx.upload_outputs("my_analysis")

          assert "test_table" in results
          assert results["test_table"].startswith("s3://")
          assert "test-bucket" in results["test_table"]
          mock_s3_client.upload_file.assert_called_once()
    finally:
      # Restore original modules
      for mod_name, original in saved.items():
        if original is not None:
          sys.modules[mod_name] = original
        else:
          sys.modules.pop(mod_name, None)


class TestDuckDBAnalyticsContextEdgeCases:
  """Edge case and error handling tests."""

  def test_context_manager_with_statement(self, tmp_path):
    """Works correctly as a with-statement context manager."""
    db_path = tmp_path / "sec.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.close()

    with (
      patch("robosystems.adapters.sec.knowledge.framework.env") as mock_env,
      patch(
        "robosystems.adapters.sec.knowledge.framework.get_staging_duckdb_path",
        return_value=str(db_path),
      ),
    ):
      mock_env.is_development.return_value = True

      with DuckDBAnalyticsContext(duckdb_source="sec") as ctx:
        assert ctx._conn is not None
        assert ctx._db_path is not None

      # After exiting, connection should be closed
      assert ctx._conn is None

  def test_exit_handles_none_connection(self):
    """__exit__ handles the case where _conn is already None."""
    ctx = DuckDBAnalyticsContext()
    # Should not raise
    ctx.__exit__(None, None, None)

  def test_exit_handles_exception_during_close(self, tmp_path):
    """__exit__ handles exceptions during connection close."""
    db_path = tmp_path / "sec.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.close()

    with (
      patch("robosystems.adapters.sec.knowledge.framework.env") as mock_env,
      patch(
        "robosystems.adapters.sec.knowledge.framework.get_staging_duckdb_path",
        return_value=str(db_path),
      ),
    ):
      mock_env.is_development.return_value = True

      ctx = DuckDBAnalyticsContext(duckdb_source="sec")
      ctx.__enter__()

      # Manually close the connection to simulate an error
      ctx._conn.close()
      # __exit__ should handle the already-closed connection gracefully
      # (DuckDB close on already-closed conn may or may not raise)
      ctx.__exit__(None, None, None)
      assert ctx._conn is None

  def test_memory_limit_applied(self, tmp_path):
    """Memory limit is applied to the DuckDB connection."""
    db_path = tmp_path / "sec.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.close()

    with (
      patch("robosystems.adapters.sec.knowledge.framework.env") as mock_env,
      patch(
        "robosystems.adapters.sec.knowledge.framework.get_staging_duckdb_path",
        return_value=str(db_path),
      ),
    ):
      mock_env.is_development.return_value = True

      with DuckDBAnalyticsContext(duckdb_source="sec", memory_limit="512MB") as ctx:
        result = ctx.conn.execute("SELECT current_setting('memory_limit')").fetchone()
        assert result is not None
        # DuckDB reports in MiB; 512MB = 488.28 MiB
        mem_str = result[0]
        assert "488" in mem_str or "512" in mem_str
