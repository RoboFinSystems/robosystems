"""Comprehensive unit tests for LadybugBackend - targeting missed coverage lines."""

import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from robosystems.graph_api.backends.lbug import LadybugBackend


def _make_backend(mock_pool=None, data_path="/test/path"):
  """Helper to create a LadybugBackend with a mocked connection pool."""
  with (
    patch("robosystems.graph_api.backends.lbug.get_connection_pool") as mock_get_pool,
    patch.object(Path, "mkdir"),
  ):
    if mock_pool is None:
      mock_pool = MagicMock()
    mock_get_pool.return_value = mock_pool
    backend = LadybugBackend(data_path=data_path)
  return backend


@pytest.mark.unit
class TestLadybugBackendExecuteQuery:
  """Tests for execute_query with various parameter combinations."""

  @pytest.mark.asyncio
  async def test_execute_query_empty_result(self):
    """Query returning zero rows."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.get_column_names.return_value = ["id"]
    mock_result.has_next.return_value = False
    mock_conn.execute.return_value = mock_result
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool)
    result = await backend.execute_query("graph1", "MATCH (n) RETURN n.id")

    assert result == []
    mock_pool.get_connection.assert_called_once_with("graph1", read_only=False)

  @pytest.mark.asyncio
  async def test_execute_query_no_parameters(self):
    """Query without parameters should call execute with cypher only."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.get_column_names.return_value = []
    mock_result.has_next.return_value = False
    mock_conn.execute.return_value = mock_result
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool)
    await backend.execute_query("graph1", "MATCH (n) RETURN n", None)

    mock_conn.execute.assert_called_once_with("MATCH (n) RETURN n")

  @pytest.mark.asyncio
  async def test_execute_query_with_parameters(self):
    """Query with parameters should call execute with cypher and params."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.get_column_names.return_value = ["count"]
    mock_result.has_next.side_effect = [True, False]
    mock_result.get_next.return_value = [42]
    mock_conn.execute.return_value = mock_result
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool)
    params = {"name": "test"}
    result = await backend.execute_query(
      "graph1", "MATCH (n {name: $name}) RETURN count(n)", params
    )

    assert result == [{"count": 42}]
    mock_conn.execute.assert_called_once_with(
      "MATCH (n {name: $name}) RETURN count(n)", params
    )

  @pytest.mark.asyncio
  async def test_execute_query_multiple_columns_multiple_rows(self):
    """Query returning multiple columns and multiple rows."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.get_column_names.return_value = ["a", "b", "c"]
    mock_result.has_next.side_effect = [True, True, True, False]
    mock_result.get_next.side_effect = [[1, "x", True], [2, "y", False], [3, "z", True]]
    mock_conn.execute.return_value = mock_result
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool)
    result = await backend.execute_query("g", "RETURN 1")

    assert len(result) == 3
    assert result[0] == {"a": 1, "b": "x", "c": True}
    assert result[2] == {"a": 3, "b": "z", "c": True}


@pytest.mark.unit
class TestLadybugBackendExecuteWrite:
  """Tests for execute_write delegation."""

  @pytest.mark.asyncio
  async def test_execute_write_delegates_to_execute_query(self):
    """execute_write should delegate to execute_query."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.get_column_names.return_value = ["count"]
    mock_result.has_next.side_effect = [True, False]
    mock_result.get_next.return_value = [5]
    mock_conn.execute.return_value = mock_result
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool)
    result = await backend.execute_write(
      "graph1", "CREATE (n:Node) RETURN count(n)", {"key": "val"}
    )

    assert result == [{"count": 5}]

  @pytest.mark.asyncio
  async def test_execute_write_database_parameter_ignored(self):
    """The database parameter is unused for LadybugDB."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.get_column_names.return_value = []
    mock_result.has_next.return_value = False
    mock_conn.execute.return_value = mock_result
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool)
    await backend.execute_write("graph1", "CREATE (n:Node)", database="some_db")

    # Should use graph_id, not database parameter
    mock_pool.get_connection.assert_called_once_with("graph1", read_only=False)


@pytest.mark.unit
class TestLadybugBackendCreateDatabase:
  """Tests for create_database lifecycle."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  @pytest.mark.asyncio
  async def test_create_database_cleans_orphaned_wal(self):
    """Should clean up orphaned WAL file if main db doesn't exist."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_conn.execute.return_value = mock_result
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool, data_path=self.temp_dir)

    # Create orphaned WAL file (no corresponding .lbug)
    wal_path = Path(self.temp_dir) / "test_db.lbug.wal"
    wal_path.touch()
    assert wal_path.exists()

    result = await backend.create_database("test_db")

    assert result is True
    assert not wal_path.exists()

  @pytest.mark.asyncio
  async def test_create_database_no_wal_cleanup_when_db_exists(self):
    """Should NOT clean WAL if the database file exists (not orphaned)."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_conn.execute.return_value = mock_result
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool, data_path=self.temp_dir)

    # Create both db and WAL
    db_path = Path(self.temp_dir) / "test_db.lbug"
    wal_path = Path(self.temp_dir) / "test_db.lbug.wal"
    db_path.touch()
    wal_path.touch()

    result = await backend.create_database("test_db")

    assert result is True
    # WAL should still exist since db exists (not orphaned)
    assert wal_path.exists()


@pytest.mark.unit
class TestLadybugBackendDeleteDatabase:
  """Tests for delete_database lifecycle."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  @pytest.mark.asyncio
  async def test_delete_database_file(self):
    """Should delete a database file."""
    mock_pool = MagicMock()
    backend = _make_backend(mock_pool, data_path=self.temp_dir)

    db_path = Path(self.temp_dir) / "test_db.lbug"
    db_path.touch()

    result = await backend.delete_database("test_db")

    assert result is True
    assert not db_path.exists()
    mock_pool.force_database_cleanup.assert_called_once_with("test_db", aggressive=True)

  @pytest.mark.asyncio
  async def test_delete_database_directory(self):
    """Should delete a database directory (legacy)."""
    mock_pool = MagicMock()
    backend = _make_backend(mock_pool, data_path=self.temp_dir)

    db_path = Path(self.temp_dir) / "test_db.lbug"
    db_path.mkdir()
    (db_path / "data.bin").touch()

    result = await backend.delete_database("test_db")

    assert result is True
    assert not db_path.exists()

  @pytest.mark.asyncio
  async def test_delete_database_with_wal(self):
    """Should delete both the database and WAL file."""
    mock_pool = MagicMock()
    backend = _make_backend(mock_pool, data_path=self.temp_dir)

    db_path = Path(self.temp_dir) / "test_db.lbug"
    wal_path = Path(self.temp_dir) / "test_db.lbug.wal"
    db_path.touch()
    wal_path.touch()

    result = await backend.delete_database("test_db")

    assert result is True
    assert not db_path.exists()
    assert not wal_path.exists()

  @pytest.mark.asyncio
  async def test_delete_database_no_files(self):
    """Should succeed even if database files don't exist."""
    mock_pool = MagicMock()
    backend = _make_backend(mock_pool, data_path=self.temp_dir)

    result = await backend.delete_database("nonexistent")

    assert result is True

  @pytest.mark.asyncio
  async def test_delete_database_pool_cleanup_failure_continues(self):
    """Should continue with file deletion even if pool cleanup fails."""
    mock_pool = MagicMock()
    mock_pool.force_database_cleanup.side_effect = RuntimeError("Pool error")
    backend = _make_backend(mock_pool, data_path=self.temp_dir)

    db_path = Path(self.temp_dir) / "test_db.lbug"
    db_path.touch()

    result = await backend.delete_database("test_db")

    assert result is True
    assert not db_path.exists()


@pytest.mark.unit
class TestLadybugBackendGetDatabaseInfo:
  """Tests for get_database_info with various scenarios."""

  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  @pytest.mark.asyncio
  async def test_get_database_info_directory_db(self):
    """Should sum up sizes for directory-based database."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()

    mock_node_result = MagicMock()
    mock_node_result.has_next.side_effect = [True]
    mock_node_result.get_next.return_value = [10]

    mock_rel_result = MagicMock()
    mock_rel_result.has_next.side_effect = [True]
    mock_rel_result.get_next.return_value = [5]

    mock_conn.execute.side_effect = [mock_node_result, mock_rel_result]
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool, data_path=self.temp_dir)

    # Create a directory-based database
    db_path = Path(self.temp_dir) / "test_db.lbug"
    db_path.mkdir()
    (db_path / "file1.bin").write_bytes(b"x" * 100)
    (db_path / "file2.bin").write_bytes(b"x" * 200)

    result = await backend.get_database_info("test_db")

    assert result.name == "test_db"
    assert result.node_count == 10
    assert result.relationship_count == 5
    assert result.size_bytes == 300

  @pytest.mark.asyncio
  async def test_get_database_info_no_file(self):
    """Should return 0 size when database file doesn't exist."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()

    mock_node_result = MagicMock()
    mock_node_result.has_next.return_value = False

    mock_rel_result = MagicMock()
    mock_rel_result.has_next.return_value = False

    mock_conn.execute.side_effect = [mock_node_result, mock_rel_result]
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool, data_path=self.temp_dir)

    result = await backend.get_database_info("nonexistent")

    assert result.size_bytes == 0
    assert result.node_count == 0
    assert result.relationship_count == 0

  @pytest.mark.asyncio
  async def test_get_database_info_no_has_next_rows(self):
    """has_next returns False immediately - counts remain 0."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()

    mock_node_result = MagicMock()
    mock_node_result.has_next.return_value = False

    mock_rel_result = MagicMock()
    mock_rel_result.has_next.return_value = False

    mock_conn.execute.side_effect = [mock_node_result, mock_rel_result]
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool, data_path=self.temp_dir)

    db_path = Path(self.temp_dir) / "test_db.lbug"
    db_path.write_bytes(b"x" * 50)

    result = await backend.get_database_info("test_db")

    assert result.node_count == 0
    assert result.relationship_count == 0
    assert result.size_bytes == 50


@pytest.mark.unit
class TestLadybugBackendS3Ingestion:
  """Tests for ingest_from_s3 with various credential modes."""

  @pytest.mark.asyncio
  async def test_ingest_with_explicit_credentials(self):
    """Should configure S3 credentials from explicit dict."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.return_value = None
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool)

    s3_creds = {
      "aws_access_key_id": "AKID",
      "aws_secret_access_key": "SECRET",
      "region": "us-west-2",
      "endpoint_url": "http://localhost:4566",
    }

    result = await backend.ingest_from_s3(
      graph_id="test",
      table_name="TestTable",
      s3_pattern="s3://bucket/data.parquet",
      s3_credentials=s3_creds,
    )

    assert "records_loaded" in result
    assert "duration_seconds" in result
    assert "query" in result

    # Verify credential-related execute calls
    call_args = [str(c) for c in mock_conn.execute.call_args_list]
    call_str = " ".join(call_args)
    assert "s3_access_key_id" in call_str
    assert "s3_secret_access_key" in call_str
    assert "s3_region" in call_str
    assert "s3_endpoint" in call_str
    assert "s3_url_style" in call_str

  @pytest.mark.asyncio
  async def test_ingest_endpoint_url_http_stripping(self):
    """Should strip http:// from endpoint URL."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.return_value = None
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool)

    s3_creds = {"endpoint_url": "http://localstack:4566"}

    await backend.ingest_from_s3(
      graph_id="test",
      table_name="T",
      s3_pattern="s3://b/d.parquet",
      s3_credentials=s3_creds,
    )

    execute_calls = [str(c) for c in mock_conn.execute.call_args_list]
    # Should have stripped http:// from endpoint
    endpoint_calls = [c for c in execute_calls if "s3_endpoint" in c]
    assert len(endpoint_calls) > 0
    assert "http://" not in endpoint_calls[0]

  @pytest.mark.asyncio
  async def test_ingest_endpoint_url_https_stripping(self):
    """Should strip https:// from endpoint URL."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.return_value = None
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool)

    s3_creds = {"endpoint_url": "https://s3.amazonaws.com"}

    await backend.ingest_from_s3(
      graph_id="test",
      table_name="T",
      s3_pattern="s3://b/d.parquet",
      s3_credentials=s3_creds,
    )

    execute_calls = [str(c) for c in mock_conn.execute.call_args_list]
    endpoint_calls = [c for c in execute_calls if "s3_endpoint" in c]
    assert len(endpoint_calls) > 0
    assert "https://" not in endpoint_calls[0]

  @pytest.mark.asyncio
  async def test_ingest_quote_escaping_in_credentials(self):
    """Should escape single quotes in credential strings."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.return_value = None
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool)

    s3_creds = {
      "aws_access_key_id": "key'with'quotes",
      "aws_secret_access_key": "sec'ret",
      "region": "us'east",
      "endpoint_url": "http://host'name:4566",
    }

    await backend.ingest_from_s3(
      graph_id="test",
      table_name="T",
      s3_pattern="s3://b/d.parquet",
      s3_credentials=s3_creds,
    )

    # Should not raise - quotes are escaped
    execute_calls = [str(c) for c in mock_conn.execute.call_args_list]
    call_str = " ".join(execute_calls)
    assert "key''with''quotes" in call_str
    assert "sec''ret" in call_str

  @pytest.mark.asyncio
  async def test_ingest_extension_already_loaded(self):
    """Should swallow 'already loaded' errors during extension loading."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()

    # First two calls are INSTALL/LOAD httpfs, raise "already loaded"
    call_count = 0

    def side_effect(*args, **kwargs):
      nonlocal call_count
      call_count += 1
      if call_count <= 2:
        # INSTALL and LOAD httpfs
        if "INSTALL" in str(args[0]) or "LOAD" in str(args[0]):
          raise RuntimeError("extension already loaded")
      return None

    mock_conn.execute.side_effect = side_effect
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool)

    # Should not raise since error message contains "already loaded"
    result = await backend.ingest_from_s3(
      graph_id="test",
      table_name="T",
      s3_pattern="s3://b/d.parquet",
      s3_credentials={"aws_access_key_id": "key", "aws_secret_access_key": "secret"},
    )

    assert "records_loaded" in result

  @pytest.mark.asyncio
  async def test_ingest_multiple_s3_patterns(self):
    """Should handle list of S3 patterns with array syntax."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.return_value = None
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool)

    patterns = [
      "s3://bucket/file1.parquet",
      "s3://bucket/file2.parquet",
      "s3://bucket/file3.parquet",
    ]

    result = await backend.ingest_from_s3(
      graph_id="test",
      table_name="T",
      s3_pattern=patterns,
      s3_credentials={"aws_access_key_id": "key", "aws_secret_access_key": "secret"},
    )

    assert result["query"].startswith("COPY T FROM [")
    assert '"s3://bucket/file1.parquet"' in result["query"]
    assert '"s3://bucket/file2.parquet"' in result["query"]
    assert '"s3://bucket/file3.parquet"' in result["query"]

  @pytest.mark.asyncio
  async def test_ingest_single_pattern(self):
    """Should handle single S3 pattern string."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.return_value = None
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool)

    result = await backend.ingest_from_s3(
      graph_id="test",
      table_name="T",
      s3_pattern="s3://bucket/data/*.parquet",
      s3_credentials={"aws_access_key_id": "key", "aws_secret_access_key": "secret"},
    )

    assert 'COPY T FROM "s3://bucket/data/*.parquet"' in result["query"]

  @pytest.mark.asyncio
  async def test_ingest_ignore_errors_flag(self):
    """Should append IGNORE_ERRORS=TRUE when ignore_errors is True."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.return_value = None
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool)

    result = await backend.ingest_from_s3(
      graph_id="test",
      table_name="T",
      s3_pattern="s3://b/d.parquet",
      s3_credentials={"aws_access_key_id": "k", "aws_secret_access_key": "s"},
      ignore_errors=True,
    )

    assert "(IGNORE_ERRORS=TRUE)" in result["query"]

  @pytest.mark.asyncio
  async def test_ingest_no_ignore_errors(self):
    """Should NOT append IGNORE_ERRORS when ignore_errors is False."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.return_value = None
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool)

    result = await backend.ingest_from_s3(
      graph_id="test",
      table_name="T",
      s3_pattern="s3://b/d.parquet",
      s3_credentials={"aws_access_key_id": "k", "aws_secret_access_key": "s"},
      ignore_errors=False,
    )

    assert "IGNORE_ERRORS" not in result["query"]

  @pytest.mark.asyncio
  async def test_ingest_copy_result_get_as_list_int(self):
    """Should parse records_loaded from get_as_list returning integer tuple."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()

    mock_copy_result = MagicMock()
    mock_copy_result.get_as_list.return_value = [(42,)]
    mock_copy_result.has_next = MagicMock(return_value=False)

    def side_effect(query, *args, **kwargs):
      if "COPY" in str(query):
        return mock_copy_result
      return None

    mock_conn.execute.side_effect = side_effect
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool)

    result = await backend.ingest_from_s3(
      graph_id="test",
      table_name="T",
      s3_pattern="s3://b/d.parquet",
      s3_credentials={"aws_access_key_id": "k", "aws_secret_access_key": "s"},
    )

    assert result["records_loaded"] == 42

  @pytest.mark.asyncio
  async def test_ingest_copy_result_records_loaded_string(self):
    """Should parse records_loaded from string with 'Records loaded: N' via str()."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()

    # The string parsing path uses str(first_row). When first_row is a
    # plain string (not a tuple), it can extract the number directly.
    mock_copy_result = MagicMock()
    mock_copy_result.get_as_list.return_value = ["Records loaded: 100"]
    mock_copy_result.has_next = MagicMock(return_value=False)

    def side_effect(query, *args, **kwargs):
      if "COPY" in str(query):
        return mock_copy_result
      return None

    mock_conn.execute.side_effect = side_effect
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool)

    result = await backend.ingest_from_s3(
      graph_id="test",
      table_name="T",
      s3_pattern="s3://b/d.parquet",
      s3_credentials={"aws_access_key_id": "k", "aws_secret_access_key": "s"},
    )

    assert result["records_loaded"] == 100

  @pytest.mark.asyncio
  async def test_ingest_copy_result_get_next_fallback(self):
    """Should parse records_loaded from get_next when get_as_list returns empty."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()

    mock_copy_result = MagicMock()
    mock_copy_result.get_as_list.return_value = []
    mock_copy_result.has_next.return_value = True
    mock_copy_result.get_next.return_value = [77]

    def side_effect(query, *args, **kwargs):
      if "COPY" in str(query):
        return mock_copy_result
      return None

    mock_conn.execute.side_effect = side_effect
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool)

    result = await backend.ingest_from_s3(
      graph_id="test",
      table_name="T",
      s3_pattern="s3://b/d.parquet",
      s3_credentials={"aws_access_key_id": "k", "aws_secret_access_key": "s"},
    )

    assert result["records_loaded"] == 77

  @pytest.mark.asyncio
  async def test_ingest_checkpoint_failure_logged(self):
    """Should log warning but not raise when CHECKPOINT fails."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()

    def side_effect(query, *args, **kwargs):
      if "CHECKPOINT" in str(query):
        raise RuntimeError("Checkpoint failed")
      return None

    mock_conn.execute.side_effect = side_effect
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool)

    # Should not raise
    result = await backend.ingest_from_s3(
      graph_id="test",
      table_name="T",
      s3_pattern="s3://b/d.parquet",
      s3_credentials={"aws_access_key_id": "k", "aws_secret_access_key": "s"},
    )

    assert "records_loaded" in result

  @pytest.mark.asyncio
  async def test_ingest_production_imds_credentials(self):
    """Should attempt IMDS v2 credential fetch in production."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.return_value = None
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool)

    mock_env = MagicMock()
    mock_env.ENVIRONMENT = "prod"
    mock_env.AWS_DEFAULT_REGION = "us-east-1"
    mock_env.AWS_REGION = "us-east-1"
    mock_env.AWS_S3_ACCESS_KEY_ID = None
    mock_env.AWS_S3_SECRET_ACCESS_KEY = None

    # Mock the requests module to simulate IMDS
    mock_token_response = MagicMock()
    mock_token_response.text = "test-token"

    mock_role_response = MagicMock()
    mock_role_response.text = "TestRole"

    mock_creds_response = MagicMock()
    mock_creds_response.json.return_value = {
      "AccessKeyId": "AKID",
      "SecretAccessKey": "SECRET",
      "Token": "SESSION",
    }

    with (
      patch("robosystems.config.env", mock_env),
      patch("requests.put", return_value=mock_token_response),
      patch(
        "requests.get",
        side_effect=[mock_role_response, mock_creds_response],
      ),
    ):
      result = await backend.ingest_from_s3(
        graph_id="test",
        table_name="T",
        s3_pattern="s3://b/d.parquet",
        s3_credentials=None,
      )

    assert "records_loaded" in result

  @pytest.mark.asyncio
  async def test_ingest_dev_env_credentials(self):
    """Should use env var credentials in dev when IMDS is not available."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.return_value = None
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    backend = _make_backend(mock_pool)

    mock_env = MagicMock()
    mock_env.ENVIRONMENT = "dev"
    mock_env.AWS_DEFAULT_REGION = "us-east-1"
    mock_env.AWS_REGION = "us-east-1"
    mock_env.AWS_S3_ACCESS_KEY_ID = "DEV_KEY"
    mock_env.AWS_S3_SECRET_ACCESS_KEY = "DEV_SECRET"

    with patch("robosystems.config.env", mock_env):
      result = await backend.ingest_from_s3(
        graph_id="test",
        table_name="T",
        s3_pattern="s3://b/d.parquet",
        s3_credentials=None,
      )

    assert "records_loaded" in result
    call_strs = [str(c) for c in mock_conn.execute.call_args_list]
    joined = " ".join(call_strs)
    assert "DEV_KEY" in joined
    assert "DEV_SECRET" in joined


@pytest.mark.unit
class TestLadybugBackendLoadHttpfsExtension:
  """Tests for _load_httpfs_extension helper."""

  def test_load_httpfs_extension(self):
    """Should call INSTALL and LOAD httpfs."""
    backend = _make_backend()
    mock_conn = MagicMock()

    backend._load_httpfs_extension(mock_conn)

    assert mock_conn.execute.call_count == 2
    mock_conn.execute.assert_any_call("INSTALL httpfs")
    mock_conn.execute.assert_any_call("LOAD httpfs")


@pytest.mark.unit
class TestLadybugBackendClose:
  """Tests for close method."""

  @pytest.mark.asyncio
  async def test_close_calls_pool_close_all(self):
    """Should delegate to connection_pool.close_all."""
    mock_pool = MagicMock()
    backend = _make_backend(mock_pool)

    await backend.close()

    mock_pool.close_all.assert_called_once()
