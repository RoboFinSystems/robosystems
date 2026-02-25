"""Comprehensive unit tests for DuckDBTableManager - targeting missed coverage lines."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.graph_api.core.duckdb.manager import (
  DuckDBTableManager,
  validate_table_name,
  validate_table_name_decorator,
)
from robosystems.graph_api.models.tables import (
  TableQueryRequest,
)


@pytest.mark.unit
class TestValidateTableName:
  """Tests for validate_table_name security function."""

  def test_valid_names(self):
    """Should accept valid table names."""
    for name in ["Entity", "my_table", "table-1", "ABC123"]:
      validate_table_name(name)  # Should not raise

  def test_empty_name_raises(self):
    """Should reject empty table name."""
    with pytest.raises(HTTPException) as exc_info:
      validate_table_name("")
    assert exc_info.value.status_code == 400

  def test_none_name_raises(self):
    """Should reject None table name."""
    with pytest.raises((HTTPException, TypeError)):
      validate_table_name(None)

  def test_special_chars_raise(self):
    """Should reject names with SQL injection characters."""
    for name in ["table; DROP", "table'name", "table name", "a/b", "a.b"]:
      with pytest.raises(HTTPException):
        validate_table_name(name)


@pytest.mark.unit
class TestValidateTableNameDecorator:
  """Tests for validate_table_name_decorator."""

  def test_decorator_validates_table_name_kwarg(self):
    """Should validate table_name from kwargs."""

    class FakeManager:
      @validate_table_name_decorator
      def method(self, table_name=None):
        return "ok"

    mgr = FakeManager()
    with pytest.raises(HTTPException):
      mgr.method(table_name="bad; DROP")

  def test_decorator_validates_request_kwarg(self):
    """Should validate table_name from request object in kwargs."""

    class FakeManager:
      @validate_table_name_decorator
      def method(self, request=None):
        return "ok"

    mock_request = MagicMock()
    mock_request.table_name = "bad; DROP"

    mgr = FakeManager()
    with pytest.raises(HTTPException):
      mgr.method(request=mock_request)

  def test_decorator_validates_positional_request(self):
    """Should validate table_name from positional request argument."""

    class FakeManager:
      @validate_table_name_decorator
      def method(self, request):
        return "ok"

    mock_request = MagicMock()
    mock_request.table_name = "bad; DROP"

    mgr = FakeManager()
    with pytest.raises(HTTPException):
      mgr.method(mock_request)

  def test_decorator_passes_valid_name(self):
    """Should pass through for valid table names."""

    class FakeManager:
      @validate_table_name_decorator
      def method(self, table_name=None):
        return "ok"

    mgr = FakeManager()
    assert mgr.method(table_name="valid_name") == "ok"


@pytest.mark.unit
class TestDuckDBTableManagerBuildTableSql:
  """Tests for _build_table_sql with deduplication logic."""

  def setup_method(self):
    self.manager = DuckDBTableManager()

  def test_build_sql_node_table_with_identifier(self):
    """Should build GROUP BY identifier SQL for node tables."""
    sql = self.manager._build_table_sql(
      '"Entity"',
      has_identifier=True,
      has_from_to=False,
      use_list=False,
      column_names=["identifier", "name", "type"],
    )

    assert "GROUP BY" in sql
    assert '"identifier"' in sql
    assert 'FIRST("name")' in sql
    assert 'FIRST("type")' in sql
    assert "?" in sql  # Parameter binding

  def test_build_sql_relationship_table(self):
    """Should build GROUP BY from/to SQL for relationship tables."""
    sql = self.manager._build_table_sql(
      '"HAS_REL"',
      has_identifier=False,
      has_from_to=True,
      use_list=True,
      column_names=["from", "to", "weight"],
    )

    assert "GROUP BY" in sql
    assert '"from" AS src' in sql
    assert '"to" AS dst' in sql
    assert 'FIRST("weight")' in sql
    assert "__FILES_PLACEHOLDER__" in sql

  def test_build_sql_unknown_table(self):
    """Should use SELECT * for unknown table types."""
    sql = self.manager._build_table_sql(
      '"Unknown"',
      has_identifier=False,
      has_from_to=False,
      use_list=False,
      column_names=None,
    )

    assert "SELECT *" in sql
    assert "GROUP BY" not in sql

  def test_build_sql_no_column_names(self):
    """Should fall through to SELECT * when column_names is None even with identifier."""
    sql = self.manager._build_table_sql(
      '"Entity"',
      has_identifier=True,
      has_from_to=False,
      use_list=False,
      column_names=None,
    )

    assert "SELECT *" in sql


@pytest.mark.unit
class TestDuckDBTableManagerBuildTableSqlWithFileId:
  """Tests for _build_table_sql_with_file_id with UNION ALL logic."""

  def setup_method(self):
    self.manager = DuckDBTableManager()

  def test_build_node_table_with_file_id(self):
    """Should inject file_id column for node tables."""
    sql = self.manager._build_table_sql_with_file_id(
      '"Entity"',
      has_identifier=True,
      has_from_to=False,
      s3_files=["s3://b/file1.parquet", "s3://b/file2.parquet"],
      file_id_map={"s3://b/file1.parquet": "f1", "s3://b/file2.parquet": "f2"},
      column_names=["identifier", "name"],
    )

    assert "UNION ALL" in sql
    assert "'f1' as file_id" in sql
    assert "'f2' as file_id" in sql
    assert "GROUP BY" in sql
    assert "FIRST(file_id)" in sql

  def test_build_relationship_with_file_id(self):
    """Should rename from/to to src/dst and add file_id."""
    sql = self.manager._build_table_sql_with_file_id(
      '"HAS_REL"',
      has_identifier=False,
      has_from_to=True,
      s3_files=["s3://b/rel.parquet"],
      file_id_map={"s3://b/rel.parquet": "r1"},
      column_names=["from", "to", "weight"],
    )

    assert '"from" as src' in sql
    assert '"to" as dst' in sql
    assert "GROUP BY src, dst" in sql

  def test_build_unknown_with_file_id(self):
    """Should just add file_id without deduplication."""
    sql = self.manager._build_table_sql_with_file_id(
      '"Other"',
      has_identifier=False,
      has_from_to=False,
      s3_files=["s3://b/data.parquet"],
      file_id_map={"s3://b/data.parquet": "d1"},
      column_names=None,
    )

    assert "'d1' as file_id" in sql
    assert "GROUP BY" not in sql

  def test_build_with_unknown_file_id(self):
    """Should use 'unknown' when file not in file_id_map."""
    sql = self.manager._build_table_sql_with_file_id(
      '"Entity"',
      has_identifier=True,
      has_from_to=False,
      s3_files=["s3://b/missing.parquet"],
      file_id_map={},
      column_names=["identifier", "name"],
    )

    assert "'unknown' as file_id" in sql


@pytest.mark.unit
class TestDuckDBTableManagerQueryTable:
  """Tests for query_table."""

  def setup_method(self):
    self.manager = DuckDBTableManager()

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_query_with_parameters(self, mock_get_pool):
    """Should pass parameters to DuckDB execute."""
    mock_pool = MagicMock()
    mock_get_pool.return_value = mock_pool
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchall.return_value = [(1, "test")]
    mock_conn.description = [("id",), ("name",)]
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    request = TableQueryRequest(
      graph_id="g1",
      sql="SELECT * FROM t WHERE id = ?",
      parameters=[1],
    )

    result = self.manager.query_table(request)

    assert result.row_count == 1
    mock_conn.execute.assert_called_once_with("SELECT * FROM t WHERE id = ?", [1])

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_query_without_parameters(self, mock_get_pool):
    """Should execute query without parameters."""
    mock_pool = MagicMock()
    mock_get_pool.return_value = mock_pool
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchall.return_value = []
    mock_conn.description = []
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    request = TableQueryRequest(
      graph_id="g1",
      sql="SELECT COUNT(*) FROM t",
    )

    result = self.manager.query_table(request)

    assert result.row_count == 0

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_query_error_raises_400(self, mock_get_pool):
    """Should raise 400 on query error."""
    mock_pool = MagicMock()
    mock_get_pool.return_value = mock_pool
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = RuntimeError("Query syntax error")
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    request = TableQueryRequest(
      graph_id="g1",
      sql="INVALID SQL",
    )

    with pytest.raises(HTTPException) as exc_info:
      self.manager.query_table(request)
    assert exc_info.value.status_code == 400


@pytest.mark.unit
class TestDuckDBTableManagerQueryTableStreaming:
  """Tests for query_table_streaming."""

  def setup_method(self):
    self.manager = DuckDBTableManager()

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_streaming_yields_chunks(self, mock_get_pool):
    """Should yield data in chunks."""
    mock_pool = MagicMock()
    mock_get_pool.return_value = mock_pool
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.description = [("id",), ("name",)]
    mock_cursor.fetchmany.side_effect = [
      [(1, "a"), (2, "b")],
      [(3, "c")],
      [],
    ]
    mock_conn.execute.return_value = mock_cursor
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    request = TableQueryRequest(
      graph_id="g1",
      sql="SELECT * FROM t",
    )

    chunks = list(self.manager.query_table_streaming(request, chunk_size=2))

    assert len(chunks) == 2
    assert chunks[0]["chunk_index"] == 0
    assert chunks[0]["row_count"] == 2
    assert chunks[1]["is_last_chunk"] is True

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_streaming_error_yields_error_chunk(self, mock_get_pool):
    """Should yield error chunk on failure."""
    mock_pool = MagicMock()
    mock_get_pool.return_value = mock_pool
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = RuntimeError("Query failed")
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    request = TableQueryRequest(
      graph_id="g1",
      sql="BAD SQL",
    )

    chunks = list(self.manager.query_table_streaming(request))

    assert len(chunks) == 1
    assert "error" in chunks[0]
    assert chunks[0]["is_last_chunk"] is True


@pytest.mark.unit
class TestDuckDBTableManagerDeleteTable:
  """Tests for delete_table."""

  def setup_method(self):
    self.manager = DuckDBTableManager()

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_delete_table_success(self, mock_get_pool):
    """Should drop table and view."""
    mock_pool = MagicMock()
    mock_get_pool.return_value = mock_pool
    mock_conn = MagicMock()
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    result = self.manager.delete_table("g1", table_name="Entity")

    assert result["status"] == "success"
    assert mock_conn.execute.call_count == 2  # DROP TABLE + DROP VIEW

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_delete_table_error_raises_500(self, mock_get_pool):
    """Should raise 500 on delete error."""
    mock_pool = MagicMock()
    mock_get_pool.return_value = mock_pool
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = RuntimeError("Delete failed")
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    with pytest.raises(HTTPException) as exc_info:
      self.manager.delete_table("g1", table_name="Entity")
    assert exc_info.value.status_code == 500


@pytest.mark.unit
class TestDuckDBTableManagerDeleteFileData:
  """Tests for delete_file_data."""

  def setup_method(self):
    self.manager = DuckDBTableManager()

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_delete_file_data_table_not_found(self, mock_get_pool):
    """Should return success with 0 rows when table doesn't exist."""
    mock_pool = MagicMock()
    mock_get_pool.return_value = mock_pool
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchone.return_value = (0,)
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    result = self.manager.delete_file_data("g1", "Entity", "file1")

    assert result["rows_deleted"] == 0

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_delete_file_data_no_file_id_column(self, mock_get_pool):
    """Should raise 400 when table has no file_id column."""
    mock_pool = MagicMock()
    mock_get_pool.return_value = mock_pool
    mock_conn = MagicMock()

    # First call: table exists
    mock_conn.execute.return_value.fetchone.return_value = (1,)
    # Second call: columns don't include file_id
    mock_conn.execute.return_value.fetchall.return_value = [("id",), ("name",)]

    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    with pytest.raises(HTTPException) as exc_info:
      self.manager.delete_file_data("g1", "Entity", "file1")
    assert exc_info.value.status_code == 400

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_delete_file_data_no_matching_rows(self, mock_get_pool):
    """Should return 0 rows deleted when no rows match file_id."""
    mock_pool = MagicMock()
    mock_get_pool.return_value = mock_pool
    mock_conn = MagicMock()

    # Setup sequential execute calls
    call_count = 0

    def execute_side_effect(query, params=None):
      nonlocal call_count
      call_count += 1
      result = MagicMock()

      if "information_schema.tables" in query:
        result.fetchone.return_value = (1,)  # Table exists
      elif "information_schema.columns" in query:
        result.fetchall.return_value = [("file_id",), ("name",)]
      elif "COUNT(*)" in query:
        result.fetchone.return_value = (0,)  # No matching rows
      return result

    mock_conn.execute.side_effect = execute_side_effect
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    result = self.manager.delete_file_data("g1", "Entity", "file1")

    assert result["rows_deleted"] == 0


@pytest.mark.unit
class TestDuckDBTableManagerListTables:
  """Tests for list_tables."""

  def setup_method(self):
    self.manager = DuckDBTableManager()

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_list_tables_empty(self, mock_get_pool):
    """Should return empty list when no tables exist."""
    mock_pool = MagicMock()
    mock_get_pool.return_value = mock_pool
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchall.return_value = []
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    result = self.manager.list_tables("g1")

    assert result == []

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_list_tables_with_row_counts(self, mock_get_pool):
    """Should return table info with row counts."""
    mock_pool = MagicMock()
    mock_get_pool.return_value = mock_pool
    mock_conn = MagicMock()

    # First call: list tables
    # Subsequent calls: count rows
    call_count = 0

    def execute_side_effect(query, params=None):
      nonlocal call_count
      call_count += 1
      result = MagicMock()

      if "information_schema" in query:
        result.fetchall.return_value = [("Entity",), ("HAS_REL",)]
      elif "COUNT(*)" in query:
        result.fetchone.return_value = (42,)
      return result

    mock_conn.execute.side_effect = execute_side_effect
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn

    result = self.manager.list_tables("g1")

    assert len(result) == 2
    assert result[0].table_name == "Entity"
    assert result[0].row_count == 42
