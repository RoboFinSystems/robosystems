import shutil
import tempfile
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.graph_api.core.duckdb.manager import (
  DuckDBTableManager,
  TableCreateRequest,
  TableCreateResponse,
  TableInfo,
  TableQueryRequest,
  TableQueryResponse,
  validate_table_name,
)


class TestValidateTableName:
  def test_valid_alphanumeric_table_name(self):
    validate_table_name("customers")
    validate_table_name("orders123")
    validate_table_name("DATA_TABLE")

  def test_valid_with_underscores(self):
    validate_table_name("customer_orders")
    validate_table_name("_private_table")
    validate_table_name("table_1_2_3")

  def test_valid_with_hyphens(self):
    validate_table_name("customer-orders")
    validate_table_name("table-123")

  def test_empty_table_name_raises_error(self):
    with pytest.raises(HTTPException) as exc_info:
      validate_table_name("")
    assert exc_info.value.status_code == 400
    assert "Invalid table name" in exc_info.value.detail

  def test_sql_injection_attempts_raise_error(self):
    injection_attempts = [
      "customers; DROP TABLE users;--",
      "orders' OR '1'='1",
      "table\x00null",
      "table;DELETE FROM",
    ]
    for attempt in injection_attempts:
      with pytest.raises(HTTPException) as exc_info:
        validate_table_name(attempt)
      assert exc_info.value.status_code == 400

  def test_special_characters_raise_error(self):
    invalid_names = [
      "table.name",
      "table/name",
      "table\\name",
      "table name",
      "table@name",
      "table#name",
    ]
    for invalid_name in invalid_names:
      with pytest.raises(HTTPException) as exc_info:
        validate_table_name(invalid_name)
      assert exc_info.value.status_code == 400


class TestDuckDBTableManager:
  def setup_method(self):
    self.temp_dir = tempfile.mkdtemp()
    self.manager = DuckDBTableManager()

  def teardown_method(self):
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_create_table_success(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
    mock_get_pool.return_value = mock_pool

    mock_probe_result = MagicMock()
    mock_probe_result.description = [("identifier", None), ("name", None)]
    mock_conn.execute.return_value = mock_probe_result

    request = TableCreateRequest(
      graph_id="test_graph",
      table_name="customers",
      s3_pattern="s3://bucket/data/*.parquet",
    )

    response = self.manager.create_table(request)

    assert response.status == "success"
    assert response.graph_id == "test_graph"
    assert response.table_name == "customers"
    assert response.execution_time_ms > 0

    mock_conn.execute.assert_called()
    execute_calls = [call[0][0] for call in mock_conn.execute.call_args_list]
    assert any("CREATE OR REPLACE TABLE" in call for call in execute_calls)
    assert any("read_parquet" in call for call in execute_calls)

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_create_table_validates_table_name(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_get_pool.return_value = mock_pool

    request = TableCreateRequest(
      graph_id="test_graph",
      table_name="invalid;DROP TABLE",
      s3_pattern="s3://bucket/data/*.parquet",
    )

    with pytest.raises(HTTPException) as exc_info:
      self.manager.create_table(request)
    assert exc_info.value.status_code == 400

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_create_table_uses_quoted_table_name(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
    mock_get_pool.return_value = mock_pool

    mock_probe_result = MagicMock()
    mock_probe_result.description = [("identifier", None), ("name", None)]
    mock_conn.execute.return_value = mock_probe_result

    request = TableCreateRequest(
      graph_id="test_graph",
      table_name="my_table",
      s3_pattern="s3://bucket/data/*.parquet",
    )

    self.manager.create_table(request)

    execute_calls = [call[0][0] for call in mock_conn.execute.call_args_list]
    assert any('"my_table"' in call for call in execute_calls)

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_create_table_handles_connection_failure(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = Exception("Connection failed")
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
    mock_get_pool.return_value = mock_pool

    request = TableCreateRequest(
      graph_id="test_graph",
      table_name="customers",
      s3_pattern="s3://bucket/data/*.parquet",
    )

    with pytest.raises(HTTPException) as exc_info:
      self.manager.create_table(request)
    assert exc_info.value.status_code == 500
    assert "Failed to create table" in exc_info.value.detail

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_query_table_success(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
    mock_get_pool.return_value = mock_pool

    mock_conn.execute.return_value.fetchall.return_value = [
      ("Alice", 30),
      ("Bob", 25),
    ]
    mock_conn.description = [("name", None), ("age", None)]

    request = TableQueryRequest(
      graph_id="test_graph", sql="SELECT name, age FROM customers"
    )

    response = self.manager.query_table(request)

    assert response.columns == ["name", "age"]
    assert response.rows == [["Alice", 30], ["Bob", 25]]
    assert response.row_count == 2
    assert response.execution_time_ms > 0

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_query_table_empty_results(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
    mock_get_pool.return_value = mock_pool

    mock_conn.execute.return_value.fetchall.return_value = []
    mock_conn.description = [("name", None), ("age", None)]

    request = TableQueryRequest(
      graph_id="test_graph", sql="SELECT name, age FROM customers WHERE FALSE"
    )

    response = self.manager.query_table(request)

    assert response.columns == ["name", "age"]
    assert response.rows == []
    assert response.row_count == 0

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_query_table_handles_sql_errors(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = Exception("Invalid SQL")
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
    mock_get_pool.return_value = mock_pool

    request = TableQueryRequest(graph_id="test_graph", sql="INVALID SQL")

    with pytest.raises(HTTPException) as exc_info:
      self.manager.query_table(request)
    assert exc_info.value.status_code == 400
    assert "Query failed" in exc_info.value.detail

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_query_table_streaming_success(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
    mock_get_pool.return_value = mock_pool

    mock_cursor = MagicMock()
    mock_cursor.description = [("id", None), ("name", None)]
    mock_cursor.fetchmany.side_effect = [
      [(1, "Alice"), (2, "Bob")],
      [(3, "Charlie")],
      [],
    ]
    mock_conn.execute.return_value = mock_cursor

    request = TableQueryRequest(graph_id="test_graph", sql="SELECT * FROM customers")

    chunks = list(self.manager.query_table_streaming(request, chunk_size=2))

    assert len(chunks) == 2
    assert chunks[0]["columns"] == ["id", "name"]
    assert chunks[0]["rows"] == [[1, "Alice"], [2, "Bob"]]
    assert chunks[0]["chunk_index"] == 0
    assert chunks[0]["is_last_chunk"] is False
    assert chunks[0]["row_count"] == 2
    assert chunks[0]["total_rows_sent"] == 2

    assert chunks[1]["rows"] == [[3, "Charlie"]]
    assert chunks[1]["chunk_index"] == 1
    assert chunks[1]["is_last_chunk"] is True
    assert chunks[1]["row_count"] == 1
    assert chunks[1]["total_rows_sent"] == 3

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_query_table_streaming_empty_results(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
    mock_get_pool.return_value = mock_pool

    mock_cursor = MagicMock()
    mock_cursor.description = [("id", None)]
    mock_cursor.fetchmany.return_value = []
    mock_conn.execute.return_value = mock_cursor

    request = TableQueryRequest(
      graph_id="test_graph", sql="SELECT * FROM customers WHERE FALSE"
    )

    chunks = list(self.manager.query_table_streaming(request))

    assert len(chunks) == 0

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_query_table_streaming_handles_errors(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = Exception("Query error")
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
    mock_get_pool.return_value = mock_pool

    request = TableQueryRequest(graph_id="test_graph", sql="INVALID SQL")

    chunks = list(self.manager.query_table_streaming(request))

    assert len(chunks) == 1
    assert "error" in chunks[0]
    assert chunks[0]["error"] == "Query error"
    assert chunks[0]["is_last_chunk"] is True

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_list_tables_success(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
    mock_get_pool.return_value = mock_pool

    mock_conn.execute.return_value.fetchall.return_value = [
      ("customers",),
      ("orders",),
    ]
    mock_conn.execute.return_value.fetchone.side_effect = [(100,), (50,)]

    tables = self.manager.list_tables("test_graph")

    assert len(tables) == 2
    assert tables[0].table_name == "customers"
    assert tables[0].row_count == 100
    assert tables[1].table_name == "orders"
    assert tables[1].row_count == 50

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_list_tables_empty_database(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
    mock_get_pool.return_value = mock_pool

    mock_conn.execute.return_value.fetchall.return_value = []

    tables = self.manager.list_tables("test_graph")

    assert len(tables) == 0

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_list_tables_handles_connection_failure(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = Exception("Connection failed")
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
    mock_get_pool.return_value = mock_pool

    with pytest.raises(HTTPException) as exc_info:
      self.manager.list_tables("test_graph")
    assert exc_info.value.status_code == 500
    assert "Failed to list tables" in exc_info.value.detail

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_delete_table_success(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
    mock_get_pool.return_value = mock_pool

    result = self.manager.delete_table("test_graph", "customers")

    assert result["status"] == "success"
    assert "deleted" in result["message"]

    execute_calls = [call[0][0] for call in mock_conn.execute.call_args_list]
    assert any("DROP TABLE IF EXISTS" in call for call in execute_calls)
    assert any("DROP VIEW IF EXISTS" in call for call in execute_calls)

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_delete_table_validates_table_name(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_get_pool.return_value = mock_pool

    with pytest.raises(HTTPException) as exc_info:
      self.manager.delete_table("test_graph", "invalid;DROP TABLE")
    assert exc_info.value.status_code == 400

  @patch("robosystems.graph_api.core.duckdb.manager.get_duckdb_pool")
  def test_delete_table_handles_failure(self, mock_get_pool):
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = Exception("Delete failed")
    mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
    mock_get_pool.return_value = mock_pool

    with pytest.raises(HTTPException) as exc_info:
      self.manager.delete_table("test_graph", "customers")
    assert exc_info.value.status_code == 500
    assert "Failed to delete table" in exc_info.value.detail


class TestTableRequestModels:
  def test_table_create_request_valid(self):
    request = TableCreateRequest(
      graph_id="test_graph",
      table_name="customers",
      s3_pattern="s3://bucket/*.parquet",
    )
    assert request.graph_id == "test_graph"
    assert request.table_name == "customers"
    assert request.s3_pattern == "s3://bucket/*.parquet"

  def test_table_create_request_forbids_extra_fields(self):
    with pytest.raises(Exception):
      TableCreateRequest(
        graph_id="test",
        table_name="test",
        s3_pattern="s3://bucket/*.parquet",
        extra_field="not_allowed",  # type: ignore[call-arg]
      )

  def test_table_query_request_valid(self):
    request = TableQueryRequest(graph_id="test_graph", sql="SELECT * FROM customers")
    assert request.graph_id == "test_graph"
    assert request.sql == "SELECT * FROM customers"

  def test_table_query_request_forbids_extra_fields(self):
    with pytest.raises(Exception):
      TableQueryRequest(graph_id="test", sql="SELECT *", extra_field="not_allowed")  # type: ignore[call-arg]

  def test_table_info_model(self):
    info = TableInfo(
      graph_id="test_graph",
      table_name="customers",
      row_count=100,
      size_bytes=1024,
      s3_location="s3://bucket/path",
    )
    assert info.graph_id == "test_graph"
    assert info.table_name == "customers"
    assert info.row_count == 100
    assert info.size_bytes == 1024
    assert info.s3_location == "s3://bucket/path"

  def test_table_create_response_model(self):
    response = TableCreateResponse(
      status="success",
      graph_id="test_graph",
      table_name="customers",
      execution_time_ms=125.5,
    )
    assert response.status == "success"
    assert response.graph_id == "test_graph"
    assert response.table_name == "customers"
    assert response.execution_time_ms == 125.5

  def test_table_query_response_model(self):
    response = TableQueryResponse(
      columns=["id", "name"],
      rows=[[1, "Alice"], [2, "Bob"]],
      row_count=2,
      execution_time_ms=50.0,
    )
    assert response.columns == ["id", "name"]
    assert response.rows == [[1, "Alice"], [2, "Bob"]]
    assert response.row_count == 2
    assert response.execution_time_ms == 50.0
