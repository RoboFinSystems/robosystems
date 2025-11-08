"""Extended tests for async Graph API client - improving coverage."""

import json
import time
from unittest.mock import Mock, patch, AsyncMock
import pytest
import httpx

from robosystems.graph_api.client.client import GraphClient
from robosystems.graph_api.client.exceptions import (
  GraphAPIError,
  GraphTimeoutError,
  GraphTransientError,
  GraphSyntaxError,
  GraphClientError,
  GraphServerError,
)


class TestKuzuClientExtended:
  """Extended test cases for async GraphClient."""

  @pytest.fixture
  def mock_env(self):
    """Mock environment variables."""
    with patch("robosystems.config.env") as mock:
      mock.GRAPH_API_KEY = "test-api-key"
      mock.ENVIRONMENT = "test"
      yield mock

  @pytest.fixture
  async def client(self, mock_env):
    """Create a test client."""
    # mock_env ensures environment is properly mocked
    _ = mock_env  # Use the fixture to avoid unused warning
    client = GraphClient(base_url="http://localhost:8001")
    yield client
    await client.close()

  # Test health_check method
  @pytest.mark.asyncio
  async def test_health_check(self, client):
    """Test health check endpoint."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"status": "healthy", "version": "1.0.0"}

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.health_check()
      assert result == {"status": "healthy", "version": "1.0.0"}

  # Test query method (non-streaming)
  @pytest.mark.asyncio
  async def test_query_non_streaming(self, client):
    """Test non-streaming query execution."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.content = (
      b'{"data": [{"name": "test"}], "columns": ["name"], "row_count": 1}'
    )
    mock_response.headers = {"content-type": "application/json"}
    mock_response.json.return_value = {
      "data": [{"name": "test"}],
      "columns": ["name"],
      "row_count": 1,
    }

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.query("MATCH (n) RETURN n", graph_id="test_db")
      assert result["data"] == [{"name": "test"}]
      assert result["row_count"] == 1

  # Test query method with empty response
  @pytest.mark.asyncio
  async def test_query_empty_response(self, client):
    """Test query with empty response body."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.content = b""
    mock_response.headers = {"content-type": "application/json"}

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.query("MATCH (n) RETURN n", graph_id="test_db")
      assert result == {"data": [], "columns": [], "row_count": 0}

  # Test query method with invalid JSON
  @pytest.mark.asyncio
  async def test_query_invalid_json(self, client):
    """Test query with invalid JSON response."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.content = b"not valid json"
    mock_response.headers = {"content-type": "application/json"}
    mock_response.json.side_effect = json.JSONDecodeError("msg", "doc", 0)

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.query("MATCH (n) RETURN n", graph_id="test_db")
      assert "error" in result
      assert result["data"] == []

  # Test streaming query
  @pytest.mark.asyncio
  async def test_query_streaming(self, client):
    """Test streaming query execution."""
    # Mock the stream context manager
    mock_stream = AsyncMock()
    mock_response = AsyncMock()
    mock_response.status_code = 200

    # Mock aiter_lines to return NDJSON chunks
    async def mock_aiter_lines():
      yield '{"data": [{"id": 1}], "chunk": 1}'
      yield '{"data": [{"id": 2}], "chunk": 2}'
      yield '{"data": [{"id": 3}], "chunk": 3}'

    mock_response.aiter_lines = mock_aiter_lines
    mock_stream.__aenter__.return_value = mock_response
    mock_stream.__aexit__.return_value = None

    with patch.object(client.client, "stream", return_value=mock_stream):
      result_gen = await client.query(
        "MATCH (n) RETURN n", graph_id="test_db", streaming=True
      )

      # Collect streaming results
      chunks = []
      async for chunk in result_gen:
        chunks.append(chunk)

      assert len(chunks) == 3
      assert chunks[0]["chunk"] == 1
      assert chunks[2]["data"][0]["id"] == 3

  # Test streaming query with error
  @pytest.mark.asyncio
  async def test_query_streaming_error(self, client):
    """Test streaming query with server error."""
    mock_stream = AsyncMock()
    mock_response = AsyncMock()
    mock_response.status_code = 400
    mock_response.aread.return_value = b'{"detail": "Invalid query"}'

    mock_stream.__aenter__.return_value = mock_response
    mock_stream.__aexit__.return_value = None

    with patch.object(client.client, "stream", return_value=mock_stream):
      result_gen = await client.query("INVALID", graph_id="test_db", streaming=True)

      with pytest.raises(GraphClientError):  # 400 errors are client errors
        async for _ in result_gen:
          pass

  # Test get_info method
  @pytest.mark.asyncio
  async def test_get_info(self, client):
    """Test get_info endpoint."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"version": "1.0.0", "databases": 5}

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.get_info()
      assert result == {"version": "1.0.0", "databases": 5}

  # Test list_databases method
  @pytest.mark.asyncio
  async def test_list_databases(self, client):
    """Test list_databases endpoint."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {
      "databases": [
        {"id": "db1", "size": 100},
        {"id": "db2", "size": 200},
      ]
    }

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.list_databases()
      assert len(result["databases"]) == 2

  # Test get_database method
  @pytest.mark.asyncio
  async def test_get_database(self, client):
    """Test get_database endpoint."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "test_db", "status": "ready"}

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.get_database("test_db")
      assert result["id"] == "test_db"
      assert result["status"] == "ready"

  # Test create_database method
  @pytest.mark.asyncio
  async def test_create_database(self, client):
    """Test create_database endpoint."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 201
    mock_response.json.return_value = {
      "database": "new_db",
      "status": "created",
      "message": "Database created successfully",
    }

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.create_database(
        graph_id="new_db",
        schema_type="entity",
        repository_name="test_repo",
        is_subgraph=False,
      )
      assert result["database"] == "new_db"
      assert result["status"] == "created"

  # Test delete_database method
  @pytest.mark.asyncio
  async def test_delete_database(self, client):
    """Test delete_database endpoint."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"message": "Database deleted"}

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.delete_database("old_db")
      assert result["message"] == "Database deleted"

  # Test ingest method (sync mode)
  @pytest.mark.asyncio
  async def test_ingest_sync_mode(self, client):
    """Test sync mode ingestion."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {
      "task_id": "task-123",
      "status": "completed",
      "rows_imported": 100,
    }

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.ingest(
        graph_id="test_db",
        file_path="/tmp/data.csv",
        table_name="TestTable",
        mode="sync",
        ignore_errors=True,
      )
      assert result["task_id"] == "task-123"
      assert result["rows_imported"] == 100

  # Test ingest method (async mode)
  @pytest.mark.asyncio
  async def test_ingest_async_mode(self, client):
    """Test async mode ingestion."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {
      "task_id": "task-456",
      "status": "queued",
    }

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.ingest(
        graph_id="test_db",
        pipeline_run_id="pipeline-123",
        bucket="my-bucket",
        files=["file1.csv", "file2.csv"],
        mode="async",
        priority=7,
      )
      assert result["task_id"] == "task-456"
      assert result["status"] == "queued"

  # Test get_task_status method
  @pytest.mark.asyncio
  async def test_get_task_status(self, client):
    """Test get_task_status endpoint."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {
      "task_id": "task-789",
      "status": "running",
      "progress": 45,
    }

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.get_task_status("task-789")
      assert result["task_id"] == "task-789"
      assert result["progress"] == 45

  # Test list_tasks method
  @pytest.mark.asyncio
  async def test_list_tasks(self, client):
    """Test list_tasks endpoint."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {
      "tasks": [
        {"id": "task-1", "status": "completed"},
        {"id": "task-2", "status": "running"},
      ]
    }

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.list_tasks(status="all")
      assert len(result["tasks"]) == 2

  # Test cancel_task method
  @pytest.mark.asyncio
  async def test_cancel_task(self, client):
    """Test cancel_task endpoint."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"message": "Task cancelled"}

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.cancel_task("task-123")
      assert result["message"] == "Task cancelled"

  # Test get_queue_info method
  @pytest.mark.asyncio
  async def test_get_queue_info(self, client):
    """Test get_queue_info endpoint."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"pending": 5, "running": 2}

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.get_queue_info()
      assert result["pending"] == 5

  # Test execute_query method
  @pytest.mark.asyncio
  async def test_execute_query(self, client):
    """Test execute_query method."""
    with patch.object(client, "query") as mock_query:
      mock_query.return_value = {"data": [{"id": 1}], "columns": ["id"]}

      result = await client.execute_query("MATCH (n) RETURN n", params={"limit": 10})
      assert result[0]["id"] == 1

  # Test execute_single method
  @pytest.mark.asyncio
  async def test_execute_single(self, client):
    """Test execute_single method."""
    with patch.object(client, "execute_query") as mock_execute:
      mock_execute.return_value = [{"count": 42}]

      result = await client.execute_single("MATCH (n) RETURN count(n) as count")
      assert result == {"count": 42}

  # Test execute_single with no results
  @pytest.mark.asyncio
  async def test_execute_single_no_results(self, client):
    """Test execute_single with empty results."""
    with patch.object(client, "execute_query") as mock_execute:
      mock_execute.return_value = []

      result = await client.execute_single("MATCH (n:Missing) RETURN n")
      assert result is None

  # Test get_schema method
  @pytest.mark.asyncio
  async def test_get_schema(self, client):
    """Test get_schema method."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {
      "tables": [
        {"type": "NODE", "name": "Person"},
        {"type": "REL", "name": "KNOWS"},
      ]
    }

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.get_schema()
      assert len(result) == 2
      assert result[0]["name"] == "Person"

  # Test install_schema method
  @pytest.mark.asyncio
  async def test_install_schema(self, client):
    """Test install_schema endpoint."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {
      "status": "success",
      "message": "Schema installed",
      "nodes_created": 5,
      "relationships_created": 3,
    }

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.install_schema(
        graph_id="test_db",
        base_schema="base",
        extensions=["ext1", "ext2"],
        custom_ddl=None,
      )
      assert result["nodes_created"] == 5
      assert result["relationships_created"] == 3

  # Test export_database method
  @pytest.mark.asyncio
  async def test_export_database(self, client):
    """Test export_database endpoint."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.content = b"database export data"
    mock_response.headers = {"content-type": "application/octet-stream"}

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.export_database("test_db")
      assert result == b"database export data"

  # Test get_database_info method
  @pytest.mark.asyncio
  async def test_get_database_info(self, client):
    """Test get_database_info endpoint."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {
      "database": "test_db",
      "size": 1024000,
      "nodes": 1000,
      "relationships": 5000,
    }

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.get_database_info("test_db")
      assert result["nodes"] == 1000
      assert result["relationships"] == 5000

  # Test get_database_metrics method
  @pytest.mark.asyncio
  async def test_get_database_metrics(self, client):
    """Test get_database_metrics endpoint."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {
      "database": "test_db",
      "query_count": 100,
      "avg_query_time": 0.5,
    }

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.get_database_metrics("test_db")
      assert result["query_count"] == 100
      assert result["avg_query_time"] == 0.5

  # Test get_metrics method
  @pytest.mark.asyncio
  async def test_get_metrics(self, client):
    """Test get_metrics endpoint."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {
      "total_queries": 1000,
      "total_databases": 10,
      "uptime": 86400,
    }

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.get_metrics()
      assert result["total_queries"] == 1000
      assert result["uptime"] == 86400

  # Test error handling - 404
  @pytest.mark.asyncio
  async def test_handle_404_error(self, client):
    """Test handling of 404 Not Found errors."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 404
    mock_response.json.return_value = {"detail": "Database not found"}

    with patch.object(client.client, "request", return_value=mock_response):
      with pytest.raises(GraphClientError):  # 404 is a client error
        await client._request("GET", "/databases/missing")

  # Test error handling - 403
  @pytest.mark.asyncio
  async def test_handle_403_error(self, client):
    """Test handling of 403 Forbidden errors."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 403
    mock_response.json.return_value = {"detail": "Access denied"}

    with patch.object(client.client, "request", return_value=mock_response):
      with pytest.raises(GraphClientError):  # 403 is a client error
        await client._request("DELETE", "/databases/protected")

  # Test circuit breaker recovery
  @pytest.mark.asyncio
  async def test_circuit_breaker_recovery(self, client):
    """Test circuit breaker recovery after timeout."""
    client.config.circuit_breaker_timeout = 0.1  # 100ms for testing

    # Open circuit breaker
    client._circuit_breaker_open = True
    client._circuit_breaker_last_failure = time.time() - 1  # 1 second ago

    async def mock_func():
      return {"success": True}

    # Should work after timeout
    result = await client._execute_with_retry(mock_func)
    assert result == {"success": True}
    assert not client._circuit_breaker_open

  # Test various exception types
  @pytest.mark.asyncio
  async def test_graph_api_error_handling(self, client):
    """Test GraphAPIError base exception."""
    error = GraphAPIError(
      "API Error", status_code=400, response_data={"detail": "Bad request"}
    )
    assert str(error) == "API Error"
    assert error.status_code == 400
    assert error.response_data == {"detail": "Bad request"}

  @pytest.mark.asyncio
  async def test_kuzu_timeout_error_retry(self, client):
    """Test that GraphTimeoutError triggers retry."""
    attempt_count = 0

    async def mock_func():
      nonlocal attempt_count
      attempt_count += 1
      if attempt_count < 2:
        raise GraphTimeoutError("Operation timed out")
      return {"success": True}

    client.config.retry_delay = 0.01
    result = await client._execute_with_retry(mock_func)
    assert result == {"success": True}
    assert attempt_count == 2

  @pytest.mark.asyncio
  async def test_kuzu_transient_error_retry(self, client):
    """Test that GraphTransientError triggers retry."""
    attempt_count = 0

    async def mock_func():
      nonlocal attempt_count
      attempt_count += 1
      if attempt_count < 3:
        raise GraphTransientError("Service temporarily unavailable")
      return {"data": "success"}

    client.config.retry_delay = 0.01
    result = await client._execute_with_retry(mock_func)
    assert result == {"data": "success"}
    assert attempt_count == 3

  @pytest.mark.asyncio
  async def test_kuzu_syntax_error_no_retry(self, client):
    """Test that GraphSyntaxError does not trigger retry."""
    attempt_count = 0

    async def mock_func():
      nonlocal attempt_count
      attempt_count += 1
      raise GraphSyntaxError("Invalid Cypher syntax")

    with pytest.raises(GraphSyntaxError, match="Invalid Cypher syntax"):
      await client._execute_with_retry(mock_func)

    assert attempt_count == 1  # Should not retry

  @pytest.mark.asyncio
  async def test_kuzu_server_error_retry(self, client):
    """Test that GraphServerError triggers retry."""
    attempt_count = 0

    async def mock_func():
      nonlocal attempt_count
      attempt_count += 1
      if attempt_count < 2:
        raise GraphServerError("Internal server error", status_code=500)
      return {"status": "recovered"}

    client.config.retry_delay = 0.01
    result = await client._execute_with_retry(mock_func)
    assert result == {"status": "recovered"}
    assert attempt_count == 2

  @pytest.mark.asyncio
  async def test_handle_500_error(self, client):
    """Test handling of 500 Internal Server Error."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 500
    mock_response.json.return_value = {"detail": "Internal server error"}

    with patch.object(client.client, "request", return_value=mock_response):
      with pytest.raises(GraphServerError):
        await client._request("GET", "/databases/test")

  @pytest.mark.asyncio
  async def test_handle_422_error(self, client):
    """Test handling of 422 Unprocessable Entity errors."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 422
    mock_response.json.return_value = {"detail": "Validation error"}

    with patch.object(client.client, "request", return_value=mock_response):
      with pytest.raises(GraphClientError):
        await client._request("POST", "/databases/test/query")
