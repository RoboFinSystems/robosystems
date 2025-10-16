"""Tests for Kuzu SSE client."""

import asyncio
import json
import pytest
from unittest.mock import Mock, patch, AsyncMock

import httpx

from robosystems.graph_api.client.sse_client import (
  KuzuIngestionSSEClient,
  monitor_ingestion_sync,
)


class MockSSEEvent:
  """Mock SSE event."""

  def __init__(self, event: str, data: str):
    self.event = event
    self.data = data


class MockEventSource:
  """Mock SSE event source."""

  def __init__(self, events: list):
    self.events = events
    self.closed = False

  async def __aenter__(self):
    return self

  async def __aexit__(self, *args):
    self.closed = True

  async def aiter_sse(self):
    """Async iterator for SSE events."""
    for event in self.events:
      if self.closed:
        break
      yield event


@pytest.fixture
def sse_client():
  """Create SSE client instance."""
  return KuzuIngestionSSEClient(
    base_url="http://localhost:8001",
    timeout=300,  # 5 minutes for testing
  )


@pytest.fixture
def mock_httpx_client():
  """Create mock httpx async client."""
  client = AsyncMock(spec=httpx.AsyncClient)
  return client


class TestKuzuIngestionSSEClient:
  """Tests for KuzuIngestionSSEClient."""

  def test_initialization(self):
    """Test SSE client initialization."""
    client = KuzuIngestionSSEClient(base_url="http://localhost:8001/", timeout=7200)
    assert client.base_url == "http://localhost:8001"  # Trailing slash removed
    assert client.timeout == 7200

  def test_initialization_defaults(self):
    """Test SSE client with default timeout."""
    client = KuzuIngestionSSEClient(base_url="http://test.com")
    assert client.base_url == "http://test.com"
    assert client.timeout == 14400  # 4 hours default

  @pytest.mark.asyncio
  async def test_start_and_monitor_ingestion_success(self, sse_client):
    """Test successful ingestion with SSE monitoring."""
    mock_post_response = AsyncMock()
    mock_post_response.raise_for_status = Mock()
    # json() should be a regular method (not async) that returns the data
    mock_post_response.json = Mock(
      return_value={"task_id": "task-123", "sse_url": "/tasks/task-123/stream"}
    )

    # Create SSE events
    events = [
      MockSSEEvent("heartbeat", json.dumps({})),
      MockSSEEvent(
        "progress",
        json.dumps(
          {"progress_percent": 50, "records_processed": 500, "estimated_records": 1000}
        ),
      ),
      MockSSEEvent(
        "completed",
        json.dumps({"result": {"records_loaded": 1000}, "duration_seconds": 10.5}),
      ),
    ]

    with patch(
      "robosystems.graph_api.client.sse_client.httpx.AsyncClient"
    ) as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.__aexit__.return_value = None
      mock_client.post.return_value = mock_post_response
      mock_client_class.return_value = mock_client

      with patch(
        "robosystems.graph_api.client.sse_client.aconnect_sse"
      ) as mock_connect:
        mock_connect.return_value = MockEventSource(events)

        result = await sse_client.start_and_monitor_ingestion(
          graph_id="test-graph",
          table_name="test_table",
          s3_pattern="s3://bucket/path/*.parquet",
          s3_credentials={"access_key": "key", "secret_key": "secret"},
          ignore_errors=False,
        )

        # Debug the actual result
        if result["status"] != "completed":
          print(f"Result: {result}")

        assert result["status"] == "completed"
        assert result["task_id"] == "task-123"
        assert result["records_loaded"] == 1000
        assert result["duration_seconds"] == 10.5

        # Verify the POST call
        mock_client.post.assert_called_once_with(
          "http://localhost:8001/databases/test-graph/ingest/background",
          json={
            "s3_pattern": "s3://bucket/path/*.parquet",
            "table_name": "test_table",
            "s3_credentials": {"access_key": "key", "secret_key": "secret"},
            "ignore_errors": False,
          },
        )

  @pytest.mark.asyncio
  async def test_start_and_monitor_ingestion_http_error(self, sse_client):
    """Test handling of HTTP errors when starting ingestion."""
    mock_response = AsyncMock()
    mock_response.status_code = 400
    mock_response.text = "Bad request"

    with patch(
      "robosystems.graph_api.client.sse_client.httpx.AsyncClient"
    ) as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.__aexit__.return_value = None

      # Make post raise an HTTPStatusError
      mock_client.post.side_effect = httpx.HTTPStatusError(
        "Bad request", request=Mock(), response=mock_response
      )
      mock_client_class.return_value = mock_client

      result = await sse_client.start_and_monitor_ingestion(
        graph_id="test-graph",
        table_name="test_table",
        s3_pattern="s3://bucket/path/*.parquet",
      )

      assert result["status"] == "failed"
      assert "HTTP 400" in result["error"]
      assert "Bad request" in result["error"]

  @pytest.mark.asyncio
  async def test_start_and_monitor_ingestion_exception(self, sse_client):
    """Test handling of general exceptions."""
    with patch(
      "robosystems.graph_api.client.sse_client.httpx.AsyncClient"
    ) as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.__aexit__.return_value = None
      mock_client.post.side_effect = Exception("Connection error")
      mock_client_class.return_value = mock_client

      result = await sse_client.start_and_monitor_ingestion(
        graph_id="test-graph",
        table_name="test_table",
        s3_pattern="s3://bucket/path/*.parquet",
      )

      assert result["status"] == "failed"
      assert "Connection error" in result["error"]

  @pytest.mark.asyncio
  async def test_monitor_via_sse_heartbeat_handling(self, sse_client):
    """Test heartbeat event handling."""
    events = [
      MockSSEEvent("heartbeat", "{}"),
      MockSSEEvent("heartbeat", "{}"),
      MockSSEEvent(
        "completed",
        json.dumps({"result": {"records_loaded": 100}, "duration_seconds": 5.0}),
      ),
    ]

    with patch(
      "robosystems.graph_api.client.sse_client.httpx.AsyncClient"
    ) as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.__aexit__.return_value = None
      mock_client_class.return_value = mock_client

      with patch(
        "robosystems.graph_api.client.sse_client.aconnect_sse"
      ) as mock_connect:
        mock_connect.return_value = MockEventSource(events)

        result = await sse_client._monitor_via_sse(
          sse_url="http://localhost:8001/stream",
          task_id="task-123",
          table_name="test_table",
        )

        assert result["status"] == "completed"
        assert result["records_loaded"] == 100

  @pytest.mark.asyncio
  async def test_monitor_via_sse_progress_logging(self, sse_client):
    """Test progress event handling and logging."""
    events = [
      MockSSEEvent(
        "progress",
        json.dumps(
          {"progress_percent": 25, "records_processed": 250, "estimated_records": 1000}
        ),
      ),
      MockSSEEvent(
        "progress",
        json.dumps(
          {"progress_percent": 75, "records_processed": 750, "estimated_records": 1000}
        ),
      ),
      MockSSEEvent(
        "completed",
        json.dumps({"result": {"records_loaded": 1000}, "duration_seconds": 20.0}),
      ),
    ]

    with patch(
      "robosystems.graph_api.client.sse_client.httpx.AsyncClient"
    ) as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.__aexit__.return_value = None
      mock_client_class.return_value = mock_client

      with patch(
        "robosystems.graph_api.client.sse_client.aconnect_sse"
      ) as mock_connect:
        mock_connect.return_value = MockEventSource(events)

        with patch("robosystems.graph_api.client.sse_client.time.time") as mock_time:
          # Simulate time progression for progress logging
          mock_time.side_effect = [0, 0, 0, 0, 35, 35, 35, 70, 70, 70]

          result = await sse_client._monitor_via_sse(
            sse_url="http://localhost:8001/stream",
            task_id="task-123",
            table_name="test_table",
          )

          assert result["status"] == "completed"
          assert result["records_loaded"] == 1000

  @pytest.mark.asyncio
  async def test_monitor_via_sse_failed_event(self, sse_client):
    """Test handling of failed events."""
    events = [
      MockSSEEvent(
        "progress",
        json.dumps(
          {"progress_percent": 50, "records_processed": 500, "estimated_records": 1000}
        ),
      ),
      MockSSEEvent("failed", json.dumps({"error": "Database connection lost"})),
    ]

    with patch(
      "robosystems.graph_api.client.sse_client.httpx.AsyncClient"
    ) as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.__aexit__.return_value = None
      mock_client_class.return_value = mock_client

      with patch(
        "robosystems.graph_api.client.sse_client.aconnect_sse"
      ) as mock_connect:
        mock_connect.return_value = MockEventSource(events)

        result = await sse_client._monitor_via_sse(
          sse_url="http://localhost:8001/stream",
          task_id="task-123",
          table_name="test_table",
        )

        assert result["status"] == "failed"
        assert result["task_id"] == "task-123"
        assert "Database connection lost" in result["error"]

  @pytest.mark.asyncio
  async def test_monitor_via_sse_error_event(self, sse_client):
    """Test handling of error events."""
    events = [MockSSEEvent("error", json.dumps({"error": "Stream interrupted"}))]

    with patch(
      "robosystems.graph_api.client.sse_client.httpx.AsyncClient"
    ) as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.__aexit__.return_value = None
      mock_client_class.return_value = mock_client

      with patch(
        "robosystems.graph_api.client.sse_client.aconnect_sse"
      ) as mock_connect:
        mock_connect.return_value = MockEventSource(events)

        result = await sse_client._monitor_via_sse(
          sse_url="http://localhost:8001/stream",
          task_id="task-123",
          table_name="test_table",
        )

        assert result["status"] == "failed"
        assert "SSE stream error: Stream interrupted" in result["error"]

  @pytest.mark.asyncio
  async def test_monitor_via_sse_invalid_json(self, sse_client):
    """Test handling of invalid JSON in events."""
    events = [
      MockSSEEvent("progress", "invalid json{"),  # Invalid JSON
      MockSSEEvent(
        "completed",
        json.dumps({"result": {"records_loaded": 100}, "duration_seconds": 5.0}),
      ),
    ]

    with patch(
      "robosystems.graph_api.client.sse_client.httpx.AsyncClient"
    ) as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.__aexit__.return_value = None
      mock_client_class.return_value = mock_client

      with patch(
        "robosystems.graph_api.client.sse_client.aconnect_sse"
      ) as mock_connect:
        mock_connect.return_value = MockEventSource(events)

        result = await sse_client._monitor_via_sse(
          sse_url="http://localhost:8001/stream",
          task_id="task-123",
          table_name="test_table",
        )

        # Should skip invalid JSON and continue to completion
        assert result["status"] == "completed"
        assert result["records_loaded"] == 100

  @pytest.mark.asyncio
  async def test_monitor_via_sse_timeout(self, sse_client):
    """Test timeout handling."""
    # Create a client with short timeout
    short_timeout_client = KuzuIngestionSSEClient(
      base_url="http://localhost:8001",
      timeout=1,  # 1 second timeout
    )

    # Create events that never complete
    events = [
      MockSSEEvent("heartbeat", "{}"),
      MockSSEEvent(
        "progress",
        json.dumps(
          {"progress_percent": 50, "records_processed": 500, "estimated_records": 1000}
        ),
      ),
    ]

    with patch(
      "robosystems.graph_api.client.sse_client.httpx.AsyncClient"
    ) as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.__aexit__.return_value = None
      mock_client_class.return_value = mock_client

      with patch(
        "robosystems.graph_api.client.sse_client.aconnect_sse"
      ) as mock_connect:
        # Create an event source that yields events slowly
        async def slow_events():
          for event in events:
            yield event
            await asyncio.sleep(0.6)  # Slow enough to trigger timeout

        mock_event_source = AsyncMock()
        mock_event_source.__aenter__.return_value = mock_event_source
        mock_event_source.__aexit__.return_value = None
        mock_event_source.aiter_sse = slow_events
        mock_connect.return_value = mock_event_source

        with patch("robosystems.graph_api.client.sse_client.time.time") as mock_time:
          # Simulate time progression beyond timeout
          mock_time.side_effect = [0, 0.5, 1.5, 2.0]  # Exceeds 1 second timeout

          result = await short_timeout_client._monitor_via_sse(
            sse_url="http://localhost:8001/stream",
            task_id="task-123",
            table_name="test_table",
          )

          assert result["status"] == "failed"
          assert "Timeout after 1 seconds" in result["error"]

  @pytest.mark.asyncio
  async def test_monitor_via_sse_asyncio_timeout(self, sse_client):
    """Test handling of asyncio.TimeoutError."""
    with patch(
      "robosystems.graph_api.client.sse_client.httpx.AsyncClient"
    ) as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.__aexit__.return_value = None
      mock_client_class.return_value = mock_client

      with patch(
        "robosystems.graph_api.client.sse_client.aconnect_sse"
      ) as mock_connect:
        mock_connect.side_effect = asyncio.TimeoutError()

        result = await sse_client._monitor_via_sse(
          sse_url="http://localhost:8001/stream",
          task_id="task-123",
          table_name="test_table",
        )

        assert result["status"] == "failed"
        assert "SSE connection timeout" in result["error"]

  @pytest.mark.asyncio
  async def test_monitor_via_sse_unexpected_exception(self, sse_client):
    """Test handling of unexpected exceptions during monitoring."""
    with patch(
      "robosystems.graph_api.client.sse_client.httpx.AsyncClient"
    ) as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.__aexit__.return_value = None
      mock_client_class.return_value = mock_client

      with patch(
        "robosystems.graph_api.client.sse_client.aconnect_sse"
      ) as mock_connect:
        mock_connect.side_effect = RuntimeError("Unexpected error")

        result = await sse_client._monitor_via_sse(
          sse_url="http://localhost:8001/stream",
          task_id="task-123",
          table_name="test_table",
        )

        assert result["status"] == "failed"
        assert "Unexpected error" in result["error"]

  @pytest.mark.asyncio
  async def test_monitor_via_sse_stream_ends_unexpectedly(self, sse_client):
    """Test handling when SSE stream ends without completion."""
    # Empty events list - stream ends immediately
    events = []

    with patch(
      "robosystems.graph_api.client.sse_client.httpx.AsyncClient"
    ) as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.__aexit__.return_value = None
      mock_client_class.return_value = mock_client

      with patch(
        "robosystems.graph_api.client.sse_client.aconnect_sse"
      ) as mock_connect:
        mock_connect.return_value = MockEventSource(events)

        result = await sse_client._monitor_via_sse(
          sse_url="http://localhost:8001/stream",
          task_id="task-123",
          table_name="test_table",
        )

        assert result["status"] == "failed"
        assert "SSE stream ended unexpectedly" in result["error"]

  @pytest.mark.asyncio
  async def test_monitor_via_sse_completion_without_records(self, sse_client):
    """Test completion event when IGNORE_ERRORS is used (no record count)."""
    events = [
      MockSSEEvent(
        "completed",
        json.dumps(
          {
            "result": {},  # No records_loaded when IGNORE_ERRORS is used
            "duration_seconds": 15.0,
          }
        ),
      )
    ]

    with patch(
      "robosystems.graph_api.client.sse_client.httpx.AsyncClient"
    ) as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.__aexit__.return_value = None
      mock_client_class.return_value = mock_client

      with patch(
        "robosystems.graph_api.client.sse_client.aconnect_sse"
      ) as mock_connect:
        mock_connect.return_value = MockEventSource(events)

        result = await sse_client._monitor_via_sse(
          sse_url="http://localhost:8001/stream",
          task_id="task-123",
          table_name="test_table",
        )

        assert result["status"] == "completed"
        assert result["records_loaded"] == 0  # Default when not provided
        assert result["duration_seconds"] == 15.0


class TestMonitorIngestionSync:
  """Tests for synchronous wrapper function."""

  def test_monitor_ingestion_sync_success(self):
    """Test synchronous wrapper for successful ingestion."""
    expected_result = {
      "status": "completed",
      "task_id": "task-123",
      "records_loaded": 1000,
      "duration_seconds": 10.0,
    }

    with patch("robosystems.graph_api.client.sse_client.asyncio.run") as mock_run:
      mock_run.return_value = expected_result

      result = monitor_ingestion_sync(
        base_url="http://localhost:8001",
        graph_id="test-graph",
        table_name="test_table",
        s3_pattern="s3://bucket/path/*.parquet",
        s3_credentials={"key": "value"},
        ignore_errors=False,
        timeout=600,
      )

      assert result == expected_result
      mock_run.assert_called_once()

  def test_monitor_ingestion_sync_with_defaults(self):
    """Test synchronous wrapper with default parameters."""
    expected_result = {"status": "completed", "task_id": "task-456"}

    with patch("robosystems.graph_api.client.sse_client.asyncio.run") as mock_run:
      mock_run.return_value = expected_result

      result = monitor_ingestion_sync(
        base_url="http://localhost:8001",
        graph_id="test-graph",
        table_name="test_table",
        s3_pattern="s3://bucket/path/*.parquet",
      )

      assert result == expected_result

  def test_monitor_ingestion_sync_failure(self):
    """Test synchronous wrapper handling failures."""
    expected_result = {"status": "failed", "error": "Connection error"}

    with patch("robosystems.graph_api.client.sse_client.asyncio.run") as mock_run:
      mock_run.return_value = expected_result

      result = monitor_ingestion_sync(
        base_url="http://localhost:8001",
        graph_id="test-graph",
        table_name="test_table",
        s3_pattern="s3://bucket/path/*.parquet",
      )

      assert result["status"] == "failed"
      assert "Connection error" in result["error"]

  def test_monitor_ingestion_sync_asyncio_exception(self):
    """Test handling when asyncio.run raises an exception."""
    with patch("robosystems.graph_api.client.sse_client.asyncio.run") as mock_run:
      mock_run.side_effect = RuntimeError("Event loop error")

      with pytest.raises(RuntimeError, match="Event loop error"):
        monitor_ingestion_sync(
          base_url="http://localhost:8001",
          graph_id="test-graph",
          table_name="test_table",
          s3_pattern="s3://bucket/path/*.parquet",
        )
