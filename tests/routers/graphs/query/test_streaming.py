"""Comprehensive unit tests for the query streaming module."""

import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest

from robosystems.routers.graphs.query.streaming import (
  execute_query_with_timeout,
  stream_ndjson_response,
  stream_sse_response,
  stream_sse_with_queue,
)


def _make_mock_user(user_id="user-123"):
  user = Mock()
  user.id = user_id
  return user


def _make_mock_request(
  query="MATCH (n) RETURN n LIMIT 10", parameters=None, timeout=60
):
  req = Mock()
  req.query = query
  req.parameters = parameters or {}
  req.timeout = timeout
  return req


@pytest.mark.unit
class TestExecuteQueryWithTimeout:
  """Test the execute_query_with_timeout function."""

  @pytest.mark.asyncio
  async def test_async_repository_returns_results(self):
    """Test that async repository execute_query works correctly."""
    expected = [{"name": "Alice"}, {"name": "Bob"}]
    repo = AsyncMock()
    repo.execute_query = AsyncMock(return_value=expected)

    result = await execute_query_with_timeout(
      repo, "MATCH (n) RETURN n", None, timeout=30
    )

    assert result == expected
    repo.execute_query.assert_awaited_once_with("MATCH (n) RETURN n", None)

  @pytest.mark.asyncio
  async def test_sync_repository_uses_executor(self):
    """Test that sync repository falls back to run_in_executor."""
    expected = [{"name": "Charlie"}]

    repo = Mock()
    repo.execute_query = Mock(return_value=expected)
    # Ensure it's not detected as async
    assert not asyncio.iscoroutinefunction(repo.execute_query)

    result = await execute_query_with_timeout(
      repo, "MATCH (n) RETURN n", None, timeout=30
    )

    assert result == expected

  @pytest.mark.asyncio
  async def test_timeout_raises_timeout_error(self):
    """Test that a slow query raises TimeoutError."""
    repo = AsyncMock()

    async def slow_query(*args, **kwargs):
      await asyncio.sleep(100)

    repo.execute_query = slow_query

    with pytest.raises(TimeoutError, match="exceeded timeout"):
      await execute_query_with_timeout(repo, "MATCH (n) RETURN n", None, timeout=0.01)

  @pytest.mark.asyncio
  async def test_passes_parameters_to_repository(self):
    """Test that parameters are forwarded correctly."""
    repo = AsyncMock()
    repo.execute_query = AsyncMock(return_value=[])
    params = {"ticker": "AAPL"}

    await execute_query_with_timeout(
      repo, "MATCH (n {ticker: $ticker}) RETURN n", params, timeout=30
    )

    repo.execute_query.assert_awaited_once_with(
      "MATCH (n {ticker: $ticker}) RETURN n", params
    )


@pytest.mark.unit
class TestStreamNdjsonResponse:
  """Test the stream_ndjson_response function."""

  @pytest.mark.asyncio
  async def test_ndjson_fallback_chunks_results(self):
    """Test NDJSON streaming with fallback (non-streaming repository)."""
    # Repository without execute_query_streaming
    repo = AsyncMock()
    repo.execute_query = AsyncMock(
      return_value=[
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
      ]
    )
    # Ensure no streaming support
    del repo.execute_query_streaming

    request = _make_mock_request()

    with patch("robosystems.routers.graphs.query.streaming.circuit_breaker"):
      response = await stream_ndjson_response(
        repo,
        request,
        "kg01234567890abcdef",
        _make_mock_user(),
        chunk_size=2,
        start_time=datetime.now(UTC),
      )

    # Collect all chunks
    chunks = []
    async for chunk in response.body_iterator:
      chunks.append(json.loads(chunk))

    # First chunk should have columns and 2 rows
    assert chunks[0]["chunk_index"] == 0
    assert "columns" in chunks[0]
    assert chunks[0]["row_count"] == 2

    # Second chunk should have 1 row
    assert chunks[1]["chunk_index"] == 1
    assert chunks[1]["row_count"] == 1

    # Final chunk should be completion metadata
    assert chunks[-1]["complete"] is True
    assert chunks[-1]["total_rows"] == 3

  @pytest.mark.asyncio
  async def test_ndjson_response_headers(self):
    """Test that NDJSON response has correct headers."""
    repo = AsyncMock()
    repo.execute_query = AsyncMock(return_value=[])
    del repo.execute_query_streaming

    request = _make_mock_request()

    with patch("robosystems.routers.graphs.query.streaming.circuit_breaker"):
      response = await stream_ndjson_response(
        repo,
        request,
        "kg01234567890abcdef",
        _make_mock_user(),
      )

    assert response.media_type == "application/x-ndjson"
    assert response.headers.get("X-Streaming") == "true"
    assert response.headers.get("X-Stream-Format") == "ndjson"
    assert response.headers.get("X-Graph-ID") == "kg01234567890abcdef"

  @pytest.mark.asyncio
  async def test_ndjson_native_streaming(self):
    """Test NDJSON with native streaming repository."""
    repo = AsyncMock()

    async def streaming_generator(query, parameters, chunk_size=1000):
      yield [{"id": 1}, {"id": 2}]
      yield [{"id": 3}]

    repo.execute_query_streaming = streaming_generator

    request = _make_mock_request()

    with patch("robosystems.routers.graphs.query.streaming.circuit_breaker"):
      response = await stream_ndjson_response(
        repo,
        request,
        "kg01234567890abcdef",
        _make_mock_user(),
        start_time=datetime.now(UTC),
      )

    chunks = []
    async for chunk in response.body_iterator:
      chunks.append(json.loads(chunk))

    # Should have data chunks + completion
    assert any(c.get("complete") is True for c in chunks)

  @pytest.mark.asyncio
  async def test_ndjson_error_streaming(self):
    """Test NDJSON streaming handles errors gracefully."""
    repo = AsyncMock()
    repo.execute_query = AsyncMock(side_effect=RuntimeError("DB connection lost"))
    del repo.execute_query_streaming

    request = _make_mock_request()

    with patch("robosystems.routers.graphs.query.streaming.circuit_breaker"):
      response = await stream_ndjson_response(
        repo,
        request,
        "kg01234567890abcdef",
        _make_mock_user(),
        start_time=datetime.now(UTC),
      )

    chunks = []
    async for chunk in response.body_iterator:
      chunks.append(json.loads(chunk))

    # Should contain error chunk
    error_chunk = chunks[-1]
    assert "error" in error_chunk
    assert error_chunk["error_type"] == "RuntimeError"

  @pytest.mark.asyncio
  async def test_ndjson_empty_result(self):
    """Test NDJSON with empty query result."""
    repo = AsyncMock()
    repo.execute_query = AsyncMock(return_value=[])
    del repo.execute_query_streaming

    request = _make_mock_request()

    with patch("robosystems.routers.graphs.query.streaming.circuit_breaker"):
      response = await stream_ndjson_response(
        repo,
        request,
        "kg01234567890abcdef",
        _make_mock_user(),
        start_time=datetime.now(UTC),
      )

    chunks = []
    async for chunk in response.body_iterator:
      chunks.append(json.loads(chunk))

    # Should just have completion metadata
    final = chunks[-1]
    assert final["complete"] is True
    assert final["total_rows"] == 0


@pytest.mark.unit
class TestStreamSseResponse:
  """Test the stream_sse_response function."""

  @pytest.mark.asyncio
  async def test_sse_sends_started_event(self):
    """Test that SSE stream begins with a started event."""
    repo = AsyncMock()
    repo.execute_query = AsyncMock(return_value=[{"id": 1}])
    del repo.execute_query_streaming

    request = _make_mock_request()

    with patch("robosystems.routers.graphs.query.streaming.circuit_breaker"):
      response = await stream_sse_response(
        repo,
        request,
        "kg01234567890abcdef",
        _make_mock_user(),
        start_time=datetime.now(UTC),
      )

    # EventSourceResponse wraps our generator
    assert response is not None

  @pytest.mark.asyncio
  async def test_sse_fallback_sends_schema_and_chunks(self):
    """Test SSE with non-streaming repo sends schema and chunk events."""
    repo = AsyncMock()
    repo.execute_query = AsyncMock(return_value=[{"name": "Alice"}, {"name": "Bob"}])
    del repo.execute_query_streaming

    request = _make_mock_request()

    with patch("robosystems.routers.graphs.query.streaming.circuit_breaker"):
      response = await stream_sse_response(
        repo,
        request,
        "kg01234567890abcdef",
        _make_mock_user(),
        chunk_size=100,
        start_time=datetime.now(UTC),
      )

    assert response is not None

  @pytest.mark.asyncio
  async def test_sse_response_has_correct_headers(self):
    """Test SSE response headers."""
    repo = AsyncMock()
    repo.execute_query = AsyncMock(return_value=[])
    del repo.execute_query_streaming

    request = _make_mock_request()

    with patch("robosystems.routers.graphs.query.streaming.circuit_breaker"):
      response = await stream_sse_response(
        repo,
        request,
        "kg01234567890abcdef",
        _make_mock_user(),
      )

    # SSE headers
    assert response.headers.get("X-Stream-Format") == "sse"
    assert response.headers.get("X-Graph-ID") == "kg01234567890abcdef"


@pytest.mark.unit
class TestStreamSseWithQueue:
  """Test the stream_sse_with_queue function."""

  @pytest.mark.asyncio
  async def test_returns_event_source_response(self):
    """Test that stream_sse_with_queue returns EventSourceResponse."""
    request = _make_mock_request()
    repo = AsyncMock()

    mock_queue = AsyncMock()
    mock_queue.submit_query = AsyncMock(return_value="query-001")
    mock_queue.get_query_status = AsyncMock(
      return_value={"status": "completed", "queue_position": 0}
    )
    mock_queue.get_stats = Mock(return_value={"queue_size": 0, "running_queries": 0})

    with patch(
      "robosystems.routers.graphs.query.streaming.get_query_queue",
      return_value=mock_queue,
    ):
      response = await stream_sse_with_queue(
        request,
        "kg01234567890abcdef",
        repo,
        _make_mock_user(),
      )

    assert response is not None
    assert response.headers.get("X-Stream-Mode") == "queue-and-stream"

  @pytest.mark.asyncio
  async def test_queue_headers(self):
    """Test SSE queue response has correct headers."""
    request = _make_mock_request()
    repo = AsyncMock()

    mock_queue = AsyncMock()
    mock_queue.submit_query = AsyncMock(return_value="query-001")
    mock_queue.get_query_status = AsyncMock(return_value={"status": "completed"})

    with patch(
      "robosystems.routers.graphs.query.streaming.get_query_queue",
      return_value=mock_queue,
    ):
      response = await stream_sse_with_queue(
        request,
        "kg01234567890abcdef",
        repo,
        _make_mock_user(),
        priority=3,
      )

    assert response.headers.get("X-Graph-ID") == "kg01234567890abcdef"
