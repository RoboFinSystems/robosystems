"""Comprehensive unit tests for the MCP streaming module."""

import json
from unittest.mock import AsyncMock

import pytest

from robosystems.routers.graphs.mcp.streaming import (
  aggregate_streamed_results,
  stream_cypher_query,
  stream_mcp_tool_execution,
  stream_schema_retrieval,
)


async def _collect_events(async_gen):
  """Helper to collect all events from an async generator."""
  events = []
  async for event in async_gen:
    events.append(event)
  return events


@pytest.mark.unit
class TestStreamMcpToolExecution:
  """Test the stream_mcp_tool_execution router."""

  @pytest.mark.asyncio
  async def test_cypher_tool_routes_to_stream_cypher_query(self):
    """Test that cypher tools route to stream_cypher_query."""
    handler = AsyncMock()
    handler.call_tool = AsyncMock(return_value={"type": "text", "text": "[]"})

    # Use a non-streaming handler (no execute_query_streaming)
    del handler.execute_query_streaming

    events = await _collect_events(
      stream_mcp_tool_execution(
        handler, "read-graph-cypher", {"query": "MATCH (n) RETURN n"}, "json_immediate"
      )
    )

    event_types = [e["event"] for e in events]
    assert "start" in event_types
    assert "complete" in event_types

  @pytest.mark.asyncio
  async def test_schema_tool_routes_to_stream_schema(self):
    """Test that schema tools route to stream_schema_retrieval."""
    handler = AsyncMock()
    handler.call_tool = AsyncMock(return_value={"type": "text", "text": "[]"})

    events = await _collect_events(
      stream_mcp_tool_execution(handler, "get-graph-schema", {}, "json_complete")
    )

    event_types = [e["event"] for e in events]
    assert "start" in event_types
    assert "progress" in event_types
    assert "complete" in event_types

  @pytest.mark.asyncio
  async def test_generic_tool_executes_directly(self):
    """Test that unknown tools execute directly with progress."""
    handler = AsyncMock()
    handler.call_tool = AsyncMock(return_value={"type": "text", "text": "info"})

    events = await _collect_events(
      stream_mcp_tool_execution(handler, "get-graph-info", {}, "json_immediate")
    )

    event_types = [e["event"] for e in events]
    assert "start" in event_types
    assert "progress" in event_types
    assert "result" in event_types
    assert "complete" in event_types

  @pytest.mark.asyncio
  async def test_start_event_has_tool_and_strategy(self):
    """Test that start event contains tool name and strategy."""
    handler = AsyncMock()
    handler.call_tool = AsyncMock(return_value={"data": "test"})

    events = await _collect_events(
      stream_mcp_tool_execution(handler, "get-graph-info", {}, "json_immediate")
    )

    start_event = next(e for e in events if e["event"] == "start")
    assert start_event["data"]["tool"] == "get-graph-info"
    assert start_event["data"]["strategy"] == "json_immediate"

  @pytest.mark.asyncio
  async def test_complete_event_has_execution_time(self):
    """Test that completion event includes execution time."""
    handler = AsyncMock()
    handler.call_tool = AsyncMock(return_value={"data": "test"})

    events = await _collect_events(
      stream_mcp_tool_execution(handler, "get-graph-info", {}, "json_immediate")
    )

    complete_event = next(e for e in events if e["event"] == "complete")
    assert "execution_time_seconds" in complete_event["data"]
    assert complete_event["data"]["tool"] == "get-graph-info"

  @pytest.mark.asyncio
  async def test_timeout_error_yields_error_event(self):
    """Test that TimeoutError produces error event."""
    handler = AsyncMock()
    handler.call_tool = AsyncMock(side_effect=TimeoutError("timed out"))

    events = await _collect_events(
      stream_mcp_tool_execution(handler, "get-graph-info", {}, "json_immediate")
    )

    error_event = next(e for e in events if e["event"] == "error")
    assert "timeout" in error_event["data"]["error"].lower()

  @pytest.mark.asyncio
  async def test_general_exception_yields_error_event(self):
    """Test that general exceptions produce error event."""
    handler = AsyncMock()
    handler.call_tool = AsyncMock(side_effect=RuntimeError("connection lost"))

    events = await _collect_events(
      stream_mcp_tool_execution(handler, "get-graph-info", {}, "json_immediate")
    )

    error_event = next(e for e in events if e["event"] == "error")
    assert error_event["data"]["error"] == "connection lost"
    assert error_event["data"]["error_type"] == "RuntimeError"


@pytest.mark.unit
class TestStreamCypherQuery:
  """Test the stream_cypher_query function."""

  @pytest.mark.asyncio
  async def test_non_streaming_handler_uses_call_tool(self):
    """Test fallback to call_tool when no streaming support."""
    handler = AsyncMock()
    handler.call_tool = AsyncMock(return_value={"text": '[{"id": 1}, {"id": 2}]'})
    del handler.execute_query_streaming

    events = await _collect_events(
      stream_cypher_query(handler, {"query": "MATCH (n) RETURN n"})
    )

    event_types = [e["event"] for e in events]
    assert "progress" in event_types
    assert "query_result" in event_types

  @pytest.mark.asyncio
  async def test_non_streaming_parses_json_result(self):
    """Test that JSON results are parsed from text format."""
    handler = AsyncMock()
    handler.call_tool = AsyncMock(
      return_value={"text": '[{"name": "Alice"}, {"name": "Bob"}]'}
    )
    del handler.execute_query_streaming

    events = await _collect_events(
      stream_cypher_query(handler, {"query": "MATCH (n) RETURN n"})
    )

    result_event = next(e for e in events if e["event"] == "query_result")
    assert result_event["data"]["row_count"] == 2

  @pytest.mark.asyncio
  async def test_non_streaming_handles_json_decode_error(self):
    """Test graceful handling of non-JSON text result."""
    handler = AsyncMock()
    handler.call_tool = AsyncMock(return_value={"text": "Not valid JSON"})
    del handler.execute_query_streaming

    events = await _collect_events(
      stream_cypher_query(handler, {"query": "MATCH (n) RETURN n"})
    )

    result_event = next(e for e in events if e["event"] == "query_result")
    assert result_event["data"]["row_count"] == 0
    assert result_event["data"]["result"] == "Not valid JSON"

  @pytest.mark.asyncio
  async def test_non_streaming_handles_non_dict_result(self):
    """Test handling when result is not a dict with 'text' key."""
    handler = AsyncMock()
    handler.call_tool = AsyncMock(return_value="raw string")
    del handler.execute_query_streaming

    events = await _collect_events(
      stream_cypher_query(handler, {"query": "MATCH (n) RETURN n"})
    )

    result_event = next(e for e in events if e["event"] == "query_result")
    assert result_event["data"]["row_count"] == 0

  @pytest.mark.asyncio
  async def test_streaming_handler_yields_chunks(self):
    """Test streaming handler produces query_chunk events."""
    handler = AsyncMock()

    async def streaming_gen(query, parameters, chunk_size=1000):
      yield {"data": [{"id": 1}, {"id": 2}], "columns": ["id"]}
      yield {"data": [{"id": 3}], "columns": ["id"]}

    handler.execute_query_streaming = streaming_gen

    events = await _collect_events(
      stream_cypher_query(handler, {"query": "MATCH (n) RETURN n"})
    )

    chunk_events = [e for e in events if e["event"] == "query_chunk"]
    assert len(chunk_events) == 2
    assert chunk_events[0]["data"]["rows_in_chunk"] == 2
    assert chunk_events[1]["data"]["rows_in_chunk"] == 1

  @pytest.mark.asyncio
  async def test_streaming_sends_query_complete_summary(self):
    """Test streaming sends final summary event."""
    handler = AsyncMock()

    async def streaming_gen(query, parameters, chunk_size=1000):
      yield {"data": [{"id": 1}], "columns": ["id"]}

    handler.execute_query_streaming = streaming_gen

    events = await _collect_events(
      stream_cypher_query(handler, {"query": "MATCH (n) RETURN n"})
    )

    complete_event = next(e for e in events if e["event"] == "query_complete")
    assert complete_event["data"]["total_rows"] == 1
    assert complete_event["data"]["total_chunks"] == 1
    assert complete_event["data"]["columns"] == ["id"]


@pytest.mark.unit
class TestStreamSchemaRetrieval:
  """Test the stream_schema_retrieval function."""

  @pytest.mark.asyncio
  async def test_list_schema_splits_nodes_and_relationships(self):
    """Test that list-format schema is split into node and relationship events."""
    handler = AsyncMock()
    schema_data = [
      {"label": "Entity", "category": "Node Tables"},
      {"label": "Report", "category": "Node Tables"},
      {"label": "FILED", "category": "Relationship Tables"},
    ]
    handler.call_tool = AsyncMock(return_value={"text": json.dumps(schema_data)})

    events = await _collect_events(
      stream_schema_retrieval(handler, "get-graph-schema", {})
    )

    event_types = [e["event"] for e in events]
    assert "schema_nodes" in event_types
    assert "schema_relationships" in event_types

    nodes_event = next(e for e in events if e["event"] == "schema_nodes")
    assert nodes_event["data"]["node_count"] == 2

    rels_event = next(e for e in events if e["event"] == "schema_relationships")
    assert rels_event["data"]["relationship_count"] == 1

  @pytest.mark.asyncio
  async def test_dict_schema_yields_schema_complete(self):
    """Test that dict-format schema yields schema_complete event."""
    handler = AsyncMock()
    handler.call_tool = AsyncMock(
      return_value={"text": json.dumps({"nodes": ["Entity"], "rels": ["FILED"]})}
    )

    events = await _collect_events(
      stream_schema_retrieval(handler, "get-graph-schema", {})
    )

    event_types = [e["event"] for e in events]
    assert "schema_complete" in event_types

  @pytest.mark.asyncio
  async def test_invalid_json_schema_yields_raw(self):
    """Test that non-JSON schema text yields raw schema."""
    handler = AsyncMock()
    handler.call_tool = AsyncMock(return_value={"text": "not valid json"})

    events = await _collect_events(
      stream_schema_retrieval(handler, "get-graph-schema", {})
    )

    event_types = [e["event"] for e in events]
    assert "schema_complete" in event_types

  @pytest.mark.asyncio
  async def test_non_dict_result_uses_schema_directly(self):
    """Test that non-dict result is used directly."""
    handler = AsyncMock()
    handler.call_tool = AsyncMock(return_value=[{"label": "Test"}])

    events = await _collect_events(
      stream_schema_retrieval(handler, "get-graph-schema", {})
    )

    # Should yield schema_complete since it's not a dict with "text" key
    # but it IS a list, so schema_nodes path
    event_types = [e["event"] for e in events]
    assert "progress" in event_types

  @pytest.mark.asyncio
  async def test_progress_events_emitted(self):
    """Test progress events are emitted during schema retrieval."""
    handler = AsyncMock()
    handler.call_tool = AsyncMock(return_value={"text": "[]"})

    events = await _collect_events(
      stream_schema_retrieval(handler, "get-graph-schema", {})
    )

    progress_events = [e for e in events if e["event"] == "progress"]
    assert len(progress_events) >= 2  # Start and end progress


@pytest.mark.unit
class TestAggregateStreamedResults:
  """Test the aggregate_streamed_results function."""

  def test_aggregates_query_chunks(self):
    """Test aggregation of query_chunk events."""
    events = [
      {"event": "start", "data": {"tool": "read-graph-cypher"}},
      {
        "event": "query_chunk",
        "data": {
          "columns": ["id", "name"],
          "data": [{"id": 1, "name": "Alice"}],
          "chunk_number": 1,
          "rows_in_chunk": 1,
          "total_rows_so_far": 1,
        },
      },
      {
        "event": "query_chunk",
        "data": {
          "data": [{"id": 2, "name": "Bob"}],
          "chunk_number": 2,
          "rows_in_chunk": 1,
          "total_rows_so_far": 2,
        },
      },
      {"event": "complete", "data": {"tool": "read-graph-cypher"}},
    ]

    result = aggregate_streamed_results(events)

    assert result["success"] is True
    assert result["tool"] == "read-graph-cypher"
    assert result["result"]["columns"] == ["id", "name"]
    assert len(result["result"]["data"]) == 2
    assert result["result"]["row_count"] == 2

  def test_aggregates_schema_parts(self):
    """Test aggregation of schema_nodes and schema_relationships events."""
    events = [
      {"event": "start", "data": {"tool": "get-graph-schema"}},
      {
        "event": "schema_nodes",
        "data": {"node_count": 2, "node_tables": [{"label": "A"}, {"label": "B"}]},
      },
      {
        "event": "schema_relationships",
        "data": {"relationship_count": 1, "relationship_tables": [{"label": "REL"}]},
      },
      {"event": "complete", "data": {"tool": "get-graph-schema"}},
    ]

    result = aggregate_streamed_results(events)

    assert result["success"] is True
    assert result["tool"] == "get-graph-schema"
    assert len(result["result"]["node_tables"]) == 2
    assert len(result["result"]["relationship_tables"]) == 1

  def test_returns_error_on_error_event(self):
    """Test that error events produce error result."""
    events = [
      {"event": "start", "data": {"tool": "read-graph-cypher"}},
      {
        "event": "error",
        "data": {"error": "Execution failed", "tool": "read-graph-cypher"},
      },
    ]

    result = aggregate_streamed_results(events)

    assert result["success"] is False
    assert result["error"] == "Execution failed"

  def test_aggregates_simple_result_event(self):
    """Test aggregation of simple result event (generic tools)."""
    events = [
      {"event": "start", "data": {"tool": "get-graph-info"}},
      {"event": "result", "data": {"result": {"graph_id": "kg123", "nodes": 100}}},
      {"event": "complete", "data": {"tool": "get-graph-info"}},
    ]

    result = aggregate_streamed_results(events)

    assert result["success"] is True
    assert result["tool"] == "get-graph-info"
    assert result["result"]["graph_id"] == "kg123"

  def test_no_recognizable_pattern_returns_failure(self):
    """Test that unrecognizable event patterns return failure."""
    events = [
      {"event": "start", "data": {"tool": "unknown-tool"}},
      {"event": "progress", "data": {"message": "doing stuff"}},
      {"event": "complete", "data": {"tool": "unknown-tool"}},
    ]

    result = aggregate_streamed_results(events)

    assert result["success"] is False
    assert "Unable to aggregate" in result["error"]

  def test_empty_events_returns_failure(self):
    """Test that empty events list returns failure."""
    result = aggregate_streamed_results([])

    assert result["success"] is False
    assert result["tool"] is None

  def test_tool_name_extracted_from_start_event(self):
    """Test that tool name is correctly extracted from start event."""
    events = [
      {"event": "start", "data": {"tool": "my-custom-tool"}},
      {"event": "result", "data": {"result": "data"}},
    ]

    result = aggregate_streamed_results(events)

    assert result["tool"] == "my-custom-tool"
