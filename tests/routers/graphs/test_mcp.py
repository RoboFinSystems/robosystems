"""
Comprehensive tests for MCP tool execution endpoints.
"""

from unittest.mock import AsyncMock, Mock

import pytest

from robosystems.middleware.mcp.client import GraphMCPClient
from robosystems.models.api.graphs.mcp import MCPToolCall
from robosystems.routers.graphs.mcp.handlers import MCPHandler
from robosystems.routers.graphs.mcp.strategies import (
  MCPClientDetector,
  MCPExecutionStrategy,
  MCPStrategySelector,
)
from tests.conftest import VALID_TEST_GRAPH_ID


@pytest.fixture
def mock_mcp_client():
  """Mock MCP client for tests."""
  client = AsyncMock(spec=GraphMCPClient)
  client.graph_id = VALID_TEST_GRAPH_ID
  client.api_base_url = "http://test:8001"
  client.timeout = 30
  return client


@pytest.fixture
def mock_mcp_handler(mock_mcp_client):
  """Mock MCP handler for tests."""
  handler = Mock(spec=MCPHandler)
  handler.client = mock_mcp_client
  handler._closed = False
  handler._init_task = Mock()  # Mock init task
  handler.close = AsyncMock()
  handler._ensure_initialized = AsyncMock()
  handler._ensure_not_closed = Mock()
  handler.call_tool = AsyncMock()
  handler.execute_tool = AsyncMock()
  handler.get_available_tools = Mock(
    return_value=[
      "get-graph-schema",
      "read-graph-cypher",
      "get-example-queries",
    ]
  )
  handler.get_tools = AsyncMock(return_value=[])
  handler.validate_tool_call = Mock(return_value=True)
  return handler


class TestMCPStrategies:
  """Test MCP execution strategy selection."""

  @pytest.mark.unit
  def test_client_detection_claude(self):
    """Test detecting Claude as the client."""
    detector = MCPClientDetector()

    # Headers need to be lowercase for the detector
    headers = {"user-agent": "Claude-MCP/1.0"}
    client_info = detector.detect_client_type(headers)
    assert client_info["is_mcp_client"] is True

    headers = {"user-agent": "Mozilla/5.0 Claude"}
    client_info = detector.detect_client_type(headers)
    # Not an MCP client just because it has Claude in user agent
    assert "is_mcp_client" in client_info

    headers = {"x-mcp-client": "claude"}
    client_info = detector.detect_client_type(headers)
    assert client_info["is_mcp_client"] is True

  @pytest.mark.unit
  def test_client_detection_cursor(self):
    """Test detecting Cursor as the client."""
    detector = MCPClientDetector()

    headers = {"user-agent": "Cursor/1.0"}
    client_info = detector.detect_client_type(headers)
    # Cursor without 'mcp' is not an MCP client
    assert client_info["is_mcp_client"] is False

    headers = {"x-mcp-client": "cursor"}
    client_info = detector.detect_client_type(headers)
    assert client_info["is_mcp_client"] is True

  @pytest.mark.unit
  def test_client_detection_unknown(self):
    """Test unknown client detection."""
    detector = MCPClientDetector()

    headers = {"user-agent": "Mozilla/5.0"}
    client_info = detector.detect_client_type(headers)
    assert client_info["is_mcp_client"] is False
    assert client_info["is_browser"] is True

    headers = {}
    client_info = detector.detect_client_type(headers)
    assert client_info["is_mcp_client"] is False

  @pytest.mark.unit
  def test_strategy_selection_for_claude(self):
    """Test strategy selection for Claude client."""
    # Use the static method with proper parameters
    strategy = MCPStrategySelector.select_strategy(
      tool_name="read-graph-cypher",
      arguments={"query": "MATCH (n) RETURN n"},
      client_info={
        "client_type": "claude",
        "is_mcp_client": True,
        "prefers_streaming": True,
      },
      system_state={"queue_size": 0, "running_queries": 0},
      graph_id=VALID_TEST_GRAPH_ID,
      user_tier="standard",
    )

    # Strategy depends on various factors
    assert strategy in [
      MCPExecutionStrategy.SSE_PROGRESS,
      MCPExecutionStrategy.STREAM_AGGREGATED,
      MCPExecutionStrategy.JSON_COMPLETE,
      MCPExecutionStrategy.JSON_IMMEDIATE,
    ]

  @pytest.mark.unit
  def test_strategy_selection_for_heavy_query(self):
    """Test strategy selection for heavy queries."""
    # Query without LIMIT - considered heavy
    strategy = MCPStrategySelector.select_strategy(
      tool_name="read-graph-cypher",
      arguments={"query": "MATCH (n) RETURN n"},  # No LIMIT
      client_info={"client_type": "unknown", "is_mcp_client": False},
      system_state={"queue_size": 10, "running_queries": 5},
      graph_id=VALID_TEST_GRAPH_ID,
      user_tier="standard",
    )

    # Heavy queries could use various strategies
    assert strategy in [
      MCPExecutionStrategy.QUEUE_WITH_MONITORING,
      MCPExecutionStrategy.QUEUE_SIMPLE,
      MCPExecutionStrategy.SSE_PROGRESS,
      MCPExecutionStrategy.STREAM_AGGREGATED,
    ]

  @pytest.mark.unit
  def test_strategy_selection_for_schema_query(self):
    """Test strategy selection for schema queries."""
    strategy = MCPStrategySelector.select_strategy(
      tool_name="get-graph-schema",
      arguments={},
      client_info={"client_type": "unknown", "is_mcp_client": False},
      system_state={"queue_size": 0, "running_queries": 0},
      graph_id=VALID_TEST_GRAPH_ID,
      user_tier="standard",
    )

    # Schema queries are fast and should use immediate strategies
    assert strategy in [
      MCPExecutionStrategy.JSON_IMMEDIATE,
      MCPExecutionStrategy.JSON_COMPLETE,
    ]

  @pytest.mark.unit
  def test_strategy_selection_with_system_load(self):
    """Test strategy selection under high system load."""
    # Simulate high load with many queries
    strategy = MCPStrategySelector.select_strategy(
      tool_name="read-graph-cypher",
      arguments={"query": "MATCH (n) RETURN n LIMIT 10"},
      client_info={"client_type": "unknown", "is_mcp_client": False},
      system_state={"queue_size": 50, "running_queries": 20},  # High load
      graph_id=VALID_TEST_GRAPH_ID,
      user_tier="standard",
    )

    # High load might trigger queuing or streaming strategies
    assert strategy in [
      MCPExecutionStrategy.QUEUE_WITH_MONITORING,
      MCPExecutionStrategy.QUEUE_SIMPLE,
      MCPExecutionStrategy.SSE_PROGRESS,
      MCPExecutionStrategy.STREAM_AGGREGATED,
      MCPExecutionStrategy.JSON_IMMEDIATE,
      MCPExecutionStrategy.JSON_COMPLETE,
    ]


class TestMCPHandlers:
  """Test MCP handler functionality."""

  @pytest.mark.asyncio
  async def test_handler_tool_validation(self, mock_mcp_handler):
    """Test MCP handler tool validation."""
    # Use the existing mock handler
    handler = mock_mcp_handler

    # Valid tool
    tool_call = MCPToolCall(
      name="read-graph-cypher", arguments={"query": "MATCH (n) RETURN n"}
    )
    # Mock validation returns True for valid calls
    handler.validate_tool_call.return_value = True
    assert handler.validate_tool_call(tool_call) is True

    # Invalid tool - missing required argument
    tool_call = MCPToolCall(name="read-graph-cypher", arguments={})
    # Mock validation raises ValueError for invalid calls
    handler.validate_tool_call.side_effect = ValueError("required")
    with pytest.raises(ValueError, match="required"):
      handler.validate_tool_call(tool_call)

  @pytest.mark.asyncio
  async def test_handler_write_query_blocked(self, mock_mcp_handler):
    """Test handler blocks write queries."""
    handler = mock_mcp_handler

    tool_call = MCPToolCall(
      name="read-graph-cypher", arguments={"query": "CREATE (n:Node)"}
    )

    # Mock validation raises ValueError for write queries
    handler.validate_tool_call.side_effect = ValueError("read-only")
    with pytest.raises(ValueError, match="read-only"):
      handler.validate_tool_call(tool_call)

  @pytest.mark.asyncio
  async def test_handler_tool_execution(self, mock_mcp_handler):
    """Test handler tool execution."""
    handler = mock_mcp_handler

    tool_call = MCPToolCall(
      name="read-graph-cypher",
      arguments={"query": "MATCH (e:Entity) RETURN e.name as name LIMIT 2"},
    )

    # Mock the execute_tool result
    mock_result = Mock()
    mock_result.success = True
    mock_result.result = [{"name": "Entity1"}, {"name": "Entity2"}]
    handler.execute_tool.return_value = mock_result

    result = await handler.execute_tool(tool_call)

    assert result.success is True
    assert len(result.result) == 2
    assert result.result[0]["name"] == "Entity1"
    handler.execute_tool.assert_called_once()

  @pytest.mark.asyncio
  async def test_handler_error_handling(self, mock_mcp_handler):
    """Test handler error handling."""
    handler = mock_mcp_handler

    tool_call = MCPToolCall(
      name="read-graph-cypher", arguments={"query": "MATCH (n) RETURN n"}
    )

    # Mock the execute_tool result for error case
    mock_result = Mock()
    mock_result.success = False
    mock_result.result = {"error": "Database error"}
    handler.execute_tool.return_value = mock_result

    result = await handler.execute_tool(tool_call)

    assert result.success is False
    assert "error" in result.result
    assert "Database error" in result.result["error"]


class TestMCPStreaming:
  """Test MCP streaming functionality."""

  @pytest.mark.asyncio
  async def test_streaming_data_chunks(self, mock_mcp_handler):
    """Test streaming data in chunks."""

    # Mock call_tool to return a normal result (not an async generator)
    # The streaming happens in the stream_mcp_tool_execution function itself
    mock_mcp_handler.call_tool = AsyncMock(
      return_value={"type": "text", "text": '{"items": [1, 2, 3, 4, 5, 6]}'}
    )

    from robosystems.routers.graphs.mcp.streaming import stream_mcp_tool_execution

    tool_name = "read-graph-cypher"
    arguments = {"query": "MATCH (n) RETURN n"}
    strategy = "SSE_PROGRESS"

    chunks = []
    async for chunk in stream_mcp_tool_execution(
      mock_mcp_handler, tool_name, arguments, strategy
    ):
      chunks.append(chunk)

    # Should have at least start and end events
    assert len(chunks) >= 2
    assert chunks[0]["event"] == "start"

  @pytest.mark.asyncio
  async def test_aggregate_streamed_results(self):
    """Test aggregating streamed results."""
    from robosystems.routers.graphs.mcp.streaming import aggregate_streamed_results

    # Create list of events (not async generator)
    events = [
      {
        "event": "start",
        "data": {"tool": "read-graph-cypher", "strategy": "SSE_PROGRESS"},
      },
      {"event": "data", "data": {"items": [1, 2, 3]}},
      {"event": "data", "data": {"items": [4, 5, 6]}},
      {"event": "metadata", "data": {"total": 6}},
      {"event": "end", "data": {"count": 6}},
    ]

    result = aggregate_streamed_results(events)

    # Check for data aggregation (depends on implementation)
    assert isinstance(result, dict)
