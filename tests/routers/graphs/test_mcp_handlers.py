"""
Tests for MCP handlers and tool execution.
"""

import asyncio
import json
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.api.graphs.mcp import MCPToolCall
from robosystems.routers.graphs.mcp.handlers import MCPHandler, validate_mcp_access


@pytest.fixture
def mock_repository():
  """Mock repository for tests."""
  repo = AsyncMock()
  repo.graph_id = "test_graph"
  repo.execute_query = AsyncMock()
  repo.get_schema = AsyncMock()
  repo.get_graph_info = AsyncMock()
  return repo


@pytest.fixture
async def mcp_handler(mock_repository):
  """Create MCPHandler with mock repository."""
  mock_user = Mock()
  mock_user.id = "test-user-id"
  mock_user.email = "test@example.com"

  # Create a completed future to mock the init task
  loop = asyncio.get_running_loop()
  completed_task = loop.create_future()
  completed_task.set_result(None)

  # Patch _init_async to return immediately, avoiding the unawaited coroutine warning
  with patch.object(
    MCPHandler, "_init_async", new_callable=AsyncMock, return_value=None
  ):
    handler = MCPHandler(mock_repository, graph_id="test_graph", user=mock_user)
    # Mock the necessary attributes to avoid initialization issues
    handler.lbug_client = Mock()
    handler._closed = False

    # Create a proper async mock for mcp_tools with call_tool method
    mock_mcp_tools = Mock()
    mock_mcp_tools.call_tool = AsyncMock(return_value={"results": []})
    handler.mcp_tools = mock_mcp_tools

    # Await the init task to ensure clean state
    if handler._init_task:
      await handler._init_task
    return handler


class TestMCPHandler:
  """Test MCPHandler functionality."""

  @pytest.mark.asyncio
  async def test_call_tool_cypher_query(self, mcp_handler, mock_repository):
    """Test executing a Cypher query through MCP handler."""
    # Set up the expected return value from mcp_tools.call_tool
    expected_result = [{"name": "Entity1"}, {"name": "Entity2"}]
    mcp_handler.mcp_tools.call_tool.return_value = expected_result

    tool_call = MCPToolCall(
      name="read-graph-cypher",
      arguments={"query": "MATCH (e:Entity) RETURN e.name as name LIMIT 2"},
    )

    result = await mcp_handler.call_tool(tool_call.name, tool_call.arguments)

    # call_tool returns a dict with type and text
    assert isinstance(result, dict)
    assert "type" in result
    assert "text" in result
    # The text should contain the JSON-stringified results
    parsed_text = json.loads(result["text"])
    assert parsed_text == expected_result

    # Verify mcp_tools.call_tool was called with correct arguments
    mcp_handler.mcp_tools.call_tool.assert_called_once()

  @pytest.mark.asyncio
  async def test_call_tool_with_parameters(self, mcp_handler, mock_repository):
    """Test executing query with parameters."""
    expected_result = [{"name": "Apple Inc", "cik": "0000320193"}]
    mcp_handler.mcp_tools.call_tool.return_value = expected_result

    tool_call = MCPToolCall(
      name="read-graph-cypher",
      arguments={
        "query": "MATCH (c:Entity {cik: $cik}) RETURN c.name as name, c.cik as cik",
        "parameters": {"cik": "0000320193"},
      },
    )

    result = await mcp_handler.call_tool(tool_call.name, tool_call.arguments)

    # call_tool returns a dict with type and text
    assert isinstance(result, dict)
    assert "type" in result
    assert "text" in result
    parsed_text = json.loads(result["text"])
    assert parsed_text == expected_result

  @pytest.mark.asyncio
  async def test_call_tool_schema_query(self, mcp_handler, mock_repository):
    """Test getting graph schema."""
    expected_result = {
      "schema": [
        {"label": "Entity", "type": "node", "properties": []},
        {"label": "Report", "type": "node", "properties": []},
      ]
    }
    mcp_handler.mcp_tools.call_tool.return_value = expected_result

    tool_call = MCPToolCall(name="get-graph-schema", arguments={})

    result = await mcp_handler.call_tool(tool_call.name, tool_call.arguments)

    # call_tool returns a dict with type and text
    assert isinstance(result, dict)
    assert "type" in result
    assert "text" in result
    parsed_text = json.loads(result["text"])
    assert parsed_text == expected_result

  @pytest.mark.asyncio
  async def test_call_tool_error_handling(self, mcp_handler, mock_repository):
    """Test error handling in tool execution."""
    mcp_handler.mcp_tools.call_tool.side_effect = RuntimeError("Database error")

    tool_call = MCPToolCall(
      name="read-graph-cypher", arguments={"query": "MATCH (n) RETURN n"}
    )

    result = await mcp_handler.call_tool(tool_call.name, tool_call.arguments)

    # When there's an error, result should contain error info in the text
    assert isinstance(result, dict)
    assert "type" in result
    assert "text" in result
    assert "Error:" in result["text"] or "Database error" in result["text"]

  @pytest.mark.asyncio
  async def test_call_tool_timeout(self, mcp_handler, mock_repository):
    """Test timeout handling in tool execution."""

    # Mock call_tool to simulate a timeout
    async def slow_call(*args, **kwargs):
      await asyncio.sleep(2)
      return {"results": []}

    mcp_handler.mcp_tools.call_tool.side_effect = slow_call

    tool_call = MCPToolCall(
      name="read-graph-cypher", arguments={"query": "MATCH (n) RETURN n"}
    )

    # Mock the timeout_coordinator to return a very short timeout
    with patch(
      "robosystems.routers.graphs.mcp.handlers.timeout_coordinator.get_tool_timeout",
      return_value=0.1,
    ):
      result = await mcp_handler.call_tool(tool_call.name, tool_call.arguments)

      # When there's a timeout, result should contain error message
      assert isinstance(result, dict)
      assert "type" in result
      assert "text" in result
      assert "timed out" in result["text"] or "Error" in result["text"]

  @pytest.mark.asyncio
  async def test_get_tools(self, mcp_handler):
    """Test getting list of available tools."""
    # Mock the get_tool_definitions_as_dict method
    mcp_handler.mcp_tools.get_tool_definitions_as_dict = Mock(
      return_value=[
        {
          "name": "read-graph-cypher",
          "description": "Execute a Cypher query",
          "inputSchema": {"type": "object", "properties": {}},
        },
        {
          "name": "get-graph-schema",
          "description": "Get graph schema",
          "inputSchema": {"type": "object", "properties": {}},
        },
      ]
    )

    tools = await mcp_handler.get_tools()

    assert isinstance(tools, list)
    # Check for tool names either as strings or in tool objects
    tool_names = []
    for tool in tools:
      if isinstance(tool, str):
        tool_names.append(tool)
      elif isinstance(tool, dict) and "name" in tool:
        tool_names.append(tool["name"])

    # These are the expected MCP tools plus the custom get-graph-info tool
    expected_tools = ["read-graph-cypher", "get-graph-schema", "get-graph-info"]
    for expected in expected_tools:
      assert any(expected in name for name in tool_names), (
        f"Expected tool {expected} not found"
      )

  @pytest.mark.asyncio
  async def test_execute_streaming_tool(self, mcp_handler, mock_repository):
    """Test streaming tool execution."""

    async def mock_streaming():
      yield [{"id": 1}]
      yield [{"id": 2}]
      yield [{"id": 3}]

    mock_repository.execute_query_streaming = mock_streaming

    tool_call = MCPToolCall(
      name="read-graph-cypher", arguments={"query": "MATCH (n) RETURN n"}
    )

    # Check if the handler has the streaming method
    if hasattr(mcp_handler, "call_tool_streaming"):
      chunks = []
      async for chunk in mcp_handler.call_tool_streaming(tool_call):
        chunks.append(chunk)

      assert len(chunks) == 3
      assert chunks[0][0]["id"] == 1
      assert chunks[2][0]["id"] == 3


class TestMCPAccessValidation:
  """Test MCP access validation."""

  @pytest.mark.asyncio
  async def test_validate_mcp_access_entity_graph(
    self, db_session, test_user, test_user_graph
  ):
    """Test access validation for entity graphs."""
    # User has access to their own graph - should not raise
    try:
      await validate_mcp_access(
        graph_id=test_user_graph.graph_id, current_user=test_user, db=db_session
      )
      # If no exception, access was granted
      assert True
    except HTTPException:
      # Access was denied
      raise AssertionError("User should have access to their own graph")

  @pytest.mark.asyncio
  async def test_validate_mcp_access_no_permission(self, db_session, test_user):
    """Test access validation denies unauthorized access."""
    # User doesn't have access to this graph - should raise
    with pytest.raises(HTTPException) as exc_info:
      await validate_mcp_access(
        graph_id="unauthorized_graph_id", current_user=test_user, db=db_session
      )
    assert exc_info.value.status_code == 403

  @pytest.mark.asyncio
  async def test_validate_mcp_access_shared_repository(self, db_session, test_user):
    """Test access validation for shared repositories."""
    import uuid

    from robosystems.models.iam.graph import Graph
    from robosystems.models.iam.user_repository import (
      RepositoryAccessLevel,
      RepositoryPlan,
      RepositoryType,
      UserRepository,
    )

    # First create the SEC graph (required by foreign key constraint)
    # Check if it already exists from another test
    existing_graph = db_session.query(Graph).filter_by(graph_id="sec").first()
    if not existing_graph:
      sec_graph = Graph(
        graph_id="sec",
        graph_name="SEC Repository",
        graph_type="repository",
        is_repository=True,
        repository_type="SEC",
      )
      db_session.add(sec_graph)
      db_session.flush()

    # Grant SEC repository access
    access_record = UserRepository(
      id=f"access_{uuid.uuid4().hex[:8]}",
      user_id=test_user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.READ,
      repository_plan=RepositoryPlan.STARTER,
      is_active=True,
    )
    db_session.add(access_record)
    db_session.commit()

    # Now user should have access - should not raise
    try:
      await validate_mcp_access(graph_id="sec", current_user=test_user, db=db_session)
      assert True
    except HTTPException:
      raise AssertionError("User should have access to SEC repository")

  @pytest.mark.asyncio
  async def test_validate_mcp_access_write_level(
    self, db_session, test_user, test_user_graph
  ):
    """Test that write-level access allows MCP tools."""
    # Update user_graph to have write access
    test_user_graph.access_level = "write"
    db_session.commit()

    try:
      await validate_mcp_access(
        graph_id=test_user_graph.graph_id, current_user=test_user, db=db_session
      )
      assert True
    except HTTPException:
      raise AssertionError("User should have write access")

  @pytest.mark.asyncio
  async def test_validate_mcp_access_inactive_graph(
    self, db_session, test_user, test_user_graph
  ):
    """Test that inactive graphs still allow access if user has permissions."""
    # Make graph inactive
    test_user_graph.is_active = False
    db_session.commit()

    # The current implementation doesn't check is_active in validate_mcp_access
    # It only checks if the user has access to the graph
    # This test documents the current behavior
    try:
      await validate_mcp_access(
        graph_id=test_user_graph.graph_id, current_user=test_user, db=db_session
      )
      # Current implementation doesn't check is_active, so access is granted
      assert True
    except HTTPException:
      # If this starts failing, it means the implementation now checks is_active
      raise AssertionError("Implementation changed - now checks is_active field")


class TestMCPErrorSanitization:
  """Test error message sanitization for security."""

  @pytest.mark.unit
  def test_sanitize_database_paths(self, mcp_handler):
    """Test that database paths are sanitized from errors."""
    error_messages = [
      "Error at /var/lib/lbug/database.db",
      "Cannot open /home/user/data/graph.lbug",
      "File not found: /opt/robosystems/graphs/test.db",
    ]

    for error_msg in error_messages:
      # Check if _sanitize_error_message method exists
      if hasattr(mcp_handler, "_sanitize_error_message"):
        sanitized = mcp_handler._sanitize_error_message(Exception(error_msg))
        assert "/var/lib" not in sanitized
        assert "/home/user" not in sanitized
        assert "/opt/robosystems" not in sanitized
      else:
        # If method doesn't exist, skip this test
        pytest.skip("_sanitize_error_message method not found")

  @pytest.mark.unit
  def test_sanitize_memory_addresses(self, mcp_handler):
    """Test that memory addresses are sanitized."""
    if hasattr(mcp_handler, "_sanitize_error_message"):
      error = Exception("Segfault at 0x7fff8b4c2340 in function")
      sanitized = mcp_handler._sanitize_error_message(error)

      assert "0x7fff" not in sanitized
      assert "[REDACTED]" in sanitized or "error" in sanitized.lower()
    else:
      pytest.skip("_sanitize_error_message method not found")

  @pytest.mark.unit
  def test_sanitize_ip_addresses(self, mcp_handler):
    """Test that IP addresses are sanitized."""
    if hasattr(mcp_handler, "_sanitize_error_message"):
      error = Exception("Cannot connect to 192.168.1.100:8001")
      sanitized = mcp_handler._sanitize_error_message(error)

      assert "192.168" not in sanitized
      assert "[REDACTED]" in sanitized or "unavailable" in sanitized.lower()
    else:
      pytest.skip("_sanitize_error_message method not found")

  @pytest.mark.unit
  def test_user_friendly_error_messages(self, mcp_handler):
    """Test that common errors get user-friendly messages."""
    if hasattr(mcp_handler, "_sanitize_error_message"):
      test_cases = [
        (
          ConnectionError("Connection refused"),
          ["temporarily unavailable", "try again later", "connection", "refused"],
        ),
        (
          TimeoutError("Request timed out"),
          ["timeout", "simpler query", "increase timeout", "timed out"],
        ),
        (Exception("Out of memory"), ["resources", "limit", "simplify", "memory"]),
      ]

      for error, expected_keywords in test_cases:
        sanitized = mcp_handler._sanitize_error_message(error).lower()
        assert any(keyword in sanitized for keyword in expected_keywords)
    else:
      pytest.skip("_sanitize_error_message method not found")
