"""
Simplified tests for MCP functionality that don't require full handler initialization.
"""

import asyncio
import json
from unittest.mock import AsyncMock, Mock, patch

import pytest
from httpx import AsyncClient
from sqlalchemy.orm import Session

from robosystems.middleware.auth.jwt import create_jwt_token
from robosystems.models.iam import User


class TestMCPEndpoints:
  """Test MCP API endpoints."""

  @pytest.mark.asyncio
  async def test_mcp_execute_endpoint_success(
    self,
    async_client: AsyncClient,
    test_user: User,
    test_graph_with_credits: dict,
    db_session: Session,
  ):
    """Test successful MCP tool execution through API endpoint."""
    test_user_graph = test_graph_with_credits["user_graph"]

    with (
      patch("robosystems.routers.graphs.mcp.execute.MCPHandler") as MockHandler,
      patch("robosystems.routers.graphs.mcp.execute.get_graph_repository") as mock_repo,
    ):
      # Mock repository
      mock_repository = Mock()
      mock_repo.return_value = mock_repository

      # Mock the handler with proper async initialization
      mock_handler = Mock()
      mock_handler._closed = False
      mock_handler._init_task = asyncio.create_task(
        asyncio.sleep(0)
      )  # Mock completed init
      mock_handler.close = AsyncMock()

      # Mock internal methods
      mock_handler._ensure_initialized = AsyncMock()
      mock_handler._ensure_not_closed = Mock()

      # Mock call_tool to return success
      mock_handler.call_tool = AsyncMock(
        return_value={
          "type": "text",
          "text": json.dumps({"entities": ["Apple Inc", "Microsoft"]}),
        }
      )

      MockHandler.return_value = mock_handler

      token = create_jwt_token(test_user.id)
      headers = {"Authorization": f"Bearer {token}"}

      request_data = {
        "name": "read-graph-cypher",
        "arguments": {"query": "MATCH (e:Entity) RETURN e.name LIMIT 2"},
      }

      # Force JSON format for consistent testing
      response = await async_client.post(
        f"/v1/graphs/{test_user_graph.graph_id}/mcp/call-tool?format=json",
        json=request_data,
        headers=headers,
      )

      assert response.status_code == 200
      data = response.json()
      assert "result" in data
      # The result should contain the mocked data
      parsed_result = (
        json.loads(data["result"]["text"])
        if isinstance(data["result"], dict)
        else data["result"]
      )
      assert "entities" in parsed_result

  @pytest.mark.asyncio
  async def test_mcp_execute_endpoint_validation_error(
    self,
    async_client: AsyncClient,
    test_user: User,
    test_graph_with_credits: dict,
    db_session: Session,
  ):
    """Test MCP endpoint with validation error."""
    test_user_graph = test_graph_with_credits["user_graph"]

    with (
      patch("robosystems.routers.graphs.mcp.execute.MCPHandler") as MockHandler,
      patch("robosystems.routers.graphs.mcp.execute.get_graph_repository") as mock_repo,
    ):
      # Mock repository
      mock_repository = Mock()
      mock_repo.return_value = mock_repository

      # Create a mock handler that has proper async initialization
      mock_handler = Mock()
      mock_handler._closed = False
      mock_handler._init_task = asyncio.create_task(
        asyncio.sleep(0)
      )  # Mock completed init
      mock_handler.close = AsyncMock()

      # Mock _ensure_initialized to complete immediately
      mock_handler._ensure_initialized = AsyncMock()
      mock_handler._ensure_not_closed = Mock()

      # Make call_tool raise an error
      mock_handler.call_tool = AsyncMock(
        side_effect=ValueError("Invalid tool arguments")
      )

      MockHandler.return_value = mock_handler

      token = create_jwt_token(test_user.id)
      headers = {"Authorization": f"Bearer {token}"}

      request_data = {
        "name": "read-graph-cypher",
        "arguments": {},  # Missing required query
      }

      # Force JSON format to avoid SSE streaming
      response = await async_client.post(
        f"/v1/graphs/{test_user_graph.graph_id}/mcp/call-tool?format=json",
        json=request_data,
        headers=headers,
      )

      # The endpoint wraps errors in HTTPException(500)
      assert response.status_code == 500
      data = response.json()
      assert "Tool execution failed" in data.get("detail", "")

  @pytest.mark.asyncio
  async def test_mcp_tools_list_endpoint(
    self,
    async_client: AsyncClient,
    test_user: User,
    test_graph_with_credits: dict,
    db_session: Session,
  ):
    """Test MCP tools listing endpoint."""
    test_user_graph = test_graph_with_credits["user_graph"]

    with (
      patch("robosystems.routers.graphs.mcp.tools.MCPHandler") as MockHandler,
      patch("robosystems.routers.graphs.mcp.tools.get_graph_repository") as mock_repo,
    ):
      # Mock repository
      mock_repository = Mock()
      mock_repo.return_value = mock_repository

      # Mock the handler with proper async initialization
      mock_handler = Mock()
      mock_handler._closed = False
      mock_handler._init_task = asyncio.create_task(
        asyncio.sleep(0)
      )  # Mock completed init
      mock_handler.close = AsyncMock()

      # Mock internal methods
      mock_handler._ensure_initialized = AsyncMock()
      mock_handler._ensure_not_closed = Mock()

      # Mock get_tools to return list of tools
      mock_handler.get_tools = AsyncMock(
        return_value=[
          {
            "name": "read-graph-cypher",
            "description": "Execute read-only Cypher queries",
            "inputSchema": {
              "type": "object",
              "properties": {"query": {"type": "string"}},
              "required": ["query"],
            },
          },
          {
            "name": "get-graph-schema",
            "description": "Get graph schema",
            "inputSchema": {"type": "object", "properties": {}},
          },
        ]
      )

      MockHandler.return_value = mock_handler

      token = create_jwt_token(test_user.id)
      headers = {"Authorization": f"Bearer {token}"}

      response = await async_client.get(
        f"/v1/graphs/{test_user_graph.graph_id}/mcp/tools", headers=headers
      )

      assert response.status_code == 200
      data = response.json()
      assert "tools" in data
      assert len(data["tools"]) >= 2

  @pytest.mark.asyncio
  async def test_mcp_unauthorized_access(self, test_user_graph, test_db):
    """Test MCP endpoint without authorization."""
    from httpx import ASGITransport, AsyncClient

    from main import app
    from robosystems.database import get_db_session

    def override_get_db():
      yield test_db

    app.dependency_overrides[get_db_session] = override_get_db

    try:
      transport = ASGITransport(app=app)
      async with AsyncClient(transport=transport, base_url="http://test") as client:
        request_data = {
          "name": "read-graph-cypher",
          "arguments": {"query": "MATCH (n) RETURN n"},
        }

        # No auth header
        response = await client.post(
          f"/v1/graphs/{test_user_graph.graph_id}/mcp/call-tool", json=request_data
        )

        assert response.status_code == 401
    finally:
      app.dependency_overrides = {}


class TestMCPStrategies:
  """Test MCP execution strategies."""

  @pytest.mark.unit
  def test_strategy_detection_from_headers(self):
    """Test detecting client and strategy from headers."""
    from robosystems.routers.graphs.mcp.strategies import MCPClientDetector

    detector = MCPClientDetector()

    # Test MCP client detection (Claude-MCP would be an MCP client)
    # Headers are case-insensitive in HTTP, but dict keys need lowercase
    headers = {"user-agent": "Claude-MCP/1.0"}
    client_info = detector.detect_client_type(headers)
    assert client_info["is_mcp_client"] is True

    # Test non-MCP client detection (cursor doesn't contain 'mcp')
    headers = {"user-agent": "Cursor/1.0"}
    client_info = detector.detect_client_type(headers)
    assert client_info["is_mcp_client"] is False

    # Test browser detection
    headers = {"user-agent": "Mozilla/5.0"}
    client_info = detector.detect_client_type(headers)
    assert client_info["is_mcp_client"] is False
    assert client_info["is_browser"] is True

  @pytest.mark.unit
  def test_strategy_selection_logic(self):
    """Test strategy selection based on tool and client."""
    from robosystems.routers.graphs.mcp.strategies import (
      MCPExecutionStrategy,
      MCPStrategySelector,
    )

    # Test schema query - should be JSON_IMMEDIATE for fast operations
    strategy = MCPStrategySelector.select_strategy(
      tool_name="get-graph-schema",
      arguments={},
      client_info={"client_type": "unknown", "is_mcp_client": False},
      system_state={"queue_size": 0, "running_queries": 0},
      graph_id="test_graph",
      user_tier="standard",
    )
    assert strategy in [
      MCPExecutionStrategy.JSON_IMMEDIATE,
      MCPExecutionStrategy.JSON_COMPLETE,
    ]

    # Test heavy query - might be SSE or queued depending on system state
    strategy = MCPStrategySelector.select_strategy(
      tool_name="read-graph-cypher",
      arguments={"query": "MATCH (n) RETURN n"},  # No LIMIT
      client_info={"client_type": "unknown", "is_mcp_client": False},
      system_state={"queue_size": 10, "running_queries": 5},
      graph_id="test_graph",
      user_tier="standard",
    )
    # Heavy queries could use various strategies based on system state
    assert strategy in [
      MCPExecutionStrategy.QUEUE_WITH_MONITORING,
      MCPExecutionStrategy.SSE_PROGRESS,
      MCPExecutionStrategy.STREAM_AGGREGATED,
    ]

    # Test MCP client preference
    strategy = MCPStrategySelector.select_strategy(
      tool_name="read-graph-cypher",
      arguments={"query": "MATCH (n) RETURN n LIMIT 100"},
      client_info={
        "client_type": "claude",
        "is_mcp_client": True,
        "prefers_streaming": True,
      },
      system_state={"queue_size": 0, "running_queries": 0},
      graph_id="test_graph",
      user_tier="standard",
    )
    # MCP clients might get various strategies
    assert strategy in [
      MCPExecutionStrategy.SSE_PROGRESS,
      MCPExecutionStrategy.STREAM_AGGREGATED,
      MCPExecutionStrategy.JSON_COMPLETE,
      MCPExecutionStrategy.JSON_IMMEDIATE,
    ]


class TestMCPAccessControl:
  """Test MCP access control."""

  @pytest.mark.asyncio
  async def test_mcp_access_validation_user_graph(
    self, db_session: Session, test_user: User, test_user_graph
  ):
    """Test MCP access validation for user graphs."""
    from robosystems.routers.graphs.mcp.handlers import validate_mcp_access

    # User should have access to their own graph
    # validate_mcp_access doesn't return a boolean, it raises HTTPException on failure
    try:
      await validate_mcp_access(
        graph_id=test_user_graph.graph_id,
        current_user=test_user,
        db=db_session,
        operation_type="read",
      )
      has_access = True
    except Exception:
      has_access = False
    assert has_access is True

  @pytest.mark.asyncio
  async def test_mcp_access_validation_no_permission(
    self, db_session: Session, test_user: User
  ):
    """Test MCP access denied for unauthorized graph."""
    from fastapi import HTTPException

    from robosystems.routers.graphs.mcp.handlers import validate_mcp_access

    # User should not have access to random graph
    with pytest.raises(HTTPException) as exc_info:
      await validate_mcp_access(
        graph_id="unauthorized_graph",
        current_user=test_user,
        db=db_session,
        operation_type="read",
      )
    assert exc_info.value.status_code == 403

  @pytest.mark.asyncio
  async def test_mcp_access_validation_shared_repository(
    self, db_session: Session, test_user: User
  ):
    """Test MCP access for shared repositories."""
    import uuid

    from robosystems.models.iam import Graph
    from robosystems.models.iam.user_repository import (
      RepositoryAccessLevel,
      RepositoryPlan,
      RepositoryType,
      UserRepository,
    )
    from robosystems.routers.graphs.mcp.handlers import validate_mcp_access

    # Create SEC repository (required for foreign key)
    Graph.find_or_create_repository(
      graph_id="sec",
      graph_name="SEC Public Filings",
      repository_type="sec",
      session=db_session,
    )

    # Grant SEC access
    access = UserRepository(
      id=f"access_{uuid.uuid4().hex[:8]}",
      user_id=test_user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.READ,
      repository_plan=RepositoryPlan.STARTER,
      is_active=True,
    )
    db_session.add(access)
    db_session.commit()

    # Now should have access
    try:
      await validate_mcp_access(
        graph_id="sec", current_user=test_user, db=db_session, operation_type="read"
      )
      has_access = True
    except Exception:
      has_access = False
    assert has_access is True
