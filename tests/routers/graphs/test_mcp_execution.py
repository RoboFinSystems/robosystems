"""
Simplified tests for MCP functionality that don't require full handler initialization.
"""

import pytest
from sqlalchemy.orm import Session

from robosystems.models.core import User


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
    self, db_session: Session, test_user: User, test_graph_with_credits
  ):
    """Test MCP access validation for user graphs."""
    from robosystems.routers.graphs.mcp.handlers import validate_mcp_access

    # User should have access to their own (subscribed) graph
    # validate_mcp_access doesn't return a boolean, it raises HTTPException on failure
    try:
      await validate_mcp_access(
        graph_id=test_graph_with_credits["graph"].graph_id,
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

    from robosystems.models.core import Graph
    from robosystems.models.core.user.user_repository import (
      RepositoryAccessLevel,
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
      repository_plan="starter",
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
