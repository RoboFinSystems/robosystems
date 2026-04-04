"""Tests for MCP materialization tools (get-graph-sync-status, materialize-graph)."""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.materialization_tools import (
  GetGraphSyncStatusTool,
  MaterializeGraphTool,
)


@pytest.fixture
def mock_client():
  client = MagicMock()
  client.graph_id = "kg_test123"
  return client


class TestGetGraphSyncStatusTool:
  def test_tool_definition(self, mock_client):
    tool = GetGraphSyncStatusTool(mock_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "get-graph-sync-status"
    assert "inputSchema" in defn

  @pytest.mark.asyncio
  async def test_returns_fresh_status(self, mock_client):
    """Fresh graph returns sync_status=fresh."""
    mock_graph = MagicMock()
    mock_graph.graph_stale = False
    mock_graph.graph_stale_reason = None
    mock_graph.graph_stale_at = None
    mock_graph.graph_metadata = {
      "last_materialized_at": datetime.now(UTC).isoformat(),
      "materialization_count": 3,
    }

    mock_session = MagicMock()

    with (
      patch(
        "robosystems.database.SessionFactory",
        return_value=mock_session,
      ),
      patch("robosystems.models.core.Graph") as mock_graph_cls,
    ):
      mock_graph_cls.get_by_id.return_value = mock_graph
      tool = GetGraphSyncStatusTool(mock_client)
      result = await tool.execute({})

    assert result["sync_status"] == "fresh"
    assert result["stale_since"] is None
    assert result["stale_duration_minutes"] is None
    assert result["materialization_count"] == 3

  @pytest.mark.asyncio
  async def test_returns_stale_status(self, mock_client):
    """Stale graph returns sync_status=stale with duration."""
    stale_at = datetime.now(UTC) - timedelta(minutes=15)
    mock_graph = MagicMock()
    mock_graph.graph_stale = True
    mock_graph.graph_stale_reason = "schedule_created"
    mock_graph.graph_stale_at = stale_at
    mock_graph.graph_metadata = {}

    mock_session = MagicMock()

    with (
      patch(
        "robosystems.database.SessionFactory",
        return_value=mock_session,
      ),
      patch("robosystems.models.core.Graph") as mock_graph_cls,
    ):
      mock_graph_cls.get_by_id.return_value = mock_graph
      tool = GetGraphSyncStatusTool(mock_client)
      result = await tool.execute({})

    assert result["sync_status"] == "stale"
    assert result["stale_reason"] == "schedule_created"
    assert result["stale_duration_minutes"] >= 14

  @pytest.mark.asyncio
  async def test_returns_error_for_missing_graph(self, mock_client):
    """Missing graph returns error dict."""
    mock_session = MagicMock()

    with (
      patch(
        "robosystems.database.SessionFactory",
        return_value=mock_session,
      ),
      patch("robosystems.models.core.Graph") as mock_graph_cls,
    ):
      mock_graph_cls.get_by_id.return_value = None
      tool = GetGraphSyncStatusTool(mock_client)
      result = await tool.execute({})

    assert "error" in result


class TestMaterializeGraphTool:
  def test_tool_definition(self, mock_client):
    tool = MaterializeGraphTool(mock_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "materialize-graph"
    assert "force" in defn["inputSchema"]["properties"]
