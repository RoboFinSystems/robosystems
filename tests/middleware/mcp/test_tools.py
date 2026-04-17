"""Tests for standalone MCP tools (BuildFactGridTool).

Graph lifecycle tools (`create-subgraph`, `delete-subgraph`,
`list-subgraphs`, `switch-workspace`, `create-backup`, `restore-backup`,
`materialize`, `get-graph-sync-status`) have dedicated coverage in
`tests/middleware/mcp/tools/test_graph_tools.py`.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.fact_grid_tool import BuildFactGridTool


@pytest.fixture
def mock_graph_client():
  """Mock GraphMCPClient for tool initialization."""
  client = MagicMock()
  client.graph_id = "kg1234567890abcdef"
  client.user = MagicMock()
  client.user.id = "user123"
  return client


class TestBuildFactGridTool:
  """Tests for BuildFactGridTool."""

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_build_fact_grid_success(self, mock_graph_client):
    """Test successful fact grid building."""
    tool = BuildFactGridTool(mock_graph_client)

    with patch(
      "robosystems.operations.roboledger.views.fact_grid_builder.FactGridBuilder"
    ) as mock_builder_class:
      mock_graph_client.execute_query = AsyncMock(
        return_value=[
          {
            "element_id": "us-gaap:Assets",
            "period_end": "2023-12-31",
            "value": 1000000,
          }
        ]
      )

      mock_builder = MagicMock()
      mock_builder_class.return_value = mock_builder
      mock_grid = MagicMock()
      mock_grid.metadata.fact_count = 1
      mock_grid.metadata.dimension_count = 2
      mock_grid.metadata.construction_time_ms = 50
      mock_grid.dimensions = []
      mock_builder.build.return_value = mock_grid

      result = await tool.execute(
        {"elements": ["us-gaap:Assets"], "periods": ["2023-12-31"]}
      )

    assert result["success"] is True
    assert result["fact_count"] == 1
    assert result["dimension_count"] == 2

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_build_fact_grid_missing_elements(self, mock_graph_client):
    """Test fact grid building fails without elements."""
    tool = BuildFactGridTool(mock_graph_client)

    result = await tool.execute({"periods": ["2023-12-31"]})

    assert result["error"] == "missing_elements"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_build_fact_grid_missing_period_filter(self, mock_graph_client):
    """Test fact grid building fails without any period scoping."""
    tool = BuildFactGridTool(mock_graph_client)

    result = await tool.execute({"elements": ["us-gaap:Assets"]})

    assert result["error"] == "missing_period_filter"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_build_fact_grid_query_failure(self, mock_graph_client):
    """Test fact grid building handles query errors."""
    tool = BuildFactGridTool(mock_graph_client)

    mock_graph_client.execute_query = AsyncMock(side_effect=Exception("Query failed"))

    result = await tool.execute(
      {"elements": ["us-gaap:Assets"], "periods": ["2023-12-31"]}
    )

    assert result["error"] == "construction_failed"
    assert "Query failed" in result["message"]
