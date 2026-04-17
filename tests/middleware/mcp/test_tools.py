"""Tests for standalone MCP tools (BuildFactGridTool).

Graph lifecycle tools (`create-subgraph`, `delete-subgraph`,
`list-subgraphs`, `switch-workspace`, `create-backup`, `restore-backup`,
`materialize`, `get-graph-sync-status`) have dedicated coverage in
`tests/middleware/mcp/tools/test_graph_tools.py`.

The Cypher construction + dedup live in the ops layer
(`operations/roboledger/views/fact_query.py`) and have their own
coverage in `tests/operations/roboledger/views/test_fact_query.py`.
These tests focus on the MCP tool's argument parsing, validation, and
response shape.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
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
  """Tests for BuildFactGridTool — thin wrapper around ops-layer query."""

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_build_fact_grid_success(self, mock_graph_client):
    """Happy path: delegates to query_fact_grid + FactGridBuilder."""
    tool = BuildFactGridTool(mock_graph_client)

    mock_grid = MagicMock()
    mock_grid.metadata.fact_count = 1
    mock_grid.metadata.dimension_count = 2
    mock_grid.metadata.construction_time_ms = 50
    mock_grid.dimensions = []
    mock_grid.facts_df = None

    mock_builder = MagicMock()
    mock_builder.build.return_value = mock_grid

    with (
      patch(
        "robosystems.middleware.mcp.tools.fact_grid_tool.query_fact_grid",
        new_callable=AsyncMock,
        return_value=pd.DataFrame(),
      ),
      patch(
        "robosystems.middleware.mcp.tools.fact_grid_tool.FactGridBuilder",
        return_value=mock_builder,
      ),
    ):
      result = await tool.execute(
        {"elements": ["us-gaap:Assets"], "periods": ["2023-12-31"]}
      )

    assert result["success"] is True
    assert result["fact_count"] == 1
    assert result["dimension_count"] == 2

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_build_fact_grid_missing_elements(self, mock_graph_client):
    """Fails validation without elements or canonical_concepts."""
    tool = BuildFactGridTool(mock_graph_client)
    result = await tool.execute({"periods": ["2023-12-31"]})
    assert result["error"] == "missing_elements"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_build_fact_grid_missing_period_filter(self, mock_graph_client):
    """Fails validation without any period scoping."""
    tool = BuildFactGridTool(mock_graph_client)
    result = await tool.execute({"elements": ["us-gaap:Assets"]})
    assert result["error"] == "missing_period_filter"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_build_fact_grid_query_errors_bubble(self, mock_graph_client):
    """Unexpected ops-layer errors propagate; we don't swallow them."""
    tool = BuildFactGridTool(mock_graph_client)

    with patch(
      "robosystems.middleware.mcp.tools.fact_grid_tool.query_fact_grid",
      new_callable=AsyncMock,
      side_effect=RuntimeError("Query failed"),
    ):
      with pytest.raises(RuntimeError, match="Query failed"):
        await tool.execute({"elements": ["us-gaap:Assets"], "periods": ["2023-12-31"]})

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_invalid_rows_config(self, mock_graph_client):
    """Non-list rows input is rejected."""
    tool = BuildFactGridTool(mock_graph_client)
    result = await tool.execute(
      {
        "elements": ["us-gaap:Assets"],
        "periods": ["2023-12-31"],
        "rows": "not-a-list",
      }
    )
    assert result["error"] == "invalid_rows"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_invalid_row_element(self, mock_graph_client):
    """Non-dict row entries are rejected."""
    tool = BuildFactGridTool(mock_graph_client)
    result = await tool.execute(
      {
        "elements": ["us-gaap:Assets"],
        "periods": ["2023-12-31"],
        "rows": ["not-a-dict"],
      }
    )
    assert result["error"] == "invalid_row_config"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_include_summary_adds_aggregates(self, mock_graph_client):
    """include_summary=true emits per-element stats."""
    tool = BuildFactGridTool(mock_graph_client)

    mock_grid = MagicMock()
    mock_grid.metadata.fact_count = 3
    mock_grid.metadata.dimension_count = 1
    mock_grid.metadata.construction_time_ms = 10
    mock_grid.dimensions = []
    mock_grid.facts_df = pd.DataFrame(
      {
        "element_name": ["Revenue", "Revenue", "Cost"],
        "value": [1000.0, 2000.0, 500.0],
      }
    )

    mock_builder = MagicMock()
    mock_builder.build.return_value = mock_grid

    with (
      patch(
        "robosystems.middleware.mcp.tools.fact_grid_tool.query_fact_grid",
        new_callable=AsyncMock,
        return_value=pd.DataFrame(),
      ),
      patch(
        "robosystems.middleware.mcp.tools.fact_grid_tool.FactGridBuilder",
        return_value=mock_builder,
      ),
    ):
      result = await tool.execute(
        {
          "elements": ["us-gaap:Assets"],
          "periods": ["2023-12-31"],
          "include_summary": True,
        }
      )

    assert "summary" in result
    assert result["summary"]["Revenue"]["count"] == 2
    assert result["summary"]["Revenue"]["total"] == 3000.0
    assert result["summary"]["Cost"]["total"] == 500.0
