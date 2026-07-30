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

    facts = [
      {
        "element_id": "us-gaap:Assets",
        "element_name": "Assets",
        "period_end": "2023-12-31",
        "value": 1000.0,
        "unit": "USD",
      }
    ]

    with patch(
      "robosystems.middleware.mcp.tools.fact_grid_tool.query_fact_grid",
      new_callable=AsyncMock,
      return_value=(facts, False),
    ):
      result = await tool.execute(
        {"elements": ["us-gaap:Assets"], "periods": ["2023-12-31"]}
      )

    assert result["success"] is True
    assert result["fact_count"] == 1
    assert result["dimension_count"] == 2
    assert result["data"] == facts
    assert result["truncated"] is False

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_shared_repo_requires_entity(self, mock_graph_client):
    """SEC hosts thousands of filers; an unscoped query returns an arbitrary
    slice of arbitrary companies."""
    mock_graph_client.graph_id = "sec"
    tool = BuildFactGridTool(mock_graph_client)

    result = await tool.execute(
      {"elements": ["us-gaap:Assets"], "periods": ["2023-12-31"]}
    )

    assert result["error"] == "missing_entity_filter"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_shared_repo_with_entity_proceeds(self, mock_graph_client):
    mock_graph_client.graph_id = "sec"
    tool = BuildFactGridTool(mock_graph_client)

    with patch(
      "robosystems.middleware.mcp.tools.fact_grid_tool.query_fact_grid",
      new_callable=AsyncMock,
      return_value=([], False),
    ):
      result = await tool.execute(
        {
          "elements": ["us-gaap:Assets"],
          "periods": ["2023-12-31"],
          "entity": "NVDA",
        }
      )

    assert result["success"] is True

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_tenant_graph_does_not_require_entity(self, mock_graph_client):
    tool = BuildFactGridTool(mock_graph_client)

    with patch(
      "robosystems.middleware.mcp.tools.fact_grid_tool.query_fact_grid",
      new_callable=AsyncMock,
      return_value=([], False),
    ):
      result = await tool.execute(
        {"elements": ["us-gaap:Assets"], "periods": ["2023-12-31"]}
      )

    assert result["success"] is True

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_limit_out_of_range_rejected(self, mock_graph_client):
    tool = BuildFactGridTool(mock_graph_client)

    result = await tool.execute(
      {
        "elements": ["us-gaap:Assets"],
        "periods": ["2023-12-31"],
        "limit": 999999,
      }
    )

    assert result["error"] == "invalid_limit"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_truncation_is_surfaced_in_message(self, mock_graph_client):
    tool = BuildFactGridTool(mock_graph_client)
    facts = [
      {
        "element_id": "us-gaap:Assets",
        "element_name": "Assets",
        "period_end": "2023-12-31",
        "value": 1000.0,
        "unit": "USD",
      }
    ]

    with patch(
      "robosystems.middleware.mcp.tools.fact_grid_tool.query_fact_grid",
      new_callable=AsyncMock,
      return_value=(facts, True),
    ):
      result = await tool.execute(
        {"elements": ["us-gaap:Assets"], "periods": ["2023-12-31"], "limit": 1}
      )

    assert result["truncated"] is True
    assert "truncated" in result["message"]

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

    facts = [
      {
        "element_id": "us-gaap:Revenues",
        "element_name": "Revenues",
        "period_end": "2024-12-31",
        "value": 1000.0,
        "unit": "USD",
      },
      {
        "element_id": "us-gaap:Revenues",
        "element_name": "Revenues",
        "period_end": "2023-12-31",
        "value": 2000.0,
        "unit": "USD",
      },
      {
        "element_id": "us-gaap:CostOfRevenue",
        "element_name": "CostOfRevenue",
        "period_end": "2024-12-31",
        "value": 500.0,
        "unit": "USD",
      },
    ]

    with patch(
      "robosystems.middleware.mcp.tools.fact_grid_tool.query_fact_grid",
      new_callable=AsyncMock,
      return_value=(facts, False),
    ):
      result = await tool.execute(
        {
          "elements": ["us-gaap:Assets"],
          "periods": ["2023-12-31"],
          "include_summary": True,
        }
      )

    assert "summary" in result
    assert result["summary"]["us-gaap:Revenues"]["count"] == 2
    assert result["summary"]["us-gaap:Revenues"]["total"] == 3000.0
    assert result["summary"]["us-gaap:CostOfRevenue"]["total"] == 500.0
