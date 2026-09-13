"""Tests for the ``disclosures`` and ``information-block`` MCP tools.

The tools are thin over the ops layer: they resolve the report the way
``financial-statement-analysis`` does, call the view, and turn the view's
domain errors into the ``{"error": ...}`` shape an MCP client can correct.
The view itself is covered in ``tests/operations/roboledger/views``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.disclosure_tools import (
  DisclosuresTool,
  InformationBlockTool,
)
from robosystems.operations.roboledger.views.information_blocks import (
  MAX_BLOCK_MEMBERS,
  MAX_BLOCK_ROWS,
  BlockNotFoundError,
  ReportNotFoundError,
  ReportSelectorError,
)

MODULE = "robosystems.middleware.mcp.tools.disclosure_tools"


def _client(graph_id: str = "sec"):
  client = MagicMock()
  client.graph_id = graph_id
  return client


class TestDefinitions:
  @pytest.mark.unit
  def test_names_and_inputs(self):
    d = DisclosuresTool(_client()).get_tool_definition()
    assert d["name"] == "disclosures"
    assert d["inputSchema"]["required"] == []
    assert set(d["inputSchema"]["properties"]) == {
      "ticker",
      "report_id",
      "fiscal_year",
      "period_type",
      "topic",
    }
    b = InformationBlockTool(_client()).get_tool_definition()
    assert b["name"] == "information-block"
    assert b["inputSchema"]["required"] == ["block"]
    assert b["inputSchema"]["properties"]["periods"]["type"] == "array"

  @pytest.mark.unit
  def test_descriptions_carry_the_section_contract_and_say_which_is_expensive(self):
    map_text = DisclosuresTool(_client()).get_tool_definition()["description"]
    block_text = InformationBlockTool(_client()).get_tool_definition()["description"]
    for section in ("WHEN TO USE", "PARAMETERS", "RETURNS", "NOTES", "RELATED TOOLS"):
      assert section in map_text and section in block_text
    assert "Cheap" in map_text
    assert "EXPENSIVE" in block_text
    # On a tenant the pair sits beside the authored envelope's tools; each
    # description says which one reads what.
    assert "get-information-block" in map_text and "get-information-block" in block_text


@pytest.mark.asyncio
class TestDisclosuresExecute:
  @pytest.mark.unit
  async def test_resolves_the_report_and_stamps_it(self):
    resolved = {"identifier": "rpt_abc", "form": "10-K"}
    with (
      patch(
        f"{MODULE}.resolve_report", new=AsyncMock(return_value=("rpt_abc", resolved))
      ) as resolve,
      patch(
        f"{MODULE}.query_disclosures",
        new=AsyncMock(
          return_value={"graph_id": "sec", "report_id": "rpt_abc", "count": 3}
        ),
      ) as query,
    ):
      out = await DisclosuresTool(_client()).execute(
        {"ticker": " nvda ", "fiscal_year": 2024, "topic": "leases"}
      )
    assert resolve.call_args.kwargs == {
      "report_id": None,
      "ticker": "nvda",
      "fiscal_year": 2024,
      "period_type": None,
    }
    assert query.call_args.args == ("sec", "rpt_abc")
    assert query.call_args.kwargs == {"topic": "leases"}
    assert out["count"] == 3
    assert out["resolved_report"]["form"] == "10-K"

  @pytest.mark.unit
  @pytest.mark.parametrize(
    "error",
    [
      ReportSelectorError("ticker is required on shared-repository graphs"),
      ReportNotFoundError("No annual filing found for NVDA in fiscal year 2005."),
    ],
  )
  async def test_a_selector_problem_is_an_error_the_client_can_fix(self, error):
    with patch(f"{MODULE}.resolve_report", new=AsyncMock(side_effect=error)):
      out = await DisclosuresTool(_client()).execute({})
    assert out == {"error": str(error)}

  @pytest.mark.unit
  async def test_an_unknown_topic_is_an_error(self):
    with (
      patch(f"{MODULE}.resolve_report", new=AsyncMock(return_value=("rpt_abc", None))),
      patch(
        f"{MODULE}.query_disclosures",
        new=AsyncMock(
          side_effect=BlockNotFoundError("No disclosure matches 'pensions'")
        ),
      ),
    ):
      out = await DisclosuresTool(_client()).execute(
        {"report_id": "rpt_abc", "topic": "pensions"}
      )
    assert "pensions" in out["error"]


@pytest.mark.asyncio
class TestInformationBlockExecute:
  @pytest.mark.unit
  async def test_block_is_required(self):
    out = await InformationBlockTool(_client()).execute({"ticker": "NVDA"})
    assert "block is required" in out["error"]

  @pytest.mark.unit
  async def test_passes_the_selection_through_and_clamps_the_caps(self):
    with (
      patch(f"{MODULE}.resolve_report", new=AsyncMock(return_value=("rpt_abc", None))),
      patch(
        f"{MODULE}.query_information_block",
        new=AsyncMock(
          return_value={"graph_id": "sec", "report_id": "rpt_abc", "rows": []}
        ),
      ) as query,
    ):
      out = await InformationBlockTool(_client()).execute(
        {
          "block": " LeasesDetails ",
          "report_id": "rpt_abc",
          "periods": ["2024", " ", "2023-12-31"],
          "member": "Widgets",
          "max_rows": 10_000,
          "max_members": 0,
        }
      )
    assert query.call_args.args == ("sec", "rpt_abc", "LeasesDetails")
    assert query.call_args.kwargs == {
      "periods": ["2024", "2023-12-31"],
      "member": "Widgets",
      "max_rows": MAX_BLOCK_ROWS,
      "max_members": 1,
    }
    assert "resolved_report" not in out
    assert MAX_BLOCK_MEMBERS >= 1

  @pytest.mark.unit
  async def test_an_unknown_block_is_an_error(self):
    with (
      patch(f"{MODULE}.resolve_report", new=AsyncMock(return_value=("rpt_abc", None))),
      patch(
        f"{MODULE}.query_information_block",
        new=AsyncMock(
          side_effect=BlockNotFoundError("No information block for 'Pensions'")
        ),
      ),
    ):
      out = await InformationBlockTool(_client()).execute(
        {"block": "Pensions", "report_id": "rpt_abc"}
      )
    assert "Pensions" in out["error"]
