"""Tests for schedule MCP read tools.

Write tools (`create-schedule`, `create-closing-entry`,
`create-manual-closing-entry`, `truncate-schedule`, `update-schedule`,
`delete-schedule`) are registrar-generated — their execution path is
covered by `tests/middleware/mcp/test_registrar.py` + the ops-layer
tests under `tests/operations/roboledger/schedules/`.

Mocks live at the **operations layer boundary** — the read tools are
thin shims that build arguments, call into `operations/roboledger/reads/
{schedules,period_drafts}.py`, and reshape responses.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.schedule_tools import (
  GetPeriodCloseStatusTool,
  GetScheduleFactsTool,
  ListPeriodDraftsTool,
  ListScheduleStructuresTool,
)
from robosystems.models.api.extensions.fiscal_calendar import (
  DraftEntryResponse,
  DraftLineItem,
  PeriodDraftsResponse,
)
from robosystems.models.api.extensions.schedules import (
  PeriodCloseItemResponse,
  PeriodCloseStatusResponse,
  ScheduleFactResponse,
  ScheduleFactsResponse,
  ScheduleListResponse,
  ScheduleSummaryResponse,
)

MODULE = "robosystems.middleware.mcp.tools.schedule_tools"


@pytest.fixture
def mock_graph_client():
  client = MagicMock()
  client.graph_id = "kgtest123"
  return client


@contextmanager
def _patch_session():
  """Patch the extensions session context manager."""
  session = MagicMock()
  cm = MagicMock()
  cm.__enter__ = MagicMock(return_value=session)
  cm.__exit__ = MagicMock(return_value=False)
  with patch(f"{MODULE}.extensions_session", return_value=cm):
    yield session


# ────────────────────────────────────────────────────────────────────────────
# list-schedule-structures
# ────────────────────────────────────────────────────────────────────────────


class TestListScheduleStructuresTool:
  def test_tool_definition(self, mock_graph_client):
    tool = ListScheduleStructuresTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "list-schedule-structures"
    assert "inputSchema" in defn

  @pytest.mark.asyncio
  async def test_returns_schedules(self, mock_graph_client):
    response = ScheduleListResponse(
      schedules=[
        ScheduleSummaryResponse(
          structure_id="struct_01",
          name="Depreciation",
          taxonomy_name="Schedules",
          entry_template={"debit_element_id": "elem_a"},
          schedule_metadata=None,
          total_periods=84,
          periods_with_entries=3,
        ),
      ]
    )
    tool = ListScheduleStructuresTool(mock_graph_client)
    with _patch_session(), patch(f"{MODULE}.ops_list_schedules", return_value=response):
      result = await tool.execute({})

    assert result["schedule_count"] == 1
    assert result["schedules"][0]["name"] == "Depreciation"

  @pytest.mark.asyncio
  async def test_handles_error(self, mock_graph_client):
    tool = ListScheduleStructuresTool(mock_graph_client)
    with patch(f"{MODULE}.extensions_session", side_effect=Exception("bad graph")):
      result = await tool.execute({})

    assert "error" in result


# ────────────────────────────────────────────────────────────────────────────
# get-schedule-facts
# ────────────────────────────────────────────────────────────────────────────


class TestGetScheduleFactsTool:
  def test_tool_definition(self, mock_graph_client):
    tool = GetScheduleFactsTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "get-schedule-facts"
    assert "structure_id" in defn["inputSchema"]["properties"]

  @pytest.mark.asyncio
  async def test_returns_facts(self, mock_graph_client):
    response = ScheduleFactsResponse(
      structure_id="struct_01",
      facts=[
        ScheduleFactResponse(
          element_id="elem_depr",
          element_name="Depreciation Expense",
          value=416.67,
          period_start=date(2026, 1, 1),
          period_end=date(2026, 1, 31),
        ),
      ],
    )
    tool = GetScheduleFactsTool(mock_graph_client)
    with (
      _patch_session(),
      patch(f"{MODULE}.ops_get_schedule_facts", return_value=response),
    ):
      result = await tool.execute({"structure_id": "struct_01"})

    assert result["fact_count"] == 1
    assert result["facts"][0]["value"] == 416.67

  @pytest.mark.asyncio
  async def test_parses_date_strings(self, mock_graph_client):
    response = ScheduleFactsResponse(structure_id="struct_01", facts=[])
    tool = GetScheduleFactsTool(mock_graph_client)
    with (
      _patch_session(),
      patch(f"{MODULE}.ops_get_schedule_facts", return_value=response) as ops,
    ):
      await tool.execute(
        {
          "structure_id": "struct_01",
          "period_start": "2026-01-01",
          "period_end": "2026-01-31",
        }
      )

    call = ops.call_args
    # ops_get_schedule_facts(session, service, structure_id, ps, pe)
    assert call.args[2] == "struct_01"
    assert call.args[3] == date(2026, 1, 1)
    assert call.args[4] == date(2026, 1, 31)


# ────────────────────────────────────────────────────────────────────────────
# get-period-close-status
# ────────────────────────────────────────────────────────────────────────────


class TestGetPeriodCloseStatusTool:
  def test_tool_definition(self, mock_graph_client):
    tool = GetPeriodCloseStatusTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "get-period-close-status"
    assert "period_start" in defn["inputSchema"]["required"]

  @pytest.mark.asyncio
  async def test_returns_status(self, mock_graph_client):
    response = PeriodCloseStatusResponse(
      fiscal_period_start=date(2026, 1, 1),
      fiscal_period_end=date(2026, 1, 31),
      period_status="open",
      schedules=[
        PeriodCloseItemResponse(
          structure_id="struct_01",
          structure_name="Depreciation",
          amount=416.67,
          status="pending",
          entry_id=None,
        ),
      ],
      total_draft=0,
      total_posted=0,
    )
    tool = GetPeriodCloseStatusTool(mock_graph_client)
    with (
      _patch_session(),
      patch(f"{MODULE}.ops_get_period_close_status", return_value=response),
    ):
      result = await tool.execute(
        {"period_start": "2026-01-01", "period_end": "2026-01-31"}
      )

    assert result["period_status"] == "open"
    assert result["schedules"]["total"] == 1
    assert result["schedules"]["pending"] == 1

  @pytest.mark.asyncio
  async def test_includes_reversal_fields(self, mock_graph_client):
    response = PeriodCloseStatusResponse(
      fiscal_period_start=date(2026, 4, 1),
      fiscal_period_end=date(2026, 4, 30),
      period_status="open",
      schedules=[
        PeriodCloseItemResponse(
          structure_id="struct_accrual",
          structure_name="Payroll Accrual",
          amount=3200.00,
          status="drafted",
          entry_id="je_01ABC",
          reversal_entry_id="je_01DEF",
          reversal_status="draft",
        ),
        PeriodCloseItemResponse(
          structure_id="struct_depr",
          structure_name="Depreciation",
          amount=212.00,
          status="pending",
          entry_id=None,
        ),
      ],
      total_draft=1,
      total_posted=0,
    )
    tool = GetPeriodCloseStatusTool(mock_graph_client)
    with (
      _patch_session(),
      patch(f"{MODULE}.ops_get_period_close_status", return_value=response),
    ):
      result = await tool.execute(
        {"period_start": "2026-04-01", "period_end": "2026-04-30"}
      )

    details = result["schedules"]["details"]
    assert details[0]["reversal_entry_id"] == "je_01DEF"
    assert details[0]["reversal_status"] == "draft"
    assert details[1]["reversal_entry_id"] is None
    assert details[1]["reversal_status"] is None


# ────────────────────────────────────────────────────────────────────────────
# list-period-drafts
# ────────────────────────────────────────────────────────────────────────────


class TestListPeriodDraftsTool:
  def test_tool_definition(self, mock_graph_client):
    tool = ListPeriodDraftsTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "list-period-drafts"
    assert "period" in defn["inputSchema"]["required"]

  @pytest.mark.asyncio
  async def test_returns_drafts(self, mock_graph_client):
    response = PeriodDraftsResponse(
      period="2026-03",
      period_start=date(2026, 3, 1),
      period_end=date(2026, 3, 31),
      draft_count=1,
      total_debit=41667,
      total_credit=41667,
      all_balanced=True,
      drafts=[
        DraftEntryResponse(
          entry_id="je_01",
          posting_date=date(2026, 3, 31),
          type="closing",
          memo="Monthly depreciation",
          provenance="schedule",
          source_structure_id="struct_01",
          source_structure_name="Office Furniture Depreciation",
          line_items=[
            DraftLineItem(
              line_item_id="li_01",
              element_id="elem_depr",
              element_code="6510",
              element_name="Depreciation Expense",
              debit_amount=41667,
              credit_amount=0,
              description=None,
            ),
            DraftLineItem(
              line_item_id="li_02",
              element_id="elem_accum",
              element_code="1810",
              element_name="Accumulated Depreciation",
              debit_amount=0,
              credit_amount=41667,
              description=None,
            ),
          ],
          total_debit=41667,
          total_credit=41667,
          balanced=True,
        )
      ],
    )
    tool = ListPeriodDraftsTool(mock_graph_client)
    with _patch_session(), patch(f"{MODULE}.list_period_drafts", return_value=response):
      result = await tool.execute({"period": "2026-03"})

    assert result["draft_count"] == 1
    assert result["all_balanced"] is True
    assert result["drafts"][0]["entry_id"] == "je_01"
    assert len(result["drafts"][0]["line_items"]) == 2

  @pytest.mark.asyncio
  async def test_handles_error(self, mock_graph_client):
    tool = ListPeriodDraftsTool(mock_graph_client)
    with patch(f"{MODULE}.extensions_session", side_effect=Exception("db error")):
      result = await tool.execute({"period": "2026-03"})

    assert "error" in result
