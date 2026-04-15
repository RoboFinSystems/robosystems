"""Tests for schedule MCP tools.

Mocks live at the **operations layer boundary** — the MCP tools are now
thin shims that build request bodies, call into `operations/roboledger/
{reads,commands}/schedules.py`, and reshape responses for the historical
MCP wire format. Tests against ScheduleService internals would only
re-test what `tests/operations/roboledger/schedules/` already covers.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.schedule_tools import (
  CreateClosingEntryTool,
  CreateManualClosingEntryTool,
  CreateScheduleTool,
  GetPeriodCloseStatusTool,
  GetScheduleFactsTool,
  ListPeriodDraftsTool,
  ListScheduleStructuresTool,
  TruncateScheduleTool,
)
from robosystems.models.api.extensions.fiscal_calendar import (
  DraftEntryResponse,
  DraftLineItem,
  PeriodDraftsResponse,
)
from robosystems.models.api.extensions.schedules import (
  ClosingEntryResponse,
  PeriodCloseItemResponse,
  PeriodCloseStatusResponse,
  ScheduleCreatedResponse,
  ScheduleFactResponse,
  ScheduleFactsResponse,
  ScheduleListResponse,
  ScheduleSummaryResponse,
  TruncateScheduleResponse,
)

MODULE = "robosystems.middleware.mcp.tools.schedule_tools"


@pytest.fixture
def mock_graph_client():
  client = MagicMock()
  client.graph_id = "kgtest123"
  return client


@contextmanager
def _patch_session():
  session = MagicMock()
  cm = MagicMock()
  cm.__enter__ = MagicMock(return_value=session)
  cm.__exit__ = MagicMock(return_value=False)
  with patch(f"{MODULE}.extensions_session", return_value=cm):
    yield session


# ────────────────────────────────────────────────────────────────────────────
# create-schedule
# ────────────────────────────────────────────────────────────────────────────


class TestCreateScheduleTool:
  def test_tool_definition(self, mock_graph_client):
    tool = CreateScheduleTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "create-schedule"
    assert "name" in defn["inputSchema"]["required"]
    assert "element_ids" in defn["inputSchema"]["required"]
    assert "monthly_amount" in defn["inputSchema"]["required"]

  @pytest.mark.asyncio
  async def test_creates_schedule(self, mock_graph_client):
    response = ScheduleCreatedResponse(
      structure_id="struct_sched_01",
      name="Office Furniture Depreciation",
      taxonomy_id="tax_sched_01",
      total_periods=84,
      total_facts=84,
    )
    tool = CreateScheduleTool(mock_graph_client)
    with (
      _patch_session(),
      patch(f"{MODULE}.ops_create_schedule", return_value=response) as ops,
    ):
      result = await tool.execute(
        {
          "name": "Office Furniture Depreciation",
          "element_ids": ["elem_depr", "elem_accum"],
          "period_start": "2026-01-01",
          "period_end": "2032-12-31",
          "monthly_amount": 41667,
          "debit_element_id": "elem_depr",
          "credit_element_id": "elem_accum",
        }
      )

    assert result["success"] is True
    assert result["structure_id"] == "struct_sched_01"
    assert result["name"] == "Office Furniture Depreciation"
    ops.assert_called_once()
    call = ops.call_args
    body = call.args[1]
    assert body.name == "Office Furniture Depreciation"
    assert body.entry_template.debit_element_id == "elem_depr"
    assert call.kwargs["created_by"] == "mcp:kgtest123"

  @pytest.mark.asyncio
  async def test_creates_with_metadata(self, mock_graph_client):
    response = ScheduleCreatedResponse(
      structure_id="struct_sched_02",
      name="Test",
      taxonomy_id="tax_01",
      total_periods=12,
      total_facts=12,
    )
    tool = CreateScheduleTool(mock_graph_client)
    with (
      _patch_session(),
      patch(f"{MODULE}.ops_create_schedule", return_value=response) as ops,
    ):
      result = await tool.execute(
        {
          "name": "Test",
          "element_ids": ["elem_a", "elem_b"],
          "period_start": "2026-01-01",
          "period_end": "2026-12-31",
          "monthly_amount": 10000,
          "debit_element_id": "elem_a",
          "credit_element_id": "elem_b",
          "method": "straight_line",
          "original_amount": 3500000,
          "useful_life_months": 84,
        }
      )

    assert result["success"] is True
    body = ops.call_args.args[1]
    assert body.schedule_metadata is not None
    assert body.schedule_metadata.original_amount == 3500000

  @pytest.mark.asyncio
  async def test_passes_auto_reverse(self, mock_graph_client):
    response = ScheduleCreatedResponse(
      structure_id="struct_sched_03",
      name="Payroll Accrual",
      taxonomy_id="tax_01",
      total_periods=12,
      total_facts=12,
    )
    tool = CreateScheduleTool(mock_graph_client)
    with (
      _patch_session(),
      patch(f"{MODULE}.ops_create_schedule", return_value=response) as ops,
    ):
      result = await tool.execute(
        {
          "name": "Payroll Accrual",
          "element_ids": ["elem_salary", "elem_accrued"],
          "period_start": "2026-01-01",
          "period_end": "2026-12-31",
          "monthly_amount": 320000,
          "debit_element_id": "elem_salary",
          "credit_element_id": "elem_accrued",
          "auto_reverse": True,
        }
      )

    assert result["success"] is True
    body = ops.call_args.args[1]
    assert body.entry_template.auto_reverse is True

  @pytest.mark.asyncio
  async def test_auto_reverse_defaults_false(self, mock_graph_client):
    response = ScheduleCreatedResponse(
      structure_id="struct_sched_04",
      name="Depreciation",
      taxonomy_id="tax_01",
      total_periods=12,
      total_facts=12,
    )
    tool = CreateScheduleTool(mock_graph_client)
    with (
      _patch_session(),
      patch(f"{MODULE}.ops_create_schedule", return_value=response) as ops,
    ):
      await tool.execute(
        {
          "name": "Depreciation",
          "element_ids": ["elem_depr", "elem_accum"],
          "period_start": "2026-01-01",
          "period_end": "2026-12-31",
          "monthly_amount": 41667,
          "debit_element_id": "elem_depr",
          "credit_element_id": "elem_accum",
        }
      )

    body = ops.call_args.args[1]
    assert body.entry_template.auto_reverse is False

  @pytest.mark.asyncio
  async def test_handles_error(self, mock_graph_client):
    tool = CreateScheduleTool(mock_graph_client)
    with patch(f"{MODULE}.extensions_session", side_effect=Exception("db error")):
      result = await tool.execute(
        {
          "name": "Test",
          "element_ids": ["elem_a"],
          "period_start": "2026-01-01",
          "period_end": "2026-12-31",
          "monthly_amount": 10000,
          "debit_element_id": "elem_a",
          "credit_element_id": "elem_b",
        }
      )

    assert "error" in result


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
# create-closing-entry
# ────────────────────────────────────────────────────────────────────────────


class TestCreateClosingEntryTool:
  def test_tool_definition(self, mock_graph_client):
    tool = CreateClosingEntryTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "create-closing-entry"
    assert "structure_id" in defn["inputSchema"]["required"]
    assert "posting_date" in defn["inputSchema"]["required"]

  @pytest.mark.asyncio
  async def test_creates_entry(self, mock_graph_client):
    response = ClosingEntryResponse(
      outcome="created",
      entry_id="je_01ABC",
      status="draft",
      posting_date=date(2026, 1, 31),
      memo="Monthly depreciation",
      debit_element_id="elem_depr",
      credit_element_id="elem_accum",
      amount=416.67,
    )
    tool = CreateClosingEntryTool(mock_graph_client)
    with (
      _patch_session(),
      patch(f"{MODULE}.ops_create_closing_entry", return_value=response),
    ):
      result = await tool.execute(
        {
          "structure_id": "struct_01",
          "posting_date": "2026-01-31",
          "period_start": "2026-01-01",
          "period_end": "2026-01-31",
        }
      )

    assert result["entry_id"] == "je_01ABC"
    assert result["status"] == "draft"
    assert len(result["line_items"]) == 2
    assert "reversal" not in result

  @pytest.mark.asyncio
  async def test_creates_entry_with_reversal(self, mock_graph_client):
    response = ClosingEntryResponse(
      outcome="created",
      entry_id="je_01ABC",
      status="draft",
      posting_date=date(2026, 1, 31),
      memo="Accrue payroll",
      debit_element_id="elem_salary",
      credit_element_id="elem_accrued",
      amount=3200.00,
      reversal=ClosingEntryResponse(
        outcome="created",
        entry_id="je_01DEF",
        status="draft",
        posting_date=date(2026, 2, 1),
        memo="Reverse: Accrue payroll",
        debit_element_id="elem_accrued",
        credit_element_id="elem_salary",
        amount=3200.00,
      ),
    )
    tool = CreateClosingEntryTool(mock_graph_client)
    with (
      _patch_session(),
      patch(f"{MODULE}.ops_create_closing_entry", return_value=response),
    ):
      result = await tool.execute(
        {
          "structure_id": "struct_01",
          "posting_date": "2026-01-31",
          "period_start": "2026-01-01",
          "period_end": "2026-01-31",
        }
      )

    assert result["entry_id"] == "je_01ABC"
    assert "reversal" in result
    rev = result["reversal"]
    assert rev["entry_id"] == "je_01DEF"
    assert rev["posting_date"] == "2026-02-01"
    assert rev["memo"] == "Reverse: Accrue payroll"
    assert rev["line_items"][0]["element_id"] == "elem_accrued"
    assert rev["line_items"][0]["debit"] == 3200.00
    assert rev["line_items"][1]["element_id"] == "elem_salary"
    assert rev["line_items"][1]["credit"] == 3200.00

  @pytest.mark.asyncio
  async def test_returns_error_for_duplicate(self, mock_graph_client):
    tool = CreateClosingEntryTool(mock_graph_client)
    with (
      _patch_session(),
      patch(
        f"{MODULE}.ops_create_closing_entry",
        side_effect=ValueError("already exists"),
      ),
    ):
      result = await tool.execute(
        {
          "structure_id": "struct_01",
          "posting_date": "2026-01-31",
          "period_start": "2026-01-01",
          "period_end": "2026-01-31",
        }
      )

    assert "error" in result
    assert "already exists" in result["error"]


# ────────────────────────────────────────────────────────────────────────────
# create-manual-closing-entry
# ────────────────────────────────────────────────────────────────────────────


class TestCreateManualClosingEntryTool:
  def test_tool_definition(self, mock_graph_client):
    tool = CreateManualClosingEntryTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "create-manual-closing-entry"
    assert "memo" in defn["inputSchema"]["required"]

  @pytest.mark.asyncio
  async def test_creates_entry(self, mock_graph_client):
    response = ClosingEntryResponse(
      outcome="created",
      entry_id="je_manual_01",
      status="draft",
      posting_date=date(2026, 3, 15),
      memo="Asset disposal",
      debit_element_id=None,
      credit_element_id=None,
      amount=480000,
    )
    tool = CreateManualClosingEntryTool(mock_graph_client)
    with (
      _patch_session(),
      patch(f"{MODULE}.ops_create_manual_closing_entry", return_value=response),
    ):
      result = await tool.execute(
        {
          "posting_date": "2026-03-15",
          "memo": "Asset disposal",
          "line_items": [
            {"element_id": "elem_cash", "debit_amount": 300000},
            {"element_id": "elem_accum", "debit_amount": 186662},
            {"element_id": "elem_asset", "credit_amount": 480000},
            {"element_id": "elem_gain", "credit_amount": 6662},
          ],
        }
      )

    assert result["entry_id"] == "je_manual_01"
    assert result["line_items_count"] == 4
    assert result["posting_date"] == "2026-03-15"


# ────────────────────────────────────────────────────────────────────────────
# truncate-schedule
# ────────────────────────────────────────────────────────────────────────────


class TestTruncateScheduleTool:
  def test_tool_definition(self, mock_graph_client):
    tool = TruncateScheduleTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "truncate-schedule"
    assert "structure_id" in defn["inputSchema"]["required"]
    assert "new_end_date" in defn["inputSchema"]["required"]
    assert "reason" in defn["inputSchema"]["required"]

  @pytest.mark.asyncio
  async def test_truncates(self, mock_graph_client):
    response = TruncateScheduleResponse(
      structure_id="struct_01",
      new_end_date=date(2026, 3, 31),
      facts_deleted=24,
      reason="Asset sold",
    )
    tool = TruncateScheduleTool(mock_graph_client)
    with (
      _patch_session(),
      patch(f"{MODULE}.ops_truncate_schedule", return_value=response),
    ):
      result = await tool.execute(
        {
          "structure_id": "struct_01",
          "new_end_date": "2026-03-31",
          "reason": "Asset sold",
        }
      )

    assert result["success"] is True
    assert result["new_end_date"] == "2026-03-31"
    assert result["facts_deleted"] == 24

  @pytest.mark.asyncio
  async def test_returns_error_for_invalid_date(self, mock_graph_client):
    tool = TruncateScheduleTool(mock_graph_client)
    with (
      _patch_session(),
      patch(
        f"{MODULE}.ops_truncate_schedule",
        side_effect=ValueError("must be the last day of a month"),
      ),
    ):
      result = await tool.execute(
        {
          "structure_id": "struct_01",
          "new_end_date": "2026-03-15",
          "reason": "test",
        }
      )

    assert "error" in result


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
