"""Tests for schedule MCP tools."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.schedule_tools import (
  CreateClosingEntryTool,
  CreateScheduleTool,
  GetPeriodCloseStatusTool,
  GetScheduleFactsTool,
  ListScheduleStructuresTool,
)


@pytest.fixture
def mock_graph_client():
  client = MagicMock()
  client.graph_id = "kgtest123"
  return client


SVC_PATH = "robosystems.operations.schedules.ScheduleService"
SESSION_PATH = "robosystems.db.extensions.extensions_session"


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
    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    mock_structure = MagicMock()
    mock_structure.id = "struct_sched_01"
    mock_structure.name = "Office Furniture Depreciation"
    mock_structure.taxonomy_id = "tax_sched_01"

    mock_svc = MagicMock()
    mock_svc.create_schedule.return_value = mock_structure

    tool = CreateScheduleTool(mock_graph_client)
    with (
      patch(SESSION_PATH, return_value=mock_session),
      patch(SVC_PATH, return_value=mock_svc),
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
    mock_svc.create_schedule.assert_called_once()
    mock_session.commit.assert_called_once()

  @pytest.mark.asyncio
  async def test_creates_with_metadata(self, mock_graph_client):
    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    mock_structure = MagicMock()
    mock_structure.id = "struct_sched_02"
    mock_structure.name = "Test"
    mock_structure.taxonomy_id = "tax_01"

    mock_svc = MagicMock()
    mock_svc.create_schedule.return_value = mock_structure

    tool = CreateScheduleTool(mock_graph_client)
    with (
      patch(SESSION_PATH, return_value=mock_session),
      patch(SVC_PATH, return_value=mock_svc),
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
    mock_session.commit.assert_called_once()
    # Verify schedule_metadata was passed
    call_kwargs = mock_svc.create_schedule.call_args[1]
    assert call_kwargs["schedule_metadata"] is not None
    assert call_kwargs["schedule_metadata"].original_amount == 3500000

  @pytest.mark.asyncio
  async def test_passes_auto_reverse(self, mock_graph_client):
    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    mock_structure = MagicMock()
    mock_structure.id = "struct_sched_03"
    mock_structure.name = "Payroll Accrual"
    mock_structure.taxonomy_id = "tax_01"

    mock_svc = MagicMock()
    mock_svc.create_schedule.return_value = mock_structure

    tool = CreateScheduleTool(mock_graph_client)
    with (
      patch(SESSION_PATH, return_value=mock_session),
      patch(SVC_PATH, return_value=mock_svc),
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
    call_kwargs = mock_svc.create_schedule.call_args[1]
    assert call_kwargs["entry_template"].auto_reverse is True

  @pytest.mark.asyncio
  async def test_auto_reverse_defaults_false(self, mock_graph_client):
    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    mock_structure = MagicMock()
    mock_structure.id = "struct_sched_04"
    mock_structure.name = "Depreciation"
    mock_structure.taxonomy_id = "tax_01"

    mock_svc = MagicMock()
    mock_svc.create_schedule.return_value = mock_structure

    tool = CreateScheduleTool(mock_graph_client)
    with (
      patch(SESSION_PATH, return_value=mock_session),
      patch(SVC_PATH, return_value=mock_svc),
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

    call_kwargs = mock_svc.create_schedule.call_args[1]
    assert call_kwargs["entry_template"].auto_reverse is False

  @pytest.mark.asyncio
  async def test_handles_error(self, mock_graph_client):
    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(side_effect=Exception("db error"))
    mock_session.__exit__ = MagicMock(return_value=False)

    tool = CreateScheduleTool(mock_graph_client)
    with patch(SESSION_PATH, return_value=mock_session):
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


class TestListScheduleStructuresTool:
  def test_tool_definition(self, mock_graph_client):
    tool = ListScheduleStructuresTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "list-schedule-structures"
    assert "inputSchema" in defn

  @pytest.mark.asyncio
  async def test_returns_schedules(self, mock_graph_client):
    from robosystems.operations.schedules.service import ScheduleSummary

    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    mock_svc = MagicMock()
    mock_svc.list_schedules.return_value = [
      ScheduleSummary(
        structure_id="struct_01",
        name="Depreciation",
        taxonomy_name="Schedules",
        entry_template={"debit_element_id": "elem_a"},
        schedule_metadata=None,
        total_periods=84,
        periods_with_entries=3,
      ),
    ]

    tool = ListScheduleStructuresTool(mock_graph_client)
    with (
      patch(SESSION_PATH, return_value=mock_session),
      patch(SVC_PATH, return_value=mock_svc),
    ):
      result = await tool.execute({})

    assert result["schedule_count"] == 1
    assert result["schedules"][0]["name"] == "Depreciation"

  @pytest.mark.asyncio
  async def test_handles_error(self, mock_graph_client):
    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(side_effect=Exception("bad graph"))
    mock_session.__exit__ = MagicMock(return_value=False)

    tool = ListScheduleStructuresTool(mock_graph_client)
    with patch(SESSION_PATH, return_value=mock_session):
      result = await tool.execute({})

    assert "error" in result


class TestGetScheduleFactsTool:
  def test_tool_definition(self, mock_graph_client):
    tool = GetScheduleFactsTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "get-schedule-facts"
    assert "structure_id" in defn["inputSchema"]["properties"]

  @pytest.mark.asyncio
  async def test_returns_facts(self, mock_graph_client):
    from robosystems.operations.schedules.service import ScheduleFact

    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    mock_svc = MagicMock()
    mock_svc.get_schedule_facts.return_value = [
      ScheduleFact(
        element_id="elem_depr",
        element_name="Depreciation Expense",
        value=416.67,
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
      ),
    ]

    tool = GetScheduleFactsTool(mock_graph_client)
    with (
      patch(SESSION_PATH, return_value=mock_session),
      patch(SVC_PATH, return_value=mock_svc),
    ):
      result = await tool.execute({"structure_id": "struct_01"})

    assert result["fact_count"] == 1
    assert result["facts"][0]["value"] == 416.67

  @pytest.mark.asyncio
  async def test_parses_date_strings(self, mock_graph_client):
    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    mock_svc = MagicMock()
    mock_svc.get_schedule_facts.return_value = []

    tool = GetScheduleFactsTool(mock_graph_client)
    with (
      patch(SESSION_PATH, return_value=mock_session),
      patch(SVC_PATH, return_value=mock_svc),
    ):
      await tool.execute(
        {
          "structure_id": "struct_01",
          "period_start": "2026-01-01",
          "period_end": "2026-01-31",
        }
      )

    # Verify dates were parsed and passed to service
    call_args = mock_svc.get_schedule_facts.call_args
    assert call_args[0][1] == "struct_01"
    assert call_args[0][2] == date(2026, 1, 1)
    assert call_args[0][3] == date(2026, 1, 31)


class TestGetPeriodCloseStatusTool:
  def test_tool_definition(self, mock_graph_client):
    tool = GetPeriodCloseStatusTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "get-period-close-status"
    assert "period_start" in defn["inputSchema"]["required"]

  @pytest.mark.asyncio
  async def test_returns_status(self, mock_graph_client):
    from robosystems.operations.schedules.service import (
      PeriodCloseItem,
      PeriodCloseStatus,
    )

    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    mock_svc = MagicMock()
    mock_svc.get_period_close_status.return_value = PeriodCloseStatus(
      fiscal_period_start=date(2026, 1, 1),
      fiscal_period_end=date(2026, 1, 31),
      period_status="open",
      schedules=[
        PeriodCloseItem(
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
      patch(SESSION_PATH, return_value=mock_session),
      patch(SVC_PATH, return_value=mock_svc),
    ):
      result = await tool.execute(
        {
          "period_start": "2026-01-01",
          "period_end": "2026-01-31",
        }
      )

    assert result["period_status"] == "open"
    assert result["schedules"]["total"] == 1
    assert result["schedules"]["pending"] == 1

  @pytest.mark.asyncio
  async def test_includes_reversal_fields(self, mock_graph_client):
    from robosystems.operations.schedules.service import (
      PeriodCloseItem,
      PeriodCloseStatus,
    )

    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    mock_svc = MagicMock()
    mock_svc.get_period_close_status.return_value = PeriodCloseStatus(
      fiscal_period_start=date(2026, 4, 1),
      fiscal_period_end=date(2026, 4, 30),
      period_status="open",
      schedules=[
        PeriodCloseItem(
          structure_id="struct_accrual",
          structure_name="Payroll Accrual",
          amount=3200.00,
          status="drafted",
          entry_id="je_01ABC",
          reversal_entry_id="je_01DEF",
          reversal_status="draft",
        ),
        PeriodCloseItem(
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
      patch(SESSION_PATH, return_value=mock_session),
      patch(SVC_PATH, return_value=mock_svc),
    ):
      result = await tool.execute(
        {"period_start": "2026-04-01", "period_end": "2026-04-30"}
      )

    details = result["schedules"]["details"]
    # Accrual has reversal
    assert details[0]["reversal_entry_id"] == "je_01DEF"
    assert details[0]["reversal_status"] == "draft"
    # Depreciation has no reversal
    assert details[1]["reversal_entry_id"] is None
    assert details[1]["reversal_status"] is None


class TestCreateClosingEntryTool:
  def test_tool_definition(self, mock_graph_client):
    tool = CreateClosingEntryTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "create-closing-entry"
    assert "structure_id" in defn["inputSchema"]["required"]
    assert "posting_date" in defn["inputSchema"]["required"]

  @pytest.mark.asyncio
  async def test_creates_entry(self, mock_graph_client):
    from robosystems.operations.schedules.service import ClosingEntryResult

    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    mock_svc = MagicMock()
    mock_svc.create_closing_entry.return_value = ClosingEntryResult(
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
      patch(SESSION_PATH, return_value=mock_session),
      patch(SVC_PATH, return_value=mock_svc),
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
    from robosystems.operations.schedules.service import ClosingEntryResult

    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    mock_svc = MagicMock()
    mock_svc.create_closing_entry.return_value = ClosingEntryResult(
      entry_id="je_01ABC",
      status="draft",
      posting_date=date(2026, 1, 31),
      memo="Accrue payroll",
      debit_element_id="elem_salary",
      credit_element_id="elem_accrued",
      amount=3200.00,
      reversal=ClosingEntryResult(
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
      patch(SESSION_PATH, return_value=mock_session),
      patch(SVC_PATH, return_value=mock_svc),
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
    # Verify DR/CR are flipped
    assert rev["line_items"][0]["element_id"] == "elem_accrued"
    assert rev["line_items"][0]["debit"] == 3200.00
    assert rev["line_items"][1]["element_id"] == "elem_salary"
    assert rev["line_items"][1]["credit"] == 3200.00

  @pytest.mark.asyncio
  async def test_returns_error_for_duplicate(self, mock_graph_client):
    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    mock_svc = MagicMock()
    mock_svc.create_closing_entry.side_effect = ValueError("already exists")

    tool = CreateClosingEntryTool(mock_graph_client)
    with (
      patch(SESSION_PATH, return_value=mock_session),
      patch(SVC_PATH, return_value=mock_svc),
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
