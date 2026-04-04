"""Tests for schedule MCP tools."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.schedule_tools import (
  CreateClosingEntryTool,
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
