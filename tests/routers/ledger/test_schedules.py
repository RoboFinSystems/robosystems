"""Unit tests for ledger schedule endpoints."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.api.extensions.schedules import (
  CreateClosingEntryRequest,
  CreateScheduleRequest,
  EntryTemplateRequest,
)
from robosystems.routers.ledger.schedules import (
  create_closing_entry,
  create_schedule,
  get_period_close_status,
  get_schedule_facts,
  list_schedules,
)

MODULE = "robosystems.routers.ledger.schedules"
GRAPH_ID = "kg01234567890abcdef"


def _make_user():
  user = MagicMock()
  user.id = "usr_test123"
  return user


class TestCreateSchedule:
  @pytest.mark.asyncio
  async def test_creates_schedule_successfully(self):
    mock_structure = MagicMock()
    mock_structure.id = "struct_sched_01"
    mock_structure.name = "Test Schedule"
    mock_structure.taxonomy_id = "tax_sched_01"

    mock_svc = MagicMock()
    mock_svc.create_schedule.return_value = mock_structure

    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    # Mock the count queries
    mock_count = MagicMock()
    mock_count.fetchone.return_value = MagicMock(cnt=6)
    mock_period = MagicMock()
    mock_period.fetchone.return_value = MagicMock(cnt=3)
    mock_session.execute.side_effect = [mock_count, mock_period]

    body = CreateScheduleRequest(
      name="Test Schedule",
      element_ids=["elem_a", "elem_b"],
      period_start=date(2026, 1, 1),
      period_end=date(2026, 3, 31),
      monthly_amount=41667,
      entry_template=EntryTemplateRequest(
        debit_element_id="elem_a",
        credit_element_id="elem_b",
      ),
    )

    with (
      patch(f"{MODULE}.extensions_session", return_value=mock_session),
      patch(f"{MODULE}._svc", mock_svc),
    ):
      result = await create_schedule(
        graph_id=GRAPH_ID,
        body=body,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.structure_id == "struct_sched_01"
    assert result.name == "Test Schedule"
    mock_svc.create_schedule.assert_called_once()

  @pytest.mark.asyncio
  async def test_rejects_end_before_start(self):
    body = CreateScheduleRequest(
      name="Bad Schedule",
      element_ids=["elem_a"],
      period_start=date(2026, 6, 1),
      period_end=date(2026, 1, 1),
      monthly_amount=10000,
      entry_template=EntryTemplateRequest(
        debit_element_id="elem_a",
        credit_element_id="elem_b",
      ),
    )

    with pytest.raises(HTTPException) as exc_info:
      await create_schedule(
        graph_id=GRAPH_ID,
        body=body,
        current_user=_make_user(),
        _rate_limit=None,
      )
    assert exc_info.value.status_code == 422


class TestListSchedules:
  @pytest.mark.asyncio
  async def test_lists_schedules(self):
    from robosystems.operations.schedules.service import ScheduleSummary

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

    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    with (
      patch(f"{MODULE}.extensions_session", return_value=mock_session),
      patch(f"{MODULE}._svc", mock_svc),
    ):
      result = await list_schedules(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.schedules) == 1
    assert result.schedules[0].name == "Depreciation"
    assert result.schedules[0].total_periods == 84


class TestGetScheduleFacts:
  @pytest.mark.asyncio
  async def test_returns_facts(self):
    from robosystems.operations.schedules.service import ScheduleFact

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

    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    with (
      patch(f"{MODULE}.extensions_session", return_value=mock_session),
      patch(f"{MODULE}._svc", mock_svc),
    ):
      result = await get_schedule_facts(
        graph_id=GRAPH_ID,
        structure_id="struct_01",
        period_start=None,
        period_end=None,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.facts) == 1
    assert result.facts[0].value == 416.67

  @pytest.mark.asyncio
  async def test_schedule_not_found(self):
    mock_svc = MagicMock()
    mock_svc.get_schedule_facts.side_effect = ValueError("not found")

    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    with (
      patch(f"{MODULE}.extensions_session", return_value=mock_session),
      patch(f"{MODULE}._svc", mock_svc),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await get_schedule_facts(
          graph_id=GRAPH_ID,
          structure_id="struct_missing",
          period_start=None,
          period_end=None,
          current_user=_make_user(),
          _rate_limit=None,
        )
    assert exc_info.value.status_code == 404


class TestGetPeriodCloseStatus:
  @pytest.mark.asyncio
  async def test_returns_status(self):
    from robosystems.operations.schedules.service import (
      PeriodCloseItem,
      PeriodCloseStatus,
    )

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

    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    with (
      patch(f"{MODULE}.extensions_session", return_value=mock_session),
      patch(f"{MODULE}._svc", mock_svc),
    ):
      result = await get_period_close_status(
        graph_id=GRAPH_ID,
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.period_status == "open"
    assert len(result.schedules) == 1
    assert result.schedules[0].status == "pending"


class TestCreateClosingEntry:
  @pytest.mark.asyncio
  async def test_creates_entry(self):
    from robosystems.operations.schedules.service import ClosingEntryResult

    mock_svc = MagicMock()
    mock_svc.create_closing_entry.return_value = ClosingEntryResult(
      entry_id="je_01ABC",
      status="draft",
      posting_date=date(2026, 1, 31),
      memo="Monthly depreciation - Office Furniture",
      debit_element_id="elem_depr",
      credit_element_id="elem_accum",
      amount=416.67,
    )

    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    body = CreateClosingEntryRequest(
      posting_date=date(2026, 1, 31),
      period_start=date(2026, 1, 1),
      period_end=date(2026, 1, 31),
    )

    with (
      patch(f"{MODULE}.extensions_session", return_value=mock_session),
      patch(f"{MODULE}._svc", mock_svc),
    ):
      result = await create_closing_entry(
        graph_id=GRAPH_ID,
        structure_id="struct_01",
        body=body,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.entry_id == "je_01ABC"
    assert result.status == "draft"
    assert result.amount == 416.67

  @pytest.mark.asyncio
  async def test_duplicate_entry_returns_422(self):
    mock_svc = MagicMock()
    mock_svc.create_closing_entry.side_effect = ValueError("already exists")

    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)

    body = CreateClosingEntryRequest(
      posting_date=date(2026, 1, 31),
      period_start=date(2026, 1, 1),
      period_end=date(2026, 1, 31),
    )

    with (
      patch(f"{MODULE}.extensions_session", return_value=mock_session),
      patch(f"{MODULE}._svc", mock_svc),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await create_closing_entry(
          graph_id=GRAPH_ID,
          structure_id="struct_01",
          body=body,
          current_user=_make_user(),
          _rate_limit=None,
        )
    assert exc_info.value.status_code == 422
