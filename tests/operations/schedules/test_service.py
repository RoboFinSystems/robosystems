"""Tests for ScheduleService — schedule lifecycle operations."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.schedules.service import (
  EntryTemplate,
  ScheduleMetadata,
  ScheduleService,
  _generate_monthly_periods,
)

# ── Utility tests ────────────────────────────────────────────────────────


class TestGenerateMonthlyPeriods:
  def test_single_month(self):
    periods = _generate_monthly_periods(date(2026, 3, 1), date(2026, 3, 31))
    assert len(periods) == 1
    assert periods[0] == (date(2026, 3, 1), date(2026, 3, 31))

  def test_full_year(self):
    periods = _generate_monthly_periods(date(2026, 1, 1), date(2026, 12, 31))
    assert len(periods) == 12
    assert periods[0] == (date(2026, 1, 1), date(2026, 1, 31))
    assert periods[11] == (date(2026, 12, 1), date(2026, 12, 31))

  def test_february_non_leap(self):
    periods = _generate_monthly_periods(date(2026, 2, 1), date(2026, 2, 28))
    assert len(periods) == 1
    assert periods[0] == (date(2026, 2, 1), date(2026, 2, 28))

  def test_february_leap_year(self):
    periods = _generate_monthly_periods(date(2028, 2, 1), date(2028, 2, 29))
    assert len(periods) == 1
    assert periods[0] == (date(2028, 2, 1), date(2028, 2, 29))

  def test_cross_year_boundary(self):
    periods = _generate_monthly_periods(date(2025, 11, 1), date(2026, 2, 28))
    assert len(periods) == 4
    assert periods[0] == (date(2025, 11, 1), date(2025, 11, 30))
    assert periods[1] == (date(2025, 12, 1), date(2025, 12, 31))
    assert periods[2] == (date(2026, 1, 1), date(2026, 1, 31))
    assert periods[3] == (date(2026, 2, 1), date(2026, 2, 28))

  def test_84_months(self):
    """7-year depreciation schedule."""
    periods = _generate_monthly_periods(date(2026, 1, 1), date(2032, 12, 31))
    assert len(periods) == 84

  def test_mid_month_start(self):
    """Start date mid-month still generates from first of that month."""
    periods = _generate_monthly_periods(date(2026, 3, 15), date(2026, 5, 31))
    assert len(periods) == 3
    assert periods[0][0] == date(2026, 3, 1)


# ── Service tests (mocked DB) ───────────────────────────────────────────


SVC_MODULE = "robosystems.operations.schedules.service"


def _mock_session():
  """Create a mock SQLAlchemy session."""
  session = MagicMock()
  session.execute.return_value = MagicMock()
  return session


def _make_entry_template():
  return EntryTemplate(
    debit_element_id="elem_depr_expense",
    credit_element_id="elem_accum_depr",
    entry_type="closing",
    memo_template="Monthly depreciation - {structure_name}",
  )


def _make_schedule_metadata():
  return ScheduleMetadata(
    method="straight_line",
    original_amount=3500000,
    residual_value=0,
    useful_life_months=84,
    asset_element_id="elem_ppe",
  )


class TestCreateSchedule:
  def test_creates_structure_and_facts(self):
    session = _mock_session()
    svc = ScheduleService()

    # Mock _ensure_schedule_taxonomy
    session.execute.return_value.fetchone.return_value = MagicMock(id="tax_sched_01")
    # Mock _get_entity_id
    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="Office Furniture Depreciation",
        taxonomy_id=None,
        element_ids=["elem_depr_expense", "elem_accum_depr"],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        monthly_amount=41667,
        entry_template=_make_entry_template(),
        schedule_metadata=_make_schedule_metadata(),
        created_by="usr_test",
      )

    # Structure was added to session
    assert session.add.called
    # Multiple adds: 1 structure + 2 associations + 3 months * 3 facts/month = 12
    assert session.add.call_count >= 12

  def test_rounding_prevents_drift(self):
    """Verify accumulated values don't drift over many periods."""
    session = _mock_session()
    svc = ScheduleService()

    session.execute.return_value.fetchone.return_value = MagicMock(id="tax_01")

    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="Test",
        taxonomy_id="tax_01",
        element_ids=["elem_a", "elem_b"],
        period_start=date(2026, 1, 1),
        period_end=date(2032, 12, 31),
        monthly_amount=41667,  # $416.67/month — repeating decimal
        entry_template=EntryTemplate(
          debit_element_id="elem_a",
          credit_element_id="elem_b",
        ),
        created_by="usr_test",
      )

    # Check all Fact objects added have properly rounded values
    for call in session.add.call_args_list:
      obj = call[0][0]
      if hasattr(obj, "value") and obj.value is not None:
        # Value should have at most 2 decimal places
        assert round(obj.value, 2) == obj.value, f"Unrounded value: {obj.value}"


class TestGetScheduleFacts:
  def test_raises_for_nonexistent_schedule(self):
    session = _mock_session()
    session.get.return_value = None  # Structure not found
    svc = ScheduleService()

    with pytest.raises(ValueError, match="not found"):
      svc.get_schedule_facts(session, "struct_nonexistent")

  def test_raises_for_wrong_type(self):
    session = _mock_session()
    mock_struct = MagicMock()
    mock_struct.structure_type = "income_statement"
    session.get.return_value = mock_struct
    svc = ScheduleService()

    with pytest.raises(ValueError, match="not found"):
      svc.get_schedule_facts(session, "struct_wrong_type")

  def test_returns_facts_for_valid_schedule(self):
    session = _mock_session()
    mock_struct = MagicMock()
    mock_struct.structure_type = "schedule"
    session.get.return_value = mock_struct

    mock_row = MagicMock()
    mock_row.element_id = "elem_depr"
    mock_row.element_name = "Depreciation Expense"
    mock_row.value = 416.67
    mock_row.period_start = date(2026, 1, 1)
    mock_row.period_end = date(2026, 1, 31)
    session.execute.return_value = [mock_row]

    svc = ScheduleService()
    facts = svc.get_schedule_facts(session, "struct_valid")

    assert len(facts) == 1
    assert facts[0].element_name == "Depreciation Expense"
    assert facts[0].value == 416.67


class TestCreateClosingEntry:
  def _mock_schedule_structure(self):
    struct = MagicMock()
    struct.structure_type = "schedule"
    struct.name = "Office Furniture Depreciation"
    struct.metadata_ = {
      "entry_template": {
        "debit_element_id": "elem_depr_expense",
        "credit_element_id": "elem_accum_depr",
        "entry_type": "closing",
        "memo_template": "Monthly depreciation - {structure_name}",
      }
    }
    return struct

  def test_raises_for_nonexistent_schedule(self):
    session = _mock_session()
    session.get.return_value = None
    svc = ScheduleService()

    with pytest.raises(ValueError, match="not found"):
      svc.create_closing_entry(
        session,
        structure_id="struct_missing",
        posting_date=date(2026, 1, 31),
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        created_by="usr_test",
      )

  def test_raises_for_missing_template(self):
    session = _mock_session()
    struct = MagicMock()
    struct.structure_type = "schedule"
    struct.metadata_ = {}
    session.get.return_value = struct
    svc = ScheduleService()

    with pytest.raises(ValueError, match="no entry template"):
      svc.create_closing_entry(
        session,
        structure_id="struct_no_template",
        posting_date=date(2026, 1, 31),
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        created_by="usr_test",
      )

  def test_raises_for_duplicate_entry(self):
    session = _mock_session()
    session.get.return_value = self._mock_schedule_structure()
    # First execute returns existing entry
    session.execute.return_value.fetchone.return_value = MagicMock(id="je_existing")
    svc = ScheduleService()

    with pytest.raises(ValueError, match="already exists"):
      svc.create_closing_entry(
        session,
        structure_id="struct_01",
        posting_date=date(2026, 1, 31),
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        created_by="usr_test",
      )

  def test_raises_for_missing_fact(self):
    session = _mock_session()
    session.get.return_value = self._mock_schedule_structure()
    # First execute: no existing entry; second execute: no fact
    session.execute.return_value.fetchone.side_effect = [None, None]
    svc = ScheduleService()

    with pytest.raises(ValueError, match="No fact found"):
      svc.create_closing_entry(
        session,
        structure_id="struct_01",
        posting_date=date(2026, 1, 31),
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        created_by="usr_test",
      )

  def test_creates_draft_entry_with_line_items(self):
    session = _mock_session()
    session.get.return_value = self._mock_schedule_structure()
    # No existing entry, then fact found
    session.execute.return_value.fetchone.side_effect = [
      None,  # no existing entry
      MagicMock(value=416.67),  # fact value
    ]
    svc = ScheduleService()

    result = svc.create_closing_entry(
      session,
      structure_id="struct_01",
      posting_date=date(2026, 1, 31),
      period_start=date(2026, 1, 1),
      period_end=date(2026, 1, 31),
      created_by="usr_test",
    )

    assert result.status == "draft"
    assert result.amount == 416.67
    assert result.debit_element_id == "elem_depr_expense"
    assert result.credit_element_id == "elem_accum_depr"
    assert result.memo == "Monthly depreciation - Office Furniture Depreciation"
    # Entry + 2 LineItems = 3 adds
    assert session.add.call_count >= 3

  def test_closing_entry_has_provenance(self):
    """Closing entries from schedules must have provenance='schedule_derived'."""
    session = _mock_session()
    session.get.return_value = self._mock_schedule_structure()
    session.execute.return_value.fetchone.side_effect = [
      None,
      MagicMock(value=416.67),
    ]
    svc = ScheduleService()

    svc.create_closing_entry(
      session,
      structure_id="struct_01",
      posting_date=date(2026, 1, 31),
      period_start=date(2026, 1, 1),
      period_end=date(2026, 1, 31),
      created_by="usr_test",
    )

    # Find the Entry object that was added to the session
    from robosystems.operations.schedules.service import Entry

    entries = [
      call[0][0] for call in session.add.call_args_list if isinstance(call[0][0], Entry)
    ]
    assert len(entries) >= 1
    assert entries[0].provenance == "schedule_derived"

  def test_memo_override(self):
    session = _mock_session()
    session.get.return_value = self._mock_schedule_structure()
    session.execute.return_value.fetchone.side_effect = [
      None,
      MagicMock(value=416.67),
    ]
    svc = ScheduleService()

    result = svc.create_closing_entry(
      session,
      structure_id="struct_01",
      posting_date=date(2026, 1, 31),
      period_start=date(2026, 1, 1),
      period_end=date(2026, 1, 31),
      created_by="usr_test",
      memo="Custom memo for January",
    )

    assert result.memo == "Custom memo for January"
