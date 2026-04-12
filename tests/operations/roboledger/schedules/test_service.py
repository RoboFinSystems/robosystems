"""Tests for ScheduleService — schedule lifecycle operations."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.roboledger.schedules.service import (
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


SVC_MODULE = "robosystems.operations.roboledger.schedules.service"


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

  def test_facts_default_to_in_scope(self):
    """Without closed_through, all facts should be in_scope."""
    session = _mock_session()
    svc = ScheduleService()

    session.execute.return_value.fetchone.return_value = MagicMock(id="tax_01")

    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="No Scope",
        taxonomy_id="tax_01",
        element_ids=["elem_a", "elem_b"],
        period_start=date(2025, 1, 1),
        period_end=date(2026, 12, 31),
        monthly_amount=10000,
        entry_template=_make_entry_template(),
        created_by="usr_test",
      )

    fact_objects = [
      call[0][0]
      for call in session.add.call_args_list
      if hasattr(call[0][0], "fact_scope")
    ]
    assert len(fact_objects) > 0
    assert all(f.fact_scope == "in_scope" for f in fact_objects)

  def test_closed_through_splits_historical_and_in_scope(self):
    """Facts with period_end ≤ closed_through → historical; else → in_scope."""
    session = _mock_session()
    svc = ScheduleService()

    session.execute.return_value.fetchone.return_value = MagicMock(id="tax_01")

    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="Scoped Schedule",
        taxonomy_id="tax_01",
        element_ids=["elem_a", "elem_b"],
        period_start=date(2025, 1, 1),
        period_end=date(2025, 12, 31),  # 12 monthly periods
        monthly_amount=10000,
        entry_template=_make_entry_template(),
        created_by="usr_test",
        closed_through=date(2025, 6, 30),
      )

    fact_objects = [
      call[0][0]
      for call in session.add.call_args_list
      if hasattr(call[0][0], "fact_scope")
    ]
    # 2 facts per month x 12 months = 24 fact objects
    assert len(fact_objects) == 24

    historical = [f for f in fact_objects if f.fact_scope == "historical"]
    in_scope = [f for f in fact_objects if f.fact_scope == "in_scope"]

    # Jan-Jun (6 months x 2 facts = 12) = historical
    assert len(historical) == 12
    # Jul-Dec (6 months x 2 facts = 12) = in_scope
    assert len(in_scope) == 12

    # Cross-check: every historical fact's period ends on/before Jun 30
    for f in historical:
      assert f.period_end <= date(2025, 6, 30)
    for f in in_scope:
      assert f.period_end > date(2025, 6, 30)

  def test_closed_through_on_period_boundary(self):
    """closed_through exactly matching a period_end classifies that period as historical."""
    session = _mock_session()
    svc = ScheduleService()

    session.execute.return_value.fetchone.return_value = MagicMock(id="tax_01")

    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="Boundary Test",
        taxonomy_id="tax_01",
        element_ids=["elem_a", "elem_b"],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        monthly_amount=10000,
        entry_template=_make_entry_template(),
        created_by="usr_test",
        closed_through=date(2026, 2, 28),  # End of February
      )

    fact_objects = [
      call[0][0]
      for call in session.add.call_args_list
      if hasattr(call[0][0], "fact_scope")
    ]
    # Jan (2 facts) + Feb (2 facts) = 4 historical. Mar (2 facts) = 2 in_scope.
    historical = [f for f in fact_objects if f.fact_scope == "historical"]
    in_scope = [f for f in fact_objects if f.fact_scope == "in_scope"]
    assert len(historical) == 4
    assert len(in_scope) == 2

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

  def test_outcome_skipped_when_no_existing_no_fact(self):
    """No prior draft and no in-scope fact → outcome='skipped'."""
    session = _mock_session()
    session.get.return_value = self._mock_schedule_structure()
    # Sequence: existing-entry lookup → None; fact lookup → None
    session.execute.return_value.fetchone.side_effect = [None, None]
    svc = ScheduleService()

    result = svc.create_closing_entry(
      session,
      structure_id="struct_01",
      posting_date=date(2026, 1, 31),
      period_start=date(2026, 1, 1),
      period_end=date(2026, 1, 31),
      created_by="usr_test",
    )
    assert result.outcome == "skipped"
    assert result.entry_id is None
    assert result.reason is not None

  def test_outcome_removed_when_existing_but_no_fact(self):
    """Prior draft exists but schedule no longer covers the period → removed."""
    session = _mock_session()
    session.get.return_value = self._mock_schedule_structure()
    # Sequence: existing-entry → a draft row; fact lookup → None
    session.execute.return_value.fetchone.side_effect = [
      MagicMock(id="je_stale", status="draft"),
      None,  # No fact anymore
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
    assert result.outcome == "removed"
    assert result.entry_id is None
    assert "stale" in (result.reason or "").lower() or "no longer" in (
      result.reason or ""
    )

  def test_outcome_created_when_no_existing(self):
    """No prior draft, fact exists → new draft created."""
    session = _mock_session()
    session.get.return_value = self._mock_schedule_structure()
    # Sequence: existing-entry → None; fact lookup → value
    session.execute.return_value.fetchone.side_effect = [
      None,  # no existing
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
    )

    assert result.outcome == "created"
    assert result.status == "draft"
    assert result.amount == 416.67
    assert result.debit_element_id == "elem_depr_expense"
    assert result.credit_element_id == "elem_accum_depr"
    assert result.memo == "Monthly depreciation - Office Furniture Depreciation"
    assert session.add.call_count >= 3

  def test_outcome_unchanged_when_existing_draft_matches(self):
    """Existing draft matches current fact → no-op, outcome='unchanged'."""
    session = _mock_session()
    session.get.return_value = self._mock_schedule_structure()
    # Sequence:
    #   1. existing-entry query → returns draft row
    #   2. fact lookup → value 416.67
    #   3. staleness comparison query → current draft details matching fact
    staleness_row = MagicMock(
      memo="Monthly depreciation - Office Furniture Depreciation",
      dr_element="elem_depr_expense",
      dr_amount=41667,  # cents
      cr_element="elem_accum_depr",
      cr_amount=41667,
    )
    session.execute.return_value.fetchone.side_effect = [
      MagicMock(id="je_existing", status="draft"),
      MagicMock(value=416.67),
      staleness_row,
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
    assert result.outcome == "unchanged"
    assert result.entry_id == "je_existing"
    # No entries were added (no create path)
    from robosystems.operations.roboledger.schedules.service import Entry

    entries_added = [
      c[0][0] for c in session.add.call_args_list if isinstance(c[0][0], Entry)
    ]
    assert len(entries_added) == 0

  def test_outcome_regenerated_when_existing_draft_is_stale(self):
    """Existing draft's amount differs from current fact → stale → regenerated."""
    session = _mock_session()
    session.get.return_value = self._mock_schedule_structure()
    # Schedule fact now says 500.00 but the existing draft has 416.67
    staleness_row = MagicMock(
      memo="old memo",
      dr_element="elem_depr_expense",
      dr_amount=41667,  # old cents
      cr_element="elem_accum_depr",
      cr_amount=41667,
    )
    session.execute.return_value.fetchone.side_effect = [
      MagicMock(id="je_stale", status="draft"),
      MagicMock(value=500.00),  # new amount from edited schedule
      staleness_row,
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
    assert result.outcome == "regenerated"
    assert result.amount == 500.00
    # A new entry was added to replace the old one
    from robosystems.operations.roboledger.schedules.service import Entry

    entries_added = [
      c[0][0] for c in session.add.call_args_list if isinstance(c[0][0], Entry)
    ]
    assert len(entries_added) >= 1

  def test_raises_for_existing_posted_entry(self):
    """An already-posted entry must not be regenerated — requires reopen flow."""
    session = _mock_session()
    session.get.return_value = self._mock_schedule_structure()
    session.execute.return_value.fetchone.side_effect = [
      MagicMock(id="je_posted", status="posted"),
    ]
    svc = ScheduleService()

    with pytest.raises(ValueError, match="already been posted"):
      svc.create_closing_entry(
        session,
        structure_id="struct_01",
        posting_date=date(2026, 1, 31),
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        created_by="usr_test",
      )

  def test_closing_entry_has_provenance(self):
    """Schedule-derived entries must have provenance='schedule_derived'."""
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

    from robosystems.operations.roboledger.schedules.service import Entry

    entries = [
      c[0][0] for c in session.add.call_args_list if isinstance(c[0][0], Entry)
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
    assert result.outcome == "created"


class TestCreateManualClosingEntry:
  def test_creates_balanced_4_line_disposal_entry(self):
    """Classic asset disposal: 4 lines, DR Cash + DR AccumDepr, CR Asset + CR Gain."""
    session = _mock_session()
    svc = ScheduleService()

    result = svc.create_manual_closing_entry(
      session,
      posting_date=date(2026, 3, 15),
      line_items=[
        {"element_id": "elem_cash", "debit_amount": 300000},
        {"element_id": "elem_accum_depr", "debit_amount": 186662},
        {"element_id": "elem_computer", "credit_amount": 480000},
        {"element_id": "elem_gain", "credit_amount": 6662},
      ],
      memo="Sold computer to Vendor X",
      created_by="usr_test",
    )

    assert result.outcome == "created"
    assert result.status == "draft"
    assert result.amount == 4866.62  # total DR in dollars
    assert result.memo == "Sold computer to Vendor X"
    # 1 entry + 4 line items = 5 session.add calls
    from robosystems.operations.roboledger.schedules.service import Entry, LineItem

    entries = [
      c[0][0] for c in session.add.call_args_list if isinstance(c[0][0], Entry)
    ]
    line_items = [
      c[0][0] for c in session.add.call_args_list if isinstance(c[0][0], LineItem)
    ]
    assert len(entries) == 1
    assert entries[0].provenance == "manual_entry"
    assert entries[0].source_structure_id is None
    assert len(line_items) == 4

  def test_rejects_unbalanced_entry(self):
    session = _mock_session()
    svc = ScheduleService()
    with pytest.raises(ValueError, match="does not balance"):
      svc.create_manual_closing_entry(
        session,
        posting_date=date(2026, 3, 15),
        line_items=[
          {"element_id": "elem_a", "debit_amount": 100000},
          {"element_id": "elem_b", "credit_amount": 90000},  # off by $100
        ],
        memo="Unbalanced",
        created_by="usr_test",
      )

  def test_rejects_posting_date_in_closed_period(self):
    """F4: refuse to draft a manual entry whose posting_date falls in a
    closed fiscal period — the draft would be orphaned because close-period
    won't re-close a closed month.
    """
    session = _mock_session()
    # Simulate a closed fiscal period row covering the posting_date
    closed_row = MagicMock(name="2026-03", status="closed")
    closed_row.name = "2026-03"
    closed_row.status = "closed"
    session.execute.return_value.fetchone.return_value = closed_row

    svc = ScheduleService()
    with pytest.raises(ValueError, match="closed period"):
      svc.create_manual_closing_entry(
        session,
        posting_date=date(2026, 3, 15),
        line_items=[
          {"element_id": "elem_a", "debit_amount": 100},
          {"element_id": "elem_b", "credit_amount": 100},
        ],
        memo="Adjustment in closed period",
        created_by="usr_test",
      )

  def test_rejects_empty_memo(self):
    session = _mock_session()
    svc = ScheduleService()
    with pytest.raises(ValueError, match="non-empty memo"):
      svc.create_manual_closing_entry(
        session,
        posting_date=date(2026, 3, 15),
        line_items=[
          {"element_id": "elem_a", "debit_amount": 100},
          {"element_id": "elem_b", "credit_amount": 100},
        ],
        memo="",
        created_by="usr_test",
      )

  def test_rejects_empty_line_items(self):
    session = _mock_session()
    svc = ScheduleService()
    with pytest.raises(ValueError, match="at least one line"):
      svc.create_manual_closing_entry(
        session,
        posting_date=date(2026, 3, 15),
        line_items=[],
        memo="No lines",
        created_by="usr_test",
      )

  def test_rejects_line_with_both_debit_and_credit(self):
    session = _mock_session()
    svc = ScheduleService()
    with pytest.raises(ValueError, match="cannot have both"):
      svc.create_manual_closing_entry(
        session,
        posting_date=date(2026, 3, 15),
        line_items=[
          {"element_id": "elem_a", "debit_amount": 100, "credit_amount": 100},
        ],
        memo="Invalid",
        created_by="usr_test",
      )

  def test_rejects_line_with_zero_on_both_sides(self):
    session = _mock_session()
    svc = ScheduleService()
    with pytest.raises(ValueError, match="non-zero"):
      svc.create_manual_closing_entry(
        session,
        posting_date=date(2026, 3, 15),
        line_items=[
          {"element_id": "elem_a", "debit_amount": 0, "credit_amount": 0},
        ],
        memo="Invalid",
        created_by="usr_test",
      )

  def test_rejects_missing_element_id(self):
    session = _mock_session()
    svc = ScheduleService()
    with pytest.raises(ValueError, match="missing element_id"):
      svc.create_manual_closing_entry(
        session,
        posting_date=date(2026, 3, 15),
        line_items=[
          {"debit_amount": 100},
          {"element_id": "elem_b", "credit_amount": 100},
        ],
        memo="Missing",
        created_by="usr_test",
      )


class TestTruncateSchedule:
  def _mock_schedule_structure(self, metadata: dict | None = None):
    struct = MagicMock()
    struct.id = "struct_01"
    struct.structure_type = "schedule"
    struct.name = "Computer Equipment Depreciation"
    struct.metadata_ = metadata or {
      "entry_template": {
        "debit_element_id": "elem_depr_expense",
        "credit_element_id": "elem_accum_depr",
      },
      "schedule_metadata": {
        "method": "straight_line",
        "original_amount": 480000,
        "useful_life_months": 36,
      },
    }
    return struct

  def test_happy_path_deletes_future_facts(self):
    session = _mock_session()
    session.get.return_value = self._mock_schedule_structure()
    # Sequence: bounds query → (first_start, last_end), posted overlap → 0
    session.execute.return_value.fetchone.side_effect = [
      MagicMock(first_start=date(2025, 1, 1), last_end=date(2027, 12, 31)),
      MagicMock(c=0),
    ]
    # DELETE rowcount for facts = 15
    delete_result = MagicMock(rowcount=15)
    # The sequence of session.execute calls:
    # 1. bounds (SELECT) — fetchone
    # 2. overlap (SELECT) — fetchone
    # 3. DELETE line_items (draft stale)
    # 4. DELETE entries (draft stale)
    # 5. DELETE facts → rowcount=15
    session.execute.side_effect = [
      MagicMock(
        fetchone=MagicMock(
          return_value=MagicMock(
            first_start=date(2025, 1, 1), last_end=date(2027, 12, 31)
          )
        )
      ),
      MagicMock(fetchone=MagicMock(return_value=MagicMock(c=0))),
      MagicMock(),  # delete line_items
      MagicMock(),  # delete entries
      delete_result,  # delete facts
    ]

    svc = ScheduleService()
    result = svc.truncate_schedule(
      session,
      structure_id="struct_01",
      new_end_date=date(2026, 3, 31),
      reason="Sold the computer",
      updated_by="usr_test",
    )

    assert result["facts_deleted"] == 15
    assert result["new_end_date"] == date(2026, 3, 31)
    assert result["reason"] == "Sold the computer"

  def test_rejects_empty_reason(self):
    session = _mock_session()
    svc = ScheduleService()
    with pytest.raises(ValueError, match="non-empty reason"):
      svc.truncate_schedule(
        session,
        structure_id="struct_01",
        new_end_date=date(2026, 3, 31),
        reason="",
        updated_by="usr_test",
      )

  def test_rejects_mid_month_new_end_date(self):
    """F5: truncate_schedule must require the last day of the month to avoid
    ambiguous 'keep partial March' semantics — schedule facts are full-month.
    """
    session = _mock_session()
    svc = ScheduleService()
    with pytest.raises(ValueError, match="last day of the month"):
      svc.truncate_schedule(
        session,
        structure_id="struct_01",
        new_end_date=date(2026, 3, 15),
        reason="Sold mid-month",
        updated_by="usr_test",
      )

  def test_accepts_february_non_leap_28th(self):
    """F5: Feb 28 (non-leap) is the last day of February → accepted."""
    session = _mock_session()
    session.get.return_value = self._mock_schedule_structure()
    session.execute.side_effect = [
      MagicMock(
        fetchone=MagicMock(
          return_value=MagicMock(
            first_start=date(2025, 1, 1), last_end=date(2027, 12, 31)
          )
        )
      ),
      MagicMock(fetchone=MagicMock(return_value=MagicMock(c=0))),
      MagicMock(),
      MagicMock(),
      MagicMock(rowcount=10),
    ]
    svc = ScheduleService()
    result = svc.truncate_schedule(
      session,
      structure_id="struct_01",
      new_end_date=date(2026, 2, 28),
      reason="Cancelled end of Feb",
      updated_by="usr_test",
    )
    assert result["new_end_date"] == date(2026, 2, 28)

  def test_accepts_february_leap_29th(self):
    """F5: Feb 29 (leap year) is the last day of February → accepted."""
    session = _mock_session()
    session.get.return_value = self._mock_schedule_structure()
    session.execute.side_effect = [
      MagicMock(
        fetchone=MagicMock(
          return_value=MagicMock(
            first_start=date(2027, 1, 1), last_end=date(2029, 12, 31)
          )
        )
      ),
      MagicMock(fetchone=MagicMock(return_value=MagicMock(c=0))),
      MagicMock(),
      MagicMock(),
      MagicMock(rowcount=10),
    ]
    svc = ScheduleService()
    result = svc.truncate_schedule(
      session,
      structure_id="struct_01",
      new_end_date=date(2028, 2, 29),  # 2028 is a leap year
      reason="Sold end of Feb",
      updated_by="usr_test",
    )
    assert result["new_end_date"] == date(2028, 2, 29)

  def test_rejects_february_non_leap_29th(self):
    """F5: Feb 29 in a non-leap year isn't a valid date at all, but if it
    were constructed somehow, the tool would reject it because it's not
    the last day. We use Feb 28 in 2026 (non-leap) and confirm Feb 27 is
    rejected as mid-month.
    """
    session = _mock_session()
    svc = ScheduleService()
    with pytest.raises(ValueError, match="last day of the month"):
      svc.truncate_schedule(
        session,
        structure_id="struct_01",
        new_end_date=date(2026, 2, 27),
        reason="too early",
        updated_by="usr_test",
      )

  def test_rejects_nonexistent_schedule(self):
    session = _mock_session()
    session.get.return_value = None
    svc = ScheduleService()
    with pytest.raises(ValueError, match="not found"):
      svc.truncate_schedule(
        session,
        structure_id="missing",
        new_end_date=date(2026, 3, 31),
        reason="test",
        updated_by="usr_test",
      )

  def test_rejects_when_posted_entries_exist_after_new_end(self):
    session = _mock_session()
    session.get.return_value = self._mock_schedule_structure()
    session.execute.side_effect = [
      MagicMock(
        fetchone=MagicMock(
          return_value=MagicMock(
            first_start=date(2025, 1, 1), last_end=date(2027, 12, 31)
          )
        )
      ),
      MagicMock(fetchone=MagicMock(return_value=MagicMock(c=2))),  # 2 posted after
    ]
    svc = ScheduleService()
    with pytest.raises(ValueError, match="posted entries"):
      svc.truncate_schedule(
        session,
        structure_id="struct_01",
        new_end_date=date(2026, 3, 31),
        reason="test",
        updated_by="usr_test",
      )

  def test_rejects_new_end_before_first_fact(self):
    session = _mock_session()
    session.get.return_value = self._mock_schedule_structure()
    session.execute.return_value.fetchone.return_value = MagicMock(
      first_start=date(2026, 1, 1), last_end=date(2028, 12, 31)
    )
    svc = ScheduleService()
    with pytest.raises(ValueError, match="earliest"):
      svc.truncate_schedule(
        session,
        structure_id="struct_01",
        new_end_date=date(2025, 6, 30),  # Before first fact
        reason="test",
        updated_by="usr_test",
      )
