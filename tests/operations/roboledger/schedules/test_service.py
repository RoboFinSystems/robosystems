"""Tests for ScheduleService — schedule lifecycle operations."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.roboledger.schedules.service import (
  CM_HAS_PART_ARCROLE,
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
  """Create a mock SQLAlchemy session.

  A real DB flush assigns ``Structure.id`` (a default ULID); the MagicMock
  session is a no-op. Production code reads ``structure.id`` immediately
  after the flush (associations, fact-set provenance), so we stamp an id
  on each added Structure that lacks one — mirroring real flush behaviour.
  """
  from robosystems.models.extensions.roboledger import Structure
  from robosystems.utils.ulid import generate_prefixed_ulid

  session = MagicMock()
  session.execute.return_value = MagicMock()
  # The cm:Debit/cm:Credit posting-role lookup (for has-part arcs) iterates
  # execute().scalars(); default to empty so schedules in the mock simply skip
  # the arcs unless a test wires the roles in explicitly.
  session.execute.return_value.scalars.return_value = []

  def _stamp_structure_id(obj):
    if isinstance(obj, Structure) and getattr(obj, "id", None) is None:
      obj.id = generate_prefixed_ulid("st")

  session.add.side_effect = _stamp_structure_id
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

  def test_inserts_fact_set_row_with_matching_ulid(self):
    """create_schedule must insert one FactSet row whose id equals
    the fact_set_id stamped on every generated fact."""
    session = _mock_session()
    svc = ScheduleService()

    session.execute.return_value.fetchone.return_value = MagicMock(id="tax_01")

    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="FactSet Test",
        taxonomy_id="tax_01",
        element_ids=["elem_a", "elem_b"],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        monthly_amount=10000,
        entry_template=_make_entry_template(),
        created_by="usr_test",
      )

    added = [call[0][0] for call in session.add.call_args_list]
    fact_sets = [o for o in added if type(o).__name__ == "FactSet"]
    facts = [o for o in added if type(o).__name__ == "Fact"]

    assert len(fact_sets) == 1
    fs = fact_sets[0]
    assert fs.factset_type == "schedule"
    assert fs.period_start == date(2026, 1, 1)
    assert fs.period_end == date(2026, 3, 31)
    assert fs.entity_id == "ent_01"
    assert fs.created_by == "usr_test"

    # Every generated fact shares the FactSet's id
    assert facts, "expected facts to be generated"
    assert all(f.fact_set_id == fs.id for f in facts)

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

  def test_omitted_schedule_metadata_stays_null_in_typed_mechanics(self):
    session = _mock_session()
    svc = ScheduleService()

    session.execute.return_value.fetchone.return_value = MagicMock(id="tax_01")

    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="No Metadata",
        taxonomy_id="tax_01",
        element_ids=["elem_a", "elem_b"],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        monthly_amount=10000,
        entry_template=_make_entry_template(),
        created_by="usr_test",
      )

    structure = session.add.call_args_list[0][0][0]
    assert structure.artifact_mechanics["schedule_metadata"] is None

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
    # 2 facts per month x 12 months = 24, + 1 roll-forward opening = 25
    assert len(fact_objects) == 25

    historical = [f for f in fact_objects if f.fact_scope == "historical"]
    in_scope = [f for f in fact_objects if f.fact_scope == "in_scope"]

    # Jan-Jun (6 months x 2 facts = 12) + the opening (period_end Jan 1) = 13
    assert len(historical) == 13
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
    # Jan (2) + Feb (2) + the opening (period_end Jan 1) = 5 historical. Mar (2) = 2 in_scope.
    historical = [f for f in fact_objects if f.fact_scope == "historical"]
    in_scope = [f for f in fact_objects if f.fact_scope == "in_scope"]
    assert len(historical) == 5
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


class TestCreateScheduleExistingStructure:
  """create_schedule(existing_structure=...) regenerates facts + the
  obligation chain on an existing Structure WITHOUT re-creating the
  structure row, its element associations, or its cm has-part arcs."""

  def test_existing_structure_path_skips_structure_and_associations(self):
    from robosystems.models.extensions.roboledger import Association, Structure

    session = _mock_session()
    svc = ScheduleService()

    existing = Structure(
      name="Rebuilt Schedule",
      block_type="schedule",
      taxonomy_id="tax_existing",
      concept_arrangement="roll_forward",
    )
    existing.id = "struct_existing_01"
    existing.metadata_ = {}
    existing.artifact_mechanics = {}

    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      result = svc.create_schedule(
        session,
        name="Rebuilt Schedule",
        # element_ids is ignored in the existing_structure path
        taxonomy_id=None,
        element_ids=[],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        monthly_amount=10_000,
        entry_template=_make_entry_template(),
        created_by="usr_test",
        existing_structure=existing,
      )

    # The returned structure is the SAME object — id preserved.
    assert result is existing
    assert result.id == "struct_existing_01"

    added = [c[0][0] for c in session.add.call_args_list]
    # No new Structure created — we reused the existing one.
    assert not any(isinstance(o, Structure) for o in added)
    # No presentation/has-part associations created (preserved on rebuild).
    assert not any(isinstance(o, Association) for o in added)
    # Facts WERE regenerated.
    facts = [o for o in added if type(o).__name__ == "Fact"]
    assert len(facts) > 0

  def test_existing_structure_uses_its_own_taxonomy(self):
    from robosystems.models.extensions.roboledger import Structure

    session = _mock_session()
    svc = ScheduleService()

    existing = Structure(
      name="Rebuilt",
      block_type="schedule",
      taxonomy_id="tax_keep_me",
      concept_arrangement="roll_forward",
    )
    existing.id = "struct_existing_02"
    existing.metadata_ = {}
    existing.artifact_mechanics = {}

    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="Rebuilt",
        # A different taxonomy_id arg must be ignored — the existing
        # structure's taxonomy is authoritative on rebuild.
        taxonomy_id="tax_should_be_ignored",
        element_ids=[],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        monthly_amount=10_000,
        entry_template=_make_entry_template(),
        created_by="usr_test",
        existing_structure=existing,
      )

    assert existing.taxonomy_id == "tax_keep_me"

  def test_existing_structure_restamps_reproducible_definition(self):
    from robosystems.models.extensions.roboledger import Structure

    session = _mock_session()
    svc = ScheduleService()

    existing = Structure(
      name="Rebuilt",
      block_type="schedule",
      taxonomy_id="tax_existing",
      concept_arrangement="roll_forward",
    )
    existing.id = "struct_existing_03"
    existing.metadata_ = {}
    existing.artifact_mechanics = {}

    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="Rebuilt",
        taxonomy_id="tax_existing",
        element_ids=[],
        period_start=date(2026, 2, 1),
        period_end=date(2026, 4, 30),
        monthly_amount=12_345,
        entry_template=_make_entry_template(),
        created_by="usr_test",
        existing_structure=existing,
      )

    # The reproducible scalar inputs are re-stamped so a subsequent
    # rebuild stays unambiguous.
    assert existing.metadata_["monthly_amount"] == 12_345
    assert existing.metadata_["period_start"] == "2026-02-01"
    assert existing.metadata_["period_end"] == "2026-04-30"
    assert existing.artifact_mechanics["monthly_amount"] == 12_345
    # The obligation chain was materialized in place.
    assert existing.metadata_["schedule_created_event_id"] is not None

  def test_existing_structure_emits_fresh_obligation_chain(self):
    from robosystems.models.extensions.roboledger import Event, Structure

    session = _mock_session()
    svc = ScheduleService()

    existing = Structure(
      name="Rebuilt",
      block_type="schedule",
      taxonomy_id="tax_existing",
      concept_arrangement="roll_forward",
    )
    existing.id = "struct_existing_04"
    existing.metadata_ = {}
    existing.artifact_mechanics = {}

    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="Rebuilt",
        taxonomy_id="tax_existing",
        element_ids=[],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),  # 3 periods
        monthly_amount=10_000,
        entry_template=_make_entry_template(),
        created_by="usr_test",
        existing_structure=existing,
      )

    events = [c[0][0] for c in session.add.call_args_list if isinstance(c[0][0], Event)]
    creators = [e for e in events if e.event_type == "schedule_created"]
    pending = [e for e in events if e.event_type == "schedule_entry_due"]
    assert len(creators) == 1
    assert len(pending) == 3


class TestCreateScheduleHasPartArcs:
  """create_schedule emits cm:Debit/cm:Credit has-part posting arcs so the
  debit/credit pairing is a first-class, queryable atom of the schedule IB."""

  def test_emits_has_part_arcs_when_cm_roles_present(self):
    session = _mock_session()
    svc = ScheduleService()

    cm_debit = MagicMock(id="elem_cm_debit", qname="cm:Debit")
    cm_credit = MagicMock(id="elem_cm_credit", qname="cm:Credit")
    cm_result = MagicMock()
    cm_result.scalars.return_value = [cm_debit, cm_credit]
    # taxonomy_id provided (no taxonomy lookup) and no original_amount (no
    # SumEquals rule), so the cm role lookup is the only execute() call.
    session.execute.return_value = cm_result

    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="Prepaid Amortization",
        taxonomy_id="tax_01",
        element_ids=["elem_expense", "elem_prepaid"],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        monthly_amount=5000,
        entry_template=EntryTemplate(
          debit_element_id="elem_expense",
          credit_element_id="elem_prepaid",
        ),
        created_by="usr_test",
      )

    added = [call[0][0] for call in session.add.call_args_list]
    has_part = [
      o
      for o in added
      if type(o).__name__ == "Association" and o.association_type == "has-part"
    ]
    assert len(has_part) == 2
    by_from = {a.from_element_id: a for a in has_part}
    assert by_from["elem_cm_debit"].to_element_id == "elem_expense"
    assert by_from["elem_cm_credit"].to_element_id == "elem_prepaid"
    for a in has_part:
      assert a.arcrole == CM_HAS_PART_ARCROLE
      assert a.structure_id is not None

  def test_no_has_part_arcs_when_cm_roles_absent(self):
    session = _mock_session()  # default execute().scalars() → []
    svc = ScheduleService()
    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="No CM Roles",
        taxonomy_id="tax_01",
        element_ids=["elem_a", "elem_b"],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        monthly_amount=5000,
        entry_template=_make_entry_template(),
        created_by="usr_test",
      )
    added = [call[0][0] for call in session.add.call_args_list]
    has_part = [
      o
      for o in added
      if type(o).__name__ == "Association"
      and getattr(o, "association_type", None) == "has-part"
    ]
    assert has_part == []


class TestCreateScheduleCreditDirection:
  """The credit fact tracks the credited account's running balance: a
  contra-asset (credit-balance) accumulates; a directly-credited debit-balance
  asset like a prepaid draws down toward zero."""

  def _credit_facts(
    self,
    session,
    *,
    credit_balance_type,
    asset_element_id=None,
    is_contra=False,
    original_amount=120_000,
  ):
    svc = ScheduleService()
    # execute order: (1) cm role lookup, (2) credit balance_type lookup,
    # (3) credit contraAsset-trait lookup — ONLY when balance_type=='debit'
    # (the service short-circuits the trait query for credit-balance
    # accounts), (4) SumEquals qname lookup.
    cm_roles_row = MagicMock()
    cm_roles_row.scalars.return_value = []
    balance_row = MagicMock()
    balance_row.scalar.return_value = credit_balance_type
    contra_row = MagicMock()
    contra_row.scalar.return_value = "elem_cr" if is_contra else None
    qname_row = MagicMock()
    qname_row.scalar.return_value = "fac:Expense"
    seq = [cm_roles_row, balance_row]
    if credit_balance_type == "debit":
      seq.append(contra_row)
    seq.append(qname_row)
    session.execute.side_effect = seq

    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="Direction Test",
        taxonomy_id="tax_01",
        element_ids=["elem_dr", "elem_cr"],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),  # 3 monthly periods
        monthly_amount=40_000,  # $400/mo
        entry_template=EntryTemplate(
          debit_element_id="elem_dr", credit_element_id="elem_cr"
        ),
        schedule_metadata=ScheduleMetadata(
          method="straight_line",
          original_amount=original_amount,  # 0 ⇒ derive from monthly x periods
          asset_element_id=asset_element_id,
        ),
        created_by="usr_test",
      )
    added = [c[0][0] for c in session.add.call_args_list]
    return [
      f
      for f in added
      if type(f).__name__ == "Fact"
      and f.element_id == "elem_cr"
      and f.period_type == "instant"
    ]

  def test_contra_credit_accumulates(self):
    # opening (0) + 3 accumulating endings
    facts = self._credit_facts(_mock_session(), credit_balance_type="credit")
    assert [f.value for f in facts] == [0.0, 400.0, 800.0, 1200.0]

  def test_debit_balance_contra_accumulates(self):
    # A QuickBooks Accumulated Depreciation account is asset/debit-normal but
    # carries the contraAsset trait — balance_type would (wrongly) say "draws
    # down", the trait makes it accumulate UP to cost like the contra it is.
    facts = self._credit_facts(
      _mock_session(), credit_balance_type="debit", is_contra=True
    )
    assert [f.value for f in facts] == [0.0, 400.0, 800.0, 1200.0]

  def test_directly_credited_asset_draws_down(self):
    # opening (1200 gross) + 3 drawing-down endings
    facts = self._credit_facts(_mock_session(), credit_balance_type="debit")
    assert [f.value for f in facts] == [1200.0, 800.0, 400.0, 0.0]

  def test_prepaid_without_original_derives_opening_and_draws_down(self):
    # The Harbinger first-close bug: a prepaid schedule created without
    # original_amount used to fall back to accumulating (balance climbing to
    # cost). The opening cost is now derived from monthly x periods so the
    # prepaid balance declines from cost to zero.
    facts = self._credit_facts(
      _mock_session(), credit_balance_type="debit", original_amount=0
    )
    assert [f.value for f in facts] == [1200.0, 800.0, 400.0, 0.0]

  def test_asset_equals_credit_element_no_double_emit(self):
    # validate.py provisions prepaids with asset_element_id == credit_element_id.
    # The credit fact already carries the drawdown, so the NBV branch must skip
    # to avoid a duplicate instant fact on the same element/period (opening too).
    facts = self._credit_facts(
      _mock_session(), credit_balance_type="debit", asset_element_id="elem_cr"
    )
    assert [f.value for f in facts] == [1200.0, 800.0, 400.0, 0.0]

  def test_opening_balance_fact_at_schedule_start(self):
    # A roll_forward block needs a Beginning Balance: one instant fact at the
    # first period's start, accumulated = 0 (gross for a drawdown asset).
    facts = self._credit_facts(_mock_session(), credit_balance_type="debit")
    opening = facts[0]
    assert opening.period_type == "instant"
    assert opening.period_start is None
    assert opening.period_end == date(2026, 1, 1)
    assert opening.value == 1200.0


class TestCreateScheduleSumEqualsRule:
  """create_schedule auto-generates a SumEquals Rule when original_amount > 0."""

  def _run(self, session, *, original_amount: int | None = 120_000):
    svc = ScheduleService()
    # taxonomy_id is provided so _ensure_schedule_taxonomy is skipped.
    # Three execute() calls: (1) the cm:Debit/cm:Credit posting-role lookup
    # for has-part arcs (no roles in the mock → arcs skipped), (2) the credit
    # element's balance_type lookup (a credit-balance contra here → credit
    # fact accumulates), then (3) the Element.qname lookup for the SumEquals
    # rule. The contraAsset-trait lookup is short-circuited because the
    # credit account is credit-balance.
    cm_roles_row = MagicMock()
    cm_roles_row.scalars.return_value = []
    credit_balance_row = MagicMock()
    credit_balance_row.scalar.return_value = "credit"
    debit_qname_row = MagicMock()
    debit_qname_row.scalar.return_value = "fac:DeprExpense"

    session.execute.side_effect = [
      cm_roles_row,
      credit_balance_row,
      debit_qname_row,
    ]

    metadata = (
      ScheduleMetadata(
        method="straight_line",
        original_amount=original_amount,
        residual_value=0,
        useful_life_months=3,
        asset_element_id="elem_ppe",
      )
      if original_amount is not None
      else None
    )

    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="Depr Test",
        taxonomy_id="tax_01",
        element_ids=["elem_depr_expense", "elem_accum_depr"],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        monthly_amount=40_000,
        entry_template=_make_entry_template(),
        schedule_metadata=metadata,
        created_by="usr_test",
      )

    return [call[0][0] for call in session.add.call_args_list]

  def test_sum_equals_rule_is_added(self):
    session = _mock_session()
    added = self._run(session)
    rules = [o for o in added if type(o).__name__ == "Rule"]
    assert len(rules) == 1
    rule = rules[0]
    assert rule.rule_pattern == "SumEquals"
    assert rule.rule_origin == "native"
    assert rule.rule_category == "ReportingSystemSpecificRule"

  def test_sum_equals_rule_expected_total_matches_original(self):
    session = _mock_session()
    added = self._run(session, original_amount=120_000)
    rules = [o for o in added if type(o).__name__ == "Rule"]
    assert rules[0].metadata_["expected_total"] == 1200.0  # cents → dollars

  def test_sum_equals_rule_uses_derived_total_when_no_original_amount(self):
    """A prepaid created without original_amount (the Harbinger case) still
    emits a SumEquals rule anchored to the derived total (monthly x periods),
    not skipped as it would be for a contra/accumulate schedule with no cost
    basis."""
    session = _mock_session()
    svc = ScheduleService()
    # debit-balance, non-contra credit element ⇒ draws down ⇒ original is
    # derived ⇒ SumEquals fires. execute order: cm, balance(debit),
    # contra-trait(None), qname.
    cm_roles_row = MagicMock()
    cm_roles_row.scalars.return_value = []
    balance_row = MagicMock()
    balance_row.scalar.return_value = "debit"
    contra_row = MagicMock()
    contra_row.scalar.return_value = None
    qname_row = MagicMock()
    qname_row.scalar.return_value = "fac:Expense"
    session.execute.side_effect = [cm_roles_row, balance_row, contra_row, qname_row]

    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="Prepaid no-original",
        taxonomy_id="tax_01",
        element_ids=["elem_dr", "elem_cr"],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),  # 3 periods
        monthly_amount=40_000,  # $400/mo → derived total $1,200
        entry_template=EntryTemplate(
          debit_element_id="elem_dr", credit_element_id="elem_cr"
        ),
        schedule_metadata=None,  # no original_amount
        created_by="usr_test",
      )

    added = [c[0][0] for c in session.add.call_args_list]
    rules = [o for o in added if type(o).__name__ == "Rule"]
    assert len(rules) == 1
    assert rules[0].rule_pattern == "SumEquals"
    assert rules[0].metadata_["expected_total"] == 1200.0  # 400 x 3, derived

  def test_sum_equals_rule_targets_structure(self):
    session = _mock_session()
    added = self._run(session)
    structure = next(o for o in added if type(o).__name__ == "Structure")
    rules = [o for o in added if type(o).__name__ == "Rule"]
    assert rules[0].target_kind == "structure"
    assert rules[0].target_structure_id == structure.id

  def test_sum_equals_rule_variable_binds_debit_element(self):
    session = _mock_session()
    added = self._run(session)
    rules = [o for o in added if type(o).__name__ == "Rule"]
    # Binds the debit element by id (works for CoA accounts whose qname is
    # null) and records the qname for display when present.
    assert rules[0].rule_variables == [
      {
        "variable_name": "periodic_amount",
        "variable_qname": "fac:DeprExpense",
        "variable_element_id": "elem_depr_expense",
      }
    ]

  def test_no_sum_equals_rule_when_no_original_amount(self):
    session = _mock_session()
    # When schedule_metadata is None, no execute call for qname lookup
    session.execute.return_value = MagicMock(
      fetchone=MagicMock(return_value=MagicMock(id="tax_01"))
    )
    svc = ScheduleService()
    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="No Metadata",
        taxonomy_id="tax_01",
        element_ids=["elem_a", "elem_b"],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        monthly_amount=10_000,
        entry_template=_make_entry_template(),
        created_by="usr_test",
      )
    added = [call[0][0] for call in session.add.call_args_list]
    rules = [o for o in added if type(o).__name__ == "Rule"]
    assert len(rules) == 0


class TestCreateScheduleSourceTransactionId:
  def test_source_transaction_id_stored_in_artifact_mechanics(self):
    session = _mock_session()
    session.execute.return_value = MagicMock(
      fetchone=MagicMock(return_value=MagicMock(id="tax_01"))
    )
    svc = ScheduleService()
    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="Linked Schedule",
        taxonomy_id="tax_01",
        element_ids=["elem_a", "elem_b"],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        monthly_amount=10_000,
        entry_template=_make_entry_template(),
        created_by="usr_test",
        source_transaction_id="txn_abc123",
      )
    structure = session.add.call_args_list[0][0][0]
    assert structure.artifact_mechanics["source_transaction_id"] == "txn_abc123"

  def test_source_transaction_id_defaults_to_none(self):
    session = _mock_session()
    session.execute.return_value = MagicMock(
      fetchone=MagicMock(return_value=MagicMock(id="tax_01"))
    )
    svc = ScheduleService()
    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="No Link",
        taxonomy_id="tax_01",
        element_ids=["elem_a", "elem_b"],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        monthly_amount=10_000,
        entry_template=_make_entry_template(),
        created_by="usr_test",
      )
    structure = session.add.call_args_list[0][0][0]
    assert structure.artifact_mechanics["source_transaction_id"] is None


class TestCreateScheduleMaterializesObligations:
  """Stream 2.A — schedule creation emits 1 schedule_created + N pending events."""

  @staticmethod
  def _added_events(session: MagicMock) -> list:
    from robosystems.models.extensions.roboledger import Event

    return [
      call[0][0] for call in session.add.call_args_list if isinstance(call[0][0], Event)
    ]

  def test_emits_one_schedule_created_plus_one_pending_per_period(self):
    session = _mock_session()
    session.execute.return_value = MagicMock(
      fetchone=MagicMock(return_value=MagicMock(id="tax_01"))
    )
    svc = ScheduleService()
    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="3-month schedule",
        taxonomy_id="tax_01",
        element_ids=["elem_depr", "elem_accum"],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        monthly_amount=10_000,
        entry_template=_make_entry_template(),
        created_by="usr_test",
      )

    events = self._added_events(session)
    creators = [e for e in events if e.event_type == "schedule_created"]
    pending = [e for e in events if e.event_type == "schedule_entry_due"]

    assert len(creators) == 1
    assert len(pending) == 3
    assert creators[0].status == "committed"
    assert all(e.status == "pending" for e in pending)
    assert all(e.event_class == "economic" for e in events)

  def test_pending_events_link_back_to_schedule_created(self):
    session = _mock_session()
    session.execute.return_value = MagicMock(
      fetchone=MagicMock(return_value=MagicMock(id="tax_01"))
    )
    svc = ScheduleService()
    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="Linked obligations",
        taxonomy_id="tax_01",
        element_ids=["elem_depr", "elem_accum"],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 2, 28),
        monthly_amount=10_000,
        entry_template=_make_entry_template(),
        created_by="usr_test",
      )

    events = self._added_events(session)
    creator = next(e for e in events if e.event_type == "schedule_created")
    pending = [e for e in events if e.event_type == "schedule_entry_due"]

    assert all(e.obligated_by_event_id == creator.id for e in pending)

  def test_pending_event_metadata_carries_period_and_schedule_id(self):
    session = _mock_session()
    session.execute.return_value = MagicMock(
      fetchone=MagicMock(return_value=MagicMock(id="tax_01"))
    )
    svc = ScheduleService()
    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="Metadata round-trip",
        taxonomy_id="tax_01",
        element_ids=["elem_depr", "elem_accum"],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 2, 28),
        monthly_amount=10_000,
        entry_template=_make_entry_template(),
        created_by="usr_test",
      )

    events = self._added_events(session)
    structure = session.add.call_args_list[0][0][0]
    pending = sorted(
      (e for e in events if e.event_type == "schedule_entry_due"),
      key=lambda e: e.metadata_["period_start"],
    )

    assert pending[0].metadata_ == {
      "schedule_id": structure.id,
      "posting_date": "2026-01-31",
      "period_start": "2026-01-01",
      "period_end": "2026-01-31",
    }
    assert pending[1].metadata_["period_start"] == "2026-02-01"
    assert pending[1].metadata_["period_end"] == "2026-02-28"
    # schedule_entry_due handler picks this up unchanged.
    assert pending[0].metadata_["schedule_id"] == structure.id

  def test_originator_event_id_stamped_on_structure(self):
    session = _mock_session()
    session.execute.return_value = MagicMock(
      fetchone=MagicMock(return_value=MagicMock(id="tax_01"))
    )
    svc = ScheduleService()
    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="Stamped",
        taxonomy_id="tax_01",
        element_ids=["elem_depr", "elem_accum"],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        monthly_amount=10_000,
        entry_template=_make_entry_template(),
        created_by="usr_test",
      )

    events = self._added_events(session)
    creator = next(e for e in events if e.event_type == "schedule_created")
    structure = session.add.call_args_list[0][0][0]

    assert structure.metadata_["schedule_created_event_id"] == creator.id
    assert structure.metadata_["pending_event_count"] == 1
    assert structure.artifact_mechanics["schedule_created_event_id"] == creator.id
    assert structure.artifact_mechanics["pending_event_count"] == 1

  def test_pending_event_occurred_at_is_period_end_end_of_day(self):
    """Sensor sweeps `occurred_at <= today`; period-end EOD is the right anchor."""
    session = _mock_session()
    session.execute.return_value = MagicMock(
      fetchone=MagicMock(return_value=MagicMock(id="tax_01"))
    )
    svc = ScheduleService()
    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="Timestamps",
        taxonomy_id="tax_01",
        element_ids=["elem_depr", "elem_accum"],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        monthly_amount=10_000,
        entry_template=_make_entry_template(),
        created_by="usr_test",
      )

    events = self._added_events(session)
    pending = next(e for e in events if e.event_type == "schedule_entry_due")
    assert pending.occurred_at.date() == date(2026, 1, 31)
    assert pending.occurred_at.hour == 23
    assert pending.occurred_at.minute == 59

  def test_historical_periods_emit_voided_not_pending(self):
    """Backfilled periods (period_end <= closed_through) emit as voided.

    Without this, a tenant who imports historical depreciation would
    have every backfilled period block the close-period gate
    indefinitely. The historical events stay as audit-trail rows but
    don't count toward pending obligations.
    """
    session = _mock_session()
    session.execute.return_value = MagicMock(
      fetchone=MagicMock(return_value=MagicMock(id="tax_01"))
    )
    svc = ScheduleService()
    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="Backfilled depreciation",
        taxonomy_id="tax_01",
        element_ids=["elem_depr", "elem_accum"],
        period_start=date(2025, 1, 1),
        period_end=date(2026, 6, 30),  # 18 periods total
        monthly_amount=10_000,
        entry_template=_make_entry_template(),
        created_by="usr_test",
        closed_through=date(2026, 3, 31),  # first 15 periods are historical
      )

    events = self._added_events(session)
    schedule_entry_due = [e for e in events if e.event_type == "schedule_entry_due"]
    pending = [e for e in schedule_entry_due if e.status == "pending"]
    voided = [e for e in schedule_entry_due if e.status == "voided"]

    assert len(schedule_entry_due) == 18
    assert len(voided) == 15  # Jan 2025 - Mar 2026 (15 months)
    assert len(pending) == 3  # Apr, May, Jun 2026
    assert all(e.metadata_.get("void_reason") == "historical" for e in voided)
    assert all("void_reason" not in e.metadata_ for e in pending)

    # Originator's pending_event_count reflects only still-open obligations
    creator = next(e for e in events if e.event_type == "schedule_created")
    assert creator.metadata_["pending_event_count"] == 3

  def test_no_closed_through_means_all_periods_pending(self):
    """Backwards-compatibility: omitting closed_through emits every period as pending."""
    session = _mock_session()
    session.execute.return_value = MagicMock(
      fetchone=MagicMock(return_value=MagicMock(id="tax_01"))
    )
    svc = ScheduleService()
    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="Fresh schedule",
        taxonomy_id="tax_01",
        element_ids=["elem_depr", "elem_accum"],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        monthly_amount=10_000,
        entry_template=_make_entry_template(),
        created_by="usr_test",
      )
    pending = [
      e
      for e in self._added_events(session)
      if e.event_type == "schedule_entry_due" and e.status == "pending"
    ]
    assert len(pending) == 3

  def test_seven_year_schedule_emits_84_pending_events(self):
    """Sanity check: the obligation register matches the period count."""
    session = _mock_session()
    session.execute.return_value = MagicMock(
      fetchone=MagicMock(return_value=MagicMock(id="tax_01"))
    )
    svc = ScheduleService()
    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="84-month depreciation",
        taxonomy_id="tax_01",
        element_ids=["elem_depr", "elem_accum"],
        period_start=date(2026, 1, 1),
        period_end=date(2032, 12, 31),
        monthly_amount=41_667,
        entry_template=_make_entry_template(),
        schedule_metadata=_make_schedule_metadata(),
        created_by="usr_test",
      )

    events = self._added_events(session)
    assert sum(1 for e in events if e.event_type == "schedule_created") == 1
    assert sum(1 for e in events if e.event_type == "schedule_entry_due") == 84

  def test_void_pending_obligations_decrements_originator_count(self):
    """Voiding pending obligations updates the originator's pending_event_count."""
    structure = MagicMock()
    structure.metadata_ = {"schedule_created_event_id": "evt_origin"}

    originator = MagicMock()
    originator.metadata_ = {"pending_event_count": 12}

    session = MagicMock()
    update_result = MagicMock(rowcount=12)
    session.execute.return_value = update_result
    session.get.return_value = originator

    svc = ScheduleService()
    voided = svc.void_pending_obligations(
      session,
      structure=structure,
      void_reason="schedule_deleted",
    )

    assert voided == 12
    assert originator.metadata_["pending_event_count"] == 0
    assert len(originator.metadata_["void_history"]) == 1
    assert originator.metadata_["void_history"][0]["voided_count"] == 12
    assert originator.metadata_["void_history"][0]["void_reason"] == "schedule_deleted"
    assert originator.metadata_["void_history"][0]["voided_by_event_id"] is None

  def test_void_pending_obligations_with_disposal_event_link(self):
    """Disposal passes voided_by_event_id; the void carries the audit link."""
    structure = MagicMock()
    structure.metadata_ = {"schedule_created_event_id": "evt_origin"}

    originator = MagicMock()
    originator.metadata_ = {"pending_event_count": 6}

    session = MagicMock()
    session.execute.return_value = MagicMock(rowcount=4)
    session.get.return_value = originator

    svc = ScheduleService()
    voided = svc.void_pending_obligations(
      session,
      structure=structure,
      void_reason="asset_disposed",
      voided_by_event_id="evt_disposal_42",
    )

    assert voided == 4
    assert originator.metadata_["pending_event_count"] == 2  # 6 - 4
    history = originator.metadata_["void_history"][0]
    assert history["voided_by_event_id"] == "evt_disposal_42"
    assert history["void_reason"] == "asset_disposed"

    # The UPDATE statement carries replaced_by_event_id when a voider exists
    update_stmt = session.execute.call_args.args[0]
    rendered = str(update_stmt.compile(compile_kwargs={"literal_binds": True}))
    assert "replaced_by_event_id='evt_disposal_42'" in rendered.replace(" = ", "=")

  def test_void_pending_obligations_no_op_when_unrecoverable(self):
    """No stamp AND no recoverable obligations (the fallback finds nothing) → 0."""
    structure = MagicMock()
    structure.metadata_ = {}
    structure.id = "struct_legacy"
    session = MagicMock()
    # The fallback recovery select returns no originator id.
    session.execute.return_value.scalar.return_value = None

    svc = ScheduleService()
    voided = svc.void_pending_obligations(
      session, structure=structure, void_reason="schedule_deleted"
    )

    assert voided == 0
    # The fallback recovery select ran, but no UPDATE followed.
    assert session.execute.call_count == 1
    session.get.assert_not_called()

  def test_void_pending_obligations_recovers_link_when_stamp_missing(self):
    """Layer 1 fix: the structure lacks the schedule_created_event_id stamp, but
    the obligations carry the link (obligated_by_event_id) — recover it and void
    anyway, instead of silently returning 0.

    This is the 2026-06-19 Harbinger orphan defect: without this fallback the
    void no-op'd and the delete orphaned 280 pending obligations that then
    double-posted at close. See specs/schedule-delete-obligation-integrity.md.
    """
    structure = MagicMock()
    structure.metadata_ = {}  # the bug condition: stamp missing
    structure.id = "struct_orphan"

    originator = MagicMock()
    originator.metadata_ = {"pending_event_count": 5}

    session = MagicMock()
    recovery = MagicMock()
    recovery.scalar.return_value = "evt_origin_recovered"
    update_result = MagicMock(rowcount=5)
    session.execute.side_effect = [recovery, update_result]
    session.get.return_value = originator

    svc = ScheduleService()
    voided = svc.void_pending_obligations(
      session, structure=structure, void_reason="schedule_deleted"
    )

    assert voided == 5
    assert originator.metadata_["pending_event_count"] == 0
    assert session.execute.call_count == 2  # recovery select + UPDATE

  def test_void_pending_obligations_no_op_when_no_pending_rows(self):
    """When the UPDATE matches zero rows, originator metadata is untouched."""
    structure = MagicMock()
    structure.metadata_ = {"schedule_created_event_id": "evt_origin"}

    session = MagicMock()
    session.execute.return_value = MagicMock(rowcount=0)

    svc = ScheduleService()
    voided = svc.void_pending_obligations(
      session, structure=structure, void_reason="schedule_deleted"
    )

    assert voided == 0
    # session.get not called since we short-circuit before the originator load
    session.get.assert_not_called()

  def test_supersede_no_op_when_schedule_predates_stream_2a(self):
    """Schedules with no schedule_created_event_id stamped return 0 cleanly."""
    structure = MagicMock()
    structure.metadata_ = {}  # legacy row with no event linkage
    session = MagicMock()

    svc = ScheduleService()
    count = svc.supersede_pending_obligations(
      session, structure=structure, created_by="usr_test"
    )

    assert count == 0
    session.execute.assert_not_called()

  def test_supersede_no_op_when_no_pending_events(self):
    """All obligations already classified/fulfilled — nothing to supersede."""
    structure = MagicMock()
    structure.metadata_ = {"schedule_created_event_id": "evt_origin"}
    session = MagicMock()
    session.execute.return_value.scalars.return_value = iter([])

    svc = ScheduleService()
    count = svc.supersede_pending_obligations(
      session, structure=structure, created_by="usr_test"
    )

    assert count == 0
    session.add.assert_not_called()

  def test_supersede_voids_pending_and_emits_replacements_with_chain_links(self):
    """Each pending event gets voided + linked to a fresh replacement event."""
    from types import SimpleNamespace

    structure = MagicMock()
    structure.id = "struct_supersede"
    structure.metadata_ = {"schedule_created_event_id": "evt_origin"}

    pending_a = SimpleNamespace(
      id="evt_old_a",
      status="pending",
      replaced_by_event_id=None,
      metadata_={
        "schedule_id": "struct_supersede",
        "posting_date": "2026-01-31",
        "period_start": "2026-01-01",
        "period_end": "2026-01-31",
      },
    )
    pending_b = SimpleNamespace(
      id="evt_old_b",
      status="pending",
      replaced_by_event_id=None,
      metadata_={
        "schedule_id": "struct_supersede",
        "posting_date": "2026-02-28",
        "period_start": "2026-02-01",
        "period_end": "2026-02-28",
      },
    )
    session = MagicMock()
    session.execute.return_value.scalars.return_value = iter([pending_a, pending_b])

    svc = ScheduleService()
    count = svc.supersede_pending_obligations(
      session, structure=structure, created_by="usr_test"
    )

    assert count == 2
    # Old events are voided + linked forward
    assert pending_a.status == "voided"
    assert pending_b.status == "voided"
    assert pending_a.replaced_by_event_id is not None
    assert pending_b.replaced_by_event_id is not None
    # Each replacement was added
    added = [c[0][0] for c in session.add.call_args_list]
    from robosystems.models.extensions.roboledger import Event

    new_events = [e for e in added if isinstance(e, Event)]
    assert len(new_events) == 2
    # Each new event points back at its predecessor and forward at the
    # same originator
    by_predecessor = {e.replaces_event_id: e for e in new_events}
    assert by_predecessor["evt_old_a"].id == pending_a.replaced_by_event_id
    assert by_predecessor["evt_old_b"].id == pending_b.replaced_by_event_id
    for new_evt in new_events:
      assert new_evt.obligated_by_event_id == "evt_origin"
      assert new_evt.status == "pending"
      assert new_evt.event_type == "schedule_entry_due"
    # Period metadata carries through unchanged — only the template differs.
    new_a = by_predecessor["evt_old_a"]
    assert new_a.metadata_["period_start"] == "2026-01-01"
    assert new_a.metadata_["period_end"] == "2026-01-31"

  def test_supersede_skips_legacy_events_missing_period_metadata(self):
    """Defensive: a malformed pending row doesn't crash the supersession."""
    from types import SimpleNamespace

    structure = MagicMock()
    structure.id = "struct_legacy"
    structure.metadata_ = {"schedule_created_event_id": "evt_origin"}

    good = SimpleNamespace(
      id="evt_good",
      status="pending",
      replaced_by_event_id=None,
      metadata_={
        "schedule_id": "struct_legacy",
        "posting_date": "2026-01-31",
        "period_start": "2026-01-01",
        "period_end": "2026-01-31",
      },
    )
    malformed = SimpleNamespace(
      id="evt_malformed",
      status="pending",
      replaced_by_event_id=None,
      metadata_={"schedule_id": "struct_legacy"},  # no period info
    )
    session = MagicMock()
    session.execute.return_value.scalars.return_value = iter([good, malformed])

    svc = ScheduleService()
    count = svc.supersede_pending_obligations(
      session, structure=structure, created_by="usr_test"
    )

    assert count == 1
    assert good.status == "voided"
    # Malformed row left as-is, NOT voided (so a follow-up data fix is possible)
    assert malformed.status == "pending"

  def test_schedule_created_metadata_round_trips_through_handler(self):
    """The schedule_created event payload validates against its registered metadata schema.

    A real DB flush would assign Structure.id; the MagicMock session is a
    no-op, so we patch session.add to stamp an id on the first Structure
    it sees. This makes the schedule_id linkage verifiable end-to-end.
    """
    from robosystems.models.extensions.roboledger import Structure
    from robosystems.operations.event_block.python_handlers.schedule_created import (
      ScheduleCreatedMetadata,
    )

    session = _mock_session()
    session.execute.return_value = MagicMock(
      fetchone=MagicMock(return_value=MagicMock(id="tax_01"))
    )

    def _stamp_structure_id(obj):
      if isinstance(obj, Structure) and obj.id is None:
        obj.id = "struct_test_01"

    session.add.side_effect = _stamp_structure_id

    svc = ScheduleService()
    with patch.object(svc, "_get_entity_id", return_value="ent_01"):
      svc.create_schedule(
        session,
        name="Schema check",
        taxonomy_id="tax_01",
        element_ids=["elem_depr", "elem_accum"],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        monthly_amount=10_000,
        entry_template=_make_entry_template(),
        created_by="usr_test",
      )

    events = self._added_events(session)
    creator = next(e for e in events if e.event_type == "schedule_created")
    parsed = ScheduleCreatedMetadata.model_validate(creator.metadata_)
    assert parsed.schedule_id == "struct_test_01"
    assert parsed.pending_event_count == 3
    assert parsed.period_start == date(2026, 1, 1)


class TestCreateClosingEntry:
  def _mock_schedule_structure(self):
    struct = MagicMock()
    struct.block_type = "schedule"
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
    struct.block_type = "schedule"
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
    struct.block_type = "schedule"
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
    struct.artifact_mechanics = {
      "kind": "closing_entry_generator",
      "entry_template": struct.metadata_["entry_template"],
      "schedule_metadata": struct.metadata_.get("schedule_metadata"),
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
    structure = session.get.return_value
    assert structure.artifact_mechanics["schedule_metadata"]["end_date"] == "2026-03-31"

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


def test_sumequals_rule_generates_for_coa_debit_with_null_qname() -> None:
  """Regression: the auto-generated SumEquals guardrail must be created even
  when the debit element is a tenant CoA account (null qname) — the normal
  case. Previously the rule was gated on the debit qname, so it silently never
  generated for CoA-debited schedules. It now binds the element by id."""
  from robosystems.models.extensions.rule import Rule

  session = _mock_session()
  # taxonomy lookup → id; debit qname lookup → None (CoA account has no qname)
  session.execute.return_value.fetchone.return_value = MagicMock(id="tax_01")
  session.execute.return_value.scalar.return_value = None

  svc = ScheduleService()
  with patch.object(svc, "_get_entity_id", return_value="ent_01"):
    svc.create_schedule(
      session,
      name="Prepaid Insurance Amortization",
      taxonomy_id=None,
      element_ids=["elem_insurance", "elem_prepaid"],
      period_start=date(2026, 1, 1),
      period_end=date(2026, 3, 31),
      monthly_amount=10000,
      entry_template=EntryTemplate(
        debit_element_id="elem_insurance",
        credit_element_id="elem_prepaid",
        entry_type="adjusting",
      ),
      schedule_metadata=ScheduleMetadata(
        method="straight_line",
        original_amount=30000,
        asset_element_id="elem_prepaid",
      ),
      created_by="usr_test",
    )

  added = [call[0][0] for call in session.add.call_args_list]
  rules = [a for a in added if isinstance(a, Rule)]
  assert len(rules) == 1, "SumEquals rule should generate despite null debit qname"
  rule = rules[0]
  assert rule.rule_pattern == "SumEquals"
  assert rule.rule_variables[0]["variable_element_id"] == "elem_insurance"
