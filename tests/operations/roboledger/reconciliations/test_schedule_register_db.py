"""The schedule check against real Postgres: each scheduled asset account's
balance against what its schedules say it carries, and the account blocks a
refresh records it on.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import patch

import pytest
from sqlalchemy import select

from robosystems.models.api.extensions.reconciliations import (
  PreviewReconciliationsRequest,
  RefreshReconciliationsRequest,
  SignOffReconciliationRequest,
)
from robosystems.models.api.extensions.schedules import (
  CreateScheduleRequest,
  EntryTemplateRequest,
  ScheduleMetadataRequest,
)
from robosystems.models.extensions import Structure
from robosystems.models.extensions.entity import Entity
from robosystems.models.extensions.roboledger import Entry, Event
from robosystems.operations.information_block import get_information_block
from robosystems.operations.roboledger.commands.reconciliations import (
  preview_reconciliations,
  refresh_reconciliations,
  sign_off_reconciliation,
)
from robosystems.operations.roboledger.commands.schedules import create_schedule
from robosystems.operations.roboledger.reads.reconciliations import (
  list_reconciliations,
)
from robosystems.operations.roboledger.reconciliations import (
  NoSourceLedgerError,
  NothingToReconcileError,
  SourceLedgerResolver,
)
from robosystems.operations.roboledger.schedules import ScheduleService

from .conftest import GRAPH_ID, classified_account, entry

pytestmark = pytest.mark.unit

_MEMBERS = (
  "robosystems.operations.roboledger.commands.reconciliations._explicit_write_members"
)


@pytest.fixture()
def native(ext_session):
  """A ledger with no source behind it: the accounts a prepaid and a fixed
  asset need, and nothing booked yet."""
  session = ext_session
  session.add(Entity(name="Fictional Co", created_by="usr"))
  accounts = {
    "cash": classified_account(session, "Checking", "asset"),
    "prepaid": classified_account(session, "Prepaid Insurance", "asset"),
    "equipment": classified_account(session, "Equipment", "asset"),
    "accumulated": classified_account(
      session, "Accumulated Depreciation", "contraAsset"
    ),
    "accrued": classified_account(
      session, "Accrued Expenses", "liability", balance_type="credit"
    ),
    "insurance": classified_account(session, "Insurance", "expense"),
    "depreciation": classified_account(session, "Depreciation Expense", "expense"),
  }
  session.commit()
  return session, accounts


def _schedule(
  session,
  name: str,
  debit: str,
  credit: str,
  *,
  start: date = date(2026, 1, 1),
  end: date = date(2026, 12, 31),
  monthly: int = 10_000,
  original: int = 120_000,
  asset: str | None = None,
  auto_reverse: bool = False,
  closed_through: date | None = None,
  booked_on: date | None = None,
) -> str:
  created = create_schedule(
    session,
    CreateScheduleRequest(
      name=name,
      element_ids=[debit, credit],
      period_start=start,
      period_end=end,
      monthly_amount=monthly,
      entry_template=EntryTemplateRequest(
        debit_element_id=debit, credit_element_id=credit, auto_reverse=auto_reverse
      ),
      schedule_metadata=ScheduleMetadataRequest(
        original_amount=original, asset_element_id=asset, booked_on=booked_on
      ),
      closed_through=closed_through,
    ),
    created_by="usr",
  )
  session.commit()
  return created.structure_id


def _book_month(session, structure_id: str, month: int, *, status: str = "posted"):
  """Draft the schedule's entry for a 2026 month, and post it unless told not to."""
  from calendar import monthrange

  start = date(2026, month, 1)
  end = date(2026, month, monthrange(2026, month)[1])
  result = ScheduleService().create_closing_entry(
    session,
    structure_id=structure_id,
    posting_date=end,
    period_start=start,
    period_end=end,
    created_by="usr",
  )
  if status != "draft":
    session.get(Entry, result.entry_id).status = status
  session.commit()


@pytest.fixture()
def prepaid(native):
  """A 1,200.00 policy bought in January and amortized 100.00 a month, with
  January to July posted. At 2026-08-31 the account will hold 400.00."""
  session, accounts = native
  entry(session, date(2026, 1, 5), accounts["prepaid"], accounts["cash"], 120_000)
  structure_id = _schedule(
    session, "Insurance policy", accounts["insurance"], accounts["prepaid"]
  )
  for month in range(1, 8):
    _book_month(session, structure_id, month)
  return session, accounts, structure_id


def _preview(session, period: str = "2026-08"):
  return preview_reconciliations(
    session,
    PreviewReconciliationsRequest(
      period=period, method="schedule_register", include_tied=True
    ),
    graph_id=GRAPH_ID,
  )


def _refresh(session, period: str = "2026-08", *, via: str = "operation"):
  with patch.object(
    SourceLedgerResolver, "_fetch", side_effect=NoSourceLedgerError("no source")
  ):
    result = refresh_reconciliations(
      session,
      RefreshReconciliationsRequest(period=period),
      graph_id=GRAPH_ID,
      created_by="usr",
      compared_via=via,
    )
  session.commit()
  return result


def _rows(comparison) -> dict[str, tuple[float, float, str]]:
  return {
    row.account_name: (row.ledger_balance, row.independent_balance, row.status)
    for row in comparison.rows
  }


# ── The comparison ──────────────────────────────────────────────────────────


def test_a_prepaid_ties_to_its_schedule(prepaid):
  session, _, structure_id = prepaid

  comparison = _preview(session)

  assert comparison.method == "schedule_register"
  assert (comparison.accounts_compared, comparison.accounts_different) == (1, 0)
  (row,) = comparison.rows
  assert (row.account_name, row.ledger_balance, row.independent_balance) == (
    "Prepaid Insurance",
    400.00,
    400.00,
  )
  assert [(c.structure_id, c.name, c.amount) for c in row.components] == [
    (structure_id, "Insurance policy", 400.00)
  ]


def test_the_comparison_does_not_move_as_the_close_drafts_and_posts(prepaid):
  """August's entry is still to come, then a draft, then posted. The balance
  the close will leave is the same at each step."""
  session, _, structure_id = prepaid

  awaiting = _preview(session)
  _book_month(session, structure_id, 8, status="draft")
  drafted = _preview(session)
  session.execute(
    select(Entry).where(Entry.status == "draft")
  ).scalar_one().status = "posted"
  session.commit()
  posted = _preview(session)

  assert _rows(awaiting) == _rows(drafted) == _rows(posted)
  assert _rows(posted) == {"Prepaid Insurance": (400.00, 400.00, "tied")}
  assert any("the close will post" in note for note in awaiting.notes)
  assert any("the close will post" in note for note in drafted.notes)
  assert not any("the close will post" in note for note in posted.notes)


def test_a_balance_with_no_schedule_behind_it_is_a_difference(prepaid):
  session, accounts, _ = prepaid
  entry(session, date(2026, 8, 12), accounts["prepaid"], accounts["cash"], 60_000)
  session.commit()

  comparison = _preview(session)

  (row,) = comparison.rows
  assert (row.ledger_balance, row.independent_balance, row.difference, row.status) == (
    1_000.00,
    400.00,
    600.00,
    "different",
  )
  assert comparison.total_difference == 600.00


def test_two_schedules_on_one_account_add_up(prepaid):
  session, accounts, _ = prepaid
  entry(session, date(2026, 7, 1), accounts["prepaid"], accounts["cash"], 60_000)
  renewal = _schedule(
    session,
    "Second policy",
    accounts["insurance"],
    accounts["prepaid"],
    start=date(2026, 7, 1),
    end=date(2026, 12, 31),
    original=60_000,
  )
  _book_month(session, renewal, 7)

  (row,) = _preview(session).rows

  assert (row.ledger_balance, row.independent_balance, row.status) == (
    800.00,
    800.00,
    "tied",
  )
  assert sorted((c.name, c.amount) for c in row.components) == [
    ("Insurance policy", 400.00),
    ("Second policy", 400.00),
  ]


def test_a_schedule_from_before_draw_down_still_reads_as_remaining_cost(prepaid):
  """Older schedules hold an accumulated amount on every credited account
  and have no opening balance fact. The carried balance does not read them."""
  from robosystems.models.extensions.roboledger import Fact

  session, accounts, structure_id = prepaid
  running = session.query(Fact).filter(
    Fact.structure_id == structure_id,
    Fact.element_id == accounts["prepaid"],
    Fact.period_type == "instant",
  )
  running.filter(Fact.period_start.is_(None)).delete(synchronize_session=False)
  for fact in running:
    fact.value = round(1_200.00 - fact.value, 2)
  session.commit()

  assert _rows(_preview(session)) == {"Prepaid Insurance": (400.00, 400.00, "tied")}


def test_a_second_fact_for_a_period_is_not_counted_twice(prepaid):
  """The close drafts one entry per schedule and period, from one fact."""
  from robosystems.models.extensions.roboledger import Fact

  session, accounts, structure_id = prepaid
  august = (
    session.query(Fact)
    .filter(
      Fact.structure_id == structure_id,
      Fact.element_id == accounts["insurance"],
      Fact.period_end == date(2026, 8, 31),
    )
    .one()
  )
  session.add(
    Fact(
      element_id=august.element_id,
      value=august.value,
      period_start=august.period_start,
      period_end=august.period_end,
      period_type="duration",
      unit="USD",
      entity_id=august.entity_id,
      structure_id=structure_id,
      fact_set_id=august.fact_set_id,
    )
  )
  session.commit()

  assert _rows(_preview(session)) == {"Prepaid Insurance": (400.00, 400.00, "tied")}


@pytest.mark.parametrize(
  ("trait", "balance_type"), [("contraAsset", "debit"), ("asset", "credit")]
)
def test_depreciation_accumulates_as_a_credit_balance(native, trait, balance_type):
  """A source chart types a contra-asset debit-normal; a native one may type
  accumulated depreciation as a credit-normal asset. Either way it accumulates."""
  session, accounts = native
  accumulated = classified_account(
    session, "Accumulated Amortization", trait, balance_type=balance_type
  )
  structure_id = _schedule(
    session,
    "Software licence",
    accounts["depreciation"],
    accumulated,
    original=120_000,
  )
  for month in (1, 2, 3):
    _book_month(session, structure_id, month)

  assert _rows(_preview(session, "2026-03")) == {
    "Accumulated Amortization": (-300.00, -300.00, "tied")
  }


def test_the_cost_account_carries_what_the_asset_cost(native):
  session, accounts = native
  structure_id = _schedule(
    session,
    "Roaster",
    accounts["depreciation"],
    accounts["accumulated"],
    asset=accounts["equipment"],
  )
  _book_month(session, structure_id, 1)

  # The schedule exists; the purchase was never booked.
  assert _rows(_preview(session, "2026-01")) == {
    "Equipment": (0.00, 1_200.00, "different"),
    "Accumulated Depreciation": (-100.00, -100.00, "tied"),
  }

  entry(session, date(2026, 1, 3), accounts["equipment"], accounts["cash"], 120_000)
  session.commit()
  assert _rows(_preview(session, "2026-01"))["Equipment"] == (
    1_200.00,
    1_200.00,
    "tied",
  )


def test_ending_an_asset_schedule_early_keeps_its_cost_for_earlier_periods(native):
  from robosystems.models.api.extensions.schedules import TerminateScheduleRequest
  from robosystems.operations.roboledger.commands.schedules import terminate_schedule

  session, accounts = native
  entry(session, date(2026, 1, 3), accounts["equipment"], accounts["cash"], 120_000)
  structure_id = _schedule(
    session,
    "Roaster",
    accounts["depreciation"],
    accounts["accumulated"],
    asset=accounts["equipment"],
  )
  for month in (1, 2, 3):
    _book_month(session, structure_id, month)
  terminate_schedule(
    session,
    TerminateScheduleRequest(
      structure_id=structure_id, new_end_date=date(2026, 3, 31), reason="Sold"
    ),
    created_by="usr",
  )
  session.commit()

  assert _rows(_preview(session, "2026-02")) == {
    "Equipment": (1_200.00, 1_200.00, "tied"),
    "Accumulated Depreciation": (-200.00, -200.00, "tied"),
  }


def test_a_schedule_from_before_its_period_bounds_were_stored_still_compares(prepaid):
  """The oldest schedules carry no cost and no period bounds in their stored
  definition. What they recognize is still in their facts."""
  from sqlalchemy.orm.attributes import flag_modified

  from robosystems.models.extensions import Structure

  session, _, structure_id = prepaid
  schedule = session.get(Structure, structure_id)
  schedule.metadata_ = {
    "entry_template": schedule.metadata_["entry_template"],
    "schedule_metadata": {"method": "straight_line", "original_amount": 0},
  }
  flag_modified(schedule, "metadata_")
  session.commit()

  assert _rows(_preview(session)) == {"Prepaid Insurance": (400.00, 400.00, "tied")}


def test_a_disposal_that_has_not_been_processed_changes_nothing(native):
  """A disposal still in the inbox has posted no entry, so the asset is
  still on the books and still on its schedule."""
  session, accounts = native
  entry(session, date(2026, 1, 3), accounts["equipment"], accounts["cash"], 120_000)
  structure_id = _schedule(
    session,
    "Roaster",
    accounts["depreciation"],
    accounts["accumulated"],
    asset=accounts["equipment"],
  )
  _book_month(session, structure_id, 1)
  session.add(
    Event(
      event_type="asset_disposed",
      event_category="adjustment",
      event_class="economic",
      occurred_at=datetime(2026, 1, 20, tzinfo=UTC),
      status="captured",
      source="manual",
      metadata_={"schedule_id": structure_id},
      created_by="usr",
    )
  )
  session.commit()

  assert _rows(_preview(session, "2026-01")) == {
    "Equipment": (1_200.00, 1_200.00, "tied"),
    "Accumulated Depreciation": (-100.00, -100.00, "tied"),
  }


def test_a_disposed_asset_carries_nothing_from_then_on(native):
  session, accounts = native
  structure_id = _schedule(
    session,
    "Roaster",
    accounts["depreciation"],
    accounts["accumulated"],
    asset=accounts["equipment"],
  )
  session.add(
    Event(
      event_type="asset_disposed",
      event_category="adjustment",
      event_class="economic",
      occurred_at=datetime(2026, 3, 15, tzinfo=UTC),
      status="fulfilled",
      source="manual",
      metadata_={"schedule_id": structure_id},
      created_by="usr",
    )
  )
  session.commit()

  before = _preview(session, "2026-02")
  after = _preview(session, "2026-03")

  assert _rows(before)["Equipment"][1] == 1_200.00
  assert {row.account_name: row.independent_balance for row in after.rows} == {
    "Equipment": 0.00,
    "Accumulated Depreciation": 0.00,
  }
  assert {c.note for row in after.rows for c in row.components} == {
    "Disposed of on 2026-03-15."
  }


def test_a_schedule_ended_early_carries_nothing_from_its_last_month(prepaid):
  """What is left in the account after the schedule stops has no schedule
  behind it."""
  from robosystems.models.api.extensions.schedules import TerminateScheduleRequest
  from robosystems.operations.roboledger.commands.schedules import terminate_schedule

  session, _, structure_id = prepaid
  # Through the command, as a user would: it also re-anchors the schedule's
  # stored basis to the shortened total.
  terminate_schedule(
    session,
    TerminateScheduleRequest(
      structure_id=structure_id,
      new_end_date=date(2026, 7, 31),
      reason="Policy cancelled",
    ),
    created_by="usr",
  )
  session.commit()

  assert _rows(_preview(session, "2026-06")) == {
    "Prepaid Insurance": (600.00, 600.00, "tied")
  }
  (row,) = _preview(session, "2026-07").rows
  assert (row.ledger_balance, row.independent_balance, row.status) == (
    500.00,
    0.00,
    "different",
  )
  assert row.components[0].note == "Ended early on 2026-07-31."


def test_months_before_the_watermark_count_without_schedule_entries(native):
  """Amortization booked before the schedule existed is in the account
  through ordinary entries; the schedule's earlier months still count."""
  session, accounts = native
  entry(session, date(2026, 1, 5), accounts["prepaid"], accounts["cash"], 120_000)
  for month in range(1, 7):
    entry(
      session,
      date(2026, month, 28),
      accounts["insurance"],
      accounts["prepaid"],
      10_000,
    )
  _schedule(
    session,
    "Insurance policy",
    accounts["insurance"],
    accounts["prepaid"],
    closed_through=date(2026, 6, 30),
  )

  assert _rows(_preview(session, "2026-06")) == {
    "Prepaid Insurance": (600.00, 600.00, "tied")
  }


def test_a_prepaid_paid_before_it_starts_is_carried_from_the_day_it_was_paid(native):
  """Next year's policy is paid in December. The schedule says when, so
  December's balance has a schedule behind it."""
  session, accounts = native
  entry(session, date(2025, 12, 20), accounts["prepaid"], accounts["cash"], 120_000)
  _schedule(
    session,
    "Insurance policy",
    accounts["insurance"],
    accounts["prepaid"],
    booked_on=date(2025, 12, 20),
  )

  assert _preview(session, "2025-11").rows == []
  (row,) = _preview(session, "2025-12").rows
  assert (row.ledger_balance, row.independent_balance, row.status) == (
    1_200.00,
    1_200.00,
    "tied",
  )
  assert [(c.name, c.amount) for c in row.components] == [
    ("Insurance policy", 1_200.00)
  ]


def test_an_asset_bought_before_it_is_placed_in_service_is_carried_at_cost(native):
  session, accounts = native
  entry(session, date(2026, 2, 10), accounts["equipment"], accounts["cash"], 120_000)
  _schedule(
    session,
    "Roaster",
    accounts["depreciation"],
    accounts["accumulated"],
    start=date(2026, 3, 1),
    end=date(2027, 2, 28),
    asset=accounts["equipment"],
    booked_on=date(2026, 2, 10),
  )

  assert _rows(_preview(session, "2026-02")) == {
    "Equipment": (1_200.00, 1_200.00, "tied"),
    "Accumulated Depreciation": (0.00, 0.00, "tied"),
  }


def test_the_date_can_be_added_to_a_schedule_without_touching_the_rest(native):
  """An existing schedule gets its booked date by naming that one field."""
  from robosystems.models.api.extensions.schedules import UpdateScheduleRequest
  from robosystems.operations.roboledger.commands.schedules import update_schedule

  session, accounts = native
  entry(session, date(2026, 2, 10), accounts["equipment"], accounts["cash"], 120_000)
  structure_id = _schedule(
    session,
    "Roaster",
    accounts["depreciation"],
    accounts["accumulated"],
    start=date(2026, 3, 1),
    end=date(2027, 2, 28),
    asset=accounts["equipment"],
  )
  assert _preview(session, "2026-02").rows == []

  update_schedule(
    session,
    UpdateScheduleRequest(
      structure_id=structure_id,
      schedule_metadata=ScheduleMetadataRequest(booked_on=date(2026, 2, 10)),
    ),
    updated_by="usr",
  )
  session.commit()

  stored = get_information_block(session, structure_id).artifact.mechanics
  assert (
    stored.schedule_metadata.original_amount,
    stored.schedule_metadata.asset_element_id,
    stored.schedule_metadata.booked_on,
  ) == (120_000, accounts["equipment"], date(2026, 2, 10))
  assert _rows(_preview(session, "2026-02"))["Equipment"] == (
    1_200.00,
    1_200.00,
    "tied",
  )


def test_the_booked_date_survives_a_rebuild(native):
  from robosystems.models.api.extensions.schedules import RebuildScheduleRequest
  from robosystems.operations.roboledger.commands.schedules import rebuild_schedule

  session, accounts = native
  structure_id = _schedule(
    session,
    "Insurance policy",
    accounts["insurance"],
    accounts["prepaid"],
    booked_on=date(2025, 12, 20),
  )

  rebuild_schedule(
    session, RebuildScheduleRequest(structure_id=structure_id), created_by="usr"
  )
  session.commit()

  envelope = get_information_block(session, structure_id)
  assert envelope.artifact.mechanics.schedule_metadata.booked_on == date(2025, 12, 20)
  assert _preview(session, "2025-12").rows[0].independent_balance == 1_200.00


def test_only_asset_accounts_that_carry_a_balance_are_compared(native):
  """A liability is settled by payments no schedule knows about, an
  auto-reversing accrual carries nothing, and a schedule that has not
  started says nothing yet."""
  session, accounts = native
  _schedule(session, "Bonus accrual", accounts["insurance"], accounts["accrued"])
  _schedule(
    session,
    "Month-end accrual",
    accounts["insurance"],
    accounts["prepaid"],
    auto_reverse=True,
  )
  _schedule(
    session,
    "Next year's policy",
    accounts["insurance"],
    accounts["prepaid"],
    start=date(2027, 1, 1),
    end=date(2027, 12, 31),
  )

  comparison = _preview(session)

  assert (comparison.accounts_compared, comparison.rows) == (0, [])


# ── The blocks ──────────────────────────────────────────────────────────────


def test_a_ledger_with_no_source_reconciles_its_scheduled_accounts(prepaid):
  session, accounts, structure_id = prepaid

  (rec,) = _refresh(session).reconciliations

  assert (rec.scope, rec.method, rec.name, rec.element_id) == (
    "account",
    "schedule_register",
    "Prepaid Insurance (schedules)",
    accounts["prepaid"],
  )
  assert (rec.status, rec.source, rec.required_for_close) == (
    "reconciled",
    "schedules",
    True,
  )
  assert (rec.ledger_balance, rec.independent_balance, rec.unreconciled_difference) == (
    400.00,
    400.00,
    0.00,
  )
  assert [(c.structure_id, c.amount) for c in rec.components] == [
    (structure_id, 400.00)
  ]
  assert rec.differences == []


def test_the_account_block_reads_as_an_information_block(prepaid):
  session, _, _ = prepaid
  (rec,) = _refresh(session).reconciliations

  envelope = get_information_block(session, rec.structure_id)

  assert envelope is not None
  assert envelope.artifact.mechanics.scope == "account"
  assert [(row.element_name, row.values) for row in envelope.view.rendering.rows] == [
    ("Ledger balance", [400.00]),
    ("Independent balance", [400.00]),
    ("Unreconciled difference", [0.00]),
  ]


def test_an_account_that_does_not_tie_at_first_does_not_hold_the_close(prepaid):
  """The difference is reported; making the close wait on it is a decision."""
  session, accounts, _ = prepaid
  entry(session, date(2026, 8, 12), accounts["prepaid"], accounts["cash"], 60_000)
  session.commit()

  (rec,) = _refresh(session).reconciliations

  assert (rec.status, rec.required_for_close, rec.unreconciled_difference) == (
    "unreconciled",
    False,
    600.00,
  )
  assert [(d.account_name, d.difference) for d in rec.differences] == [
    ("Prepaid Insurance", 600.00)
  ]


def test_a_scheduled_amount_the_ledger_does_not_hold_is_unreconciled(native):
  session, accounts = native
  structure_id = _schedule(
    session,
    "Roaster",
    accounts["depreciation"],
    accounts["accumulated"],
    asset=accounts["equipment"],
  )
  _book_month(session, structure_id, 1)

  by_name = {rec.name: rec for rec in _refresh(session, "2026-01").reconciliations}

  assert by_name["Equipment (schedules)"].status == "unreconciled"
  assert by_name["Equipment (schedules)"].unreconciled_difference == -1_200.00
  assert by_name["Accumulated Depreciation (schedules)"].status == "reconciled"


def test_an_account_that_tied_and_then_drifts_holds_the_close(prepaid):
  from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar
  from robosystems.operations.roboledger.fiscal_calendar import FiscalCalendarService

  session, accounts, _ = prepaid
  session.add(FiscalCalendar(graph_id=GRAPH_ID, closed_through_period="2026-07"))
  _refresh(session)
  entry(session, date(2026, 8, 12), accounts["prepaid"], accounts["cash"], 60_000)
  session.commit()

  (rec,) = _refresh(session).reconciliations
  gate = FiscalCalendarService().closeable_gate(
    session, GRAPH_ID, "2026-08", today=date(2026, 10, 1)
  )

  assert (rec.status, rec.required_for_close) == ("unreconciled", True)
  assert "unreconciled_accounts" in gate.blockers
  assert gate.unreconciled_account_sample == [
    "Prepaid Insurance (schedules): unreconciled"
  ]


def test_an_unscheduled_entry_after_the_comparison_makes_the_account_stale(prepaid):
  session, accounts, _ = prepaid
  _refresh(session)
  entry(session, date(2026, 8, 12), accounts["prepaid"], accounts["cash"], 60_000)
  session.commit()

  (rec,) = list_reconciliations(session, "2026-08").reconciliations

  assert rec.status == "stale"


def test_a_sync_refreshes_account_blocks_and_creates_none(prepaid):
  session, accounts, _ = prepaid

  assert _refresh(session, via="sync").reconciliations == []
  assert session.query(Structure).filter_by(block_type="reconciliation").count() == 0

  _refresh(session)
  structure_id = _schedule(
    session,
    "Roaster",
    accounts["depreciation"],
    accounts["accumulated"],
    asset=accounts["equipment"],
  )
  _book_month(session, structure_id, 1)

  (rec,) = _refresh(session, via="sync").reconciliations

  assert (rec.name, rec.compared_via) == ("Prepaid Insurance (schedules)", "sync")


def test_an_account_no_schedule_reaches_any_more_compares_against_zero(prepaid):
  session, _, structure_id = prepaid
  _refresh(session)
  session.get(Structure, structure_id).is_active = False
  session.commit()

  (rec,) = _refresh(session).reconciliations

  # August's obligation is still pending, so the close would post it.
  assert (rec.status, rec.ledger_balance, rec.independent_balance) == (
    "unreconciled",
    400.00,
    0.00,
  )
  assert rec.components == []


def test_a_source_that_cannot_be_read_does_not_stop_the_other_checks(prepaid):
  """QuickBooks is rate-limiting. The schedule check needs no source, so it
  is still recorded, and the refresh says what it could not compare."""
  from robosystems.operations.roboledger.reconciliations import (
    SourceLedgerUnavailableError,
  )

  session, _, _ = prepaid
  failure = SourceLedgerUnavailableError("QuickBooks could not be read just now.")
  with patch.object(SourceLedgerResolver, "_fetch", side_effect=failure):
    result = refresh_reconciliations(
      session,
      RefreshReconciliationsRequest(period="2026-08"),
      graph_id=GRAPH_ID,
      created_by="usr",
    )
  session.commit()

  assert [(r.name, r.status) for r in result.reconciliations] == [
    ("Prepaid Insurance (schedules)", "reconciled")
  ]
  assert result.notes == [
    "The source ledger was not compared: QuickBooks could not be read just now."
  ]


def test_a_source_failure_is_the_answer_when_no_other_check_applies(native):
  from robosystems.operations.roboledger.reconciliations import (
    SourceLedgerUnavailableError,
  )

  session, _ = native
  failure = SourceLedgerUnavailableError("QuickBooks could not be read just now.")
  with (
    patch.object(SourceLedgerResolver, "_fetch", side_effect=failure),
    pytest.raises(SourceLedgerUnavailableError),
  ):
    refresh_reconciliations(
      session,
      RefreshReconciliationsRequest(period="2026-08"),
      graph_id=GRAPH_ID,
      created_by="usr",
    )


def test_a_transport_failure_reading_the_report_is_reported_as_unavailable():
  from quickbooks.exceptions import QuickbooksException

  from robosystems.operations.roboledger.reconciliations import (
    ReconciliationWindow,
    SourceLedgerUnavailableError,
  )

  class _Client:
    def get_trial_balance(self, start, end):
      raise QuickbooksException("ThrottleExceeded", 3001)

  window = ReconciliationWindow("2026-08", date(2026, 8, 31), date(2026, 1, 1))
  with pytest.raises(SourceLedgerUnavailableError, match="QuickbooksException"):
    SourceLedgerResolver._read_report(_Client(), window)


def test_a_ledger_with_nothing_to_check_is_refused(native):
  session, _ = native

  with pytest.raises(NothingToReconcileError, match="Nothing to reconcile"):
    _refresh(session)


def test_a_sign_off_on_an_account_survives_the_close_posting_its_entries(prepaid):
  """The reviewer signed the balance the close would leave, so posting
  August's entry does not lapse the review."""
  session, _, structure_id = prepaid
  (rec,) = _refresh(session).reconciliations
  with patch(_MEMBERS, return_value={"usr"}):
    sign_off_reconciliation(
      session,
      SignOffReconciliationRequest(structure_id=rec.structure_id, period="2026-08"),
      graph_id=GRAPH_ID,
      created_by="usr",
    )
  session.commit()

  _book_month(session, structure_id, 8)
  _refresh(session)

  (after,) = list_reconciliations(session, "2026-08").reconciliations
  assert (after.status, after.reviewed_by) == ("reviewed", "usr")
