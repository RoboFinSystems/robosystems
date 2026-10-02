"""The statement check against real Postgres: recording a statement's ending
balance for an account and reconciling the ledger to it.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pytest
from sqlalchemy import select

from robosystems.models.api.extensions.reconciliations import (
  PreviewReconciliationsRequest,
  RecordStatementBalanceRequest,
  RefreshReconciliationsRequest,
  SetReconciliationPolicyRequest,
  SignOffReconciliationRequest,
)
from robosystems.models.extensions.entity import Entity
from robosystems.models.extensions.roboledger import Entry, Event
from robosystems.operations.roboledger.commands.reconciliations import (
  StatementAccountError,
  StatementAccountNotFoundError,
  StatementDocumentNotFoundError,
  preview_reconciliations,
  record_statement_balance,
  refresh_reconciliations,
  set_reconciliation_policy,
  sign_off_reconciliation,
)
from robosystems.operations.roboledger.reads.reconciliations import (
  list_reconciliations,
)
from robosystems.operations.roboledger.reconciliations import (
  NoSourceLedgerError,
  NothingToReconcileError,
  SourceLedgerResolver,
)

from .conftest import GRAPH_ID, classified_account, entry

pytestmark = pytest.mark.unit

_COMMANDS = "robosystems.operations.roboledger.commands.reconciliations"


@pytest.fixture()
def books(ext_session):
  """A ledger with no source behind it. At 2026-08-31: checking 4,800.00,
  a loan of 4,800.00 owed, a card with 350.00 owed (300.00 at 08-15)."""
  session = ext_session
  session.add(Entity(name="Fictional Co", created_by="usr"))
  accounts = {
    "cash": classified_account(session, "Checking", "asset"),
    "loan": classified_account(
      session, "Equipment Loan", "liability", balance_type="credit"
    ),
    "card": classified_account(
      session, "Company Card", "liability", balance_type="credit"
    ),
    "supplies": classified_account(session, "Supplies", "expense"),
  }
  entry(session, date(2026, 1, 10), accounts["cash"], accounts["loan"], 500_000)
  entry(session, date(2026, 8, 5), accounts["loan"], accounts["cash"], 20_000)
  entry(session, date(2026, 8, 10), accounts["supplies"], accounts["card"], 30_000)
  entry(session, date(2026, 8, 20), accounts["supplies"], accounts["card"], 5_000)
  session.commit()
  return session, accounts


def _record(session, element_id, balance, as_of=date(2026, 8, 31), **fields):
  result = record_statement_balance(
    session,
    RecordStatementBalanceRequest(
      element_id=element_id, as_of=as_of, balance=balance, **fields
    ),
    graph_id=GRAPH_ID,
    created_by="usr",
  )
  session.commit()
  return result


def _refresh(session, period="2026-08"):
  with patch.object(
    SourceLedgerResolver, "_fetch", side_effect=NoSourceLedgerError("no source")
  ):
    result = refresh_reconciliations(
      session,
      RefreshReconciliationsRequest(period=period),
      graph_id=GRAPH_ID,
      created_by="usr",
    )
  session.commit()
  return result


def _observations(session):
  return list(
    session.execute(
      select(Event)
      .where(Event.event_type == "balance_observed")
      .order_by(Event.id.asc())
    ).scalars()
  )


def test_a_statement_that_agrees_reconciles_the_account(books):
  """The loan statement shows 4,800.00 owed: entered as a positive number,
  compared as the credit balance it is."""
  session, accounts = books

  rec = _record(session, accounts["loan"], 4_800.00)

  assert (rec.scope, rec.method, rec.name, rec.element_id) == (
    "account",
    "statement",
    "Equipment Loan (statement)",
    accounts["loan"],
  )
  assert (rec.status, rec.source, rec.required_for_close) == (
    "reconciled",
    "statement",
    False,
  )
  assert (rec.ledger_balance, rec.independent_balance, rec.balance_as_of) == (
    -4_800.00,
    -4_800.00,
    date(2026, 8, 31),
  )
  (component,) = rec.components
  assert (component.name, component.amount) == (
    "Statement ending 2026-08-31",
    -4_800.00,
  )
  assert component.event_id is not None


def test_the_recorded_balance_is_a_support_event_that_writes_no_books(books):
  session, accounts = books
  entries_before = session.query(Entry).count()

  _record(session, accounts["cash"], 4_800.00, note="August statement")

  (event,) = _observations(session)
  assert (event.event_class, event.event_category, event.status, event.source) == (
    "support",
    "reconciliation",
    "committed",
    "manual",
  )
  assert (event.resource_type, event.resource_element_id, event.amount) == (
    "money",
    accounts["cash"],
    480_000,
  )
  assert event.metadata_["kind"] == "statement_ending"
  assert event.metadata_["as_of"] == "2026-08-31"
  assert event.metadata_["note"] == "August statement"
  assert session.query(Entry).count() == entries_before


def test_a_statement_that_ends_mid_period_is_compared_at_its_own_date(books):
  """The card's statement closes on the 15th, before the second charge."""
  session, accounts = books

  rec = _record(session, accounts["card"], 300.00, as_of=date(2026, 8, 15))

  assert (rec.status, rec.ledger_balance, rec.balance_as_of, rec.period) == (
    "reconciled",
    -300.00,
    date(2026, 8, 15),
    "2026-08",
  )


def test_a_statement_that_disagrees_is_unreconciled(books):
  session, accounts = books

  rec = _record(session, accounts["cash"], 4_750.00)

  assert (rec.status, rec.unreconciled_difference) == ("unreconciled", 50.00)
  assert [(d.account_name, d.difference) for d in rec.differences] == [
    ("Checking", 50.00)
  ]


def test_recording_the_same_date_again_replaces_the_balance(books):
  session, accounts = books
  _record(session, accounts["cash"], 4_750.00)

  rec = _record(session, accounts["cash"], 4_800.00)
  again = _record(session, accounts["cash"], 4_800.00)

  assert (rec.status, again.status) == ("reconciled", "reconciled")
  first, second = _observations(session)
  assert (first.status, first.replaced_by_event_id) == ("superseded", second.id)
  assert (second.status, second.replaces_event_id, second.amount) == (
    "committed",
    first.id,
    480_000,
  )


def test_the_ledger_side_counts_drafts_the_close_will_post(books):
  session, accounts = books
  entry(
    session,
    date(2026, 8, 28),
    accounts["supplies"],
    accounts["cash"],
    10_000,
    status="draft",
  )
  session.commit()

  rec = _record(session, accounts["cash"], 4_700.00)

  assert (rec.status, rec.ledger_balance) == ("reconciled", 4_700.00)


def test_only_a_balance_sheet_chart_account_takes_a_statement(books):
  session, accounts = books

  with pytest.raises(StatementAccountError, match="not a balance-sheet account"):
    _record(session, accounts["supplies"], 350.00)
  with pytest.raises(StatementAccountNotFoundError):
    _record(session, "elem_missing", 350.00)
  assert _observations(session) == []


def test_the_statement_document_must_be_on_the_graph(books):
  session, accounts = books

  with (
    patch(f"{_COMMANDS}._statement_document_exists", return_value=False),
    pytest.raises(StatementDocumentNotFoundError),
  ):
    _record(session, accounts["cash"], 4_800.00, document_id="doc_missing")
  session.rollback()

  with patch(f"{_COMMANDS}._statement_document_exists", return_value=True):
    rec = _record(session, accounts["cash"], 4_800.00, document_id="doc_stmt")

  assert rec.components[0].document_id == "doc_stmt"


def test_a_refresh_compares_the_statement_with_the_ledger_as_it_now_stands(books):
  session, accounts = books
  _record(session, accounts["cash"], 4_800.00)
  entry(session, date(2026, 8, 30), accounts["supplies"], accounts["cash"], 2_500)
  session.commit()

  (rec,) = _refresh(session).reconciliations

  assert (rec.status, rec.ledger_balance, rec.independent_balance) == (
    "unreconciled",
    4_775.00,
    4_800.00,
  )


def test_a_period_with_no_statement_has_not_started(books):
  session, accounts = books
  _record(session, accounts["cash"], 4_800.00)

  (rec,) = list_reconciliations(session, "2026-09").reconciliations

  assert rec.status == "not_started"
  with pytest.raises(NothingToReconcileError):
    _refresh(session, "2026-09")


def test_a_required_statement_holds_the_close_until_one_is_recorded(books):
  from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar
  from robosystems.operations.roboledger.fiscal_calendar import FiscalCalendarService

  session, accounts = books
  session.add(FiscalCalendar(graph_id=GRAPH_ID, closed_through_period="2026-08"))
  rec = _record(session, accounts["loan"], 4_800.00)
  with patch(f"{_COMMANDS}._explicit_write_members", return_value={"usr"}):
    set_reconciliation_policy(
      session,
      SetReconciliationPolicyRequest(
        structure_id=rec.structure_id, required_for_close=True
      ),
      graph_id=GRAPH_ID,
      created_by="usr",
    )
  session.commit()

  def gate():
    return FiscalCalendarService().closeable_gate(
      session, GRAPH_ID, "2026-09", today=date(2026, 11, 1)
    )

  held = gate()
  assert "unreconciled_accounts" in held.blockers
  assert held.unreconciled_account_sample == ["Equipment Loan (statement): not_started"]

  _record(session, accounts["loan"], 4_800.00, as_of=date(2026, 9, 30))
  assert "unreconciled_accounts" not in gate().blockers


def test_a_corrected_balance_lapses_the_sign_off_and_the_original_restores_it(books):
  """The period is closed and signed off. A statement can still be recorded
  for it, since it writes no books, but the review was of the first balance."""
  from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar

  session, accounts = books
  session.add(FiscalCalendar(graph_id=GRAPH_ID, closed_through_period="2026-08"))
  rec = _record(session, accounts["loan"], 4_800.00)
  with patch(f"{_COMMANDS}._explicit_write_members", return_value={"usr"}):
    sign_off_reconciliation(
      session,
      SignOffReconciliationRequest(structure_id=rec.structure_id, period="2026-08"),
      graph_id=GRAPH_ID,
      created_by="usr",
    )
  session.commit()

  corrected = _record(session, accounts["loan"], 4_900.00)
  restored = _record(session, accounts["loan"], 4_800.00)

  assert (corrected.status, corrected.reviewed_by) == ("unreconciled", None)
  assert (restored.status, restored.reviewed_by) == ("reviewed", "usr")


def test_the_cents_recorded_are_the_ones_typed(books):
  session, accounts = books

  _record(session, accounts["cash"], 4_800.005)

  (event,) = _observations(session)
  assert event.amount == 480_001


def test_a_statement_reconciliation_can_be_previewed_and_signed_off(books):
  session, accounts = books
  rec = _record(session, accounts["loan"], 4_800.00)

  preview = preview_reconciliations(
    session,
    PreviewReconciliationsRequest(
      period="2026-08", method="statement", include_tied=True
    ),
    graph_id=GRAPH_ID,
  )
  with patch(f"{_COMMANDS}._explicit_write_members", return_value={"usr"}):
    signed = sign_off_reconciliation(
      session,
      SignOffReconciliationRequest(structure_id=rec.structure_id, period="2026-08"),
      graph_id=GRAPH_ID,
      created_by="usr",
    )
  session.commit()

  assert [(r.account_name, r.status) for r in preview.rows] == [
    ("Equipment Loan", "tied")
  ]
  assert (signed.status, signed.reviewed_by) == ("reviewed", "usr")
