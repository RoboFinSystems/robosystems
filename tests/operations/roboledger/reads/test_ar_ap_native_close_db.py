"""An invoice and its payment written on native books age once a close posts
them: a drafted document is not in the books yet, and a posted one is,
whatever status its event was left in."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import select

from robosystems.models.api.event_block import (
  CreateEventBlockRequest,
  UpdateEventBlockRequest,
)
from robosystems.models.api.extensions.journal_entries import (
  ReverseJournalEntryRequest,
)
from robosystems.models.api.fact_provenance import AssertedProvenance
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger import Agent, Event
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.operations.event_block.commands import (
  create_event_block_in_session,
  update_event_block,
)
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  close_period,
  reopen_period,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  reverse_journal_entry,
)
from robosystems.operations.roboledger.fact_set import create_fact_set
from robosystems.operations.roboledger.fiscal_calendar import (
  FiscalCalendarService,
  PeriodCloseService,
)
from robosystems.operations.roboledger.reads.ar_ap import (
  get_aged_balances,
  list_open_items,
)
from robosystems.operations.roboledger.reports.statement_sets import (
  StatementStampResult,
)
from tests.ledger_entity import entity_account
from tests.operations.roboledger.conftest import (  # noqa: F401
  tenant_session,
  two_entities,
)

pytestmark = pytest.mark.unit

GRAPH_ID = "kg0123456789abcdef09"
_COMMANDS = "robosystems.operations.roboledger.commands.fiscal_calendar"
JULY_END = date(2026, 7, 31)


@pytest.fixture(autouse=True)
def _no_platform():
  with (
    patch(f"{_COMMANDS}.exclusive_period_fence"),
    patch("robosystems.operations.extensions.staleness.mark_graph_stale"),
    patch(
      "robosystems.operations.roboledger.reads.fiscal_calendar.keeper_sync_state",
      return_value=(False, None),
    ),
  ):
    yield


def _stamp(session, *, graph_id, period_start, period_end, actor_id, entity_id):
  fact_set = create_fact_set(
    session,
    period_start=period_start,
    period_end=period_end,
    factset_type="report",
    entity_id=entity_id,
    provenance=AssertedProvenance(
      source_system="test", asserted_by=actor_id, basis_note="fixture"
    ),
    created_by=actor_id,
  )
  session.flush()
  return StatementStampResult(stamped=True, fact_set_ids={"statement": fact_set.id})


@pytest.fixture()
def books(two_entities):  # noqa: F811
  """The parent's native books, closed through June with July open."""
  t = two_entities
  entity = t.parent.id
  t.service = FiscalCalendarService()
  t.service.initialize(
    t.session, GRAPH_ID, closed_through="2026-06", actor_id="usr_1", entity_id=entity
  )
  t.service.ensure_fiscal_periods(
    t.session,
    GRAPH_ID,
    start_period="2026-06",
    end_period="2026-08",
    closed_through="2026-06",
    entity_id=entity,
  )
  t.cash = entity_account(t.session, entity, "Checking")
  t.revenue = entity_account(t.session, entity, "Consulting Revenue")
  t.receivables = entity_account(t.session, entity, "Accounts Receivable")
  t.session.get(Element, t.receivables).metadata_ = {
    "account_type": "Accounts Receivable"
  }
  t.agent = Agent(agent_type="customer", name="Acme", created_by="test")
  t.session.add(t.agent)
  t.session.commit()
  return t


def _document(
  t,
  event_type: str,
  cents: int,
  day: int,
  *,
  debit: str,
  credit: str,
  settles: str | None = None,
) -> str:
  at = datetime(2026, 7, day, tzinfo=UTC)
  body = CreateEventBlockRequest(
    event_type=event_type,
    event_category="sales",
    source="manual",
    occurred_at=at,
    amount=cents,
    agent_id=t.agent.id,
    discharges_event_id=settles,
    apply_handlers=True,
    metadata={
      "posting_date": at.date().isoformat(),
      "memo": event_type,
      "line_items": [
        {"element_id": debit, "debit_amount": cents},
        {"element_id": credit, "credit_amount": cents},
      ],
    },
  )
  event, _ = create_event_block_in_session(t.session, body, "usr_1", graph_id=GRAPH_ID)
  t.session.commit()
  return str(event.id)


def _close_july(t) -> None:
  close_period(
    t.session,
    MagicMock(),
    GRAPH_ID,
    "2026-07",
    actor_id="usr_1",
    allow_stale_sync=False,
    note=None,
    service=t.service,
    close_service=PeriodCloseService(t.service, statement_stamper=_stamp),
    entity_id=t.parent.id,
  )
  t.session.commit()
  t.session.expire_all()


def _open_receivables(t) -> list[int]:
  items = list_open_items(
    t.session, "receivables", as_of=JULY_END, entity_id=t.parent.id
  )
  return [item.open_cents for item in items]


def test_a_drafted_invoice_is_not_a_receivable_yet(books):
  t = books
  _document(t, "invoice_issued", 120_000, 5, debit=t.receivables, credit=t.revenue)

  assert _open_receivables(t) == []


def test_an_invoice_the_close_posted_ages(books):
  t = books
  invoice = _document(
    t, "invoice_issued", 120_000, 5, debit=t.receivables, credit=t.revenue
  )

  _close_july(t)

  assert t.session.get(Event, invoice).status == "classified"
  assert _open_receivables(t) == [120_000]
  aged = get_aged_balances(
    t.session, "receivables", as_of=JULY_END, entity_id=t.parent.id
  )
  assert aged.buckets.total_cents == 120_000


def test_a_payment_the_close_posted_settles_its_invoice(books):
  t = books
  invoice = _document(
    t, "invoice_issued", 120_000, 5, debit=t.receivables, credit=t.revenue
  )
  _document(
    t,
    "payment_received",
    50_000,
    20,
    debit=t.cash,
    credit=t.receivables,
    settles=invoice,
  )

  _close_july(t)

  assert _open_receivables(t) == [70_000]
  aged = get_aged_balances(
    t.session, "receivables", as_of=JULY_END, entity_id=t.parent.id
  )
  (tie_out,) = aged.tie_outs
  assert tie_out.open_items_cents == 70_000
  assert tie_out.control_balance_cents == 70_000


def _reopen_and_reverse(t, event_id: str) -> None:
  """The correction on native books: reopen the month and reverse the entry."""
  reopen_period(
    t.session,
    MagicMock(),
    GRAPH_ID,
    "2026-07",
    "usr_1",
    "document was wrong",
    None,
    t.service,
    entity_id=t.parent.id,
  )
  t.session.commit()
  entry = t.session.execute(
    select(Entry).where(Entry.triggered_by_event_id == event_id)
  ).scalar_one()
  reverse_journal_entry(
    t.session,
    ReverseJournalEntryRequest(entry_id=str(entry.id), posting_date=JULY_END),
    "usr_1",
  )
  t.session.commit()
  t.session.expire_all()


def test_a_reversed_invoice_no_longer_ages(books):
  t = books
  invoice = _document(
    t, "invoice_issued", 120_000, 5, debit=t.receivables, credit=t.revenue
  )
  _close_july(t)

  _reopen_and_reverse(t, invoice)

  assert _open_receivables(t) == []
  aged = get_aged_balances(
    t.session, "receivables", as_of=JULY_END, entity_id=t.parent.id
  )
  (tie_out,) = aged.tie_outs
  assert aged.buckets.total_cents == 0
  assert (tie_out.control_balance_cents, tie_out.difference_cents) == (0, 0)


def test_a_reversed_payment_no_longer_settles_its_invoice(books):
  t = books
  invoice = _document(
    t, "invoice_issued", 120_000, 5, debit=t.receivables, credit=t.revenue
  )
  payment = _document(
    t,
    "payment_received",
    50_000,
    20,
    debit=t.cash,
    credit=t.receivables,
    settles=invoice,
  )
  _close_july(t)

  _reopen_and_reverse(t, payment)

  assert _open_receivables(t) == [120_000]
  aged = get_aged_balances(
    t.session, "receivables", as_of=JULY_END, entity_id=t.parent.id
  )
  (tie_out,) = aged.tie_outs
  assert (tie_out.open_items_cents, tie_out.control_balance_cents) == (
    120_000,
    120_000,
  )


AUGUST_END = date(2026, 8, 31)


def _committed_bank_receipt(t, cents: int, *, settles: str) -> str:
  """A bank-feed deposit committed against the invoice it pays, in a month
  still open: its entry is a draft the close will post."""
  body = CreateEventBlockRequest(
    event_type="bank_transaction",
    event_category="treasury",
    source="manual",
    occurred_at=datetime(2026, 8, 12, tzinfo=UTC),
    amount=cents,
    agent_id=t.agent.id,
    resource_type="money",
    resource_element_id=t.cash,
  )
  event, _ = create_event_block_in_session(t.session, body, "usr_1", graph_id=GRAPH_ID)
  t.session.commit()
  update_event_block(
    t.session,
    UpdateEventBlockRequest(
      event_id=str(event.id),
      transition_to="committed",
      discharges_event_id=settles,
      metadata_patch={"classified_element_id": t.receivables},
    ),
    "usr_1",
    graph_id=GRAPH_ID,
  )
  t.session.commit()
  t.session.expire_all()
  return str(event.id)


def test_a_payment_awaiting_the_close_ties_out_as_pending(books):
  """The aging takes a committed payment as settling its invoice at once; the
  control account moves when the close posts the payment's entry. The tie-out
  carries the draft as pending, so the two agree before the close."""
  t = books
  invoice = _document(
    t, "invoice_issued", 120_000, 5, debit=t.receivables, credit=t.revenue
  )
  _close_july(t)
  payment = _committed_bank_receipt(t, 120_000, settles=invoice)

  entry = t.session.execute(
    select(Entry).where(Entry.triggered_by_event_id == payment)
  ).scalar_one()
  assert (t.session.get(Event, payment).status, entry.status) == (
    "committed",
    "draft",
  )
  aged = get_aged_balances(
    t.session, "receivables", as_of=AUGUST_END, entity_id=t.parent.id
  )
  (tie_out,) = aged.tie_outs
  assert aged.buckets.total_cents == 0
  assert (
    tie_out.control_balance_cents,
    tie_out.pending_close_cents,
    tie_out.difference_cents,
  ) == (120_000, -120_000, 0)


def test_a_voided_lines_draft_is_not_pending(books):
  """A voided event's entry stays a draft the close never posts: it is not
  pending in the tie-out."""
  t = books
  invoice = _document(
    t, "invoice_issued", 120_000, 5, debit=t.receivables, credit=t.revenue
  )
  _close_july(t)
  payment = _committed_bank_receipt(t, 120_000, settles=invoice)
  update_event_block(
    t.session,
    UpdateEventBlockRequest(event_id=payment, transition_to="voided"),
    "usr_1",
    graph_id=GRAPH_ID,
  )
  t.session.commit()
  t.session.expire_all()

  aged = get_aged_balances(
    t.session, "receivables", as_of=AUGUST_END, entity_id=t.parent.id
  )
  (tie_out,) = aged.tie_outs
  assert aged.buckets.total_cents == 120_000
  assert (tie_out.pending_close_cents, tie_out.difference_cents) == (0, 0)
