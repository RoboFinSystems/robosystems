"""A bank statement against a bank-fed account, on real Postgres: the lines
the bank had not cleared, the statement cycle, and carrying a statement that
ends early to the period's last day.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import patch

import pytest

from robosystems.models.api.extensions.reconciliations import (
  RecordStatementBalanceRequest,
  RefreshReconciliationsRequest,
  SetReconciliationPolicyRequest,
)
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger import Entry, Event, LineItem
from robosystems.operations.roboledger.commands.reconciliations import (
  record_statement_balance,
  refresh_reconciliations,
  set_reconciliation_policy,
)
from robosystems.operations.roboledger.reads.reconciliations import (
  list_reconciliations,
)
from robosystems.operations.roboledger.reconciliations import (
  NoSourceLedgerError,
  NothingToReconcileError,
  SourceLedgerResolver,
)
from robosystems.operations.roboledger.reconciliations.cleared import account_lines
from robosystems.operations.roboledger.reconciliations.observations import (
  BANK_CURRENT,
  FeedBalance,
  record_feed_balance,
)
from robosystems.operations.roboledger.reconciliations.resolvers import cycle_start
from tests.ledger_entity import PARENT_ENTITY_ID

from .conftest import GRAPH_ID, classified_account, entry

pytestmark = pytest.mark.unit


def _feed_line(session, posting_date: date, debit: str, credit: str, cents: int):
  """A line the bank feed brought and a person committed: the entry its
  handler wrote, triggered by a Plaid event."""
  event = Event(
    entity_id=PARENT_ENTITY_ID,
    event_type="bank_transaction",
    event_category="treasury",
    event_class="economic",
    resource_type="money",
    occurred_at=datetime.combine(posting_date, datetime.min.time()),
    effective_at=datetime.combine(posting_date, datetime.min.time()),
    status="committed",
    source="plaid",
    amount=cents,
    description="Bank line",
    created_by="test",
  )
  session.add(event)
  session.flush()
  row = Entry(
    entity_id=PARENT_ENTITY_ID,
    posting_date=posting_date,
    status="posted",
    type="standard",
    provenance="event_handler",
    triggered_by_event_id=event.id,
    created_by="test",
  )
  session.add(row)
  session.flush()
  session.add(
    LineItem(entry_id=row.id, element_id=debit, debit_amount=cents, line_order=1)
  )
  session.add(
    LineItem(entry_id=row.id, element_id=credit, credit_amount=cents, line_order=2)
  )
  session.flush()
  return row


def _transfer(session, from_element, to_element, cents, sent: date, arrived: date):
  """A transfer between two fed accounts, committed: one entry on the day the
  money left, each leg's own date on the event."""
  event = Event(
    entity_id=PARENT_ENTITY_ID,
    event_type="internal_transfer",
    event_category="treasury",
    event_class="economic",
    resource_type="money",
    occurred_at=datetime.combine(sent, datetime.min.time()),
    status="committed",
    source="plaid",
    amount=cents,
    resource_element_id=to_element,
    description="Transfer Checking to Savings",
    metadata_={
      "kind": "internal_transfer",
      "from_element_id": from_element,
      "to_element_id": to_element,
      "from_date": sent.isoformat(),
      "to_date": arrived.isoformat(),
    },
    created_by="test",
  )
  session.add(event)
  session.flush()
  row = Entry(
    entity_id=PARENT_ENTITY_ID,
    posting_date=sent,
    status="posted",
    type="standard",
    provenance="event_handler",
    triggered_by_event_id=event.id,
    created_by="test",
  )
  session.add(row)
  session.flush()
  session.add(
    LineItem(entry_id=row.id, element_id=to_element, debit_amount=cents, line_order=1)
  )
  session.add(
    LineItem(
      entry_id=row.id, element_id=from_element, credit_amount=cents, line_order=2
    )
  )
  session.flush()


@pytest.fixture()
def bank(ext_session):
  """A checking account the feed keeps. An opening balance of 1,000.00 on
  08-31, entered by hand before the feed began; feed lines of +500.00 on
  09-02 and -200.00 on 09-10; a vendor payment of 50.00 recorded by hand on
  09-12. The bank holds 1,300.00 from 09-10; the ledger 1,250.00 from 09-12."""
  session = ext_session
  accounts = {
    "cash": classified_account(session, "Checking", "asset"),
    "equity": classified_account(
      session, "Members Equity", "equity", balance_type="credit"
    ),
    "revenue": classified_account(session, "Revenue", "revenue", balance_type="credit"),
    "supplies": classified_account(session, "Supplies", "expense"),
  }
  entry(session, date(2026, 8, 31), accounts["cash"], accounts["equity"], 100_000)
  _feed_line(session, date(2026, 9, 2), accounts["cash"], accounts["revenue"], 50_000)
  _feed_line(session, date(2026, 9, 10), accounts["supplies"], accounts["cash"], 20_000)
  entry(session, date(2026, 9, 12), accounts["supplies"], accounts["cash"], 5_000)
  session.commit()
  return session, accounts


def _record(session, element_id, balance, as_of=date(2026, 9, 30)):
  result = record_statement_balance(
    session,
    RecordStatementBalanceRequest(element_id=element_id, as_of=as_of, balance=balance),
    graph_id=GRAPH_ID,
    created_by="usr",
  )
  session.commit()
  return result


def _refresh(session, period):
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


def _statement_rec(session, period):
  (rec,) = [
    r
    for r in list_reconciliations(session, period).reconciliations
    if r.method == "statement"
  ]
  return rec


def test_a_hand_recorded_payment_is_outstanding_and_explains_the_difference(bank):
  """The bank says 1,300.00 and the ledger 1,250.00; the 50.00 payment the
  bank has not cleared is what lies between them."""
  session, accounts = bank

  rec = _record(session, accounts["cash"], 1_300.00)

  assert rec.status == "reconciled"
  assert (rec.ledger_balance, rec.independent_balance) == (1_250.00, 1_250.00)
  statement, outstanding = rec.components
  assert (statement.kind, statement.amount) == ("statement", 1_300.00)
  assert (outstanding.kind, outstanding.amount, outstanding.posting_date) == (
    "outstanding",
    -50.00,
    date(2026, 9, 12),
  )
  assert outstanding.entry_id is not None


def test_history_before_the_feed_began_is_not_outstanding(bank):
  """The opening balance was entered by hand, but before the feed's first
  line, so it is what the feed started from, not a line the bank owes."""
  session, accounts = bank

  rec = _record(session, accounts["cash"], 1_300.00)

  assert [c.posting_date for c in rec.components if c.kind == "outstanding"] == [
    date(2026, 9, 12)
  ]


def test_a_payment_and_its_reversal_cancel(bank):
  session, accounts = bank
  payment = session.query(Entry).filter(Entry.posting_date == date(2026, 9, 12)).one()
  payment.status = "reversed"
  reversal = Entry(
    entity_id=PARENT_ENTITY_ID,
    posting_date=date(2026, 9, 14),
    status="posted",
    type="reversing",
    reversal_of=payment.id,
    provenance="manual_entry",
    created_by="test",
  )
  session.add(reversal)
  session.flush()
  session.add(
    LineItem(
      entry_id=reversal.id,
      element_id=accounts["cash"],
      debit_amount=5_000,
      line_order=1,
    )
  )
  session.add(
    LineItem(
      entry_id=reversal.id,
      element_id=accounts["supplies"],
      credit_amount=5_000,
      line_order=2,
    )
  )
  session.commit()

  rec = _record(session, accounts["cash"], 1_300.00)

  assert rec.status == "reconciled"
  assert [c.kind for c in rec.components] == ["statement"]


@pytest.fixture()
def in_transit(bank):
  """Savings, fed since a +100.00 line on 09-03, and 200.00 sent from
  checking on 09-30 that savings receives on 10-02."""
  session, accounts = bank
  savings = classified_account(session, "Savings", "asset")
  _feed_line(session, date(2026, 9, 3), savings, accounts["revenue"], 10_000)
  _transfer(
    session, accounts["cash"], savings, 20_000, date(2026, 9, 30), date(2026, 10, 2)
  )
  session.commit()
  return session, accounts, savings


def test_a_transfer_books_the_day_it_left_and_reconciles_the_sender(in_transit):
  """Checking's statement shows the 200.00 gone on 09-30, as the ledger does;
  only the hand-recorded payment lies between them."""
  session, accounts, _savings = in_transit

  rec = _record(session, accounts["cash"], 1_100.00)

  assert rec.status == "reconciled"
  assert [(c.kind, c.amount) for c in rec.components] == [
    ("statement", 1_100.00),
    ("outstanding", -50.00),
  ]


def test_a_transfer_is_in_transit_on_the_receiver_until_its_leg_date(in_transit):
  """Savings' statement at 09-30 has not seen the 200.00; the ledger has, so
  it is outstanding there, dated the day it was sent."""
  session, _accounts, savings = in_transit

  rec = _record(session, savings, 100.00)

  assert rec.status == "reconciled"
  assert (rec.ledger_balance, rec.independent_balance) == (300.00, 300.00)
  statement, transit = rec.components
  assert (statement.kind, statement.amount) == ("statement", 100.00)
  assert (transit.kind, transit.amount, transit.posting_date) == (
    "outstanding",
    200.00,
    date(2026, 9, 30),
  )


def test_a_transfer_clears_on_the_receiver_at_its_leg_date(in_transit):
  session, _accounts, savings = in_transit

  lines = account_lines(
    session, savings, date(2026, 10, 31), entity_id=PARENT_ENTITY_ID
  )

  assert lines.outstanding(date(2026, 10, 1)) != []
  assert lines.outstanding(date(2026, 10, 2)) == []
  assert lines.feed_activity(date(2026, 9, 30), date(2026, 10, 31)) == (1, 20_000)


def test_an_account_no_feed_keeps_is_compared_as_before(bank):
  """Without feed lines nothing can be called cleared, so the difference
  stays one unexplained number."""
  session, accounts = bank
  savings = classified_account(session, "Savings", "asset")
  entry(session, date(2026, 9, 5), savings, accounts["equity"], 10_000)
  session.commit()

  rec = _record(session, savings, 90.00)

  assert rec.status == "unreconciled"
  assert rec.unreconciled_difference == 10.00
  assert [c.kind for c in rec.components] == ["statement"]
  assert rec.roll_forward is None


def test_a_statement_ending_mid_month_is_carried_to_the_period_end(bank):
  """A cycle ending 09-05: the bank held 1,500.00 then. The feed's -200.00
  on 09-10 carries it to 1,300.00 at 09-30; the ledger holds 1,250.00, the
  difference being the 50.00 payment recorded by hand after the statement."""
  session, accounts = bank

  rec = _record(session, accounts["cash"], 1_500.00, as_of=date(2026, 9, 5))

  assert (rec.status, rec.balance_as_of) == ("reconciled", date(2026, 9, 5))
  assert [c.kind for c in rec.components] == ["statement"]
  carried = rec.roll_forward
  assert carried is not None
  assert (carried.statement_as_of, carried.through) == (
    date(2026, 9, 5),
    date(2026, 9, 30),
  )
  assert (carried.bank_lines, carried.bank_activity, carried.bank_balance) == (
    1,
    -200.00,
    1_300.00,
  )
  assert (carried.ledger_balance, carried.outstanding) == (1_250.00, -50.00)
  assert carried.feed_balance is None


def test_the_feeds_own_balance_cross_checks_the_carried_one(bank):
  """The feed read 1,400.00 on 10-03, after a +100.00 line on 10-02: less
  that line, 1,300.00 at 09-30, which agrees with the carried balance."""
  session, accounts = bank
  _feed_line(session, date(2026, 10, 2), accounts["cash"], accounts["revenue"], 10_000)
  record_feed_balance(
    session,
    element=session.get(Element, accounts["cash"]),
    entity_id=PARENT_ENTITY_ID,
    source="plaid",
    connection_id="conn",
    reading=FeedBalance(
      account_id="acct",
      kind=BANK_CURRENT,
      as_of=date(2026, 10, 3),
      stated_cents=140_000,
      observed_at=datetime(2026, 10, 3, 12, tzinfo=UTC),
    ),
    created_by="test",
  )
  session.commit()

  rec = _record(session, accounts["cash"], 1_500.00, as_of=date(2026, 9, 5))

  carried = rec.roll_forward
  assert carried is not None
  assert (carried.feed_balance, carried.feed_balance_read_on) == (
    1_300.00,
    date(2026, 10, 3),
  )


@pytest.mark.parametrize(
  ("period_end", "cycle", "start"),
  [
    (date(2026, 9, 30), "monthly", date(2026, 9, 1)),
    (date(2026, 11, 30), "quarterly", date(2026, 9, 1)),
    (date(2026, 2, 28), "quarterly", date(2025, 12, 1)),
    (date(2026, 9, 30), "annual", date(2025, 10, 1)),
  ],
)
def test_the_cycle_that_ends_with_a_period(period_end, cycle, start):
  assert cycle_start(period_end, cycle) == start


def test_a_quarterly_statement_covers_the_months_until_the_next(bank):
  session, accounts = bank
  rec = _record(session, accounts["cash"], 1_300.00)

  with pytest.raises(NothingToReconcileError):
    _refresh(session, "2026-11")

  set_reconciliation_policy(
    session,
    SetReconciliationPolicyRequest(
      structure_id=rec.structure_id, statement_cycle="quarterly"
    ),
    graph_id=GRAPH_ID,
    created_by="usr",
  )
  session.commit()
  _refresh(session, "2026-11")

  november = _statement_rec(session, "2026-11")
  assert (november.status, november.statement_cycle, november.balance_as_of) == (
    "reconciled",
    "quarterly",
    date(2026, 9, 30),
  )
  assert november.roll_forward is not None
  assert november.roll_forward.through == date(2026, 11, 30)


def test_only_a_statement_block_takes_a_cycle(bank):
  from robosystems.operations.roboledger.reconciliations.blocks import (
    create_account_reconciliation,
  )

  session, accounts = bank
  structure = create_account_reconciliation(
    session,
    method="schedule_register",
    element_id=accounts["cash"],
    account_name="Checking",
    entity_id=PARENT_ENTITY_ID,
    required_for_close=False,
    created_by="usr",
  )
  session.commit()

  with pytest.raises(ValueError, match="only a statement block"):
    set_reconciliation_policy(
      session,
      SetReconciliationPolicyRequest(
        structure_id=str(structure.id), statement_cycle="quarterly"
      ),
      graph_id=GRAPH_ID,
      created_by="usr",
    )


# ── a bank change settled after its month closed ─────────────────────────────


@pytest.fixture()
def revised_line(ext_session, monkeypatch):
  """Opening 1,000.00 entered by hand on 08-01; the feed brought -100.00 on
  08-20 and +50.00 on 09-05. August is closed. The bank then revised the
  08-20 line to -120.00, which flags it: the bank holds 930.00 at 09-30."""
  from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod

  monkeypatch.setattr(
    "robosystems.operations.event_block.commands._validate_event_source",
    lambda source, graph_id: None,
  )
  session = ext_session
  accounts = {
    "cash": classified_account(session, "Checking", "asset"),
    "equity": classified_account(
      session, "Members Equity", "equity", balance_type="credit"
    ),
    "revenue": classified_account(session, "Revenue", "revenue", balance_type="credit"),
    "supplies": classified_account(session, "Supplies", "expense"),
  }
  for name, start, end, status in (
    ("2026-08", date(2026, 8, 1), date(2026, 8, 31), "closed"),
    ("2026-09", date(2026, 9, 1), date(2026, 9, 30), "open"),
  ):
    session.add(
      FiscalPeriod(
        entity_id=PARENT_ENTITY_ID,
        graph_id=GRAPH_ID,
        name=name,
        start_date=start,
        end_date=end,
        period_type="month",
        status=status,
      )
    )
  entry(session, date(2026, 8, 1), accounts["cash"], accounts["equity"], 100_000)
  revised = _feed_line(
    session, date(2026, 8, 20), accounts["supplies"], accounts["cash"], 10_000
  )
  _feed_line(session, date(2026, 9, 5), accounts["cash"], accounts["revenue"], 5_000)
  item = session.get(Event, revised.triggered_by_event_id)
  bank_says = {
    "status": "posted",
    "source_amount": 12_000,
    "source_posted_date": "2026-08-20",
    "posting_date": "2026-08-20",
    "memo": "Bank line",
    "line_items": [
      {"element_id": accounts["supplies"], "debit_amount": 12_000, "credit_amount": 0},
      {"element_id": accounts["cash"], "debit_amount": 0, "credit_amount": 12_000},
    ],
  }
  item.metadata_ = {
    **(item.metadata_ or {}),
    "drift_payload": bank_says,
    "drift_detected_at": "2026-09-03T10:00:00+00:00",
  }
  item.payload_drift = True
  session.commit()
  return session, accounts, item


def test_a_catch_up_of_a_bank_change_is_the_feeds_business(revised_line):
  """The catch-up puts -20.00 on Checking. The bank already holds that change,
  so the line is not one it has yet to clear."""
  from robosystems.models.api.extensions.reconciling_items import (
    ResolveReconcilingItemRequest,
  )
  from robosystems.operations.roboledger.commands.reconciling_items import (
    plan_reconciling_item,
    resolve_reconciling_item,
  )

  session, accounts, item = revised_line
  plan = plan_reconciling_item(session, str(item.id), graph_id=GRAPH_ID)
  assert plan.default_disposition == "catch_up"
  resolve_reconciling_item(
    session,
    ResolveReconcilingItemRequest(
      event_id=str(item.id),
      expected_drift_detected_at=plan.drift_detected_at,
      posting_date=date(2026, 9, 30),
    ),
    "usr",
    graph_id=GRAPH_ID,
  )
  session.commit()

  rec = _record(session, accounts["cash"], 930.00)

  assert rec.status == "reconciled"
  assert [c.kind for c in rec.components] == ["statement"]


def test_an_alignment_that_settles_a_bank_change_is_the_feeds_business(revised_line):
  """The same change settled by a hand alignment that names the line: its
  Checking line is the feed's business too, whichever door settled it."""
  from robosystems.models.api.event_block import CreateEventBlockRequest
  from robosystems.operations.event_block.commands import create_event_block_in_session
  from robosystems.operations.roboledger.commands.reconciling_items import (
    plan_reconciling_item,
  )

  session, accounts, item = revised_line
  plan = plan_reconciling_item(session, str(item.id), graph_id=GRAPH_ID)
  assert plan.drift_detected_at is not None
  create_event_block_in_session(
    session,
    CreateEventBlockRequest(
      event_type="journal_entry_recorded",
      event_category="adjustment",
      source="manual",
      occurred_at=datetime(2026, 9, 30),
      apply_handlers=True,
      metadata={
        "posting_date": "2026-09-30",
        "memo": "The bank revised the 08-20 supplies line",
        "line_items": [
          {"element_id": accounts["supplies"], "debit_amount": 2_000},
          {"element_id": accounts["cash"], "credit_amount": 2_000},
        ],
        "resolves_reconciling_items": [str(item.id)],
      },
    ),
    "usr",
    graph_id=GRAPH_ID,
  )
  session.commit()

  rec = _record(session, accounts["cash"], 930.00)

  assert rec.status == "reconciled"
  assert [c.kind for c in rec.components] == ["statement"]
