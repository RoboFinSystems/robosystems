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
