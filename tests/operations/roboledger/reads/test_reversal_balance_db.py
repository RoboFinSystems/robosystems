"""Reversing an entry must net to zero — asserted on the balance, not the status.

The reversal defect was invisible to the existing suites because they assert the
*mechanism*: `test_journal_entries` checks that the original's status becomes
`reversed` and that an offsetting entry is written, which is exactly what the
code did. Both halves were right. What nobody asserted was the number that comes
out the other side.

It came out wrong. Every balance read filtered `status = 'posted'`, so the
original dropped out of the sums while its offset — an ordinary `posted` row —
stayed in. Reversing a $5,000 rent accrual left rent expense at **-$5,000** and
cash at **+$5,000**, and because each entry is internally balanced the trial
balance still footed. Nothing raised a flag.

So these tests run the real SQL against a real Postgres and assert the balance.
A mocked session cannot see this class of defect, and a status assertion actively
ratifies it.
"""

from __future__ import annotations

import os
import uuid
from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions import Element
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.operations.roboledger.reads.trial_balance import get_trial_balance

pytestmark = pytest.mark.unit

MARCH_START = date(2026, 3, 1)
MARCH_END = date(2026, 3, 31)
RENT_CENTS = 500_000  # $5,000.00


@pytest.fixture()
def ext_session():
  """Extensions schema in the test Postgres DB, one throwaway schema per test."""
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_rev_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))

  session = sessionmaker(bind=engine)()
  session.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=session.connection())
  session.commit()
  session.execute(text(f'SET search_path TO "{schema}"'))

  try:
    yield session
  finally:
    session.rollback()
    session.close()
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


@pytest.fixture()
def accounts(ext_session):
  rent = Element(name="Rent Expense", code="6100", balance_type="debit")
  cash = Element(name="Cash", code="1010", balance_type="debit")
  ext_session.add_all([rent, cash])
  ext_session.flush()
  return rent, cash


def _entry(
  ext_session,
  *,
  status,
  debit_element,
  credit_element,
  cents,
  type_="standard",
  reversal_of=None,
):
  entry = Entry(
    posting_date=date(2026, 3, 10),
    status=status,
    type=type_,
    reversal_of=reversal_of,
    created_by="usr_test",
  )
  ext_session.add(entry)
  ext_session.flush()
  ext_session.add_all(
    [
      LineItem(
        entry_id=entry.id,
        element_id=debit_element.id,
        debit_amount=cents,
        credit_amount=0,
      ),
      LineItem(
        entry_id=entry.id,
        element_id=credit_element.id,
        debit_amount=0,
        credit_amount=cents,
      ),
    ]
  )
  ext_session.flush()
  return entry


def _net_by_code(response, code):
  for row in response.rows:
    if row.account_code == code:
      return row.net_balance
  return None


def test_reversed_pair_nets_to_zero(ext_session, accounts):
  """The whole point. DR rent / CR cash, then its reversal, nets both to zero.

  Before the fix this returned rent -5000.00 and cash +5000.00: the original was
  excluded as `reversed` while the reversing entry stayed in as `posted`.
  """
  rent, cash = accounts
  original = _entry(
    ext_session,
    status="reversed",
    debit_element=rent,
    credit_element=cash,
    cents=RENT_CENTS,
  )
  # The offsetting entry the reversal writes: flipped legs, posted, landed.
  _entry(
    ext_session,
    status="posted",
    debit_element=cash,
    credit_element=rent,
    cents=RENT_CENTS,
    type_="reversing",
    reversal_of=original.id,
  )

  tb = get_trial_balance(ext_session, MARCH_START, MARCH_END)

  assert _net_by_code(tb, "6100") == 0.0, "rent expense must net to zero, not -5000"
  assert _net_by_code(tb, "1010") == 0.0, "cash must net to zero, not +5000"
  assert tb.total_debits == tb.total_credits


def test_unreversed_entry_still_counts(ext_session, accounts):
  """Guard against over-correction: an ordinary posted entry is unchanged."""
  rent, cash = accounts
  _entry(
    ext_session,
    status="posted",
    debit_element=rent,
    credit_element=cash,
    cents=RENT_CENTS,
  )

  tb = get_trial_balance(ext_session, MARCH_START, MARCH_END)

  assert _net_by_code(tb, "6100") == 5000.00
  assert _net_by_code(tb, "1010") == -5000.00


def test_draft_is_still_excluded(ext_session, accounts):
  """`draft` stays out. Widening the predicate to `reversed` must not widen it
  to everything — entries of voided events remain `draft` and have not landed."""
  rent, cash = accounts
  _entry(
    ext_session,
    status="draft",
    debit_element=rent,
    credit_element=cash,
    cents=RENT_CENTS,
  )

  tb = get_trial_balance(ext_session, MARCH_START, MARCH_END)

  assert tb.rows == []
  assert tb.total_debits == 0.0
