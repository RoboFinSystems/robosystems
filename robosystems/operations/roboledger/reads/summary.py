"""Ledger summary read operations."""

from __future__ import annotations

from datetime import date
from typing import NamedTuple

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from robosystems.models.extensions import Element, Entry, LineItem, Transaction


class LedgerCounts(NamedTuple):
  """Aggregate counts + date range for a ledger, read from the extensions DB."""

  account_count: int
  transaction_count: int
  entry_count: int
  line_item_count: int
  earliest_transaction_date: date | None
  latest_transaction_date: date | None


def get_ledger_counts(session: Session) -> LedgerCounts:
  """Return element/transaction/entry/line-item counts plus date range.

  Does not touch the platform DB — the caller merges in connection
  metadata separately. Separating the two reads keeps the test surface
  simple and lets a platform-DB failure be logged without aborting the
  summary response.
  """
  account_count = (
    session.execute(select(func.count()).select_from(Element)).scalar() or 0
  )
  transaction_count = (
    session.execute(select(func.count()).select_from(Transaction)).scalar() or 0
  )
  entry_count = session.execute(select(func.count()).select_from(Entry)).scalar() or 0
  line_item_count = (
    session.execute(select(func.count()).select_from(LineItem)).scalar() or 0
  )

  date_range = session.execute(
    select(func.min(Transaction.date), func.max(Transaction.date))
  ).one()

  return LedgerCounts(
    account_count=account_count,
    transaction_count=transaction_count,
    entry_count=entry_count,
    line_item_count=line_item_count,
    earliest_transaction_date=date_range[0],
    latest_transaction_date=date_range[1],
  )
