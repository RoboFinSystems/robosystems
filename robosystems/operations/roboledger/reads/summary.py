"""Ledger summary read operations."""

from __future__ import annotations

from datetime import date
from typing import NamedTuple

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from robosystems.models.extensions import Element, Entry, LineItem, Transaction
from robosystems.operations.roboledger.reads.accounts import (
  account_scope,
  entity_accounts_clause,
)


class LedgerCounts(NamedTuple):
  """Aggregate counts + date range for one entity's ledger, read from the
  extensions DB."""

  account_count: int
  transaction_count: int
  entry_count: int
  line_item_count: int
  earliest_transaction_date: date | None
  latest_transaction_date: date | None
  entity_id: str | None = None


def get_ledger_counts(session: Session, entity_id: str | None = None) -> LedgerCounts:
  """Return one entity's account/transaction/entry/line-item counts plus date
  range, default the group parent's.

  Extensions DB only; the caller merges platform connection metadata, so a
  platform failure cannot abort the summary.
  """
  scope = account_scope(session, entity_id)
  entity_id = scope.entity_id
  account_count = (
    session.execute(
      select(func.count()).select_from(Element).where(entity_accounts_clause(scope))
    ).scalar()
    or 0
  )
  transactions = select(func.count()).select_from(Transaction)
  entries = select(func.count()).select_from(Entry)
  line_items = (
    select(func.count())
    .select_from(LineItem)
    .join(Entry, LineItem.entry_id == Entry.id)
  )
  dates = select(func.min(Transaction.date), func.max(Transaction.date))
  if entity_id is not None:
    transactions = transactions.where(Transaction.entity_id == entity_id)
    entries = entries.where(Entry.entity_id == entity_id)
    line_items = line_items.where(Entry.entity_id == entity_id)
    dates = dates.where(Transaction.entity_id == entity_id)
  transaction_count = session.execute(transactions).scalar() or 0
  entry_count = session.execute(entries).scalar() or 0
  line_item_count = session.execute(line_items).scalar() or 0

  date_range = session.execute(dates).one()

  return LedgerCounts(
    entity_id=entity_id,
    account_count=account_count,
    transaction_count=transaction_count,
    entry_count=entry_count,
    line_item_count=line_item_count,
    earliest_transaction_date=date_range[0],
    latest_transaction_date=date_range[1],
  )
