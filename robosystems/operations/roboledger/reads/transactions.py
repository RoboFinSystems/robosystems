"""Transaction read operations."""

from __future__ import annotations

from datetime import date

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from robosystems.models.api.common import create_pagination_info
from robosystems.models.api.extensions import cents_to_dollars
from robosystems.models.api.extensions.transactions import (
  LedgerEntryResponse,
  LedgerLineItemResponse,
  LedgerTransactionDetailResponse,
  LedgerTransactionListResponse,
  LedgerTransactionSummaryResponse,
)
from robosystems.models.extensions import Element, Entry, LineItem, Transaction


def _txn_to_summary(row: Transaction) -> LedgerTransactionSummaryResponse:
  return LedgerTransactionSummaryResponse(
    id=row.id,
    number=row.number,
    type=row.type,
    category=row.category,
    amount=cents_to_dollars(row.amount),
    currency=row.currency,
    date=row.date,
    due_date=row.due_date,
    merchant_name=row.merchant_name,
    reference_number=row.reference_number,
    description=row.description,
    source=row.source,
    status=row.status,
  )


def list_transactions(
  session: Session,
  *,
  type: str | None = None,
  start_date: date | None = None,
  end_date: date | None = None,
  limit: int = 100,
  offset: int = 0,
) -> LedgerTransactionListResponse:
  """List transactions filtered by type and date range, paginated."""
  query = select(Transaction)
  count_query = select(func.count()).select_from(Transaction)

  if type is not None:
    query = query.where(Transaction.type == type)
    count_query = count_query.where(Transaction.type == type)
  if start_date is not None:
    query = query.where(Transaction.date >= start_date)
    count_query = count_query.where(Transaction.date >= start_date)
  if end_date is not None:
    query = query.where(Transaction.date <= end_date)
    count_query = count_query.where(Transaction.date <= end_date)

  total = session.execute(count_query).scalar() or 0
  rows = (
    session.execute(
      query.order_by(Transaction.date.desc(), Transaction.id)
      .offset(offset)
      .limit(limit)
    )
    .scalars()
    .all()
  )

  return LedgerTransactionListResponse(
    transactions=[_txn_to_summary(r) for r in rows],
    pagination=create_pagination_info(total, limit, offset),
  )


def get_transaction(
  session: Session, transaction_id: str
) -> LedgerTransactionDetailResponse | None:
  """Return the full transaction detail (entries + line items), or None.

  Returns None when no transaction row exists with the given id. The
  caller translates None into a 404.
  """
  txn = session.execute(
    select(Transaction).where(Transaction.id == transaction_id)
  ).scalar_one_or_none()

  if txn is None:
    return None

  entries = (
    session.execute(
      select(Entry)
      .where(Entry.transaction_id == transaction_id)
      .order_by(Entry.posting_date)
    )
    .scalars()
    .all()
  )

  entry_responses: list[LedgerEntryResponse] = []
  for entry in entries:
    line_items = session.execute(
      select(LineItem, Element.name, Element.code)
      .join(Element, LineItem.element_id == Element.id)
      .where(LineItem.entry_id == entry.id)
      .order_by(LineItem.line_order)
    ).all()

    li_responses = [
      LedgerLineItemResponse(
        id=li.id,
        account_id=li.element_id,
        account_name=acct_name,
        account_code=acct_code,
        debit_amount=cents_to_dollars(li.debit_amount),
        credit_amount=cents_to_dollars(li.credit_amount),
        description=li.description,
        line_order=li.line_order,
      )
      for li, acct_name, acct_code in line_items
    ]

    entry_responses.append(
      LedgerEntryResponse(
        id=entry.id,
        number=entry.number,
        type=entry.type,
        posting_date=entry.posting_date,
        memo=entry.memo,
        status=entry.status,
        posted_at=entry.posted_at,
        line_items=li_responses,
      )
    )

  return LedgerTransactionDetailResponse(
    id=txn.id,
    number=txn.number,
    type=txn.type,
    category=txn.category,
    amount=cents_to_dollars(txn.amount),
    currency=txn.currency,
    date=txn.date,
    due_date=txn.due_date,
    merchant_name=txn.merchant_name,
    reference_number=txn.reference_number,
    description=txn.description,
    source=txn.source,
    source_id=txn.source_id,
    status=txn.status,
    posted_at=txn.posted_at,
    entries=entry_responses,
  )
