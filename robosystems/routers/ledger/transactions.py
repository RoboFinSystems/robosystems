"""Ledger transaction endpoints."""

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy import func, select
from sqlalchemy.exc import ProgrammingError

from robosystems.db.ledger import oltp_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import create_error_response, create_pagination_info
from robosystems.models.api.ledger import cents_to_dollars
from robosystems.models.api.ledger.transactions import (
  LedgerEntryResponse,
  LedgerLineItemResponse,
  LedgerTransactionDetailResponse,
  LedgerTransactionListResponse,
  LedgerTransactionSummaryResponse,
)
from robosystems.models.iam import User
from robosystems.models.ledger import Account, Entry, LineItem, Transaction

router = APIRouter()


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


@router.get(
  "/transactions",
  response_model=LedgerTransactionListResponse,
  operation_id="listLedgerTransactions",
  summary="List Transactions",
  tags=["Ledger"],
)
async def list_transactions(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  type: str | None = Query(None, description="Filter by transaction type"),
  start_date: date | None = Query(None, description="Start date (inclusive)"),
  end_date: date | None = Query(None, description="End date (inclusive)"),
  limit: int = Query(100, ge=1, le=1000),
  offset: int = Query(0, ge=0),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with oltp_session(graph_id) as session:
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
  except ValueError:
    raise HTTPException(
      status_code=404,
      detail="Ledger not initialized. Connect a data source first.",
    )
  except ProgrammingError:
    raise HTTPException(
      status_code=404,
      detail="Ledger not initialized. Connect a data source first.",
    )


@router.get(
  "/transactions/{transaction_id}",
  response_model=LedgerTransactionDetailResponse,
  operation_id="getLedgerTransaction",
  summary="Transaction Detail",
  tags=["Ledger"],
)
async def get_transaction(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  transaction_id: str = Path(...),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with oltp_session(graph_id) as session:
      txn = session.execute(
        select(Transaction).where(Transaction.id == transaction_id)
      ).scalar_one_or_none()

      if not txn:
        raise create_error_response(404, f"Transaction {transaction_id} not found")

      entries = (
        session.execute(
          select(Entry)
          .where(Entry.transaction_id == transaction_id)
          .order_by(Entry.posting_date)
        )
        .scalars()
        .all()
      )

      entry_responses = []
      for entry in entries:
        line_items = session.execute(
          select(LineItem, Account.name, Account.code)
          .join(Account, LineItem.account_id == Account.id)
          .where(LineItem.entry_id == entry.id)
          .order_by(LineItem.line_order)
        ).all()

        li_responses = [
          LedgerLineItemResponse(
            id=li.id,
            account_id=li.account_id,
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
  except ValueError:
    raise HTTPException(
      status_code=404,
      detail="Ledger not initialized. Connect a data source first.",
    )
  except ProgrammingError:
    raise HTTPException(
      status_code=404,
      detail="Ledger not initialized. Connect a data source first.",
    )
