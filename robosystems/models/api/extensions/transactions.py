"""Transaction write and read models."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel

from robosystems.models.api.common import PaginationInfo

# ── Create ────────────────────────────────────────────────────────────────


class CreateTransactionRequest(BaseModel):
  """Create a standalone business-event Transaction.

  Used internally by the `transaction_recorded` event handler
  (see operations/roboledger/commands/event_block/python_handlers/);
  the public surface is `create-event-block(event_type='transaction_recorded')`.

  `amount` is in minor currency units (cents). `type` is free-form but
  common values are: invoice, payment, bill, expense, deposit, transfer,
  journal_entry.
  """

  type: str
  date: date
  amount: int
  currency: str = "USD"
  description: str | None = None
  merchant_name: str | None = None
  reference_number: str | None = None
  number: str | None = None
  category: str | None = None
  due_date: date | None = None
  status: Literal["pending", "posted"] = "pending"


class TransactionResponse(BaseModel):
  """Response shape for a standalone Transaction create. Used internally
  by the `transaction_recorded` event handler."""

  id: str
  type: str
  date: date
  amount: int
  currency: str
  description: str | None = None
  merchant_name: str | None = None
  reference_number: str | None = None
  number: str | None = None
  category: str | None = None
  due_date: date | None = None
  status: Literal["pending", "posted", "void"]
  source: str


# ── Read ──────────────────────────────────────────────────────────────────


class LedgerTransactionSummaryResponse(BaseModel):
  id: str
  number: str | None = None
  type: str
  category: str | None = None
  amount: float
  currency: str
  date: date
  due_date: date | None = None
  merchant_name: str | None = None
  reference_number: str | None = None
  description: str | None = None
  source: str
  status: str


class LedgerTransactionListResponse(BaseModel):
  transactions: list[LedgerTransactionSummaryResponse]
  pagination: PaginationInfo


class LedgerLineItemResponse(BaseModel):
  id: str
  account_id: str
  account_name: str | None = None
  account_code: str | None = None
  debit_amount: float
  credit_amount: float
  description: str | None = None
  line_order: int


class LedgerEntryResponse(BaseModel):
  id: str
  number: str | None = None
  type: str
  posting_date: date
  memo: str | None = None
  status: str
  posted_at: datetime | None = None
  line_items: list[LedgerLineItemResponse]


class LedgerTransactionDetailResponse(BaseModel):
  id: str
  number: str | None = None
  type: str
  category: str | None = None
  amount: float
  currency: str
  date: date
  due_date: date | None = None
  merchant_name: str | None = None
  reference_number: str | None = None
  description: str | None = None
  source: str
  source_id: str | None = None
  status: str
  posted_at: datetime | None = None
  entries: list[LedgerEntryResponse]
