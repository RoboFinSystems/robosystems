"""Transaction response models."""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel

from robosystems.models.api.common import PaginationInfo


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
