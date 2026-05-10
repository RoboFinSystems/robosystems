"""Transaction read models.

Standalone Transaction creation is no longer a public surface — every
GL write goes through `create-event-block(event_type='journal_entry_recorded')`,
which auto-creates the Transaction parent inside `create_journal_entry`.
The read shapes below are used by ledger-display routes and GraphQL.
"""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel

from robosystems.models.api.common import PaginationInfo

# ── Read ──────────────────────────────────────────────────────────────────


class LedgerTransactionSummaryResponse(BaseModel):
  """Transaction header — list/grid view without entries.

  Transaction is the business-event level (what happened in the real
  world). Entries (journal entries) live one level down and are loaded
  in the detail view. ``source`` distinguishes integration-imported
  rows (quickbooks / xero / plaid) from native-created ones.
  """

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
  """Paginated transaction listing — header view."""

  transactions: list[LedgerTransactionSummaryResponse]
  pagination: PaginationInfo


class LedgerLineItemResponse(BaseModel):
  """One debit/credit line within a journal entry. Always exactly one
  side has a non-zero amount.
  """

  id: str
  account_id: str
  account_name: str | None = None
  account_code: str | None = None
  debit_amount: float
  credit_amount: float
  description: str | None = None
  line_order: int


class LedgerEntryResponse(BaseModel):
  """A journal entry — accounting interpretation of a transaction.

  Each transaction has 1+ entries; each entry has 2+ line items that
  must balance. ``status`` is the draft/posted/reversed lifecycle;
  ``type`` is the entry classification ('standard' | 'adjusting' |
  'closing' | 'reversing').
  """

  id: str
  number: str | None = None
  type: str
  posting_date: date
  memo: str | None = None
  status: str
  posted_at: datetime | None = None
  line_items: list[LedgerLineItemResponse]


class LedgerTransactionDetailResponse(BaseModel):
  """Full transaction detail — header + every journal entry + every
  line item underneath. Used by the transaction detail page."""

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
