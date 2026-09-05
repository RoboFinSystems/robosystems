"""Transaction read models.

Read-only by design: Transactions are created as a side effect of
`create-event-block(event_type='journal_entry_recorded')`, which mints the
parent Transaction inside `create_journal_entry`. These shapes serve the
ledger-display routes and GraphQL.
"""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, Field

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


# ── Journal entries (entry-centric read) ──────────────────────────────────


class LedgerJournalEntryResponse(BaseModel):
  """A posted or draft journal entry, read on its own terms.

  Distinct from `LedgerEntryResponse`, which is an entry *underneath* a
  transaction in the detail view. An entry does not need a parent:
  `Entry.transaction_id` is nullable by design (see the model comment on
  `Entry.triggered_by_event_id`), and the schedule engine and event
  handlers create entries with no transaction at all. Those entries are
  invisible to the transaction-centric reads, which is what this shape
  exists to fix — so `transaction_id` is projected explicitly, and a
  `None` here means a standalone entry rather than missing data.
  """

  id: str
  number: str | None = None
  transaction_id: str | None = Field(
    None,
    description=(
      "Parent transaction, or null for a standalone entry (schedule-derived "
      "closing entries and event-handler entries have no parent)"
    ),
  )
  type: str = Field(
    ..., description="'standard' | 'adjusting' | 'closing' | 'reversing'"
  )
  status: str = Field(..., description="'draft' | 'posted' | 'reversed'")
  posting_date: date
  memo: str | None = None
  provenance: str | None = Field(
    None,
    description=(
      "Where the entry came from (ENTRY_PROVENANCE_VALUES): source_sync, "
      "ai_generated, manual_entry, schedule_derived, system_computed, event_handler"
    ),
  )
  source_structure_id: str | None = Field(
    None, description="Schedule structure that generated this entry (if any)"
  )
  source_structure_name: str | None = Field(
    None, description="Human-readable name of the source schedule"
  )
  triggered_by_event_id: str | None = Field(
    None, description="Business event that caused this entry (if any)"
  )
  reversal_of: str | None = Field(
    None, description="The entry this one reverses (if any)"
  )
  posted_at: datetime | None = None
  line_items: list[LedgerLineItemResponse]
  total_debit: float
  total_credit: float
  balanced: bool = Field(..., description="True if total_debit == total_credit")


class LedgerJournalEntryListResponse(BaseModel):
  """Paginated journal listing — entries with their line items expanded.

  Pagination counts *entries*, not line-item rows.
  """

  entries: list[LedgerJournalEntryResponse]
  pagination: PaginationInfo
