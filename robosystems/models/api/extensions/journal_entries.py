"""Journal entry write models — native accounting CRUD surface.

The Pydantic shapes used by:
  - `create-event-block(event_type='journal_entry_recorded')` for new
    entries and `event_type='journal_entry_reversed'` for reversals (via
    the metadata schema → `CreateJournalEntryRequest` /
    `ReverseJournalEntryRequest` adapter calls).
  - `update-journal-entry` / `delete-journal-entry` ops for in-place
    draft correction.

For display/read models of existing transactions and entries, see
`transactions.py` (`LedgerEntryResponse`, `LedgerTransactionDetailResponse`,
etc.).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field

# ── Line item shapes ─────────────────────────────────────────────────────


class JournalEntryLineItemInput(BaseModel):
  """One debit/credit line in a journal entry create/update payload.

  Exactly one of `debit_amount` / `credit_amount` must be non-zero; the
  other must be zero. Amounts are in minor currency units (cents)."""

  element_id: str
  debit_amount: int = 0
  credit_amount: int = 0
  description: str | None = None


class JournalEntryLineItemResponse(BaseModel):
  """One line in a journal entry response."""

  id: str
  element_id: str
  debit_amount: int
  credit_amount: int
  description: str | None = None
  line_order: int


# ── Entry response ───────────────────────────────────────────────────────


class JournalEntryResponse(BaseModel):
  """The common response shape for journal entry write operations."""

  id: str
  transaction_id: str | None = None
  type: str
  status: str
  posting_date: date
  memo: str | None = None
  provenance: str | None = None
  reversal_of: str | None = None
  posted_at: datetime | None = None
  line_items: list[JournalEntryLineItemResponse]
  total_debit: int
  total_credit: int


# ── Create ────────────────────────────────────────────────────────────────


class CreateJournalEntryRequest(BaseModel):
  """Create a new journal entry with balanced line items.

  Defaults to `status='draft'` for ongoing native writes (the normal
  workflow: draft → review → post via close-period). Pass
  `status='posted'` for historical data import where entries represent
  already-happened business events that don't need review.

  Total debit amount must equal total credit amount or the request
  is rejected with 422. `line_items` must contain at least two rows
  (at least one debit, at least one credit).
  """

  posting_date: date
  memo: str
  line_items: list[JournalEntryLineItemInput] = Field(..., min_length=2)
  type: Literal["standard", "adjusting", "closing", "reversing"] = "standard"
  status: Literal["draft", "posted"] = "draft"
  transaction_id: str | None = None


# ── Update ────────────────────────────────────────────────────────────────


class UpdateJournalEntryRequest(BaseModel):
  """Update a draft journal entry. Posted entries cannot be updated —
  create a reversal and a corrected entry instead.

  Omitted fields are left unchanged. If `line_items` is provided, the
  existing line items are replaced entirely (atomic delete + insert)
  and the new set must still balance. If `line_items` is omitted, the
  existing line items are left untouched.
  """

  entry_id: str
  posting_date: date | None = None
  memo: str | None = None
  type: Literal["standard", "adjusting", "closing", "reversing"] | None = None
  line_items: list[JournalEntryLineItemInput] | None = None


# ── Delete ────────────────────────────────────────────────────────────────


class DeleteJournalEntryRequest(BaseModel):
  """Hard delete a draft journal entry. Posted entries cannot be
  deleted — reverse them instead."""

  entry_id: str


# ── Reverse ───────────────────────────────────────────────────────────────


class ReverseJournalEntryRequest(BaseModel):
  """Reverse a posted journal entry.

  Creates a new entry whose line items flip the originals
  (debits → credits, credits → debits), sets `reversal_of` on the new
  entry to the original's id, marks the original as
  `status='reversed'`, and posts the reversing entry immediately.

  This is how accountants correct posted entries — the original stays
  in the audit trail, the reversal cancels its effect, and a
  corrected entry can be created separately.
  """

  entry_id: str
  posting_date: date | None = None  # defaults to today
  memo: str | None = None  # auto-generated if None
