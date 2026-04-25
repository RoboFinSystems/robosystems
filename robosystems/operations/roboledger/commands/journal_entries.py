"""Journal entry CRUD commands — native accounting write path.

Pure functions over an extensions `Session`, returning Pydantic
response models. These are the single source of truth for journal
entry writes; REST routes, MCP tools, and agents all delegate here.

Design notes:

- **Transaction auto-created.** Every journal entry needs a parent
  Transaction so the graph has a traversal path (Entity → Transaction →
  Entry → LineItem). When the caller does not supply a `transaction_id`,
  a synthetic Transaction of type "journal_entry" is created in the same
  flush. Callers that already hold a Transaction (e.g., the QB pipeline)
  pass it explicitly and no synthetic row is written.

- **Draft-only edits.** `update_journal_entry` and `delete_journal_entry`
  only operate on entries with `status='draft'`. Posted entries are
  immutable and must be corrected via `reverse_journal_entry`. This
  matches standard double-entry bookkeeping practice — the audit trail
  never silently loses posted entries.

- **Reversal semantics.** `reverse_journal_entry` creates a new Entry
  with flipped line items (debits ↔ credits), sets `reversal_of` to
  the original's id, and marks the original `status='reversed'`. The
  reversing entry is posted immediately. Both rows stay in the ledger
  forever — the audit trail shows original + reversal side by side.

- **Closed-period gate.** Enforced via `assert_period_not_closed()` from
  `_guards.py`. Rejects writes whose `posting_date` falls in a closed
  fiscal period. Applied to create, update (when posting_date changes),
  and reverse (on the reversal's posting_date).
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  DeleteJournalEntryRequest,
  JournalEntryLineItemInput,
  JournalEntryLineItemResponse,
  JournalEntryResponse,
  ReverseJournalEntryRequest,
  UpdateJournalEntryRequest,
)
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.models.extensions.roboledger.transaction import Transaction
from robosystems.operations.roboledger.commands._guards import (
  assert_period_not_closed,
)


class JournalEntryNotFoundError(LookupError):
  """Raised when a journal entry is not found by id."""

  def __init__(self, entry_id: str) -> None:
    super().__init__(f"Journal entry not found: {entry_id}")
    self.entry_id = entry_id


class JournalEntryNotDraftError(ValueError):
  """Raised when trying to update or delete a non-draft entry.

  Posted and reversed entries are immutable — the caller must use
  `reverse_journal_entry` instead of editing or deleting them directly.
  """

  def __init__(self, entry_id: str, status: str) -> None:
    super().__init__(
      f"Journal entry {entry_id} is {status!r}; only draft entries can be "
      f"updated or deleted. Fire `create-event-block(event_type="
      f"'journal_entry_reversed', metadata={{entry_id: ...}})` for "
      f"posted entries."
    )
    self.entry_id = entry_id
    self.status = status


class JournalEntryNotPostedError(ValueError):
  """Raised when trying to reverse a non-posted entry.

  Draft entries should be deleted; already-reversed entries should
  not be reversed again.
  """

  def __init__(self, entry_id: str, status: str) -> None:
    super().__init__(
      f"Journal entry {entry_id} is {status!r}; only posted entries can be "
      f"reversed. Draft entries should be deleted instead."
    )
    self.entry_id = entry_id
    self.status = status


class UnbalancedJournalEntryError(ValueError):
  """Raised when the line items in a journal entry do not balance."""

  def __init__(self, total_debit: int, total_credit: int) -> None:
    super().__init__(
      f"Journal entry does not balance: "
      f"total_debit={total_debit} total_credit={total_credit} "
      f"(difference={total_debit - total_credit})"
    )
    self.total_debit = total_debit
    self.total_credit = total_credit


# ── Helpers ──────────────────────────────────────────────────────────────


def validate_and_normalize_lines(
  lines: list[JournalEntryLineItemInput],
) -> tuple[list[dict], int, int]:
  """Validate each line and compute totals. Returns (normalized, dr, cr).

  Each input line must have exactly one positive amount (debit XOR
  credit), both must be non-negative, and `element_id` is required.
  The returned `normalized` list is ready to pass into `LineItem(...)`.
  """
  if not lines:
    raise ValueError("Journal entry requires at least one line item")

  normalized: list[dict] = []
  total_debit = 0
  total_credit = 0
  for i, li in enumerate(lines):
    if not li.element_id:
      raise ValueError(f"Line item {i}: missing element_id")
    debit = int(li.debit_amount or 0)
    credit = int(li.credit_amount or 0)
    if debit < 0 or credit < 0:
      raise ValueError(f"Line item {i}: amounts must be non-negative")
    if debit == 0 and credit == 0:
      raise ValueError(f"Line item {i}: must have a non-zero debit or credit amount")
    if debit > 0 and credit > 0:
      raise ValueError(f"Line item {i}: cannot have both debit and credit amounts")
    total_debit += debit
    total_credit += credit
    normalized.append(
      {
        "element_id": li.element_id,
        "debit_amount": debit,
        "credit_amount": credit,
        "description": li.description,
      }
    )

  if total_debit != total_credit:
    raise UnbalancedJournalEntryError(total_debit, total_credit)

  return normalized, total_debit, total_credit


def _entry_to_response(
  entry: Entry, line_items: list[LineItem]
) -> JournalEntryResponse:
  """Build a `JournalEntryResponse` from an Entry row and its line items."""
  line_item_responses = [
    JournalEntryLineItemResponse(
      id=li.id,
      element_id=li.element_id,
      debit_amount=int(li.debit_amount),
      credit_amount=int(li.credit_amount),
      description=li.description,
      line_order=int(li.line_order),
    )
    for li in sorted(line_items, key=lambda li: li.line_order)
  ]
  total_debit = sum(int(li.debit_amount) for li in line_items)
  total_credit = sum(int(li.credit_amount) for li in line_items)
  return JournalEntryResponse(
    id=entry.id,
    transaction_id=entry.transaction_id,
    type=entry.type,
    status=entry.status,
    posting_date=entry.posting_date,
    memo=entry.memo,
    provenance=entry.provenance,
    reversal_of=entry.reversal_of,
    posted_at=entry.posted_at,
    line_items=line_item_responses,
    total_debit=total_debit,
    total_credit=total_credit,
  )


def _load_line_items(session: Session, entry_id: str) -> list[LineItem]:
  return list(
    session.execute(select(LineItem).where(LineItem.entry_id == entry_id))
    .scalars()
    .all()
  )


def _load_entry_or_404(session: Session, entry_id: str) -> Entry:
  entry = session.execute(
    select(Entry).where(Entry.id == entry_id)
  ).scalar_one_or_none()
  if entry is None:
    raise JournalEntryNotFoundError(entry_id)
  return entry


# ── Create ───────────────────────────────────────────────────────────────


def create_journal_entry(
  session: Session,
  body: CreateJournalEntryRequest,
  created_by: str,
) -> JournalEntryResponse:
  """Create a journal entry with balanced line items.

  Defaults to `status='draft'` for ongoing native writes. Pass
  `status='posted'` for historical data import — the entry is
  immediately posted with `posted_at=now()`, bypassing the
  draft→review→close-period workflow.

  The closed-period gate applies to both statuses: you cannot create
  an entry (draft or posted) with a `posting_date` in a closed period.
  Reopen the period first if the entry belongs there.

  Raises:
    `ClosedPeriodError` (422) if `posting_date` falls in a closed period.
    `UnbalancedJournalEntryError` (422) if total debits ≠ total credits.
    `ValueError` (422) for invalid line items (negative amounts, missing
      element_id, both debit and credit set, etc.).
  """
  assert_period_not_closed(session, body.posting_date)

  normalized, total_debit, _total_credit = validate_and_normalize_lines(body.line_items)

  status = body.status
  now = datetime.now(UTC) if status == "posted" else None

  transaction_id = body.transaction_id
  if not transaction_id:
    txn = Transaction(
      type="journal_entry",
      amount=total_debit,
      date=body.posting_date,
      description=body.memo,
      source="native",
      status="posted" if status == "posted" else "pending",
      posted_at=now,
      created_by=created_by,
    )
    session.add(txn)
    session.flush()
    transaction_id = txn.id

  entry = Entry(
    transaction_id=transaction_id,
    type=body.type,
    status=status,
    posting_date=body.posting_date,
    memo=body.memo,
    provenance="manual_entry",
    posted_at=now,
    created_by=created_by,
  )
  session.add(entry)
  session.flush()

  for order, li in enumerate(normalized, 1):
    session.add(
      LineItem(
        entry_id=entry.id,
        element_id=li["element_id"],
        debit_amount=li["debit_amount"],
        credit_amount=li["credit_amount"],
        description=li["description"],
        line_order=order,
      )
    )
  session.flush()

  line_items = _load_line_items(session, entry.id)
  return _entry_to_response(entry, line_items)


# ── Update ───────────────────────────────────────────────────────────────


def update_journal_entry(
  session: Session, body: UpdateJournalEntryRequest
) -> JournalEntryResponse:
  """Update a draft journal entry.

  Only `status='draft'` entries can be updated. Omitted fields are
  left unchanged. If `line_items` is provided, the existing line items
  are replaced atomically and the new set must balance.

  Raises:
    `JournalEntryNotFoundError` (404) if the entry does not exist.
    `JournalEntryNotDraftError` (422) if the entry is posted or reversed.
    `ClosedPeriodError` (422) if the new `posting_date` falls in a closed
      period.
    `UnbalancedJournalEntryError` (422) if replacement line items don't
      balance.
  """
  entry = _load_entry_or_404(session, body.entry_id)
  if entry.status != "draft":
    raise JournalEntryNotDraftError(entry.id, entry.status)

  # If the caller is changing posting_date, check the new date
  # isn't in a closed period.
  if body.posting_date is not None:
    assert_period_not_closed(session, body.posting_date)

  # Scalar field updates (only mutate what the caller explicitly set).
  updates = body.model_dump(exclude_unset=True)
  updates.pop("entry_id", None)
  replacement_lines = updates.pop("line_items", None)

  for key, value in updates.items():
    setattr(entry, key, value)

  if replacement_lines is not None:
    # Validate + normalize the new line items before touching the
    # database so a bad batch never clobbers the existing ones.
    new_line_inputs = [
      JournalEntryLineItemInput(**li) if isinstance(li, dict) else li
      for li in replacement_lines
    ]
    normalized, _dr, _cr = validate_and_normalize_lines(new_line_inputs)

    session.query(LineItem).filter(LineItem.entry_id == entry.id).delete(
      synchronize_session=False
    )
    session.flush()

    for order, li in enumerate(normalized, 1):
      session.add(
        LineItem(
          entry_id=entry.id,
          element_id=li["element_id"],
          debit_amount=li["debit_amount"],
          credit_amount=li["credit_amount"],
          description=li["description"],
          line_order=order,
        )
      )

  session.flush()
  line_items = _load_line_items(session, entry.id)
  return _entry_to_response(entry, line_items)


# ── Delete ───────────────────────────────────────────────────────────────


def delete_journal_entry(session: Session, body: DeleteJournalEntryRequest) -> dict:
  """Hard delete a draft journal entry.

  Only `status='draft'` entries can be deleted. Line items are
  removed via the `ondelete="CASCADE"` on `LineItem.entry_id`.

  Raises:
    `JournalEntryNotFoundError` (404) if the entry does not exist.
    `JournalEntryNotDraftError` (422) if the entry is posted or reversed.
  """
  entry = _load_entry_or_404(session, body.entry_id)
  if entry.status != "draft":
    raise JournalEntryNotDraftError(entry.id, entry.status)

  session.delete(entry)
  session.flush()
  return {"deleted": True}


# ── Reverse ───────────────────────────────────────────────────────────────


def reverse_journal_entry(
  session: Session,
  body: ReverseJournalEntryRequest,
  created_by: str,
) -> JournalEntryResponse:
  """Reverse a posted journal entry.

  Creates a new Entry with flipped line items (debits ↔ credits),
  points its `reversal_of` at the original, marks the original as
  `status='reversed'`, and posts the reversing entry immediately.

  Posting date defaults to today if not provided; memo defaults to
  an auto-generated string citing the original entry id.

  Raises:
    `JournalEntryNotFoundError` (404) if the entry does not exist.
    `JournalEntryNotPostedError` (422) if the entry is draft or
      already reversed.
    `ClosedPeriodError` (422) if the reversal's posting_date falls in
      a closed period.
  """
  original = _load_entry_or_404(session, body.entry_id)
  if original.status != "posted":
    raise JournalEntryNotPostedError(original.id, original.status)

  original_lines = _load_line_items(session, original.id)
  if not original_lines:
    raise ValueError(f"Journal entry {original.id} has no line items to reverse")

  posting_date = body.posting_date or datetime.now(UTC).date()
  assert_period_not_closed(session, posting_date)
  memo = body.memo or f"Reversal of journal entry {original.id}"
  now = datetime.now(UTC)

  reversing_entry = Entry(
    transaction_id=original.transaction_id,
    type="reversing",
    status="posted",
    posting_date=posting_date,
    memo=memo,
    provenance="manual_entry",
    reversal_of=original.id,
    posted_at=now,
    created_by=created_by,
  )
  session.add(reversing_entry)
  session.flush()

  for order, li in enumerate(sorted(original_lines, key=lambda x: x.line_order), 1):
    session.add(
      LineItem(
        entry_id=reversing_entry.id,
        element_id=li.element_id,
        # Flip: original debit → reversal credit, and vice versa.
        debit_amount=int(li.credit_amount),
        credit_amount=int(li.debit_amount),
        description=(
          f"Reversal of line {li.line_order}"
          + (f": {li.description}" if li.description else "")
        ),
        line_order=order,
      )
    )

  original.status = "reversed"
  session.flush()

  reversing_lines = _load_line_items(session, reversing_entry.id)
  return _entry_to_response(reversing_entry, reversing_lines)
