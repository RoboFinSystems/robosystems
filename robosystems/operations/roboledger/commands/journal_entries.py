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
  `_guards.py`. That guard takes the shared period fence, so a write
  cannot land after close has finished. Applied to create, update
  (existing date and any new date), delete, and reverse (original and
  reversal dates).
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from robosystems.logger import logger
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
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.models.extensions.roboledger.transaction import Transaction
from robosystems.operations.locking import RowLockedError, lock_by_id
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


class JournalEntryAlreadyReversedError(ValueError):
  """Raised when the entry already has a reversing entry against it.

  Reachable without any race: a schedule with ``auto_reverse`` creates the
  reversal at generation time and leaves the original's status untouched, so
  the ``status == 'posted'`` check below passes on an entry that is already
  reversed. Before `uq_entries_one_reversal_per_original` that produced a
  second reversal — the double-post this work exists to stop; after it, the
  insert would fail on the index. Neither is an answer a caller can act on,
  so the state gets named here instead.
  """

  def __init__(self, entry_id: str, reversing_entry_id: str) -> None:
    super().__init__(
      f"Journal entry {entry_id} already has a reversing entry "
      f"({reversing_entry_id}); an entry is reversed at most once."
    )
    self.entry_id = entry_id
    self.reversing_entry_id = reversing_entry_id


class JournalEntryOwnedByEventError(ValueError):
  """Raised when a delete would leave a live event with no ledger rows.

  A draft is the ledger's version of its event; the event is the record of
  what happened. Deleting the last draft of an event that has not been
  retracted leaves a committed event `execute` will still publish, with
  nothing behind it in the books. The retraction belongs on the event —
  void or supersede it — after which its leftover drafts are deletable.
  Multi-entry events keep the surface: deleting one of several drafts is a
  correction, and the rows that remain are what publishes.
  """

  def __init__(self, entry_id: str, event_id: str, event_status: str) -> None:
    super().__init__(
      f"Journal entry {entry_id} is the only ledger entry of event {event_id} "
      f"(status {event_status!r}). Void or supersede the event instead of "
      "deleting its draft; a retracted event's drafts can then be deleted."
    )
    self.entry_id = entry_id
    self.event_id = event_id
    self.event_status = event_status


_RETRACTED_EVENT_STATUSES = frozenset({"voided", "superseded"})


def _lock_owning_event(session: Session, entry: Entry) -> Event | None:
  """Lock the event a draft belongs to, before the draft's own row.

  `execute_event_block` and close both take the event row and then write its
  entries; a correction that took the entry first and the event never (or
  second) would either interleave with a publish — QuickBooks receiving the
  version from before the correction — or acquire in the opposite order.
  Event, then entry, everywhere. Bounded like the entry lock.
  """
  if entry.triggered_by_event_id is None:
    return None
  return lock_by_id(
    session,
    Event,
    entry.triggered_by_event_id,
    f"Event {entry.triggered_by_event_id} is being written by another "
    "process. Retry in a moment.",
  )


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


_FLOW_TAG_KEY = "transaction_description_code"


def resolve_flow_element_id(session: Session, metadata: dict | None) -> str | None:
  """Resolve a line's flow tag (``transaction_description_code`` qname) to its
  Element id for ``LineItem.flow_element_id``.

  Source-named: points at the element the qname names directly — rs-gaap in
  production (the enrichment classifier emits rs-gaap-valued tags), the source
  vocabulary (e.g. ``mini``) in cross-taxonomy projections. Returns ``None`` when
  the tag is absent or doesn't resolve. Logs (advisory) when the resolved element
  lacks an ``activityType`` trait — i.e. isn't a recognized flow concept; trait
  coverage is backfilled in the rs-gaap content phase.
  """
  if not metadata:
    return None
  qname = metadata.get(_FLOW_TAG_KEY)
  if not qname:
    return None
  row = session.execute(
    text("SELECT id FROM elements WHERE qname = :qname LIMIT 1"),
    {"qname": qname},
  ).fetchone()
  if row is None:
    logger.warning(
      "flow tag %s did not resolve to an element; flow_element_id left NULL", qname
    )
    return None
  element_id = row[0]
  has_activity_type = session.execute(
    text(
      """
      SELECT 1 FROM element_traits et
      JOIN traits t ON t.id = et.trait_id
      WHERE et.element_id = :eid AND t.category = 'activityType'
      LIMIT 1
      """
    ),
    {"eid": element_id},
  ).fetchone()
  if has_activity_type is None:
    logger.warning(
      "flow element %s (%s) has no activityType trait — not a recognized flow "
      "concept (advisory; trait coverage backfilled in the rs-gaap content phase)",
      qname,
      element_id,
    )
  return element_id


def _split_flow_tag(metadata: dict | None) -> dict:
  """Return a shallow copy of ``metadata`` with the flow-tag key removed.

  The flow concept is a first-class ``flow_element_id`` FK, so its qname must
  not also ride in JSONB. Other metadata keys (source-system refs, etc.) are
  preserved.
  """
  meta = dict(metadata or {})
  meta.pop(_FLOW_TAG_KEY, None)
  return meta


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
        "metadata": li.metadata or {},
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


def _raise_if_posting_date_moved(entry: Entry, peeked_date) -> None:
  """Refuse to take a period fence after the entry row is already locked.

  Fence-then-row is the documented order. Re-fencing here because the
  posting date moved under us would invert that — a close holding the
  new period's exclusive fence and waiting to update this entry
  deadlocks. The caller retries, peeks the current date, and acquires
  in the right order.
  """
  if entry.posting_date != peeked_date:
    raise RowLockedError(
      f"Journal entry {entry.id} was moved to another period by another "
      "process. Retry in a moment."
    )


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
      type=body.transaction_type,
      amount=total_debit,
      date=body.posting_date,
      description=body.memo,
      source=body.source or "native",
      connection_id=body.connection_id,
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
        flow_element_id=resolve_flow_element_id(session, li.get("metadata")),
        debit_amount=li["debit_amount"],
        credit_amount=li["credit_amount"],
        description=li["description"],
        line_order=order,
        metadata_=_split_flow_tag(li.get("metadata")),
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
    `ClosedPeriodError` (422) if the existing or new `posting_date`
      falls in a closed period.
    `RowLockedError` (409) if another writer holds the entry, or if the
      posting date moved under us (retry so locks are acquired in order).
    `UnbalancedJournalEntryError` (422) if replacement line items don't
      balance.
  """
  # Peek dates, fence, then lock and refresh. An unlocked load that we
  # then trust for `status` can mutate a posted entry: another request
  # posts it while we wait on the fence. Capture the peeked date as a
  # value — after `lock_by_id` the peeked instance is the same identity
  # and `populate_existing` would make `peek.posting_date` lie.
  peek = _load_entry_or_404(session, body.entry_id)
  peeked_date = peek.posting_date
  dates = [peeked_date]
  if body.posting_date is not None:
    dates.append(body.posting_date)
  assert_period_not_closed(session, *dates)

  # Event before entry — the order `execute` and close use — so a
  # correction and a publish serialize, and QuickBooks receives the rows as
  # corrected rather than the version a concurrent publish read first.
  _lock_owning_event(session, peek)
  entry = lock_by_id(
    session,
    Entry,
    body.entry_id,
    f"Journal entry {body.entry_id} is being written by another process. "
    "Retry in a moment.",
  )
  if entry is None:
    raise JournalEntryNotFoundError(body.entry_id)
  _raise_if_posting_date_moved(entry, peeked_date)
  if entry.status != "draft":
    raise JournalEntryNotDraftError(entry.id, entry.status)

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
          flow_element_id=resolve_flow_element_id(session, li.get("metadata")),
          debit_amount=li["debit_amount"],
          credit_amount=li["credit_amount"],
          description=li["description"],
          line_order=order,
          metadata_=_split_flow_tag(li.get("metadata")),
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
    `ClosedPeriodError` (422) if the entry's posting_date is in a closed
      period.
    `RowLockedError` (409) if another writer holds the entry, or if the
      posting date moved under us (retry so locks are acquired in order).
    `JournalEntryOwnedByEventError` (422) if this is the last ledger entry
      of an event that has not been voided or superseded.
  """
  peek = _load_entry_or_404(session, body.entry_id)
  peeked_date = peek.posting_date
  assert_period_not_closed(session, peeked_date)

  # Event before entry — see `_lock_owning_event`. Under the event lock the
  # sibling count below cannot change before the delete.
  owner = _lock_owning_event(session, peek)
  entry = lock_by_id(
    session,
    Entry,
    body.entry_id,
    f"Journal entry {body.entry_id} is being written by another process. "
    "Retry in a moment.",
  )
  if entry is None:
    raise JournalEntryNotFoundError(body.entry_id)
  _raise_if_posting_date_moved(entry, peeked_date)
  if entry.status != "draft":
    raise JournalEntryNotDraftError(entry.id, entry.status)

  if owner is not None and owner.status not in _RETRACTED_EVENT_STATUSES:
    siblings = session.execute(
      select(func.count())
      .select_from(Entry)
      .where(Entry.triggered_by_event_id == owner.id)
    ).scalar_one()
    if siblings <= 1:
      raise JournalEntryOwnedByEventError(
        str(entry.id), str(owner.id), str(owner.status)
      )

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
  # Period fence first, then the entry row. Close takes the exclusive
  # fence and then bulk-updates entries; a reversal that locked the
  # entry first and the fence second would deadlock with that order.
  peek = session.get(Entry, body.entry_id)
  if peek is None:
    raise JournalEntryNotFoundError(body.entry_id)
  posting_date = body.posting_date or datetime.now(UTC).date()
  assert_period_not_closed(session, peek.posting_date, posting_date)

  # Locked, because everything below decides from `original.status`. Two
  # concurrent reversals of the same entry both read 'posted', both pass the
  # guard, and both write a full reversing entry — the entry gets reversed
  # twice. Each reversing entry is internally balanced, so the trial balance
  # stays happy while the books sit a full entry off in the other direction.
  # `uq_entries_one_reversal_per_original` is the database's half of this;
  # the lock is what turns a would-be IntegrityError into a clean 409.
  original = lock_by_id(
    session,
    Entry,
    body.entry_id,
    f"Journal entry {body.entry_id} is being written by another process. "
    "Retry in a moment.",
  )
  if original is None:
    raise JournalEntryNotFoundError(body.entry_id)
  if original.status != "posted":
    raise JournalEntryNotPostedError(original.id, original.status)

  # Checked under the lock, so the answer cannot go stale between here and the
  # insert. `uq_entries_one_reversal_per_original` still backs it — this turns
  # the constraint from the thing that reports the problem into the thing that
  # guarantees it.
  existing_reversal = session.execute(
    select(Entry.id).where(Entry.reversal_of == original.id)
  ).scalar_one_or_none()
  if existing_reversal is not None:
    raise JournalEntryAlreadyReversedError(str(original.id), str(existing_reversal))

  original_lines = _load_line_items(session, original.id)
  if not original_lines:
    raise ValueError(f"Journal entry {original.id} has no line items to reverse")
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
        # Carry the flow concept through to the reversal so the rollforward
        # filter engine sees the offsetting flow on the reversal period.
        flow_element_id=li.flow_element_id,
        # Flip: original debit → reversal credit, and vice versa.
        debit_amount=int(li.credit_amount),
        credit_amount=int(li.debit_amount),
        # Preserve any remaining per-line metadata (source-system refs, etc.).
        # Shallow copy is sufficient (JSON-shaped dict).
        metadata_=dict(li.metadata_ or {}),
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
