"""Journal entry write commands: create, update and delete drafts, reverse posted.

Posted entries are immutable and corrected only by reversal, which posts a new
entry with flipped lines and marks the original ``reversed``. An entry created
without a ``transaction_id`` gets a synthetic ``journal_entry`` Transaction so
the graph keeps its Entity → Transaction → Entry path. Every write takes the
closed-period fence (``_guards.assert_period_not_closed``).
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
  assert_accounts_postable,
  assert_period_not_closed,
)


class JournalEntryNotFoundError(LookupError):
  def __init__(self, entry_id: str) -> None:
    super().__init__(f"Journal entry not found: {entry_id}")
    self.entry_id = entry_id


class JournalEntryNotDraftError(ValueError):
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
  """The entry already has a reversing entry.

  Reachable without a race: a schedule with ``auto_reverse`` creates the
  reversal up front and leaves the original ``posted``.
  """

  def __init__(self, entry_id: str, reversing_entry_id: str) -> None:
    super().__init__(
      f"Journal entry {entry_id} already has a reversing entry "
      f"({reversing_entry_id}); an entry is reversed at most once."
    )
    self.entry_id = entry_id
    self.reversing_entry_id = reversing_entry_id


class JournalEntryOwnedByEventError(ValueError):
  """A delete would leave a live (not voided or superseded) event with no
  ledger rows, which `execute` would still publish. Deleting one of several
  drafts is allowed."""

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
  """Lock the draft's event before the draft itself.

  Lock order is event, then entry, everywhere (`execute_event_block` and close
  use it), so a correction serializes with a publish instead of deadlocking or
  letting QuickBooks receive the pre-correction rows.
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
  def __init__(self, entry_id: str, status: str) -> None:
    super().__init__(
      f"Journal entry {entry_id} is {status!r}; only posted entries can be "
      f"reversed. Draft entries should be deleted instead."
    )
    self.entry_id = entry_id
    self.status = status


class UnbalancedJournalEntryError(ValueError):
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
  """Resolve a line's flow tag (``transaction_description_code`` qname) to the
  Element id it names, or ``None`` when absent or unresolved. Warns, without
  failing, when the element has no ``activityType`` trait.
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
  """``metadata`` without the flow tag, which lives in ``flow_element_id``."""
  meta = dict(metadata or {})
  meta.pop(_FLOW_TAG_KEY, None)
  return meta


def validate_and_normalize_lines(
  lines: list[JournalEntryLineItemInput],
) -> tuple[list[dict], int, int]:
  """Validate lines (element required, exactly one positive side, balanced)
  and return ``(normalized, total_debit, total_credit)``."""
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
  """Refuse instead of re-fencing: lock order is fence, then row, and fencing
  the new period while holding the row could deadlock with close. The caller
  retries."""
  if entry.posting_date != peeked_date:
    raise RowLockedError(
      f"Journal entry {entry.id} was moved to another period by another "
      "process. Retry in a moment."
    )


# ── Create ───────────────────────────────────────────────────────────────


# Sources the platform itself emits. `source` is otherwise an open vocabulary
# of connection providers, so anything not listed here is a sync.
_PLATFORM_SOURCE_PROVENANCE = {
  "manual": "manual_entry",
  "native": "manual_entry",
  "system": "system_computed",
  "schedule": "schedule_derived",
}


def provenance_for_source(source: str | None) -> str:
  """Map a Transaction/Event source onto `Entry.provenance`."""
  if not source:
    return "manual_entry"
  return _PLATFORM_SOURCE_PROVENANCE.get(source, "source_sync")


def create_journal_entry(
  session: Session,
  body: CreateJournalEntryRequest,
  created_by: str,
) -> JournalEntryResponse:
  """Create a journal entry with balanced line items.

  ``status='posted'`` (historical import) posts immediately, bypassing the
  draft-review-close workflow. Either status is refused in a closed period.

  Raises:
    `ClosedPeriodError`, `UnbalancedJournalEntryError`, `ValueError` for a
      malformed line.
    `InactiveAccountError` if a line names a retired account, except for a
      synced ledger's replayed history (a synced `source` with `status='posted'`).
  """
  assert_period_not_closed(session, body.posting_date)

  normalized, total_debit, _total_credit = validate_and_normalize_lines(body.line_items)
  # Only posted (replayed) history carries the source into the exemption; a
  # draft is authored whatever source it names.
  assert_accounts_postable(
    session,
    (li["element_id"] for li in normalized),
    source=body.source if body.status == "posted" else None,
  )

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
    provenance=provenance_for_source(body.source),
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
  """Update a draft journal entry; omitted fields are unchanged, and
  ``line_items`` replaces the whole set.

  Raises:
    `JournalEntryNotFoundError`, `JournalEntryNotDraftError`,
    `UnbalancedJournalEntryError`.
    `ClosedPeriodError` if the existing or new `posting_date` is closed.
    `RowLockedError` if another writer holds the entry or its date moved.
  """
  # Peek, fence, then lock and re-check status. Keep the peeked date as a
  # value: after `lock_by_id` refreshes the same identity, `peek.posting_date`
  # would report the new date.
  peek = _load_entry_or_404(session, body.entry_id)
  peeked_date = peek.posting_date
  dates = [peeked_date]
  if body.posting_date is not None:
    dates.append(body.posting_date)
  assert_period_not_closed(session, *dates)

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

  updates = body.model_dump(exclude_unset=True)
  updates.pop("entry_id", None)
  replacement_lines = updates.pop("line_items", None)

  for key, value in updates.items():
    setattr(entry, key, value)

  if replacement_lines is not None:
    # Validate before deleting, so a bad batch never clobbers the old lines.
    new_line_inputs = [
      JournalEntryLineItemInput(**li) if isinstance(li, dict) else li
      for li in replacement_lines
    ]
    normalized, _dr, _cr = validate_and_normalize_lines(new_line_inputs)
    # A draft edit is authored, whatever the entry's provenance: retired
    # accounts are closed to it.
    assert_accounts_postable(session, (li["element_id"] for li in normalized))

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
  """Hard delete a draft journal entry (line items cascade).

  Raises:
    `JournalEntryNotFoundError`, `JournalEntryNotDraftError`,
    `ClosedPeriodError`, `JournalEntryOwnedByEventError`.
    `RowLockedError` if another writer holds the entry or its date moved.
  """
  peek = _load_entry_or_404(session, body.entry_id)
  peeked_date = peek.posting_date
  assert_period_not_closed(session, peeked_date)

  # Under the event lock the sibling count below cannot change.
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
  """Post a reversing entry with flipped lines and mark the original ``reversed``.

  ``posting_date`` defaults to today.

  Raises:
    `JournalEntryNotFoundError`, `JournalEntryNotPostedError`,
    `JournalEntryAlreadyReversedError`.
    `ClosedPeriodError` if the original's or the reversal's date is closed.
  """
  # Fence first, then the row: close takes the fence and then updates entries.
  peek = session.get(Entry, body.entry_id)
  if peek is None:
    raise JournalEntryNotFoundError(body.entry_id)
  posting_date = body.posting_date or datetime.now(UTC).date()
  assert_period_not_closed(session, peek.posting_date, posting_date)

  # Locked so two concurrent reversals cannot both see 'posted' and reverse
  # twice (balanced, so the trial balance would not catch it).
  # `uq_entries_one_reversal_per_original` backs this at the database.
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
    # Not inherited: reversing even a synced entry is a decision made here.
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
        # Rollforwards need the offsetting flow on the reversal period.
        flow_element_id=li.flow_element_id,
        debit_amount=int(li.credit_amount),
        credit_amount=int(li.debit_amount),
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
