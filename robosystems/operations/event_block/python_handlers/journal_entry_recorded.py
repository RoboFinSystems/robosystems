"""journal_entry_recorded handler: records balanced journal entries via
`create_journal_entry` and links them to the event.

Metadata is either flat (one entry: top-level ``posting_date``/``memo``/
``line_items``) or nested (an ``entries`` array, e.g. QB ingest, whose lines
may carry ``element_external_id`` resolved at dispatch). The event ends
``classified`` for drafts and ``fulfilled`` when ``metadata.status='posted'``.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, Field, StrictBool, model_validator
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  JournalEntryLineItemInput,
)
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.transaction import Transaction
from robosystems.operations.locking import RowLockedError
from robosystems.operations.roboledger.commands._guards import (
  ClosedPeriodError,
  assert_period_not_closed,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  UnbalancedJournalEntryError,
  create_journal_entry,
  validate_and_normalize_lines,
)

from .types import (
  EventBlockPythonHandler,
  HandlerPreview,
  HandlerResult,
)


class NestedJournalEntryLineItem(BaseModel):
  """One line in a nested-shape entry: exactly one of ``element_id`` or
  ``element_external_id``. ``metadata`` passes through to
  ``LineItem.metadata_``.
  """

  element_id: str | None = None
  element_external_id: str | None = None
  debit_amount: int = 0
  credit_amount: int = 0
  description: str | None = None
  metadata: dict[str, Any] | None = None

  @model_validator(mode="after")
  def _exactly_one_element_ref(self) -> NestedJournalEntryLineItem:
    if (self.element_id is None) == (self.element_external_id is None):
      raise ValueError(
        "Line item must specify exactly one of element_id or element_external_id."
      )
    return self


class NestedJournalEntrySpec(BaseModel):
  """One entry inside the nested-shape ``entries`` array."""

  posting_date: date
  memo: str
  line_items: list[NestedJournalEntryLineItem] = Field(..., min_length=2)
  type: Literal["standard", "adjusting", "closing", "reversing"] = "standard"
  external_id: str | None = None


class JournalEntryRecordedMetadata(BaseModel):
  """Metadata for a journal_entry_recorded event: flat or nested, never both."""

  # Flat shape
  posting_date: date | None = None
  memo: str | None = None
  line_items: list[JournalEntryLineItemInput] | None = None
  type: Literal["standard", "adjusting", "closing", "reversing"] = "standard"
  transaction_id: str | None = None

  # Shared
  status: Literal["draft", "posted"] = "draft"

  # Overrides the source-based write-back default (unset: follow
  # ``Event.source``). ``False`` keeps an entry that mirrors an upstream
  # change local. StrictBool because the raw dict is persisted and matched
  # in SQL; a lax ``"yes"`` would match neither branch.
  publish_to_source: StrictBool | None = None

  # Nested shape
  entries: list[NestedJournalEntrySpec] | None = None

  # Scopes nested-shape element lookups.
  connection_id: str | None = None

  @model_validator(mode="after")
  def _exactly_one_shape(self) -> JournalEntryRecordedMetadata:
    has_flat = self.line_items is not None
    has_nested = self.entries is not None
    if has_flat and has_nested:
      raise ValueError(
        "journal_entry_recorded metadata: provide either flat fields "
        "(posting_date/memo/line_items) or 'entries' array, not both."
      )
    if not has_flat and not has_nested:
      raise ValueError(
        "journal_entry_recorded metadata: must provide either flat fields "
        "(posting_date/memo/line_items) or an 'entries' array."
      )
    if has_flat:
      if self.posting_date is None or self.memo is None:
        raise ValueError(
          "Flat-shape journal_entry_recorded metadata requires posting_date "
          "and memo when line_items is set."
        )
      if len(self.line_items or []) < 2:
        raise ValueError("Flat-shape line_items must contain at least 2 lines.")
    return self

  @property
  def is_nested(self) -> bool:
    return self.entries is not None


class ElementResolutionError(Exception):
  """An element_external_id in nested metadata could not be resolved."""


def resolve_external_ids(
  session: Session,
  *,
  external_ids: set[str],
  source: str,
  connection_id: str | None,
) -> dict[str, str]:
  """Bulk-resolve element_external_ids → element.id within ``source``.

  Pass ``connection_id`` whenever known: two books on one graph can share
  account external_ids.
  """
  if not external_ids:
    return {}
  stmt = select(Element.id, Element.external_id).where(
    Element.external_source == source,
    Element.external_id.in_(external_ids),
  )
  if connection_id is not None:
    stmt = stmt.where(Element.connection_id == connection_id)
  rows = session.execute(stmt).all()
  return {ext_id: el_id for el_id, ext_id in rows}


def _resolve_nested_line_items(
  session: Session,
  *,
  entries: list[NestedJournalEntrySpec],
  event: Event,
  connection_id: str | None,
) -> list[list[JournalEntryLineItemInput]]:
  """Resolve element refs across all entries in one query; returns line lists
  aligned with ``entries``. Raises one ``ElementResolutionError`` naming
  every unresolved id.
  """
  unresolved_external_ids: set[str] = set()
  for entry in entries:
    for line in entry.line_items:
      if line.element_external_id and not line.element_id:
        unresolved_external_ids.add(line.element_external_id)

  resolved = resolve_external_ids(
    session,
    external_ids=unresolved_external_ids,
    source=event.source,
    connection_id=connection_id,
  )

  missing: list[tuple[int, str]] = []
  for entry_idx, entry in enumerate(entries):
    for line in entry.line_items:
      if line.element_id:
        continue
      external_id = line.element_external_id
      if external_id and external_id not in resolved:
        missing.append((entry_idx, external_id))

  if missing:
    seen: dict[str, int] = {}
    for idx, ext_id in missing:
      seen.setdefault(ext_id, idx)
    summary = ", ".join(f"'{ext_id}' (entry {idx})" for ext_id, idx in seen.items())
    raise ElementResolutionError(
      f"Event {event.id}: {len(seen)} element_external_id(s) could not be "
      f"resolved against source='{event.source}', "
      f"connection_id='{connection_id}': {summary}. These accounts are not in "
      f"RoboLedger's chart of accounts yet; sync the source so they arrive, "
      f"then approve again."
    )

  out: list[list[JournalEntryLineItemInput]] = []
  for entry in entries:
    line_items: list[JournalEntryLineItemInput] = []
    for line in entry.line_items:
      element_id = line.element_id or resolved[line.element_external_id]  # type: ignore[index]
      line_items.append(
        JournalEntryLineItemInput(
          element_id=element_id,
          debit_amount=line.debit_amount,
          credit_amount=line.credit_amount,
          description=line.description,
          metadata=line.metadata,
        )
      )
    out.append(line_items)
  return out


def _link_entry_and_txn(
  session: Session,
  *,
  event_id: str,
  entry_id: str | None,
  transaction_id: str | None,
  entry_ids: list[str],
  transaction_ids: list[str],
) -> None:
  """Stamp triggered_by_event_id on the freshly-created Entry + Transaction."""
  if entry_id:
    session.execute(
      update(Entry).where(Entry.id == entry_id).values(triggered_by_event_id=event_id)
    )
    entry_ids.append(entry_id)
  if transaction_id and transaction_id not in transaction_ids:
    session.execute(
      update(Transaction)
      .where(Transaction.id == transaction_id)
      .values(triggered_by_event_id=event_id)
    )
    transaction_ids.append(transaction_id)


def _dispatch_flat(
  session: Session,
  event: Event,
  metadata: JournalEntryRecordedMetadata,
  created_by: str,
) -> HandlerResult:
  body = CreateJournalEntryRequest(
    posting_date=metadata.posting_date,
    memo=metadata.memo,
    line_items=metadata.line_items,
    type=metadata.type,
    status=metadata.status,
    transaction_id=metadata.transaction_id,
    source=event.source,
    connection_id=metadata.connection_id,
    # Keep the originating kind (bill_paid, ...) rather than ``journal_entry``.
    transaction_type=event.event_type,
  )
  response = create_journal_entry(session, body, created_by)

  entry_ids: list[str] = []
  transaction_ids: list[str] = []
  _link_entry_and_txn(
    session,
    event_id=event.id,
    entry_id=response.id,
    transaction_id=response.transaction_id,
    entry_ids=entry_ids,
    transaction_ids=transaction_ids,
  )

  logger.info(
    "journal_entry_recorded event %s fired (flat): entry=%s txn=%s status=%s",
    event.id,
    response.id,
    response.transaction_id,
    metadata.status,
  )
  return HandlerResult(entry_ids=entry_ids, transaction_ids=transaction_ids)


def _dispatch_nested(
  session: Session,
  event: Event,
  metadata: JournalEntryRecordedMetadata,
  created_by: str,
) -> HandlerResult:
  """One journal entry per item in metadata.entries, all on one Transaction."""
  assert metadata.entries is not None  # guaranteed by validator
  resolved_lines = _resolve_nested_line_items(
    session,
    entries=metadata.entries,
    event=event,
    connection_id=metadata.connection_id,
  )

  entry_ids: list[str] = []
  transaction_ids: list[str] = []
  shared_txn_id: str | None = metadata.transaction_id

  for entry_idx, entry_spec in enumerate(metadata.entries):
    body = CreateJournalEntryRequest(
      posting_date=entry_spec.posting_date,
      memo=entry_spec.memo,
      line_items=resolved_lines[entry_idx],
      type=entry_spec.type,
      status=metadata.status,
      transaction_id=shared_txn_id,
      # Lets re-syncs scope deletes by origin.
      source=event.source,
      connection_id=metadata.connection_id,
      transaction_type=event.event_type,
    )
    response = create_journal_entry(session, body, created_by)
    if shared_txn_id is None:
      shared_txn_id = response.transaction_id
    _link_entry_and_txn(
      session,
      event_id=event.id,
      entry_id=response.id,
      transaction_id=response.transaction_id,
      entry_ids=entry_ids,
      transaction_ids=transaction_ids,
    )

  logger.info(
    "journal_entry_recorded event %s fired (nested): %d entries on txn=%s status=%s",
    event.id,
    len(entry_ids),
    shared_txn_id,
    metadata.status,
  )
  return HandlerResult(entry_ids=entry_ids, transaction_ids=transaction_ids)


def dispatch(
  session: Session,
  event: Event,
  metadata: JournalEntryRecordedMetadata,
  created_by: str,
) -> HandlerResult:
  """Create the journal entries and link them to the event; sets the event
  ``fulfilled`` when ``metadata.status == 'posted'``."""
  if metadata.is_nested:
    result = _dispatch_nested(session, event, metadata, created_by)
  else:
    result = _dispatch_flat(session, event, metadata, created_by)

  if metadata.status == "posted":
    event.status = "fulfilled"

  return result


def _preview_planned_entries(
  metadata: JournalEntryRecordedMetadata,
) -> tuple[list[dict[str, Any]], int, int, list[str]]:
  """Returns ``(planned_entries, total_debit, total_credit, errors)``."""
  errors: list[str] = []
  planned: list[dict[str, Any]] = []
  total_debit = 0
  total_credit = 0

  if metadata.is_nested:
    for entry_idx, entry in enumerate(metadata.entries or []):
      try:
        # Preview checks the math only, not whether elements exist.
        as_journal_lines = [
          JournalEntryLineItemInput(
            element_id=li.element_id or li.element_external_id or "preview",
            debit_amount=li.debit_amount,
            credit_amount=li.credit_amount,
            description=li.description,
          )
          for li in entry.line_items
        ]
        _normalized, debit, credit = validate_and_normalize_lines(as_journal_lines)
        total_debit += debit
        total_credit += credit
      except UnbalancedJournalEntryError as e:
        errors.append(f"Entry {entry_idx}: {e}")
      except ValueError as e:
        errors.append(f"Entry {entry_idx}: {e}")
      planned.append(
        {
          "posting_date": str(entry.posting_date),
          "memo": entry.memo,
          "entry_type": entry.type,
          "line_items": [li.model_dump() for li in entry.line_items],
        }
      )
    return planned, total_debit, total_credit, errors

  try:
    _normalized, total_debit, total_credit = validate_and_normalize_lines(
      metadata.line_items or []
    )
  except UnbalancedJournalEntryError as e:
    errors.append(str(e))
  except ValueError as e:
    errors.append(str(e))

  planned.append(
    {
      "posting_date": str(metadata.posting_date),
      "memo": metadata.memo,
      "entry_type": metadata.type,
      "line_items": [li.model_dump() for li in (metadata.line_items or [])],
    }
  )
  return planned, total_debit, total_credit, errors


def dispatch_preview(
  session: Session,
  body: CreateEventBlockRequest,
  metadata: JournalEntryRecordedMetadata,
) -> HandlerPreview:
  """Validate balance + closed-period + line items without persisting."""
  errors: list[str] = []

  posting_dates: list[date] = []
  if metadata.is_nested:
    posting_dates = [e.posting_date for e in metadata.entries or []]
  elif metadata.posting_date is not None:
    posting_dates = [metadata.posting_date]
  for pd in posting_dates:
    try:
      assert_period_not_closed(session, pd)
    except (ClosedPeriodError, RowLockedError) as e:
      errors.append(str(e))

  planned, total_debit, total_credit, balance_errors = _preview_planned_entries(
    metadata
  )
  errors.extend(balance_errors)

  if errors:
    return HandlerPreview(
      would_succeed=False,
      planned_entries=[],
      computed_values={
        "total_debit_cents": total_debit,
        "total_credit_cents": total_credit,
      },
      validation_errors=errors,
    )

  return HandlerPreview(
    would_succeed=True,
    planned_entries=planned,
    computed_values={
      "total_debit_cents": total_debit,
      "total_credit_cents": total_credit,
      "target_status": "fulfilled" if metadata.status == "posted" else "classified",
      "entry_count": len(planned),
    },
    validation_errors=[],
  )


JOURNAL_ENTRY_RECORDED_HANDLER = EventBlockPythonHandler(
  event_type="journal_entry_recorded",
  display_name="Journal Entry Recorded",
  metadata_schema=JournalEntryRecordedMetadata,
  target_status="classified",
  dispatch=dispatch,
  dispatch_preview=dispatch_preview,
)
