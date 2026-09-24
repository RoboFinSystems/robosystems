"""Bank-feed event handlers: post a classified bank line.

A feed (Mercury, Plaid) captures each transaction with the bank leg on
``resource_element_id``; nothing posts until the inbox classifies it via
metadata (``classified_element_id``, a ``classified_allocations`` split in
cents, ``accept_suggestion``, or an internal transfer's
``from_element_id``/``to_element_id``). On commit this writes a local-lane
draft through ``journal_entry_recorded``: money in is DR bank / CR contra.

When the bank's record changed after posting (``source_amount`` /
``source_posted_date``), the bank's values post; a retracted line
(``source_removed``) is refused. An unclassified line falls back to the
tenant's DSL rules and refuses to commit when none matches.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.api.extensions.journal_entries import (
  JournalEntryLineItemInput,
)
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.event_block.engine import (
  apply_handler,
  posting_date_for_event,
  resolve_agent_type,
)
from robosystems.operations.event_block.registry import (
  HandlerAmbiguousError,
  HandlerNotFoundError,
  resolve_handler,
)

from .journal_entry_recorded import JournalEntryRecordedMetadata
from .journal_entry_recorded import dispatch as journal_dispatch
from .journal_entry_recorded import dispatch_preview as journal_dispatch_preview
from .types import (
  EventBlockPythonHandler,
  HandlerMetadataValidationError,
  HandlerPreview,
  HandlerResult,
)

BANK_TRANSACTION = "bank_transaction"
BANK_FEE = "bank_fee"
EXTERNAL_TRANSFER = "external_transfer"
INTERNAL_TRANSFER = "internal_transfer"
BANK_EVENT_TYPES: tuple[str, ...] = (
  BANK_TRANSACTION,
  BANK_FEE,
  EXTERNAL_TRANSFER,
  INTERNAL_TRANSFER,
)


class BankEventNotClassifiedError(HandlerMetadataValidationError):
  """The event carries no account choice and no rule matches it."""


class BankAllocation(BaseModel):
  element_id: str = Field(..., min_length=1)
  amount: int = Field(..., gt=0, description="Cents; shares the line's direction.")


class BankFeedMetadata(BaseModel):
  """A bank event's metadata; open schema, only classification keys typed."""

  model_config = ConfigDict(extra="allow")

  classified_element_id: str | None = None
  classified_allocations: list[BankAllocation] | None = None
  accept_suggestion: bool = False
  suggested_element_id: str | None = None
  suggested_account_name: str | None = None
  from_element_id: str | None = None
  to_element_id: str | None = None
  classified_by: str | None = None
  basis: str | None = None
  connection_id: str | None = None
  # What the bank now says, when it differs from the captured row.
  source_amount: int | None = None
  source_posted_date: date | None = None
  source_removed: bool = False


def contra_allocations(
  metadata: BankFeedMetadata, *, amount: int
) -> list[BankAllocation] | None:
  """The contra side of a bank line, or ``None`` when nothing chose one.

  A split wins over a single account; the suggestion counts only when the
  caller accepted it. Amounts are validated against the transaction.
  """
  magnitude = abs(amount)
  if metadata.classified_allocations:
    total = sum(a.amount for a in metadata.classified_allocations)
    if total != magnitude:
      raise HandlerMetadataValidationError(
        f"classified_allocations sum to {total} but the transaction is "
        f"{magnitude}; a split must account for the whole amount."
      )
    return list(metadata.classified_allocations)
  element_id = metadata.classified_element_id
  if not element_id and metadata.accept_suggestion:
    element_id = metadata.suggested_element_id
  if not element_id:
    return None
  return [BankAllocation(element_id=element_id, amount=magnitude)]


def unclassified_reason(metadata: BankFeedMetadata) -> str:
  """Why this line has no contra account yet, and what would give it one."""
  suggested_name = metadata.suggested_account_name
  if metadata.accept_suggestion and not metadata.suggested_element_id:
    if suggested_name:
      return (
        f"accept_suggestion was set, but the suggestion '{suggested_name}' "
        "matches no account on this chart. Choose one: set "
        "classified_element_id or classified_allocations."
      )
    return (
      "accept_suggestion was set, but this line carries no suggestion. "
      "Set classified_element_id or classified_allocations."
    )
  if metadata.suggested_element_id:
    take = (
      f"accept_suggestion: true to take the suggestion '{suggested_name}'"
      if suggested_name
      else "accept_suggestion: true to take the suggestion"
    )
    return (
      "No account chosen. Set classified_element_id or classified_allocations, "
      f"or {take}."
    )
  return "No account chosen. Set classified_element_id or classified_allocations."


def plan_lines(
  *,
  event_type: str,
  resource_element_id: str | None,
  amount: int | None,
  metadata: BankFeedMetadata,
) -> list[JournalEntryLineItemInput] | None:
  """The balanced entry for one bank event, or ``None`` when unclassified.

  Pure. Raises on shapes that can never post.
  """
  if amount is None or amount == 0:
    raise HandlerMetadataValidationError(
      "A bank event needs a non-zero amount to post."
    )
  magnitude = abs(amount)

  if event_type == INTERNAL_TRANSFER:
    to_element = metadata.to_element_id or resource_element_id
    from_element = metadata.from_element_id
    if not to_element or not from_element:
      raise HandlerMetadataValidationError(
        "An internal transfer needs both bank legs: metadata.from_element_id "
        "and metadata.to_element_id (or resource_element_id) — link the chart "
        "accounts for both bank accounts, then re-sync."
      )
    if to_element == from_element:
      raise HandlerMetadataValidationError(
        "An internal transfer's two legs resolve to the same chart account."
      )
    return [
      JournalEntryLineItemInput(element_id=to_element, debit_amount=magnitude),
      JournalEntryLineItemInput(element_id=from_element, credit_amount=magnitude),
    ]

  if not resource_element_id:
    raise HandlerMetadataValidationError(
      "This bank event has no chart account linked to its bank account "
      "(resource_element_id); link the account and re-sync before posting."
    )
  contras = contra_allocations(metadata, amount=amount)
  if contras is None:
    return None
  if any(a.element_id == resource_element_id for a in contras):
    raise HandlerMetadataValidationError(
      "A bank line cannot be classified to the bank account it moved through."
    )

  money_in = amount > 0
  bank_line = JournalEntryLineItemInput(
    element_id=resource_element_id,
    debit_amount=magnitude if money_in else 0,
    credit_amount=0 if money_in else magnitude,
  )
  contra_lines = [
    JournalEntryLineItemInput(
      element_id=allocation.element_id,
      debit_amount=0 if money_in else allocation.amount,
      credit_amount=allocation.amount if money_in else 0,
    )
    for allocation in contras
  ]
  return [bank_line, *contra_lines] if money_in else [*contra_lines, bank_line]


def _journal_metadata(
  *,
  posting_date: date,
  memo: str,
  lines: list[JournalEntryLineItemInput],
  connection_id: str | None,
) -> JournalEntryRecordedMetadata:
  return JournalEntryRecordedMetadata(
    posting_date=posting_date,
    memo=memo,
    line_items=lines,
    status="draft",
    connection_id=connection_id,
    publish_to_source=False,
  )


def _pin_to_local_lane(event: Event) -> None:
  """A bank line is never published to a source system. Close reads the lane
  from the persisted event's ``metadata.publish_to_source``, so the pin must
  land on the row.
  """
  metadata = dict(event.metadata_ or {})
  if metadata.get("publish_to_source") is not False:
    metadata["publish_to_source"] = False
    event.metadata_ = metadata


def _memo(event: Event) -> str:
  return (event.description or event.event_type or "Bank transaction")[:255]


def _apply_dsl_floor(
  session: Session,
  event: Event,
  created_by: str,
  *,
  metadata: BankFeedMetadata | None = None,
) -> HandlerResult:
  """An unclassified bank event posts through a matching tenant rule, if any."""
  if metadata is None:
    metadata = BankFeedMetadata.model_validate(dict(event.metadata_ or {}))
  try:
    handler = resolve_handler(
      session,
      event_type=event.event_type,
      event_category=event.event_category,
      source=event.source,
      agent_type=resolve_agent_type(session, event.agent_id),
      resource_type=event.resource_type,
      metadata=dict(event.metadata_ or {}),
    )
  except HandlerNotFoundError:
    raise BankEventNotClassifiedError(
      f"Bank event {event.id} is not classified and no rule matches it. "
      f"{unclassified_reason(metadata)} Classify it first, then commit."
    ) from None
  except HandlerAmbiguousError as exc:
    raise BankEventNotClassifiedError(
      f"Bank event {event.id} matches more than one rule ({exc}); classify it "
      "explicitly instead."
    ) from exc
  transactions = apply_handler(session, event, handler, created_by=created_by)
  transaction_ids = [str(t.id) for t in transactions]
  entry_ids = [
    str(entry_id)
    for (entry_id,) in session.execute(
      select(Entry.id).where(Entry.transaction_id.in_(transaction_ids))
    ).all()
  ]
  logger.info(
    "bank event %s posted through rule '%s': %d transaction(s)",
    event.id,
    handler.name,
    len(transaction_ids),
  )
  return HandlerResult(entry_ids=entry_ids, transaction_ids=transaction_ids)


def source_amount(metadata: BankFeedMetadata, amount: int | None) -> int | None:
  """The amount to post: what the bank now says, else the captured amount."""
  return metadata.source_amount if metadata.source_amount is not None else amount


def source_posting_date(
  metadata: BankFeedMetadata,
  *,
  effective_at: datetime | None,
  occurred_at: datetime | None,
) -> date:
  """The posting date: what the bank now says, else the event's own."""
  if metadata.source_posted_date is not None:
    return metadata.source_posted_date
  return posting_date_for_event(effective_at=effective_at, occurred_at=occurred_at)


def source_refusal(metadata: BankFeedMetadata, *, unclassified: bool) -> str | None:
  """Why the bank's current record stops this line posting, or ``None``.

  Refuses a retracted line, and an unclassified one whose bank record
  changed (a tenant rule would re-post the captured values).
  """
  if metadata.source_removed:
    return (
      "The bank retracted this line; it cannot be posted. Resolve the "
      "reconciling item as catch_up to reverse it."
    )
  if unclassified and (
    metadata.source_amount is not None or metadata.source_posted_date is not None
  ):
    return (
      "This line was posted through a tenant rule, which would re-post the "
      "captured amount and date rather than the bank's. Resolve the "
      "reconciling item as catch_up."
    )
  return None


def dispatch(
  session: Session,
  event: Event,
  metadata: BankFeedMetadata,
  created_by: str,
) -> HandlerResult:
  """Write the classified bank line as a draft entry, or refuse.

  Fires at the first commit and again when a reconciling item is restated,
  so it posts what the bank *now* says whenever the metadata carries it.
  """
  refusal = source_refusal(metadata, unclassified=False)
  if refusal:
    raise HandlerMetadataValidationError(f"Bank event {event.id}: {refusal}")
  amount = source_amount(metadata, event.amount)
  lines = plan_lines(
    event_type=event.event_type,
    resource_element_id=event.resource_element_id,
    amount=amount,
    metadata=metadata,
  )
  if lines is None:
    refusal = source_refusal(metadata, unclassified=True)
    if refusal:
      raise HandlerMetadataValidationError(f"Bank event {event.id}: {refusal}")
    _pin_to_local_lane(event)
    return _apply_dsl_floor(session, event, created_by, metadata=metadata)

  _pin_to_local_lane(event)
  journal = _journal_metadata(
    posting_date=source_posting_date(
      metadata, effective_at=event.effective_at, occurred_at=event.occurred_at
    ),
    memo=_memo(event),
    lines=lines,
    connection_id=metadata.connection_id,
  )
  result = journal_dispatch(session, event, journal, created_by)
  logger.info(
    "bank event %s (%s) posted: %s by %s",
    event.id,
    event.event_type,
    "split" if metadata.classified_allocations else "single account",
    metadata.classified_by or "caller",
  )
  return result


def dispatch_preview(
  session: Session,
  body: CreateEventBlockRequest,
  metadata: BankFeedMetadata,
) -> HandlerPreview:
  """The entry the commit would write — the inbox's preview of an approve.

  Refuses exactly what ``dispatch`` refuses, so preview and commit agree.
  """
  refusal = source_refusal(metadata, unclassified=False)
  if refusal:
    return HandlerPreview(would_succeed=False, validation_errors=[refusal])
  try:
    lines = plan_lines(
      event_type=body.event_type,
      resource_element_id=body.resource_element_id,
      amount=source_amount(metadata, body.amount),
      metadata=metadata,
    )
  except HandlerMetadataValidationError as exc:
    return HandlerPreview(would_succeed=False, validation_errors=[str(exc)])
  if lines is None:
    refusal = source_refusal(metadata, unclassified=True)
    return HandlerPreview(
      would_succeed=False,
      validation_errors=[refusal or unclassified_reason(metadata)],
      computed_values={"suggested_element_id": metadata.suggested_element_id},
    )
  journal = _journal_metadata(
    posting_date=source_posting_date(
      metadata, effective_at=body.effective_at, occurred_at=body.occurred_at
    ),
    memo=(body.description or body.event_type)[:255],
    lines=lines,
    connection_id=metadata.connection_id,
  )
  preview = journal_dispatch_preview(session, body, journal)
  preview.computed_values = {
    **(preview.computed_values or {}),
    "direction": "in" if (body.amount or 0) > 0 else "out",
    "contra_element_ids": [
      line.element_id for line in lines if line.element_id != body.resource_element_id
    ],
  }
  return preview


def validate_classification(event: Event, metadata: BankFeedMetadata) -> None:
  """Refuse ``captured → classified`` when the commit could not post it."""
  lines = plan_lines(
    event_type=event.event_type,
    resource_element_id=event.resource_element_id,
    amount=event.amount,
    metadata=metadata,
  )
  if lines is None:
    raise BankEventNotClassifiedError(
      f"Bank event {event.id} cannot be marked classified: "
      f"{unclassified_reason(metadata)}"
    )


def _handler(event_type: str, display_name: str) -> EventBlockPythonHandler:
  return EventBlockPythonHandler(
    event_type=event_type,
    display_name=display_name,
    metadata_schema=BankFeedMetadata,
    target_status="classified",
    dispatch=dispatch,
    dispatch_preview=dispatch_preview,
    validate_classification=validate_classification,
  )


BANK_TRANSACTION_HANDLER = _handler(BANK_TRANSACTION, "Bank Transaction")
BANK_FEE_HANDLER = _handler(BANK_FEE, "Bank Fee")
EXTERNAL_TRANSFER_HANDLER = _handler(EXTERNAL_TRANSFER, "External Transfer")
INTERNAL_TRANSFER_HANDLER = _handler(INTERNAL_TRANSFER, "Internal Transfer")

BANK_FEED_HANDLERS: dict[str, EventBlockPythonHandler] = {
  handler.event_type: handler
  for handler in (
    BANK_TRANSACTION_HANDLER,
    BANK_FEE_HANDLER,
    EXTERNAL_TRANSFER_HANDLER,
    INTERNAL_TRANSFER_HANDLER,
  )
}


def classification_summary(metadata: dict[str, Any]) -> str:
  """One line for logs and the inbox: what the event is classified to."""
  if metadata.get("classified_allocations"):
    return f"split across {len(metadata['classified_allocations'])} accounts"
  if metadata.get("classified_element_id"):
    return f"account {metadata['classified_element_id']}"
  if metadata.get("accept_suggestion") and metadata.get("suggested_element_id"):
    return f"suggested account {metadata['suggested_element_id']}"
  return "unclassified"
