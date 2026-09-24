"""payment_received handler: the journal entry GL write, plus the AR
discharge link (``event.discharges_event_id`` → the invoice it settles).

The invoice is matched by ``metadata.qb_linked_txns`` first, then by
``qb_reference_number`` ↔ ``qb_doc_number`` within the same agent.

Limitation: a payment settling several invoices links only the first match,
so the AR query undercounts for splits (all refs stay in metadata).
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.extensions.roboledger.event import Event

from .journal_entry_recorded import (
  JournalEntryRecordedMetadata,
)
from .journal_entry_recorded import (
  dispatch as journal_dispatch,
)
from .journal_entry_recorded import (
  dispatch_preview as journal_dispatch_preview,
)
from .types import EventBlockPythonHandler, HandlerPreview, HandlerResult

# Retracted invoices can't be discharged. ``captured``/``classified`` ones
# can: one sync batch commits invoices and payments in no guaranteed order,
# and the AR aggregate filters by status separately.
_TERMINAL_INVALID_STATUSES: tuple[str, ...] = ("voided", "superseded")


def _resolve_by_linked_txns(
  session: Session,
  *,
  source: str,
  linked_txns: list[dict[str, str]],
  candidate_event_types: tuple[str, ...],
) -> Event | None:
  """Match on external_id ``f"{txn_type}_{txn_id}"`` (as the QB importer's
  ``_flatten_txn_header`` builds it), restricted to allowed event types."""
  if not linked_txns:
    return None
  candidate_ext_ids = [
    f"{ref['txn_type']}_{ref['txn_id']}"
    for ref in linked_txns
    if ref.get("txn_id") and ref.get("txn_type")
  ]
  if not candidate_ext_ids:
    return None
  stmt = (
    select(Event)
    .where(
      Event.source == source,
      Event.external_id.in_(candidate_ext_ids),
      Event.event_type.in_(candidate_event_types),
      Event.status.notin_(_TERMINAL_INVALID_STATUSES),
    )
    .order_by(Event.occurred_at.asc())
  )
  return session.execute(stmt).scalars().first()


def _resolve_by_reference_number(
  session: Session,
  *,
  source: str,
  reference_number: str,
  agent_id: str | None,
  candidate_event_types: tuple[str, ...],
) -> Event | None:
  """Fallback match: payment.qb_reference_number ↔ invoice.qb_doc_number.

  Scoped to the same ``agent_id`` (NULL matches only NULL) so a shared doc
  number never cross-links counterparties.
  """
  if not reference_number:
    return None
  stmt = (
    select(Event)
    .where(
      Event.source == source,
      Event.event_type.in_(candidate_event_types),
      Event.metadata_["qb_doc_number"].astext == reference_number,
      Event.status.notin_(_TERMINAL_INVALID_STATUSES),
    )
    .order_by(Event.occurred_at.asc())
  )
  if agent_id is None:
    stmt = stmt.where(Event.agent_id.is_(None))
  else:
    stmt = stmt.where(Event.agent_id == agent_id)
  return session.execute(stmt).scalars().first()


def _resolve_originating_event(
  session: Session,
  payment: Event,
  *,
  candidate_event_types: tuple[str, ...],
) -> Event | None:
  """The event a payment discharges, or ``None`` (a valid state, e.g.
  cash-basis income with no AR cycle)."""
  raw_meta = payment.metadata_ or {}
  linked_txns = raw_meta.get("qb_linked_txns") or []
  if isinstance(linked_txns, list):
    match = _resolve_by_linked_txns(
      session,
      source=payment.source,
      linked_txns=linked_txns,
      candidate_event_types=candidate_event_types,
    )
    if match is not None:
      return match

  reference_number = (raw_meta.get("qb_reference_number") or "").strip()
  if reference_number:
    return _resolve_by_reference_number(
      session,
      source=payment.source,
      reference_number=reference_number,
      agent_id=payment.agent_id,
      candidate_event_types=candidate_event_types,
    )
  return None


def _link_discharge(
  session: Session,
  payment: Event,
  candidate_event_types: tuple[str, ...],
) -> None:
  """Resolve and stamp ``discharges_event_id`` if not already set."""
  if payment.discharges_event_id is not None:
    return
  originating = _resolve_originating_event(
    session, payment, candidate_event_types=candidate_event_types
  )
  if originating is None:
    logger.info(
      "payment_received event %s (ext=%s): no originating event matched "
      "via LinkedTxn or reference_number — left unlinked",
      payment.id,
      payment.external_id,
    )
    return
  payment.discharges_event_id = originating.id
  logger.info(
    "payment_received event %s discharges event %s (type=%s, ext=%s)",
    payment.id,
    originating.id,
    originating.event_type,
    originating.external_id,
  )


# Event types a payment may discharge. SalesReceipt covers QB's occasional
# receipt-then-Payment pair; CreditMemo is a separate flow.
_AR_ORIGINATING_TYPES: tuple[str, ...] = ("invoice_issued", "sales_receipt_recorded")


def dispatch(
  session: Session,
  event: Event,
  metadata: JournalEntryRecordedMetadata,
  created_by: str,
) -> HandlerResult:
  """Run the journal-entry GL writes, then stamp the discharge link."""
  result = journal_dispatch(session, event, metadata, created_by)
  _link_discharge(session, event, _AR_ORIGINATING_TYPES)
  return result


def dispatch_preview(
  session: Session,
  body: CreateEventBlockRequest,
  metadata: JournalEntryRecordedMetadata,
) -> HandlerPreview:
  """The journal_entry_recorded GL plan; discharge resolution runs only on
  dispatch."""
  return journal_dispatch_preview(session, body, metadata)


PAYMENT_RECEIVED_HANDLER = EventBlockPythonHandler(
  event_type="payment_received",
  display_name="Payment Received",
  metadata_schema=JournalEntryRecordedMetadata,
  target_status="classified",
  dispatch=dispatch,
  dispatch_preview=dispatch_preview,
)
