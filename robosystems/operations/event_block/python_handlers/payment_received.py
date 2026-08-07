"""Payment received event handler — AR-side duality resolution.

QB Payment events arrive with the same nested-entries metadata shape as
``journal_entry_recorded`` (the GL side is identical — a balanced DR
Cash / CR AR entry), plus a ``qb_linked_txns`` blob that names the
invoice this payment settles. This handler delegates the GL writes to
the journal handler unchanged, then resolves the discharge link:
``event.discharges_event_id`` points at the originating
``invoice_issued`` event.

Resolution order:
  1. ``metadata.qb_linked_txns`` — each ref is ``{txn_id, txn_type}``,
     and the originating event's ``external_id`` is
     ``f"{txn_type}_{txn_id}"`` by construction in the QB importer.
     This is QB's canonical "this payment settles those invoices"
     vocabulary; trust it when present.
  2. Fallback: ``metadata.qb_reference_number`` ↔ originating event's
     ``metadata.qb_doc_number``, scoped to the same ``agent_id`` and
     ``source``. Used when LinkedTxn is missing (older payments,
     manual entries imported via spreadsheet).

Idempotency: a re-sync that re-fires the handler skips resolution when
``discharges_event_id`` is already set, so handler retries are
no-cost.

Deliberate limitations:

- Multi-invoice splits. QB's Payment.Line may carry multiple LinkedTxn
  refs (one payment settling N invoices, each line for a different
  amount). All refs are captured in metadata but only the first match
  populates ``discharges_event_id``; the rest stay queryable in metadata.
  The ``sale.amount - SUM(discharges.amount)`` AR query is therefore
  correct for single-invoice payments and undercounts for splits.
- No bidirectional update. Only the payment carries
  ``discharges_event_id``; the invoice row is untouched. "What payments
  settled invoice X?" reads the same column in the opposite direction, so
  a back-reference would be redundant.
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

# Originating events in these terminal states can never legitimately
# receive a discharge — a voided invoice was retracted; a superseded
# one was replaced by a successor that owns its own discharge chain.
# Linking a fresh payment to either would leave a stale pointer the
# moment the originating row changes hands. We deliberately do NOT
# exclude ``captured`` / ``classified`` here: a single QB sync batch
# captures invoices and payments together, and handler firing order
# inside the loader's auto-commit pass is dict-iteration order, not
# guaranteed chronological. Allowing pre-commit invoices to be linked
# keeps the in-batch case working — the AR aggregate separately
# filters originating events by ``status IN ('committed', 'fulfilled')``
# so an unlinked-but-still-captured invoice can't pollute open totals.
_TERMINAL_INVALID_STATUSES: tuple[str, ...] = ("voided", "superseded")


def _resolve_by_linked_txns(
  session: Session,
  *,
  source: str,
  linked_txns: list[dict[str, str]],
  candidate_event_types: tuple[str, ...],
) -> Event | None:
  """Look up the originating event by composite external_id.

  Each LinkedTxn ref is ``{txn_id, txn_type}``; the originating event's
  ``external_id`` is ``f"{txn_type}_{txn_id}"`` by construction in
  ``_flatten_txn_header``. Returns the first event whose event_type is
  in the allowed set — guards against a Payment's LinkedTxn pointing
  at, say, a CreditMemo, which would not be a valid discharge target.
  Voided / superseded originating events are excluded so a payment
  doesn't end up pointing at a row that was retracted or replaced.
  """
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

  Requires same ``agent_id`` to disambiguate when two unrelated
  customers/vendors happen to share a doc number (rare but possible
  across long-running tenants). When ``agent_id`` is NULL on the
  payment we still attempt the lookup but only match invoices with
  NULL ``agent_id`` — never silently cross-link an unscoped payment to
  someone's invoice. Voided / superseded events are excluded for the
  same reason as in ``_resolve_by_linked_txns``.
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
  """Find the originating event a payment discharges.

  Walks the two-stage match (LinkedTxn → reference_number). Returns
  ``None`` when nothing matches — the payment lands without a
  ``discharges_event_id``, which is the correct steady state for
  payments that genuinely don't settle a tracked obligation (e.g.,
  cash-basis income with no AR cycle).
  """
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
  """Resolve and stamp ``discharges_event_id`` if not already set.

  Idempotent — a re-fire that finds the column already populated skips
  the lookup entirely. When no originating event matches, leaves the
  column NULL and logs an info-level note so re-sync miss-rate is
  observable but not noisy.
  """
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


# Event types whose row a payment_received event is allowed to discharge.
# Invoice is the canonical AR scenario; SalesReceipt is included because
# QB occasionally emits a SalesReceipt + later Payment pair (e.g., a
# deposit applied to a previously-recorded receipt). Excluded:
# JournalEntry (no AR semantic), CreditMemo (negative AR, separate flow).
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
  """Preview is identical to journal_entry_recorded.

  Discharge resolution runs only on real dispatch — it reads committed
  metadata and cross-event state — so the preview is just the GL plan.
  """
  return journal_dispatch_preview(session, body, metadata)


PAYMENT_RECEIVED_HANDLER = EventBlockPythonHandler(
  event_type="payment_received",
  display_name="Payment Received",
  metadata_schema=JournalEntryRecordedMetadata,
  target_status="classified",
  dispatch=dispatch,
  dispatch_preview=dispatch_preview,
)
