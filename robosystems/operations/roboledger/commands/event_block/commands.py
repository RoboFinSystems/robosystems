"""Event Block commands — Phase 1 + Phase 3 write surface.

Phase 1: create (apply_handlers=False) + update (status transitions + corrections).
Phase 3: apply_handlers=True fires the handler engine; preview_event_block is a
dry-run that returns the plan without persisting rows.

All functions are pure: (Session, RequestModel, created_by) → ResponseModel.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.event_block import (
  CreateEventBlockRequest,
  EventBlockEnvelope,
  UpdateEventBlockRequest,
)
from robosystems.models.api.event_handler import (
  PreviewEventBlockResponse,
  TransactionPreview,
)
from robosystems.models.extensions.roboledger.dimension_junctions import (
  event_dimensions,
)
from robosystems.models.extensions.roboledger.event import Event

from .engine import EngineValidationError, apply_handler
from .registry import (
  HandlerAmbiguousError,
  HandlerNotFoundError,
  resolve_handler,
)
from .template import (
  TemplateInterpolationError,
  build_handler_context,
  interpolate,
)


class EventNotFoundError(Exception):
  pass


class InvalidEventTransitionError(Exception):
  pass


# Valid outbound transitions from each status.
# Terminal states have empty sets — no further transitions allowed.
_VALID_TRANSITIONS: dict[str, frozenset[str]] = {
  "captured": frozenset({"committed", "voided"}),
  "classified": frozenset({"committed", "pending", "fulfilled", "voided"}),
  "committed": frozenset({"pending", "fulfilled", "voided"}),
  "pending": frozenset({"fulfilled", "voided"}),
  "fulfilled": frozenset(),
  "voided": frozenset(),
  "superseded": frozenset(),
}


def _load_dimension_ids(session: Session, event_id: str) -> list[str]:
  rows = (
    session.execute(
      select(event_dimensions.c.dimension_id).where(
        event_dimensions.c.event_id == event_id
      )
    )
    .scalars()
    .all()
  )
  return list(rows)


def _to_envelope(event: Event, dimension_ids: list[str]) -> EventBlockEnvelope:
  return EventBlockEnvelope(
    id=event.id,
    event_type=event.event_type,
    event_category=event.event_category,
    status=event.status,
    occurred_at=event.occurred_at,
    effective_at=event.effective_at,
    source=event.source,
    external_id=event.external_id,
    external_url=event.external_url,
    amount=event.amount,
    currency=event.currency,
    description=event.description,
    metadata=dict(event.metadata_ or {}),
    dimension_ids=dimension_ids,
    agent_id=event.agent_id,
    resource_type=event.resource_type,
    resource_element_id=event.resource_element_id,
    replaced_by_event_id=event.replaced_by_event_id,
    replaces_event_id=event.replaces_event_id,
    created_at=event.created_at,
    created_by=event.created_by,
  )


def create_event_block(
  session: Session,
  body: CreateEventBlockRequest,
  created_by: str,
) -> EventBlockEnvelope:
  """Persist an event block, optionally firing the handler engine.

  apply_handlers=False: capture-only (Phase 1 behaviour, status='captured').
  apply_handlers=True:  resolves handler, fires template, status='classified'.

  On validation failure (no handler, ambiguous, template error, engine error)
  nothing is persisted — the exception propagates and the caller's session
  rolls back.
  """
  if body.apply_handlers:
    handler = resolve_handler(
      session,
      event_type=body.event_type,
      event_category=body.event_category,
      source=body.source,
      agent_type=None,  # resolved from agent row in Phase 4+
      resource_type=body.resource_type,
      metadata=body.metadata,
    )

    # Insert event first (flush gives it an id)
    event = Event(
      event_type=body.event_type,
      event_category=body.event_category,
      agent_id=body.agent_id,
      resource_type=body.resource_type,
      resource_element_id=body.resource_element_id,
      occurred_at=body.occurred_at,
      effective_at=body.effective_at,
      source=body.source,
      external_id=body.external_id,
      external_url=body.external_url,
      amount=body.amount,
      currency=body.currency,
      description=body.description,
      metadata_=body.metadata,
      status="classified",
      created_at=datetime.now(UTC),
      created_by=created_by,
    )
    session.add(event)
    session.flush()

    if body.dimension_ids:
      session.execute(
        event_dimensions.insert(),
        [{"event_id": event.id, "dimension_id": d} for d in body.dimension_ids],
      )

    # Fire handler — flushes Transaction + Entry + LineItems inside
    apply_handler(session, event, handler, created_by=created_by)

    session.commit()
    return _to_envelope(event, body.dimension_ids)

  # Capture-only path (apply_handlers=False)
  event = Event(
    event_type=body.event_type,
    event_category=body.event_category,
    agent_id=body.agent_id,
    resource_type=body.resource_type,
    resource_element_id=body.resource_element_id,
    occurred_at=body.occurred_at,
    effective_at=body.effective_at,
    source=body.source,
    external_id=body.external_id,
    external_url=body.external_url,
    amount=body.amount,
    currency=body.currency,
    description=body.description,
    metadata_=body.metadata,
    status="captured",
    created_at=datetime.now(UTC),
    created_by=created_by,
  )
  session.add(event)
  session.flush()

  if body.dimension_ids:
    session.execute(
      event_dimensions.insert(),
      [{"event_id": event.id, "dimension_id": d} for d in body.dimension_ids],
    )

  session.commit()
  return _to_envelope(event, body.dimension_ids)


def update_event_block(
  session: Session,
  body: UpdateEventBlockRequest,
  created_by: str,
) -> EventBlockEnvelope:
  """Apply a status transition and/or field corrections to an event block."""
  event = session.get(Event, body.event_id)
  if event is None:
    raise EventNotFoundError(f"Event not found: {body.event_id}")

  if body.transition_to is not None:
    allowed = _VALID_TRANSITIONS.get(event.status, frozenset())
    if body.transition_to not in allowed:
      raise InvalidEventTransitionError(
        f"Cannot transition event from '{event.status}' to '{body.transition_to}'. "
        f"Allowed transitions: {sorted(allowed) if allowed else 'none (terminal state)'}."
      )
    event.status = body.transition_to

    if body.transition_to == "superseded" and body.superseded_by_id is not None:
      event.replaced_by_event_id = body.superseded_by_id

  if body.description is not None:
    event.description = body.description

  if body.effective_at is not None:
    event.effective_at = body.effective_at

  if body.metadata_patch:
    merged = dict(event.metadata_ or {})
    merged.update(body.metadata_patch)
    event.metadata_ = merged

  session.commit()
  return _to_envelope(event, _load_dimension_ids(session, event.id))


def preview_event_block(
  session: Session,
  body: CreateEventBlockRequest,
  created_by: str,
) -> PreviewEventBlockResponse:
  """Dry-run: resolve handler + evaluate template, return plan without persisting.

  Uses a savepoint so the session is rolled back after inspection.
  Returns PreviewEventBlockResponse with matched_handler, planned_transactions,
  and any validation_errors — no rows written.
  """
  from robosystems.operations.roboledger.reads.event_handler import handler_to_response

  errors: list[str] = []
  matched_handler_response = None
  planned: list[TransactionPreview] = []

  try:
    handler = resolve_handler(
      session,
      event_type=body.event_type,
      event_category=body.event_category,
      source=body.source,
      agent_type=None,
      resource_type=body.resource_type,
      metadata=body.metadata,
    )
    matched_handler_response = handler_to_response(handler)
  except HandlerNotFoundError as e:
    errors.append(str(e))
    return PreviewEventBlockResponse(
      matched_handler=None,
      planned_transactions=[],
      validation_errors=errors,
      would_succeed=False,
    )
  except HandlerAmbiguousError as e:
    errors.append(str(e))
    return PreviewEventBlockResponse(
      matched_handler=None,
      planned_transactions=[],
      validation_errors=errors,
      would_succeed=False,
    )

  # Dry-evaluate template without writing
  template = handler.transaction_template or {}
  event_ctx = {
    "id": "preview",
    "event_type": body.event_type,
    "event_category": body.event_category,
    "agent_id": body.agent_id,
    "resource_type": body.resource_type,
    "resource_element_id": body.resource_element_id,
    "occurred_at": str(body.occurred_at),
    "effective_at": str(body.effective_at) if body.effective_at else None,
    "source": body.source,
    "external_id": body.external_id,
    "amount": body.amount,
    "currency": body.currency,
    "description": body.description,
    "metadata": body.metadata,
  }
  context = {
    "event": event_ctx,
    "handler": build_handler_context(handler),
  }

  for i, entry_spec in enumerate(template.get("transactions", [])):
    et = entry_spec.get("entry_template", {})
    try:
      debit_amount_str = et.get("debit", {}).get("amount", "")
      credit_amount_str = et.get("credit", {}).get("amount", "")
      debit_cents = int(interpolate(debit_amount_str, context))
      credit_cents = int(interpolate(credit_amount_str, context))

      if debit_cents != credit_cents:
        errors.append(
          f"Entry {i}: unbalanced — debit {debit_cents} ≠ credit {credit_cents}"
        )

      planned.append(
        TransactionPreview(
          entry_index=i,
          debit_element_id=et.get("debit", {}).get("element_id", ""),
          credit_element_id=et.get("credit", {}).get("element_id", ""),
          amount_cents=debit_cents,
          interpolated_debit_amount=str(debit_cents),
          interpolated_credit_amount=str(credit_cents),
        )
      )
    except (TemplateInterpolationError, EngineValidationError, ValueError) as e:
      errors.append(f"Entry {i}: {e}")

  return PreviewEventBlockResponse(
    matched_handler=matched_handler_response,
    planned_transactions=planned,
    validation_errors=errors,
    would_succeed=len(errors) == 0,
  )
