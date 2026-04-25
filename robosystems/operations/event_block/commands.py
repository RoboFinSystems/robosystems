"""Event Block commands — Phase 1 + Phase 3 write surface.

Phase 1: create (apply_handlers=False) + update (status transitions + corrections).
Phase 3: apply_handlers=True fires the handler engine; preview_event_block is a
dry-run that returns the plan without persisting rows.

All functions are pure: (Session, RequestModel, created_by) → ResponseModel.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import ValidationError
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
from robosystems.models.extensions.roboledger.agent import Agent
from robosystems.models.extensions.roboledger.dimension_junctions import (
  event_dimensions,
)
from robosystems.models.extensions.roboledger.event import Event

from .engine import EngineValidationError, apply_handler
from .python_handlers import get_python_handler
from .python_handlers.types import HandlerMetadataValidationError
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


def _resolve_agent_type(session: Session, agent_id: str | None) -> str | None:
  """Load the counterparty's agent_type for DSL handler matching."""
  if agent_id is None:
    return None
  agent = session.get(Agent, agent_id)
  if agent is None:
    return None
  return agent.agent_type


def _build_event_row(
  body: CreateEventBlockRequest,
  created_by: str,
  status: str,
) -> Event:
  return Event(
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
    status=status,
    created_at=datetime.now(UTC),
    created_by=created_by,
  )


def create_event_block(
  session: Session,
  body: CreateEventBlockRequest,
  created_by: str,
) -> EventBlockEnvelope:
  """Persist an event block, optionally firing the handler engine.

  apply_handlers=False: capture-only (Phase 1 behaviour, status='captured').
  apply_handlers=True: resolves handler and fires it atomically.

  Handler resolution:
    1. Python registry (hub-defined complex workflows, e.g., asset_disposed)
    2. DSL registry (event_handlers table, tenant-configurable simple templates)

  On validation failure (no handler, ambiguous, template error, engine error)
  nothing is persisted — the exception propagates and the caller's session
  rolls back.
  """
  if body.apply_handlers:
    # 1. Python registry wins over DSL registry
    python_handler = get_python_handler(body.event_type)
    if python_handler is not None:
      try:
        typed_metadata = python_handler.metadata_schema.model_validate(body.metadata)
      except ValidationError as e:
        raise HandlerMetadataValidationError(
          f"event_type='{body.event_type}' metadata validation failed: {e}"
        )

      event = _build_event_row(body, created_by, status=python_handler.target_status)
      session.add(event)
      session.flush()

      if body.dimension_ids:
        session.execute(
          event_dimensions.insert(),
          [{"event_id": event.id, "dimension_id": d} for d in body.dimension_ids],
        )

      # Handler executes; errors roll back the whole unit of work.
      python_handler.dispatch(session, event, typed_metadata, created_by)

      session.commit()
      return _to_envelope(event, body.dimension_ids)

    # 2. Fall through to the DSL registry (Phase 3 path)
    agent_type = _resolve_agent_type(session, body.agent_id)
    handler = resolve_handler(
      session,
      event_type=body.event_type,
      event_category=body.event_category,
      source=body.source,
      agent_type=agent_type,
      resource_type=body.resource_type,
      metadata=body.metadata,
    )

    event = _build_event_row(body, created_by, status="classified")
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
  event = _build_event_row(body, created_by, status="captured")
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


def _python_preview_to_response(preview) -> PreviewEventBlockResponse:
  """Map a Python HandlerPreview to the public PreviewEventBlockResponse shape."""
  planned: list[TransactionPreview] = []
  for entry_idx, entry in enumerate(preview.planned_entries):
    line_items = entry.get("line_items", [])
    # Python handlers emit multi-leg entries; expose them as a simple list
    # with (entry_index, debit_element_id, credit_element_id) where possible.
    # For entries with >2 legs (disposal), pick the first debit + first credit
    # pair as a summary — full detail is available in handler_metadata /
    # the echoed planned_entries via the underlying preview data.
    first_debit = next((li for li in line_items if li.get("debit_amount", 0) > 0), None)
    first_credit = next(
      (li for li in line_items if li.get("credit_amount", 0) > 0), None
    )
    if first_debit and first_credit:
      debit_amount = first_debit.get("debit_amount", 0)
      credit_amount = first_credit.get("credit_amount", 0)
      planned.append(
        TransactionPreview(
          entry_index=entry_idx,
          debit_element_id=first_debit.get("element_id", ""),
          credit_element_id=first_credit.get("element_id", ""),
          amount_cents=debit_amount,
          interpolated_debit_amount=str(debit_amount),
          interpolated_credit_amount=str(credit_amount),
        )
      )
  return PreviewEventBlockResponse(
    matched_handler=None,  # Python handlers aren't rows in event_handlers
    planned_transactions=planned,
    validation_errors=preview.validation_errors,
    would_succeed=preview.would_succeed,
    handler_metadata=preview.computed_values,
  )


def preview_event_block(
  session: Session,
  body: CreateEventBlockRequest,
  created_by: str,
) -> PreviewEventBlockResponse:
  """Dry-run: resolve handler + evaluate template, return plan without persisting.

  Python registry wins over DSL registry, same as create_event_block.
  No rows are written.
  """
  from robosystems.operations.roboledger.reads.event_handler import handler_to_response

  # 1. Python registry
  python_handler = get_python_handler(body.event_type)
  if python_handler is not None:
    try:
      typed_metadata = python_handler.metadata_schema.model_validate(body.metadata)
    except ValidationError as e:
      return PreviewEventBlockResponse(
        matched_handler=None,
        planned_transactions=[],
        validation_errors=[f"metadata validation: {e}"],
        would_succeed=False,
      )
    preview = python_handler.dispatch_preview(session, body, typed_metadata)
    return _python_preview_to_response(preview)

  # 2. DSL registry fallback
  errors: list[str] = []
  matched_handler_response = None
  planned: list[TransactionPreview] = []
  agent_type = _resolve_agent_type(session, body.agent_id)

  try:
    handler = resolve_handler(
      session,
      event_type=body.event_type,
      event_category=body.event_category,
      source=body.source,
      agent_type=agent_type,
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
      debit_raw = interpolate(debit_amount_str, context)
      credit_raw = interpolate(credit_amount_str, context)
      try:
        debit_cents = int(debit_raw)
        credit_cents = int(credit_raw)
      except (TypeError, ValueError):
        errors.append(
          f"Entry {i}: amount expression resolved to non-numeric value "
          f"(debit={debit_raw!r}, credit={credit_raw!r})"
        )
        continue

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
    except (TemplateInterpolationError, EngineValidationError) as e:
      errors.append(f"Entry {i}: {e}")

  return PreviewEventBlockResponse(
    matched_handler=matched_handler_response,
    planned_transactions=planned,
    validation_errors=errors,
    would_succeed=len(errors) == 0,
  )
