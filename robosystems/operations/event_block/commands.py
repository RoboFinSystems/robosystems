"""Event Block commands — create, update, and preview operations.

Functions are pure: ``(Session, RequestModel, created_by) → ResponseModel``.

- ``create_event_block``: persists an event row. With ``apply_handlers=False``
  the row lands in ``status='captured'`` and nothing else is written. With
  ``apply_handlers=True`` the event is resolved against the Python registry
  first, falling back to the DSL registry; the matched handler fires and
  produces GL rows atomically with the event row.
- ``update_event_block``: applies status transitions and field corrections to
  an existing event.
- ``preview_event_block``: dry-runs handler resolution + template evaluation
  and returns the plan without persisting anything.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import ValidationError
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
from robosystems.operations.roboledger.reads.event_block import (
  _load_dimension_ids,
  _to_envelope,
)

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
# Terminal states (fulfilled, voided, superseded) have empty sets — no further
# transitions allowed. `superseded` is reachable from any non-terminal state
# so corrections can replace an event regardless of how far it has progressed.
_VALID_TRANSITIONS: dict[str, frozenset[str]] = {
  "captured": frozenset({"committed", "voided", "superseded"}),
  "classified": frozenset(
    {"committed", "pending", "fulfilled", "voided", "superseded"}
  ),
  "committed": frozenset({"pending", "fulfilled", "voided", "superseded"}),
  "pending": frozenset({"fulfilled", "voided", "superseded"}),
  "fulfilled": frozenset(),
  "voided": frozenset(),
  "superseded": frozenset(),
}


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
    event_class=body.event_class,
    event_action=body.event_action,
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
    obligated_by_event_id=body.obligated_by_event_id,
    discharges_event_id=body.discharges_event_id,
    created_at=datetime.now(UTC),
    created_by=created_by,
  )


def create_event_block(
  session: Session,
  body: CreateEventBlockRequest,
  created_by: str,
) -> EventBlockEnvelope:
  """Persist an event block, optionally firing the handler engine.

  ``apply_handlers=False``: capture-only — persists the event row in
  ``status='captured'`` and writes no GL rows.
  ``apply_handlers=True``: resolves the event_type to a handler and fires
  it atomically alongside the event row.

  Handler resolution order:
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

    # 2. Fall through to the DSL registry
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


def fire_handler_on_commit(
  session: Session,
  event: Event,
  created_by: str,
) -> None:
  """Resolve and fire a Python handler against the captured event metadata.

  Called when an event transitions to ``committed`` from a pre-handler
  state (``captured`` or ``classified``). For events whose ``event_type``
  has a registered Python handler, this validates the captured metadata
  against the handler's schema and dispatches — producing the GL rows
  the handler is responsible for. Events whose ``event_type`` has no
  handler fall through silently (e.g., support events with no GL
  impact, or future event types not yet wired).

  Errors propagate to the caller so the surrounding transaction rolls
  back — a bad approval cannot leave the event in ``committed`` with
  no GL rows behind it.

  Public (no underscore) because the loader's auto-commit path also
  calls this from outside this module.
  """
  python_handler = get_python_handler(event.event_type)
  if python_handler is None:
    return

  raw_metadata = dict(event.metadata_ or {})
  try:
    typed_metadata = python_handler.metadata_schema.model_validate(raw_metadata)
  except ValidationError as e:
    raise HandlerMetadataValidationError(
      f"Event {event.id} (type={event.event_type}): captured metadata fails "
      f"handler validation — cannot commit. {e}"
    )

  python_handler.dispatch(session, event, typed_metadata, created_by)


def update_event_block(
  session: Session,
  body: UpdateEventBlockRequest,
  created_by: str,
) -> EventBlockEnvelope:
  """Apply a status transition and/or field corrections to an event block.

  When the requested transition is ``captured → committed`` or
  ``classified → committed``, the event's Python handler (if any) fires
  against the captured metadata to produce the corresponding GL rows.
  Handler errors roll back the entire update, including the status
  change — a failed commit leaves the event in its pre-approval state.
  """
  event = session.get(Event, body.event_id)
  if event is None:
    raise EventNotFoundError(f"Event not found: {body.event_id}")

  fire_handler = False
  if body.transition_to is not None:
    allowed = _VALID_TRANSITIONS.get(event.status, frozenset())
    if body.transition_to not in allowed:
      raise InvalidEventTransitionError(
        f"Cannot transition event from '{event.status}' to '{body.transition_to}'. "
        f"Allowed transitions: {sorted(allowed) if allowed else 'none (terminal state)'}."
      )

    if body.transition_to == "superseded":
      if body.superseded_by_id is None:
        raise InvalidEventTransitionError(
          "transition_to='superseded' requires superseded_by_id."
        )
      if body.superseded_by_id == event.id:
        raise InvalidEventTransitionError("An event cannot supersede itself.")
      successor = session.get(Event, body.superseded_by_id)
      if successor is None:
        raise EventNotFoundError(
          f"Superseding event not found: {body.superseded_by_id}"
        )
      # Set both sides of the correction chain atomically so the backward
      # link query ("which event does B replace?") resolves.
      event.replaced_by_event_id = successor.id
      successor.replaces_event_id = event.id

    fire_handler = body.transition_to == "committed" and event.status in (
      "captured",
      "classified",
    )

    event.status = body.transition_to

  if body.description is not None:
    event.description = body.description

  if body.effective_at is not None:
    event.effective_at = body.effective_at

  if body.metadata_patch:
    merged = dict(event.metadata_ or {})
    merged.update(body.metadata_patch)
    event.metadata_ = merged

  if body.obligated_by_event_id is not None:
    event.obligated_by_event_id = body.obligated_by_event_id

  if body.discharges_event_id is not None:
    event.discharges_event_id = body.discharges_event_id

  if body.event_action is not None:
    event.event_action = body.event_action

  if fire_handler:
    # Handler runs after metadata patches so it sees the final shape.
    # Errors propagate; the surrounding transaction rolls back.
    fire_handler_on_commit(session, event, created_by)

  session.commit()
  return _to_envelope(event, _load_dimension_ids(session, event.id))


def _python_preview_to_response(preview) -> PreviewEventBlockResponse:
  """Map a Python HandlerPreview to the public PreviewEventBlockResponse shape."""

  def _line_element_ref(li: dict) -> str:
    # The metadata schema requires exactly one of element_id or
    # element_external_id per line. QB-captured lines carry only
    # element_external_id; manual/native lines may carry element_id
    # already. Either is human-meaningful for preview display.
    return li.get("element_id") or li.get("element_external_id") or ""

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
          debit_element_id=_line_element_ref(first_debit),
          credit_element_id=_line_element_ref(first_credit),
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
