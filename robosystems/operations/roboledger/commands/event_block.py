"""Event Block commands — Phase 1 capture-only write surface.

Phase 1: create (apply_handlers=False only) + update (status transitions +
field corrections). The handler engine (apply_handlers=True, event_handlers
table, transaction_template DSL) ships in Phase 3.

Both functions are pure: (Session, RequestModel, created_by) → ResponseModel.
The REST router and MCP auto-generated tool both delegate here.
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
from robosystems.models.extensions.roboledger.dimension_junctions import (
  event_dimensions,
)
from robosystems.models.extensions.roboledger.event import Event


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
  """Persist an event block in capture-only mode.

  apply_handlers=True raises NotImplementedError (501) — the handler engine
  ships in Phase 3. Callers must pass apply_handlers=False explicitly.
  """
  if body.apply_handlers:
    raise NotImplementedError(
      "Handler engine not yet implemented. Pass apply_handlers=False for capture-only mode "
      "(Phase 3 will enable apply_handlers=True)."
    )

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
  session.flush()  # Generates the id before inserting junction rows

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
