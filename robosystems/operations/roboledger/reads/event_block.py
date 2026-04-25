"""Event Block reads — get and list event envelopes."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.event_block import EventBlockEnvelope
from robosystems.models.extensions.roboledger.dimension_junctions import (
  event_dimensions,
)
from robosystems.models.extensions.roboledger.event import Event


def _load_dimension_ids(session: Session, event_id: str) -> list[str]:
  return list(
    session.execute(
      select(event_dimensions.c.dimension_id).where(
        event_dimensions.c.event_id == event_id
      )
    )
    .scalars()
    .all()
  )


def _to_envelope(session: Session, event: Event) -> EventBlockEnvelope:
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
    dimension_ids=_load_dimension_ids(session, event.id),
    agent_id=event.agent_id,
    resource_type=event.resource_type,
    resource_element_id=event.resource_element_id,
    replaced_by_event_id=event.replaced_by_event_id,
    replaces_event_id=event.replaces_event_id,
    created_at=event.created_at,
    created_by=event.created_by,
  )


def get_event_block(session: Session, event_id: str) -> EventBlockEnvelope | None:
  event = session.get(Event, event_id)
  if event is None:
    return None
  return _to_envelope(session, event)


def list_event_blocks(
  session: Session,
  *,
  event_type: str | None = None,
  event_category: str | None = None,
  status: str | None = None,
  agent_id: str | None = None,
  source: str | None = None,
  limit: int = 50,
  offset: int = 0,
) -> list[EventBlockEnvelope]:
  stmt = select(Event)
  if event_type is not None:
    stmt = stmt.where(Event.event_type == event_type)
  if event_category is not None:
    stmt = stmt.where(Event.event_category == event_category)
  if status is not None:
    stmt = stmt.where(Event.status == status)
  if agent_id is not None:
    stmt = stmt.where(Event.agent_id == agent_id)
  if source is not None:
    stmt = stmt.where(Event.source == source)

  stmt = stmt.order_by(Event.occurred_at.desc()).limit(limit).offset(offset)
  events = session.execute(stmt).scalars().all()
  return [_to_envelope(session, e) for e in events]
