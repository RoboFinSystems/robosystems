"""Read operations for event handlers."""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from robosystems.models.api.common import create_pagination_info
from robosystems.models.api.event_handler import (
  EventHandlerListResponse,
  EventHandlerResponse,
)
from robosystems.models.extensions.roboledger.event_handler import EventHandler


def handler_to_response(handler: EventHandler) -> EventHandlerResponse:
  return EventHandlerResponse(
    id=handler.id,
    name=handler.name,
    description=handler.description,
    event_type=handler.event_type,
    event_category=handler.event_category,
    match_source=handler.match_source,
    match_agent_type=handler.match_agent_type,
    match_resource_type=handler.match_resource_type,
    match_metadata_expression=handler.match_metadata_expression,
    transaction_template=dict(handler.transaction_template or {}),
    priority=handler.priority,
    is_active=handler.is_active,
    origin=handler.origin,
    suggested_by=handler.suggested_by,
    confidence=handler.confidence,
    approved_by=handler.approved_by,
    approved_at=handler.approved_at,
    awaiting_approval=handler.suggested_by == "ai" and handler.approved_by is None,
    created_at=handler.created_at,
    updated_at=handler.updated_at,
    created_by=handler.created_by,
  )


def get_event_handler(session: Session, handler_id: str) -> EventHandlerResponse | None:
  handler = session.get(EventHandler, handler_id)
  if handler is None:
    return None
  return handler_to_response(handler)


def list_event_handlers(
  session: Session,
  *,
  event_type: str | None = None,
  is_active: bool | None = True,
  origin: str | None = None,
  approved: bool | None = None,
  limit: int = 50,
  offset: int = 0,
) -> list[EventHandlerResponse]:
  query = select(EventHandler)
  if event_type is not None:
    query = query.where(EventHandler.event_type == event_type)
  if is_active is not None:
    query = query.where(EventHandler.is_active.is_(is_active))
  if origin is not None:
    query = query.where(EventHandler.origin == origin)
  if approved is True:
    query = query.where(EventHandler.approved_by.isnot(None))
  elif approved is False:
    query = query.where(EventHandler.approved_by.is_(None)).where(
      EventHandler.suggested_by == "ai"
    )
  query = (
    query.order_by(EventHandler.priority.desc(), EventHandler.name)
    .limit(limit)
    .offset(offset)
  )
  handlers = session.execute(query).scalars().all()
  return [handler_to_response(h) for h in handlers]


def list_event_handlers_page(
  session: Session,
  *,
  event_type: str | None = None,
  is_active: bool | None = None,
  awaiting_approval: bool | None = None,
  limit: int = 100,
  offset: int = 0,
) -> EventHandlerListResponse:
  """Every handler on the graph, highest priority first, active or not.
  ``awaiting_approval`` narrows to the AI's suggestions no one has approved
  (true) or to everything else (false)."""
  conditions = []
  if event_type is not None:
    conditions.append(EventHandler.event_type == event_type)
  if is_active is not None:
    conditions.append(EventHandler.is_active.is_(is_active))
  # coalesce: a person's handler has no suggested_by, and NULL = 'ai' negates
  # to NULL, which would drop it from the "everything else" side.
  pending = (func.coalesce(EventHandler.suggested_by, "") == "ai") & (
    EventHandler.approved_by.is_(None)
  )
  if awaiting_approval is True:
    conditions.append(pending)
  elif awaiting_approval is False:
    conditions.append(~pending)
  total = (
    session.execute(
      select(func.count()).select_from(EventHandler).where(*conditions)
    ).scalar()
    or 0
  )
  handlers = (
    session.execute(
      select(EventHandler)
      .where(*conditions)
      .order_by(EventHandler.priority.desc(), EventHandler.name, EventHandler.id)
      .offset(offset)
      .limit(limit)
    )
    .scalars()
    .all()
  )
  return EventHandlerListResponse(
    event_handlers=[handler_to_response(h) for h in handlers],
    pagination=create_pagination_info(total, limit, offset),
  )
