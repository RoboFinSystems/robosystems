"""EventHandler commands — create and update handler rules."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy.orm import Session

from robosystems.models.api.event_handler import (
  CreateEventHandlerRequest,
  EventHandlerResponse,
  UpdateEventHandlerRequest,
)
from robosystems.models.extensions.roboledger.event_handler import EventHandler
from robosystems.operations.roboledger.reads.event_handler import handler_to_response


class EventHandlerNotFoundError(Exception):
  def __init__(self, handler_id: str) -> None:
    super().__init__(f"EventHandler not found: {handler_id}")
    self.handler_id = handler_id


class TemplateValidationError(Exception):
  """Raised when a transaction_template fails structural validation."""


def _validate_template(template_dict: dict) -> None:
  """Check the template has the minimum required shape."""
  entries = template_dict.get("transactions")
  if not entries or not isinstance(entries, list):
    raise TemplateValidationError(
      "transaction_template must have a 'transactions' list with at least one entry"
    )
  for i, entry in enumerate(entries):
    et = entry.get("entry_template")
    if not et:
      raise TemplateValidationError(
        f"transactions[{i}] must have an 'entry_template' key"
      )
    for side in ("debit", "credit"):
      leg = et.get(side)
      if not leg:
        raise TemplateValidationError(
          f"transactions[{i}].entry_template must have a '{side}' key"
        )
      if not leg.get("element_id"):
        raise TemplateValidationError(
          f"transactions[{i}].entry_template.{side} must have an 'element_id'"
        )
      if not leg.get("amount"):
        raise TemplateValidationError(
          f"transactions[{i}].entry_template.{side} must have an 'amount' expression"
        )


def create_event_handler(
  session: Session,
  body: CreateEventHandlerRequest,
  created_by: str,
) -> EventHandlerResponse:
  template_dict = body.transaction_template.model_dump()
  _validate_template(template_dict)

  handler = EventHandler(
    name=body.name,
    description=body.description,
    event_type=body.event_type,
    event_category=body.event_category,
    match_source=body.match_source,
    match_agent_type=body.match_agent_type,
    match_resource_type=body.match_resource_type,
    match_metadata_expression=body.match_metadata_expression,
    transaction_template=template_dict,
    priority=body.priority,
    is_active=body.is_active,
    origin=body.origin,
    metadata_=body.metadata,
    created_at=datetime.now(UTC),
    updated_at=datetime.now(UTC),
    created_by=created_by,
  )
  session.add(handler)
  session.commit()
  session.refresh(handler)
  return handler_to_response(handler)


def update_event_handler(
  session: Session,
  body: UpdateEventHandlerRequest,
  created_by: str,
) -> EventHandlerResponse:
  handler = session.get(EventHandler, body.event_handler_id)
  if handler is None:
    raise EventHandlerNotFoundError(body.event_handler_id)

  for field in (
    "name",
    "description",
    "event_category",
    "match_source",
    "match_agent_type",
    "match_resource_type",
    "match_metadata_expression",
    "priority",
    "is_active",
  ):
    value = getattr(body, field)
    if value is not None:
      setattr(handler, field, value)

  if body.transaction_template is not None:
    template_dict = body.transaction_template.model_dump()
    _validate_template(template_dict)
    handler.transaction_template = template_dict

  if body.approve is True:
    handler.approved_by = created_by
    handler.approved_at = datetime.now(UTC)
  elif body.approve is False:
    handler.approved_by = None
    handler.approved_at = None

  if body.metadata_patch:
    merged = dict(handler.metadata_ or {})
    merged.update(body.metadata_patch)
    handler.metadata_ = merged

  handler.updated_at = datetime.now(UTC)
  session.commit()
  session.refresh(handler)
  return handler_to_response(handler)
