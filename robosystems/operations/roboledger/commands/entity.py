"""Write operations for the ledger entities: the group parent and the
subsidiaries under it."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.entity import (
  CreateEntityRequest,
  LedgerEntityResponse,
  UpdateEntityRequest,
)
from robosystems.models.extensions import Entity
from robosystems.operations.graph.reporting_style_defaults import default_style_for
from robosystems.operations.roboledger.commands.reporting_style import (
  require_reporting_style,
)
from robosystems.operations.roboledger.entity_scope import (
  find_parent_entity,
  resolve_entity,
)
from robosystems.operations.roboledger.reads.entity import entity_to_response
from robosystems.utils.ulid import generate_prefixed_ulid

__all__ = [
  "EntityHierarchyError",
  "EntityTickerTakenError",
  "ParentEntityNotFoundError",
  "create_entity",
  "update_entity",
  "update_parent_entity",
]

_TICKER_MAX = 10


class ParentEntityNotFoundError(LookupError):
  """The graph has no primary entity to update."""

  def __init__(self) -> None:
    super().__init__("No entity found. Create an entity graph first.")


class EntityHierarchyError(ValueError):
  """A hierarchy field that cannot hold on this entity."""


class EntityTickerTakenError(ValueError):
  """Another entity of the graph already has this ticker."""

  def __init__(self, ticker: str) -> None:
    super().__init__(
      f"Ticker {ticker!r} is already taken by another entity of this graph."
    )


def _is_group_parent(entity: Entity) -> bool:
  return bool(entity.is_parent) and entity.parent_entity_id is None


def _apply_updates(session: Session, entity: Entity, updates: dict[str, Any]):
  """Set ``updates`` on ``entity`` and commit; the response is built before
  the commit expires the row."""
  if "ownership_pct" in updates and _is_group_parent(entity):
    raise EntityHierarchyError(
      "The group parent has no owner in this graph; ownership_pct applies to "
      "a subsidiary."
    )
  for field_name, value in updates.items():
    setattr(entity, field_name, value)

  entity.updated_at = datetime.now(UTC)
  # Build the response before committing: commit expires the instance, and a
  # refresh would run on a pooled connection whose search_path was reset.
  session.flush()
  response = entity_to_response(entity)
  session.commit()
  return response


def update_parent_entity(
  session: Session, updates: dict[str, Any], entity_id: str | None = None
) -> LedgerEntityResponse | None:
  """Apply `updates` to the named entity, default the group parent, and commit.

  Returns `None` if there is no entity to update. Commits whatever it is
  given; the caller validates that `updates` is non-empty. Raises
  `EntityNotInGraphError` for an id that is not this graph's.
  """
  entity = resolve_entity(session, entity_id) if entity_id else None
  if entity is None:
    entity = find_parent_entity(session)
  if entity is None:
    return None
  return _apply_updates(session, entity, updates)


def update_entity(
  session: Session,
  body: UpdateEntityRequest,
  created_by: str,
) -> LedgerEntityResponse:
  """Update an entity of the group from a validated request body.

  Only non-null fields are applied. Raises `ValueError` for an empty body,
  `ParentEntityNotFoundError` when the graph has no entity, and
  `EntityNotInGraphError` for an `entity_id` that is not this graph's.
  ``created_by`` is unused (registrar signature).
  """
  del created_by

  updates = body.model_dump(exclude_none=True, exclude={"entity_id"})
  if not updates:
    raise ValueError("No fields provided for update.")

  result = update_parent_entity(session, updates, entity_id=body.entity_id)
  if result is None:
    raise ParentEntityNotFoundError()
  return result


def _initials(name: str) -> str:
  """The ticker graph creation derives: initials of the name's words, else
  its first four characters."""
  words = re.sub(r"[^a-zA-Z0-9\s]", "", name).split()
  if len(words) >= 2:
    return "".join(w[0].upper() for w in words if w)[:6]
  return name[:4].upper().replace(" ", "") or "ENT"


def _ticker_taken(session: Session, ticker: str) -> bool:
  return (
    session.execute(
      select(Entity.id)
      .where(func.lower(Entity.ticker) == ticker.lower(), Entity.source != "linked")
      .limit(1)
    ).first()
    is not None
  )


def _ticker_for(session: Session, requested: str | None, name: str) -> str:
  """A ticker unique among the graph's own entities: the requested one, refused
  when taken, else the name's initials with a numeric suffix if those are."""
  if requested:
    ticker = requested.strip().upper()
    if _ticker_taken(session, ticker):
      raise EntityTickerTakenError(ticker)
    return ticker
  base = _initials(name)
  candidate, suffix = base, 1
  while _ticker_taken(session, candidate):
    suffix += 1
    candidate = f"{base[: _TICKER_MAX - len(str(suffix))]}{suffix}"
  return candidate


def create_entity(
  session: Session,
  body: CreateEntityRequest,
  created_by: str,
) -> LedgerEntityResponse:
  """Add an entity to the group: a subsidiary of ``parent_entity_id``, default
  the group parent. A graph with no entity yet gets this one as its parent.

  Raises `EntityNotInGraphError` for a parent that is not this graph's,
  `EntityHierarchyError` for an ownership share on the group parent,
  `EntityTickerTakenError`, and `ReportingStyleInvalidError` for a named
  Style the graph cannot render.
  """
  if body.parent_entity_id:
    parent: Entity | None = resolve_entity(session, body.parent_entity_id)
  else:
    parent = find_parent_entity(session)

  if parent is None and body.ownership_pct is not None:
    raise EntityHierarchyError(
      "This graph has no entity yet, so this one becomes its group parent, "
      "which has no owner in the graph; omit ownership_pct."
    )

  if body.reporting_style_id:
    require_reporting_style(session, body.reporting_style_id)
    reporting_style_id = body.reporting_style_id
  else:
    reporting_style_id = default_style_for(body.entity_type)

  entity_id = generate_prefixed_ulid("ent")
  now = datetime.now(UTC)
  entity = Entity(
    id=entity_id,
    name=body.name,
    legal_name=body.legal_name or body.name,
    uri=body.uri or f"https://robosystems.ai/entities#{entity_id}",
    ticker=_ticker_for(session, body.ticker, body.name),
    cik=body.cik,
    sic=body.sic,
    sic_description=body.sic_description,
    category=body.category,
    state_of_incorporation=body.state_of_incorporation,
    fiscal_year_end=body.fiscal_year_end
    or (parent.fiscal_year_end if parent is not None else None),
    tax_id=body.tax_id,
    lei=body.lei,
    industry=body.industry,
    entity_type=body.entity_type,
    reporting_style_id=reporting_style_id,
    phone=body.phone,
    website=body.website,
    status="active",
    is_parent=parent is None,
    parent_entity_id=parent.id if parent is not None else None,
    ownership_pct=body.ownership_pct,
    source="native",
    address_line1=body.address_line1,
    address_city=body.address_city,
    address_state=body.address_state,
    address_postal_code=body.address_postal_code,
    address_country=body.address_country or "US",
    created_by=created_by,
    created_at=now,
    updated_at=now,
  )
  session.add(entity)
  session.flush()
  response = entity_to_response(entity)
  session.commit()
  return response
