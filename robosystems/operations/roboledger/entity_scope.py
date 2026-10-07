"""The one place that decides which entity a ledger operation acts on.

An operation either names an entity, or acts on the group parent: the
non-linked ``is_parent`` row, earliest created. Linked rows are other graphs'
companies received with a shared report, never a scope of this ledger.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from robosystems.models.extensions import Entity


class NoEntityError(LookupError, ValueError):
  """The graph has no entity of its own yet."""


class EntityNotInGraphError(LookupError, ValueError):
  """The named entity is not a ledger entity of this graph."""


_PARENT_SQL = text(
  "SELECT id FROM entities "
  "WHERE is_parent = true AND source <> 'linked' "
  "ORDER BY created_at ASC LIMIT 1"
)
_NAMED_SQL = text("SELECT id FROM entities WHERE id = :eid AND source <> 'linked'")
_ANY_OWN_ENTITY_SQL = text("SELECT 1 FROM entities WHERE source <> 'linked' LIMIT 1")


def resolve_entity_id(session: Session, entity_id: str | None = None) -> str:
  """The named entity's id, validated in this graph, else the group parent's."""
  if entity_id:
    row = session.execute(_NAMED_SQL, {"eid": entity_id}).fetchone()
    if row is None:
      raise EntityNotInGraphError(f"Entity {entity_id!r} not found in this graph.")
    return str(row.id)
  row = session.execute(_PARENT_SQL).fetchone()
  if row is None:
    raise NoEntityError("No entity found. Import data or initialize the ledger first.")
  return str(row.id)


def ensure_entity_id(session: Session, entity_id: str | None = None) -> str:
  """:func:`resolve_entity_id` for a write that starts a ledger's books.

  A graph can be created without its entity. Its first ledger write gives it
  the group parent: ``entity_<graph_id>``, the id graph creation gives one,
  named after the graph until someone renames it. A graph that has entities
  but no parent among them is not repaired here.
  """
  try:
    return resolve_entity_id(session, entity_id)
  except NoEntityError:
    if session.execute(_ANY_OWN_ENTITY_SQL).first() is not None:
      raise
  graph_id = str(session.execute(text("SELECT current_schema()")).scalar_one())
  parent_id = f"entity_{graph_id}"
  # Two first writes can race here; either row is the same row.
  session.execute(
    pg_insert(Entity.__table__)
    .values(
      id=parent_id,
      name=graph_id,
      is_parent=True,
      source="native",
      created_by="system",
    )
    .on_conflict_do_nothing(index_elements=["id"])
  )
  return parent_id


def find_entity_id(session: Session, entity_id: str | None = None) -> str | None:
  """:func:`resolve_entity_id` for a read: None on a graph with no entity yet,
  where there is nothing of anyone's to return."""
  try:
    return resolve_entity_id(session, entity_id)
  except NoEntityError:
    return None


def is_group_parent(session: Session, entity_id: str) -> bool:
  """Whether ``entity_id`` is the group parent, the entity the graph's own
  source connection books for."""
  return find_entity_id(session) == entity_id


def resolve_entity(session: Session, entity_id: str | None = None) -> Entity:
  """The ORM row for :func:`resolve_entity_id`."""
  resolved_id = resolve_entity_id(session, entity_id)
  entity = session.get(Entity, resolved_id)
  if entity is None:
    raise EntityNotInGraphError(f"Entity {resolved_id!r} not found in this graph.")
  return entity


def find_parent_entity(session: Session) -> Entity | None:
  """The group parent, or None when the graph has no entity yet."""
  try:
    return resolve_entity(session)
  except NoEntityError:
    return None


def owner_entity_id(session: Session, owner: object) -> str:
  """The entity a row belongs to (a schedule, a reconciliation, an event).
  A row from before rows carried one belongs to the group parent."""
  entity_id = getattr(owner, "entity_id", None)
  return str(entity_id) if entity_id else resolve_entity_id(session)


def report_entity_id(session: Session, report_id: str) -> str | None:
  """The entity a report's facts belong to; None for a report with no facts.

  A report is generated for one entity, so its fact sets agree; the earliest
  set decides if they ever do not.
  """
  row = session.execute(
    text(
      "SELECT entity_id FROM fact_sets "
      "WHERE report_id = :rid AND entity_id IS NOT NULL "
      "ORDER BY created_at ASC, id ASC LIMIT 1"
    ),
    {"rid": report_id},
  ).fetchone()
  return str(row.entity_id) if row else None


def find_linked_entity_id(
  session: Session,
  source_graph_id: str,
  source_entity_id: str | None = None,
  *,
  match_unkeyed: bool = True,
) -> str | None:
  """The linked row standing for a sharing graph's entity.

  Keyed on the source graph and the source entity, so two subsidiaries of one
  sending graph stay two rows. A row from before the key stood for the
  sender's parent; it matches after an exact match, and only when
  ``match_unkeyed`` (pass False for an entity that is not the sender's parent).
  """
  row = session.execute(
    text(
      "SELECT id FROM entities "
      "WHERE source = 'linked' AND metadata->>'source_graph_id' = :sgid "
      "  AND (CAST(:seid AS text) IS NULL "
      "       OR metadata->>'source_entity_id' = :seid "
      "       OR (:unkeyed AND metadata->>'source_entity_id' IS NULL)) "
      "ORDER BY (metadata->>'source_entity_id' IS NULL), created_at ASC LIMIT 1"
    ),
    {"sgid": source_graph_id, "seid": source_entity_id, "unkeyed": match_unkeyed},
  ).fetchone()
  return str(row.id) if row else None
