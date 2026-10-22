"""The one place that decides which entity a ledger operation acts on.

An operation either names an entity, or acts on the group parent: the
non-linked ``is_parent`` row, earliest created. Linked rows are other graphs'
companies received with a shared report, never a scope of this ledger.
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, timedelta

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.extensions import Entity
from robosystems.operations.roboledger.entry_status import SUPERSEDED_ENTRY_STATUS


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


def graph_display_name(graph_id: str) -> str:
  """The graph's name from its platform record, else its id.

  Best effort, and never a reason for a write to fail: the platform database
  is a different one, and a test tenant has no graph record at all.
  """
  try:
    from robosystems.db.platform import SessionFactory
    from robosystems.models.core import Graph

    with SessionFactory() as platform_session:
      name = platform_session.execute(
        select(Graph.graph_name).where(Graph.graph_id == graph_id)
      ).scalar_one_or_none()
  except Exception as exc:
    logger.warning(f"Graph name lookup failed for {graph_id}: {exc}")
    return graph_id
  return name or graph_id


def ensure_entity_id(session: Session, entity_id: str | None = None) -> str:
  """:func:`resolve_entity_id` for a write that starts a ledger's books.

  A graph can be created without its entity. Its first ledger write gives it
  the group parent: ``entity_<graph_id>``, the id graph creation gives one,
  carrying the graph's name (its id when the platform record cannot be read)
  until someone renames it. A graph that has entities but no parent among
  them is not repaired here.
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
      name=graph_display_name(graph_id),
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


def is_consolidation_entity(session: Session, entity_id: str) -> bool:
  """Whether ``entity_id`` is the group's consolidation entity, whose books
  hold the consolidating entries and nothing a company transacts."""
  entity = session.get(Entity, entity_id)
  return entity is not None and entity.entity_role == "consolidation"


def books_entity_id(session: Session, entity_id: str) -> str:
  """The entity whose chart and Reporting Style ``entity_id``'s statements
  are read under. The consolidation entity's statements are the group's
  consolidated ones, read under the group parent's; any other entity's are
  its own."""
  if is_consolidation_entity(session, entity_id):
    return resolve_entity_id(session)
  return entity_id


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


def find_consolidation_entity(session: Session) -> Entity | None:
  """The group's consolidation entity, or None: the first by creation, the
  one the consolidation engine posts in. A graph has at most one."""
  return session.execute(
    select(Entity)
    .where(Entity.entity_role == "consolidation", Entity.source != "linked")
    .order_by(Entity.created_at, Entity.id)
    .limit(1)
  ).scalar()


def own_entity_ids(session: Session, entity_id: str | None = None) -> list[str]:
  """The group's own entities — never a linked counterparty — with the named
  one (default the group parent) first, then the rest by creation."""
  first = resolve_entity_id(session, entity_id)
  rows = session.execute(
    text(
      "SELECT id FROM entities WHERE source <> 'linked' AND id <> :first "
      "ORDER BY created_at ASC, id ASC"
    ),
    {"first": first},
  ).scalars()
  return [first, *(str(row) for row in rows)]


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


@dataclass(frozen=True)
class GroupMember:
  """An entity a group read sums, with the months it is summed for."""

  id: str
  role: str
  joined_on: date | None
  left_on: date | None

  def is_member_for(self, start: date, end: date) -> bool:
    """Whether the period ``start`` to ``end`` falls inside the membership
    interval. Membership is by month: any day of the month joined or left
    counts, so a sub acquired on the 14th is summed for that month. The
    consolidation entity is the group's own, never a member, so it is read
    for every period whatever its row says."""
    if self.role == "consolidation":
      return True
    return (self.joined_on is None or self.joined_on <= end) and (
      self.left_on is None or self.left_on >= start
    )


def group_members(
  session: Session, parent_id: str, *, consolidated: bool
) -> list[GroupMember]:
  """The entities a group read sums, the parent first: every operating
  entity consolidated in full, by creation, then — when ``consolidated`` —
  the consolidation entity, whose entries are the worksheet's adjustments
  column. An equity-method or cost member is never summed; its effect
  reaches the group through that column. Each carries its membership
  interval, and the read decides per period."""
  rows = session.execute(
    text(
      "SELECT id, entity_role, joined_on, left_on FROM entities "
      "WHERE source <> 'linked' AND id <> :parent "
      "  AND ((entity_role = 'operating' AND consolidation_method = 'full') "
      "       OR (:consolidated AND entity_role = 'consolidation')) "
      "ORDER BY (entity_role = 'consolidation'), created_at ASC, id ASC"
    ),
    {"parent": parent_id, "consolidated": consolidated},
  ).all()
  return [
    # The parent is a member of its own group for every period, whatever its
    # row says; only the others carry an interval the read consults.
    GroupMember(parent_id, "operating", None, None),
    *(
      GroupMember(str(row.id), str(row.entity_role), row.joined_on, row.left_on)
      for row in rows
    ),
  ]


def acquired_eve(session: Session, member: GroupMember) -> date | None:
  """The eve of the month a member joined when its books open there with a
  recorded balance sheet: a member acquired, whose balances a group read
  sees arrive. None for one formed in the group, which opens on nothing.

  An opening sits on the eve of the books' first fiscal period, so this
  holds when the member's calendar starts in the month it joined; a
  calendar starting later opens after the join, and its cash reads as
  beginning cash."""
  # Imported here: opening_balances resolves its entity through this module.
  from robosystems.operations.roboledger.commands.opening_balances import OPENING

  if member.role == "consolidation" or member.joined_on is None:
    return None
  eve = member.joined_on.replace(day=1) - timedelta(days=1)
  opened = session.execute(
    text(
      "SELECT 1 FROM entries WHERE entity_id = :id AND type = :opening "
      "AND status <> :superseded AND posting_date = :eve LIMIT 1"
    ),
    {
      "id": member.id,
      "opening": OPENING,
      "superseded": SUPERSEDED_ENTRY_STATUS,
      "eve": eve,
    },
  ).first()
  return eve if opened else None


def membership_window(
  member: GroupMember, start: date, end: date
) -> tuple[date, date] | None:
  """The part of ``start`` to ``end`` the member belongs to, by whole months:
  from the first day of the month it joined to the last of the month it
  left. None when it belongs to none of it. The consolidation entity is the
  group's own, read for all of it."""
  if member.role == "consolidation":
    return start, end
  first = start
  if member.joined_on is not None:
    first = max(start, member.joined_on.replace(day=1))
  last = end
  if member.left_on is not None:
    left = member.left_on
    last = min(end, left.replace(day=calendar.monthrange(left.year, left.month)[1]))
  return (first, last) if first <= last else None
