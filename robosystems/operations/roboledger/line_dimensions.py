"""Management dimensions on ledger lines: which members a line may carry,
writing and reading them, and the predicate a sliced read filters lines by.

A line carries at most one member per axis. An untagged line is in the axis's
Unallocated member, so for any axis the members plus Unallocated sum to the
undimensioned total; a sliced read only ever narrows the lines it sums.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from sqlalchemy import delete, insert, select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.dimensions import LineDimension
from robosystems.models.extensions.dimension import Dimension
from robosystems.models.extensions.roboledger.dimension_junctions import (
  line_item_dimensions,
)

# Axes that tag facts, never lines: a forecast's scenario.
RESERVED_DIMENSION_TYPES = frozenset({"scenario"})


class DimensionNotFoundError(LookupError):
  def __init__(self, dimension_id: str) -> None:
    super().__init__(f"Dimension not found: {dimension_id}")
    self.dimension_id = dimension_id


def resolve_members(
  session: Session,
  dimension_ids: Iterable[str],
  *,
  kept: Iterable[str] = (),
) -> dict[str, Dimension]:
  """The members named, by id. Refuses an unknown id, a reserved axis, and a
  retired member unless it is in ``kept`` (already on the lines being
  rewritten, which may keep it)."""
  wanted = set(dimension_ids)
  if not wanted:
    return {}
  found = {
    str(d.id): d
    for d in session.execute(select(Dimension).where(Dimension.id.in_(wanted)))
    .scalars()
    .all()
  }
  unknown = sorted(wanted - found.keys())
  if unknown:
    raise ValueError(f"No dimension member {', '.join(unknown)}.")
  reserved = sorted(
    i for i, d in found.items() if d.dimension_type in RESERVED_DIMENSION_TYPES
  )
  if reserved:
    raise ValueError(
      f"{', '.join(reserved)} tag forecast facts, not ledger lines "
      f"(axis {', '.join(sorted(RESERVED_DIMENSION_TYPES))})."
    )
  keep = set(kept)
  retired = sorted(i for i, d in found.items() if not d.is_active and i not in keep)
  if retired:
    raise ValueError(
      f"Dimension member {', '.join(retired)} is retired; it stays on the lines "
      "that carry it and goes on no new ones."
    )
  return found


def check_one_per_axis(
  label: str, dimension_ids: Iterable[str], members: dict[str, Dimension]
) -> list[str]:
  """The line's members, deduplicated in order; refuses two on one axis."""
  ids = list(dict.fromkeys(dimension_ids))
  axes: dict[str, str] = {}
  for dimension_id in ids:
    axis = str(members[dimension_id].dimension_type)
    if axis in axes:
      raise ValueError(
        f"{label} carries two {axis} members ({axes[axis]}, {dimension_id}); a "
        "line takes one per axis, so split it into two lines."
      )
    axes[axis] = dimension_id
  return ids


def validate_line_dimensions(
  session: Session,
  lines: list[list[str]],
  *,
  kept: list[list[str]] | None = None,
) -> list[list[str]]:
  """Each line's members, checked: every id a live member of an open axis,
  one per axis. ``kept[n]`` are the members line ``n`` already carries, which
  it may keep after they retire; a retired member goes on no other line."""
  held = kept or [[] for _ in lines]
  members = resolve_members(
    session,
    (i for ids in lines for i in ids),
    kept={i for ids in held for i in ids},
  )
  for n, ids in enumerate(lines):
    own = set(held[n]) if n < len(held) else set()
    retired = sorted(i for i in ids if not members[i].is_active and i not in own)
    if retired:
      raise ValueError(
        f"Dimension member {', '.join(retired)} is retired; it stays on the "
        "lines that carry it and goes on no new ones."
      )
  return [
    check_one_per_axis(f"Line item {n}", ids, members) for n, ids in enumerate(lines)
  ]


def write_line_dimensions(session: Session, tags: dict[str, list[str]]) -> None:
  """Set each line's members, replacing what it carried. ``tags`` maps line
  id to member ids, already validated."""
  if not tags:
    return
  session.execute(
    delete(line_item_dimensions).where(
      line_item_dimensions.c.line_item_id.in_(list(tags))
    )
  )
  rows = [
    {"line_item_id": line_id, "dimension_id": dimension_id}
    for line_id, ids in tags.items()
    for dimension_id in ids
  ]
  if rows:
    session.execute(insert(line_item_dimensions), rows)
  session.flush()


def add_line_dimensions(session: Session, tags: dict[str, list[str]]) -> None:
  """Add members to lines that carry none on their axes; a member on an axis
  the line already has is left off, so a line's own tag wins."""
  if not tags:
    return
  held = line_dimensions(session, tags)
  wanted = {
    str(m.id): m
    for m in session.execute(
      select(Dimension).where(
        Dimension.id.in_({i for ids in tags.values() for i in ids})
      )
    )
    .scalars()
    .all()
  }
  rows = []
  for line_id, ids in tags.items():
    axes = {d.dimension_type for d in held.get(line_id, [])}
    for dimension_id in ids:
      member = wanted.get(dimension_id)
      if member is None or member.dimension_type in axes:
        continue
      axes.add(member.dimension_type)
      rows.append({"line_item_id": line_id, "dimension_id": dimension_id})
  if rows:
    session.execute(insert(line_item_dimensions), rows)
    session.flush()


def inherit_event_dimensions(session: Session, event_id: str) -> None:
  """Give the lines an event's handler wrote the event's own members, on the
  axes each line does not already carry: an event tagged to a department
  posts to it, and a line tagged otherwise keeps its own."""
  from robosystems.models.extensions.roboledger.dimension_junctions import (
    event_dimensions,
  )
  from robosystems.models.extensions.roboledger.entry import Entry
  from robosystems.models.extensions.roboledger.line_item import LineItem

  members = [
    str(i)
    for (i,) in session.execute(
      select(event_dimensions.c.dimension_id).where(
        event_dimensions.c.event_id == event_id
      )
    ).all()
  ]
  if not members:
    return
  line_ids = [
    str(i)
    for (i,) in session.execute(
      select(LineItem.id)
      .join(Entry, Entry.id == LineItem.entry_id)
      .where(Entry.triggered_by_event_id == event_id)
    ).all()
  ]
  add_line_dimensions(session, dict.fromkeys(line_ids, members))


def line_dimension_ids(
  session: Session, line_ids: Iterable[str]
) -> dict[str, list[str]]:
  """Each line's member ids, by axis then name; a line with none is absent."""
  return {
    line_id: [d.id for d in members]
    for line_id, members in line_dimensions(session, line_ids).items()
  }


def line_dimensions(
  session: Session, line_ids: Iterable[str]
) -> dict[str, list[LineDimension]]:
  """Each line's members, by axis then name; a line with none is absent."""
  ids = [str(i) for i in line_ids]
  if not ids:
    return {}
  rows = session.execute(
    select(
      line_item_dimensions.c.line_item_id,
      Dimension.id,
      Dimension.dimension_type,
      Dimension.name,
    )
    .join(Dimension, Dimension.id == line_item_dimensions.c.dimension_id)
    .where(line_item_dimensions.c.line_item_id.in_(ids))
    .order_by(Dimension.dimension_type, Dimension.name)
  ).all()
  out: dict[str, list[LineDimension]] = {}
  for line_id, dimension_id, dimension_type, name in rows:
    out.setdefault(str(line_id), []).append(
      LineDimension(id=str(dimension_id), dimension_type=dimension_type, name=name)
    )
  return out


@dataclass(frozen=True)
class LineSlice:
  """The lines a sliced read sums: one member's, or with ``member_id`` None
  the axis's Unallocated, the lines no member of it tags."""

  dimension_type: str
  member_id: str | None = None


# Interpolated into a reader's WHERE beside its ``li`` alias; true for every
# line when no slice is bound.
LINE_SLICE_SQL = """(
  CAST(:slice_type AS TEXT) IS NULL
  OR (
    CAST(:slice_member AS TEXT) IS NOT NULL
    AND EXISTS (
      SELECT 1 FROM line_item_dimensions lid
      WHERE lid.line_item_id = li.id AND lid.dimension_id = :slice_member
    )
  )
  OR (
    CAST(:slice_member AS TEXT) IS NULL
    AND NOT EXISTS (
      SELECT 1 FROM line_item_dimensions lid
      JOIN dimensions d ON d.id = lid.dimension_id
      WHERE lid.line_item_id = li.id AND d.dimension_type = :slice_type
    )
  )
)"""


def slice_params(line_slice: LineSlice | None) -> dict[str, str | None]:
  """The bind parameters ``LINE_SLICE_SQL`` reads."""
  if line_slice is None:
    return {"slice_type": None, "slice_member": None}
  return {
    "slice_type": line_slice.dimension_type,
    "slice_member": line_slice.member_id,
  }
