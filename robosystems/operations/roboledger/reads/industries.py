"""The industry classification, read from ``public.industries``.

One global vocabulary: no tenant holds a copy, and an entity carries one of its
ids in ``industry``.
"""

from __future__ import annotations

from difflib import get_close_matches

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.entity import IndustryResponse
from robosystems.models.extensions import Industry

__all__ = ["UnknownIndustryError", "list_industries", "resolve_industry"]

_SUGGESTIONS = 5


class UnknownIndustryError(ValueError):
  """The value is not an id in the industry classification."""

  def __init__(self, value: str, suggestions: list[str]) -> None:
    hint = (
      f" Closest: {', '.join(suggestions)}."
      if suggestions
      else " Read the `industries` field for the list."
    )
    super().__init__(f"Industry {value!r} is not in the classification.{hint}")
    self.suggestions = suggestions


def _rows(session: Session) -> list[Industry]:
  return list(session.execute(select(Industry).order_by(Industry.id)).scalars())


def list_industries(session: Session) -> list[IndustryResponse]:
  """Every node, ordered so a parent precedes its children. Retired nodes are
  included with ``replaced_by`` set, so a stored id always has a name."""
  rows = _rows(session)
  by_id = {str(r.id): r for r in rows}

  def path(node_id: str) -> list[str]:
    parts = node_id.split("/")
    ancestors = ["/".join(parts[: i + 1]) for i in range(len(parts))]
    # A carried industry repeats its subsector's name, so it is shown once.
    return [
      str(by_id[a].label) for a in ancestors if a in by_id and not by_id[a].carried
    ]

  return [
    IndustryResponse(
      id=str(r.id),
      parent_id=str(r.parent_id) if r.parent_id else None,
      level=str(r.level),
      label=str(r.label),
      path=path(str(r.id)),
      carried=bool(r.carried),
      replaced_by=str(r.replaced_by) if r.replaced_by else None,
    )
    for r in rows
  ]


def resolve_industry(session: Session, value: str) -> str:
  """The id to store for ``value``: itself, or its successor when it is retired.

  Raises `UnknownIndustryError`, naming the closest ids, for anything else.
  """
  wanted = value.strip()
  node = session.get(Industry, wanted)
  hops = 0
  while node is not None and node.replaced_by and hops < 8:
    node = session.get(Industry, str(node.replaced_by))
    hops += 1
  if node is not None and not node.replaced_by:
    return str(node.id)

  active = [r for r in _rows(session) if not r.replaced_by]
  needle = wanted.lower()
  # A label typed as the value ("Biotechnology") names its nodes outright.
  by_label = [str(r.id) for r in active if str(r.label).lower() == needle]
  ids = [str(r.id) for r in active]
  slug = needle.replace(" & ", "-and-").replace(" ", "-")
  by_tail = [i for i in ids if i.rsplit("/", 1)[-1] == slug]
  close = get_close_matches(slug, ids, n=_SUGGESTIONS, cutoff=0.6)
  suggestions = list(dict.fromkeys([*by_label, *by_tail, *close]))[:_SUGGESTIONS]
  raise UnknownIndustryError(value, suggestions)
