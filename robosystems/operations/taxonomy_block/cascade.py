"""Delete dependency preflight + cascade helpers.

Shared across the three tenant taxonomy handlers (CoA,
reporting_extension, custom_ontology). `preflight_delete` counts every
row that blocks or triggers a cascade; `cascade_delete_taxonomy`
removes the taxonomy and its atoms in dependency order.

Facts and line_items are not tenant-ontology atoms — they're business
data that references elements. Facts can be cascade-deleted
(``cascade_facts=True``); line_items never cascade via this path
(journal entries are the source of truth — clear them through the
ledger surface first).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from robosystems.models.extensions import (
  Association,
  Element,
  ElementLabel,
  ElementReference,
  ElementTrait,
  Rule,
  Structure,
  Taxonomy,
)
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.models.extensions.roboledger.line_item import LineItem


@dataclass(frozen=True)
class DeletePreflight:
  """Counts of rows that either block or will cascade on delete."""

  fact_count: int = 0
  line_item_count: int = 0
  cross_taxonomy_mapping_count: int = 0
  cross_taxonomy_mapping_ids: list[str] = field(default_factory=list)


def preflight_delete(session: Session, taxonomy_id: str) -> DeletePreflight:
  """Query dependency counts without touching any rows."""
  element_ids = [
    row[0]
    for row in session.execute(
      select(Element.id).where(Element.taxonomy_id == taxonomy_id)
    ).all()
  ]

  if not element_ids:
    return DeletePreflight()

  fact_count = (
    session.execute(select(Fact.id).where(Fact.element_id.in_(element_ids)))
    .scalars()
    .all()
  )

  line_item_count = (
    session.execute(select(LineItem.id).where(LineItem.element_id.in_(element_ids)))
    .scalars()
    .all()
  )

  all_cross_mapping_ids = (
    session.execute(
      select(Association.id)
      .join(Structure, Association.structure_id == Structure.id)
      .where(
        Structure.taxonomy_id != taxonomy_id,
        (
          Association.from_element_id.in_(element_ids)
          | Association.to_element_id.in_(element_ids)
        ),
      )
    )
    .scalars()
    .all()
  )

  return DeletePreflight(
    fact_count=len(fact_count),
    line_item_count=len(line_item_count),
    cross_taxonomy_mapping_count=len(all_cross_mapping_ids),
    cross_taxonomy_mapping_ids=list(all_cross_mapping_ids[:10]),
  )


def cascade_delete_taxonomy(
  session: Session, taxonomy_id: str, *, cascade_facts: bool
) -> int:
  """Delete the taxonomy + every child row, in dependency-safe order.

  Returns the count of facts deleted (0 when ``cascade_facts=False``).
  Callers must have already run :func:`preflight_delete` and rejected
  any blocking dependencies (line_items, cross-taxonomy mappings).
  """
  element_ids = [
    row[0]
    for row in session.execute(
      select(Element.id).where(Element.taxonomy_id == taxonomy_id)
    ).all()
  ]
  structure_ids = [
    row[0]
    for row in session.execute(
      select(Structure.id).where(Structure.taxonomy_id == taxonomy_id)
    ).all()
  ]

  facts_deleted = 0
  if cascade_facts and element_ids:
    result = session.execute(delete(Fact).where(Fact.element_id.in_(element_ids)))
    facts_deleted = result.rowcount or 0

  # Rules — those hosted by this taxonomy or targeting any of its atoms.
  rule_predicates = [
    Rule.taxonomy_id == taxonomy_id,
    Rule.target_taxonomy_id == taxonomy_id,
  ]
  if structure_ids:
    rule_predicates.append(Rule.target_structure_id.in_(structure_ids))
  if element_ids:
    rule_predicates.append(Rule.target_element_id.in_(element_ids))
  rule_clause = rule_predicates[0]
  for pred in rule_predicates[1:]:
    rule_clause = rule_clause | pred
  session.execute(delete(Rule).where(rule_clause))

  # Associations inside this taxonomy's structures.
  if structure_ids:
    session.execute(
      delete(Association).where(Association.structure_id.in_(structure_ids))
    )

  # Element side-tables.
  if element_ids:
    session.execute(
      delete(ElementTrait).where(ElementTrait.element_id.in_(element_ids))
    )
    session.execute(
      delete(ElementLabel).where(ElementLabel.element_id.in_(element_ids))
    )
    session.execute(
      delete(ElementReference).where(ElementReference.element_id.in_(element_ids))
    )

  # Elements.
  if element_ids:
    session.execute(delete(Element).where(Element.id.in_(element_ids)))

  # Structures.
  if structure_ids:
    session.execute(delete(Structure).where(Structure.id.in_(structure_ids)))

  # Taxonomy row itself.
  session.execute(delete(Taxonomy).where(Taxonomy.id == taxonomy_id))
  session.flush()

  return facts_deleted


__all__ = [
  "DeletePreflight",
  "cascade_delete_taxonomy",
  "preflight_delete",
]
