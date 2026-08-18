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
  AssociationClassification,
  Element,
  ElementLabel,
  ElementReference,
  ElementTrait,
  EntityTaxonomy,
  FactSet,
  Rule,
  Structure,
  Taxonomy,
  VerificationResult,
)
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.operations.taxonomy_block.immutability import assert_facts_deletable


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
  structure_ids = [
    row[0]
    for row in session.execute(
      select(Structure.id).where(Structure.taxonomy_id == taxonomy_id)
    ).all()
  ]

  if not element_ids and not structure_ids:
    return DeletePreflight()

  # Facts die two ways: by referencing this taxonomy's elements, and by
  # membership in a FactSet attached to one of its structures (report
  # snapshots and standing text-block/metric sets cascade with their set,
  # including snapshot copies of other taxonomies' concepts).
  fact_predicates = []
  if element_ids:
    fact_predicates.append(Fact.element_id.in_(element_ids))
  if structure_ids:
    fact_predicates.append(
      Fact.fact_set_id.in_(
        select(FactSet.id).where(FactSet.structure_id.in_(structure_ids))
      )
    )
  fact_clause = fact_predicates[0]
  for pred in fact_predicates[1:]:
    fact_clause = fact_clause | pred
  fact_count = session.execute(select(Fact.id).where(fact_clause)).scalars().all()

  if not element_ids:
    return DeletePreflight(fact_count=len(fact_count))

  line_item_count = (
    session.execute(
      select(LineItem.id).where(
        LineItem.element_id.in_(element_ids) | LineItem.flow_element_id.in_(element_ids)
      )
    )
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

  fact_set_ids = []
  if structure_ids:
    fact_set_ids = (
      session.execute(select(FactSet.id).where(FactSet.structure_id.in_(structure_ids)))
      .scalars()
      .all()
    )

  # Curation never destroys a filed report's snapshot or a closed month's
  # canonical statement sets — no flag reaches them. Checked before the
  # first delete so a refusal leaves nothing half-done.
  assert_facts_deletable(session, structure_ids=structure_ids, element_ids=element_ids)

  facts_deleted = 0
  if cascade_facts:
    if element_ids:
      result = session.execute(delete(Fact).where(Fact.element_id.in_(element_ids)))
      facts_deleted += result.rowcount or 0
    if fact_set_ids:
      # Snapshot copies of other taxonomies' concepts inside this taxonomy's
      # FactSets — the element-keyed delete above can't see them.
      result = session.execute(delete(Fact).where(Fact.fact_set_id.in_(fact_set_ids)))
      facts_deleted += result.rowcount or 0

  association_ids = []
  if structure_ids:
    association_ids = (
      session.execute(
        select(Association.id).where(Association.structure_id.in_(structure_ids))
      )
      .scalars()
      .all()
    )

  # Rules — those hosted by this taxonomy or targeting any of its atoms
  # (including rules hosted elsewhere that target this taxonomy's
  # associations, whose FK would otherwise block the association delete).
  rule_predicates = [
    Rule.taxonomy_id == taxonomy_id,
    Rule.target_taxonomy_id == taxonomy_id,
  ]
  if structure_ids:
    rule_predicates.append(Rule.target_structure_id.in_(structure_ids))
  if element_ids:
    rule_predicates.append(Rule.target_element_id.in_(element_ids))
  if association_ids:
    rule_predicates.append(Rule.target_association_id.in_(association_ids))
  rule_clause = rule_predicates[0]
  for pred in rule_predicates[1:]:
    rule_clause = rule_clause | pred
  rule_ids = session.execute(select(Rule.id).where(rule_clause)).scalars().all()

  # Verification results FK the rules and structures with no ON DELETE —
  # they're derived evaluation records and always cascade, or the Rule and
  # Structure deletes below die on the constraint.
  vr_predicates = []
  if rule_ids:
    vr_predicates.append(VerificationResult.rule_id.in_(rule_ids))
  if structure_ids:
    vr_predicates.append(VerificationResult.structure_id.in_(structure_ids))
  if vr_predicates:
    vr_clause = vr_predicates[0]
    for pred in vr_predicates[1:]:
      vr_clause = vr_clause | pred
    session.execute(delete(VerificationResult).where(vr_clause))

  if rule_ids:
    session.execute(delete(Rule).where(Rule.id.in_(rule_ids)))

  # FactSets attached to this taxonomy's structures — standing disclosure/
  # metric sets and report snapshots. Any facts still inside them cascade at
  # the DB level (facts.fact_set_id is ON DELETE CASCADE); when
  # cascade_facts=False the preflight already guaranteed there are none.
  if fact_set_ids:
    session.execute(delete(FactSet).where(FactSet.id.in_(fact_set_ids)))

  # Associations inside this taxonomy's structures — classification rows FK
  # the associations with no ON DELETE, so they go first.
  if association_ids:
    session.execute(
      delete(AssociationClassification).where(
        AssociationClassification.association_id.in_(association_ids)
      )
    )
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

  # The entity's adoption of this taxonomy (a chart of accounts is linked to
  # the parent entity at creation; `entity_taxonomies.taxonomy_id` is
  # RESTRICT) — without this the taxonomy DELETE dies on the constraint and
  # a CoA can never be deleted through the API.
  session.execute(
    delete(EntityTaxonomy).where(EntityTaxonomy.taxonomy_id == taxonomy_id)
  )

  # Taxonomy row itself.
  session.execute(delete(Taxonomy).where(Taxonomy.id == taxonomy_id))
  session.flush()

  return facts_deleted


__all__ = [
  "DeletePreflight",
  "cascade_delete_taxonomy",
  "preflight_delete",
]
