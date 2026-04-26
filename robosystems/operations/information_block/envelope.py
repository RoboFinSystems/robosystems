"""Cross-type assembly helpers for building Information Block envelopes.

Block-type handlers (``schedule.py``, ``statement.py``, ``metric.py``)
each own the logic that's specific to their shape. The helpers here are
the generic atom -> Lite conversions shared by every handler — one
place to maintain the ORM -> wire-shape mapping so handlers don't
diverge.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from robosystems.models.api.information_block import (
  ClassificationLite,
  ConnectionLite,
  ElementLite,
  FactLite,
  FactSetLite,
  RuleLite,
  RuleTargetLite,
  RuleVariableLite,
  VerificationResultLite,
)
from robosystems.models.extensions import (
  Association,
  AssociationClassification,
  Classification,
  Element,
  Rule,
  Structure,
  VerificationResult,
)
from robosystems.models.extensions.roboledger import Fact, FactSet


def element_to_lite(element: Element) -> ElementLite:
  """Project an :class:`Element` ORM row onto :class:`ElementLite`."""
  return ElementLite(
    id=element.id,
    qname=element.qname,
    name=element.name,
    code=element.code,
    element_type=element.element_type,
    is_abstract=element.is_abstract,
    is_monetary=element.is_monetary,
    balance_type=element.balance_type,
    period_type=element.period_type,
  )


def association_to_connection(
  association: Association,
  classifications: list[ClassificationLite] | None = None,
) -> ConnectionLite:
  """Project an :class:`Association` ORM row onto :class:`ConnectionLite`.

  ``classifications`` is an optional pre-resolved list; when omitted the
  connection surfaces with an empty classification list. Envelope builders
  that want the classifications populated call
  :func:`load_classifications_for_associations` once per envelope and
  hand the lookup into this projector — O(1) junction lookup per
  association, no N+1.
  """
  return ConnectionLite(
    id=association.id,
    from_element_id=association.from_element_id,
    to_element_id=association.to_element_id,
    association_type=association.association_type,
    arcrole=association.arcrole,
    order_value=association.order_value,
    weight=association.weight,
    classifications=classifications or [],
  )


def load_classifications_for_associations(
  session: Session, association_ids: list[str]
) -> dict[str, list[ClassificationLite]]:
  """Fetch association classifications keyed by association id.

  Single query over the `association_classifications` junction joined to
  the `classifications` vocabulary; grouped in-memory. Returns an empty
  dict when ``association_ids`` is empty so callers can unconditionally
  call this without a length check.
  """
  if not association_ids:
    return {}

  rows = session.execute(
    select(AssociationClassification, Classification)
    .join(
      Classification,
      Classification.id == AssociationClassification.classification_id,
    )
    .where(AssociationClassification.association_id.in_(association_ids))
    .order_by(
      AssociationClassification.association_id,
      AssociationClassification.is_primary.desc(),
      Classification.category,
      Classification.identifier,
    )
  ).all()

  grouped: dict[str, list[ClassificationLite]] = {}
  for ac, cls in rows:
    grouped.setdefault(ac.association_id, []).append(
      ClassificationLite(
        id=cls.id,
        category=cls.category,
        identifier=cls.identifier,
        is_primary=bool(ac.is_primary),
        confidence=ac.confidence,
        source=ac.source,
      )
    )
  return grouped


def fact_to_lite(fact: Fact) -> FactLite:
  """Project a :class:`Fact` ORM row onto :class:`FactLite`."""
  return FactLite(
    id=fact.id,
    element_id=fact.element_id,
    value=fact.value,
    period_start=fact.period_start,
    period_end=fact.period_end,
    period_type=fact.period_type,
    unit=fact.unit,
    fact_scope=fact.fact_scope,
    fact_set_id=fact.fact_set_id,
  )


def fact_set_to_lite(fact_set: FactSet) -> FactSetLite:
  """Project a :class:`FactSet` ORM row onto :class:`FactSetLite`."""
  return FactSetLite(
    id=fact_set.id,
    structure_id=fact_set.structure_id,
    period_start=fact_set.period_start,
    period_end=fact_set.period_end,
    factset_type=fact_set.factset_type,
    entity_id=fact_set.entity_id,
    report_id=fact_set.report_id,
  )


def verification_result_to_lite(row: VerificationResult) -> VerificationResultLite:
  """Project a :class:`VerificationResult` ORM row onto
  :class:`VerificationResultLite`."""
  return VerificationResultLite(
    id=row.id,
    rule_id=row.rule_id,
    structure_id=row.structure_id,
    fact_set_id=row.fact_set_id,
    status=row.status,
    message=row.message,
    period_start=row.period_start,
    period_end=row.period_end,
    evaluated_at=row.evaluated_at,
  )


def load_verification_results_for_structure(
  session: Session, structure_id: str
) -> list[VerificationResultLite]:
  """Fetch verification results scoped to a Structure.

  Returns the full history ordered by ``evaluated_at`` descending so
  the envelope exposes the most recent run first. Empty list when no
  rows — the common case until the rule engine writes results.
  """
  rows = (
    session.execute(
      select(VerificationResult)
      .where(VerificationResult.structure_id == structure_id)
      .order_by(VerificationResult.evaluated_at.desc())
    )
    .scalars()
    .all()
  )
  return [verification_result_to_lite(r) for r in rows]


def load_latest_fact_set_for_structure(
  session: Session, structure_id: str
) -> FactSetLite | None:
  """Fetch the most recent FactSet for a Structure, if any.

  Ordered by ``period_end`` descending then ``created_at`` descending so
  a Structure with multiple FactSets (e.g. a Schedule accumulating
  period runs) surfaces the latest slice. Returns ``None`` when no
  FactSet row exists — typically library-seeded Structures whose
  tenant has not yet generated facts.
  """
  row = session.execute(
    select(FactSet)
    .where(FactSet.structure_id == structure_id)
    .order_by(FactSet.period_end.desc(), FactSet.created_at.desc())
    .limit(1)
  ).scalar()
  return fact_set_to_lite(row) if row is not None else None


def load_fact_set_by_id_for_structure(
  session: Session, structure_id: str, fact_set_id: str
) -> FactSetLite | None:
  """Fetch a specific FactSet pinned to a Structure.

  Used by the Report Block rehydration path to load the exact FactSet
  snapshot that a ``ReportBlockItem`` pins, instead of the latest one.
  Returns ``None`` when the FactSet doesn't exist or doesn't belong to
  the named Structure — callers surface that as a clean miss to the
  envelope reader.
  """
  row = session.execute(
    select(FactSet).where(
      FactSet.id == fact_set_id, FactSet.structure_id == structure_id
    )
  ).scalar()
  return fact_set_to_lite(row) if row is not None else None


def rule_to_lite(rule: Rule) -> RuleLite:
  """Project a :class:`Rule` ORM row onto :class:`RuleLite`.

  Unpacks the polymorphic target columns into a single typed
  :class:`RuleTargetLite` and the JSONB variable blob into typed
  :class:`RuleVariableLite` entries.
  """
  target: RuleTargetLite | None = None
  if rule.target_kind == "structure" and rule.target_structure_id is not None:
    target = RuleTargetLite(
      target_kind="structure", target_ref_id=rule.target_structure_id
    )
  elif rule.target_kind == "element" and rule.target_element_id is not None:
    target = RuleTargetLite(target_kind="element", target_ref_id=rule.target_element_id)
  elif rule.target_kind == "association" and rule.target_association_id is not None:
    target = RuleTargetLite(
      target_kind="association", target_ref_id=rule.target_association_id
    )
  elif rule.target_kind == "taxonomy" and rule.target_taxonomy_id is not None:
    target = RuleTargetLite(
      target_kind="taxonomy", target_ref_id=rule.target_taxonomy_id
    )

  raw_vars = rule.rule_variables or []
  variables = [
    RuleVariableLite(
      variable_name=v.get("variable_name", ""),
      variable_qname=v.get("variable_qname", ""),
    )
    for v in raw_vars
  ]

  return RuleLite(
    id=rule.id,
    rule_category=rule.rule_category,
    rule_pattern=rule.rule_pattern,
    rule_expression=rule.rule_expression,
    rule_target=target,
    rule_variables=variables,
    rule_message=rule.rule_message,
    rule_severity=rule.rule_severity,
    rule_origin=rule.rule_origin,
  )


def load_rules_for_structure(
  session: Session,
  structure_id: str,
  element_ids: list[str] | None = None,
  association_ids: list[str] | None = None,
) -> list[RuleLite]:
  """Fetch every rule scoped to a Structure (plus its inner atoms).

  Pulls three buckets in one query:

  * ``target_structure_id = structure_id`` — rules whose target *is* the
    Structure (the common case for library-seeded Seattle Method rules).
  * ``target_element_id IN element_ids`` — element-scoped rules for
    elements belonging to the Structure.
  * ``target_association_id IN association_ids`` — association-scoped
    rules.

  Results are ordered by ``rule_category`` then ``id`` so the envelope is
  deterministic across calls; UIs that group by category get the order
  for free.
  """
  conditions = [Rule.target_structure_id == structure_id]
  if element_ids:
    conditions.append(Rule.target_element_id.in_(element_ids))
  if association_ids:
    conditions.append(Rule.target_association_id.in_(association_ids))

  rules = (
    session.execute(
      select(Rule).where(or_(*conditions)).order_by(Rule.rule_category, Rule.id)
    )
    .scalars()
    .all()
  )
  return [rule_to_lite(r) for r in rules]


@dataclass(frozen=True)
class BaseEnvelopeAtoms:
  """Shared atoms loaded by every block-type ``build_envelope``.

  The block-specific handlers add mechanics + the per-block fact filter
  on top; everything else (Structure, taxonomy_name, associations,
  elements, rules, classifications, fact_set, verification_results) is
  identical across the registered block types.
  """

  structure: Structure
  taxonomy_name: str | None
  associations: list[Association]
  elements: list[Element]
  element_ids: list[str]
  rules: list[RuleLite]
  classifications_by_assoc: dict[str, list[ClassificationLite]]
  fact_set: FactSetLite | None
  verification_results: list[VerificationResultLite]


def load_base_envelope_atoms(
  session: Session,
  structure_id: str,
  *,
  expected_block_type: str,
  fact_set_id: str | None = None,
) -> BaseEnvelopeAtoms | None:
  """Load every atom shared by Information Block envelope builders.

  Returns ``None`` when the Structure doesn't exist or its
  ``structure_type`` doesn't match ``expected_block_type`` — handlers
  surface that as a clean miss to :func:`get_information_block`. Loading
  order: Structure -> taxonomy name -> associations -> elements -> rules
  -> classifications -> fact_set -> verification_results, matching the
  order each individual handler used to inline.

  When ``fact_set_id`` is provided the atoms are pinned to that specific
  FactSet (used by Report Block rehydration). Returns ``None`` if the
  named FactSet doesn't belong to this Structure — callers treat the
  pin mismatch as a clean miss.
  """
  from robosystems.models.extensions import Taxonomy

  structure = session.get(Structure, structure_id)
  if structure is None or structure.structure_type != expected_block_type:
    return None

  # Validate the FactSet pin (when provided) before doing the heavy
  # association / element / rule loads — a mismatched pin is a clean
  # miss and shouldn't pay for atoms that won't be returned.
  if fact_set_id is not None:
    fact_set = load_fact_set_by_id_for_structure(session, structure_id, fact_set_id)
    if fact_set is None:
      return None
  else:
    fact_set = load_latest_fact_set_for_structure(session, structure_id)

  taxonomy_name = session.execute(
    select(Taxonomy.name).where(Taxonomy.id == structure.taxonomy_id)
  ).scalar()

  associations = list(
    session.execute(select(Association).where(Association.structure_id == structure_id))
    .scalars()
    .all()
  )

  element_id_set = {a.from_element_id for a in associations} | {
    a.to_element_id for a in associations
  }
  element_ids = [e for e in element_id_set if e is not None]
  if element_ids:
    elements = list(
      session.execute(select(Element).where(Element.id.in_(element_ids)))
      .scalars()
      .all()
    )
  else:
    elements = []

  rules = load_rules_for_structure(
    session,
    structure_id,
    element_ids=element_ids,
    association_ids=[a.id for a in associations],
  )

  classifications_by_assoc = load_classifications_for_associations(
    session, [a.id for a in associations]
  )

  verification_results = load_verification_results_for_structure(session, structure_id)

  return BaseEnvelopeAtoms(
    structure=structure,
    taxonomy_name=taxonomy_name,
    associations=associations,
    elements=elements,
    element_ids=element_ids,
    rules=rules,
    classifications_by_assoc=classifications_by_assoc,
    fact_set=fact_set,
    verification_results=verification_results,
  )


__all__ = [
  "BaseEnvelopeAtoms",
  "association_to_connection",
  "element_to_lite",
  "fact_set_to_lite",
  "fact_to_lite",
  "load_base_envelope_atoms",
  "load_classifications_for_associations",
  "load_fact_set_by_id_for_structure",
  "load_latest_fact_set_for_structure",
  "load_rules_for_structure",
  "load_verification_results_for_structure",
  "rule_to_lite",
  "verification_result_to_lite",
]
