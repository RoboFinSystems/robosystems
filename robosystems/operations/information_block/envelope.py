"""Cross-type assembly helpers for building Information Block envelopes.

Block-type handlers (``schedule.py`` today, more in future phases) each
own the logic that's specific to their shape. The helpers here are the
generic atom → Lite conversions shared by every handler — one place to
maintain the ORM → wire-shape mapping so Phase b's Statement handler
doesn't diverge from Phase a's Schedule handler.
"""

from __future__ import annotations

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from robosystems.models.api.information_block import (
  ConnectionLite,
  ElementLite,
  FactLite,
  RuleLite,
  RuleTargetLite,
  RuleVariableLite,
)
from robosystems.models.extensions import Association, Element, Rule
from robosystems.models.extensions.roboledger import Fact


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


def association_to_connection(association: Association) -> ConnectionLite:
  """Project an :class:`Association` ORM row onto :class:`ConnectionLite`."""
  return ConnectionLite(
    id=association.id,
    from_element_id=association.from_element_id,
    to_element_id=association.to_element_id,
    association_type=association.association_type,
    arcrole=association.arcrole,
    order_value=association.order_value,
    weight=association.weight,
  )


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


__all__ = [
  "association_to_connection",
  "element_to_lite",
  "fact_to_lite",
  "load_rules_for_structure",
  "rule_to_lite",
]
