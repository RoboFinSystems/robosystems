"""Envelope rule projection — ``rules`` table → ``TaxonomyBlockRule`` list.

Shared across CoA / reporting_extension / custom_ontology /
reporting_standard / schedule_container handlers'
``build_envelope`` implementations.

One SELECT per taxonomy projection — pulls every rule that targets
the taxonomy itself OR any of its in-taxonomy structures OR any of
its in-taxonomy elements. `target_kind='association'` rules would
also belong here once that pattern lands; for now we don't surface
association-level rules on Taxonomy Blocks (the envelope model
doesn't carry association objects keyed by id, just by from/to
qnames).
"""

from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from robosystems.models.api.taxonomy_block import TaxonomyBlockRule
from robosystems.models.extensions import Rule


def project_rules(
  session: Session,
  taxonomy_id: str,
  element_ids: Iterable[str],
  structure_ids: Iterable[str],
  qname_by_element_id: dict[str, str],
) -> list[TaxonomyBlockRule]:
  """Fetch and project rules scoped to this taxonomy.

  ``qname_by_element_id`` is consumed for projecting
  ``target_kind='element'`` rules as an element qname (the
  human-readable identifier) rather than the element's id.
  """
  element_id_list = list(element_ids)
  structure_id_list = list(structure_ids)

  conditions = [Rule.target_taxonomy_id == taxonomy_id]
  if structure_id_list:
    conditions.append(Rule.target_structure_id.in_(structure_id_list))
  if element_id_list:
    conditions.append(Rule.target_element_id.in_(element_id_list))

  rows = (
    session.execute(select(Rule).where(or_(*conditions)).order_by(Rule.created_at))
    .scalars()
    .all()
  )

  return [_rule_to_lite(rule, qname_by_element_id) for rule in rows]


def _rule_to_lite(rule: Rule, qname_by_element_id: dict[str, str]) -> TaxonomyBlockRule:
  """Project one Rule row onto :class:`TaxonomyBlockRule`."""
  target_ref: str | None = None
  if rule.target_kind == "structure" and rule.target_structure_id is not None:
    target_ref = str(rule.target_structure_id)
  elif rule.target_kind == "element" and rule.target_element_id is not None:
    element_id = str(rule.target_element_id)
    target_ref = qname_by_element_id.get(element_id) or element_id
  elif rule.target_kind == "association" and rule.target_association_id is not None:
    target_ref = str(rule.target_association_id)
  elif rule.target_kind == "taxonomy" and rule.target_taxonomy_id is not None:
    target_ref = str(rule.target_taxonomy_id)

  metadata = rule.metadata_ or {}
  name = metadata.get("rule_name") or f"rule-{rule.id}"

  return TaxonomyBlockRule(
    id=rule.id,
    name=name,
    rule_category=rule.rule_category,
    rule_pattern=rule.rule_pattern,
    rule_expression=rule.rule_expression,
    severity=rule.rule_severity,
    origin=rule.rule_origin,
    target_kind=rule.target_kind,
    target_ref=target_ref,
  )


__all__ = ["project_rules"]
