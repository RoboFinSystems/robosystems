"""Tenant rule persistence — envelope ``rules[]`` → ``rules`` table rows.

Shared across CoA / reporting_extension / custom_ontology handlers.
Callers pass the envelope's symbol tables (``elements_by_qname``,
``structures_by_name``) after those atoms have flushed, so FK
resolution is immediate.

The validator has already run by this point — all target refs are
known to resolve. This helper is the pure mapping step; it raises
:class:`ValueError` defensively if a ref somehow didn't make it
through the validator (kept as a safety net, not as a primary error
path).
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from robosystems.models.api.taxonomy_block import TaxonomyBlockRuleRequest
from robosystems.models.extensions import Element, Rule, Structure, Taxonomy


def persist_tenant_rules(
  session: Session,
  taxonomy: Taxonomy,
  rule_requests: list[TaxonomyBlockRuleRequest],
  elements_by_qname: dict[str, Element],
  structures_by_name: dict[str, Structure],
  created_by: str,
) -> list[Rule]:
  """Insert one Rule row per request and return the new rows."""
  inserted: list[Rule] = []
  for req in rule_requests:
    target_kind: str | None = None
    target_structure_id: str | None = None
    target_element_id: str | None = None
    target_taxonomy_id: str | None = None

    if req.target_structure_ref is not None:
      structure = structures_by_name.get(req.target_structure_ref)
      if structure is None:
        raise ValueError(
          f"rule {req.name!r} target_structure_ref "
          f"{req.target_structure_ref!r} did not resolve in the envelope."
        )
      target_kind = "structure"
      target_structure_id = str(structure.id)
    elif req.target_element_qname is not None:
      element = elements_by_qname.get(req.target_element_qname)
      if element is None:
        raise ValueError(
          f"rule {req.name!r} target_element_qname "
          f"{req.target_element_qname!r} did not resolve in the envelope."
        )
      target_kind = "element"
      target_element_id = str(element.id)
    elif req.target_taxonomy_self:
      target_kind = "taxonomy"
      target_taxonomy_id = str(taxonomy.id)

    # The Rule table doesn't have dedicated name/description columns —
    # stash them in metadata_ so the envelope projection can surface them.
    rule_metadata = dict(req.metadata)
    rule_metadata.setdefault("rule_name", req.name)
    if req.description is not None:
      rule_metadata.setdefault("rule_description", req.description)

    rule = Rule(
      taxonomy_id=taxonomy.id,
      rule_category=req.rule_category,
      rule_pattern=req.rule_pattern,
      rule_expression=req.expression,
      rule_message=req.message,
      rule_severity=req.severity,
      rule_origin="native",
      target_kind=target_kind,
      target_structure_id=target_structure_id,
      target_element_id=target_element_id,
      target_taxonomy_id=target_taxonomy_id,
      rule_variables=list(req.variables),
      metadata_=rule_metadata,
      created_by=created_by,
    )
    session.add(rule)
    inserted.append(rule)

  return inserted


__all__ = ["persist_tenant_rules"]
