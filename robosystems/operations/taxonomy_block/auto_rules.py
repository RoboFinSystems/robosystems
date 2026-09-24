"""Structural auto-rule emission for Taxonomy Block envelopes.

Each taxonomy block gets ``rule_origin='auto'`` rules encoding the invariants
the create validator checks, so ``evaluate-rules`` can re-check them later.
``rules_to_remove`` can't remove them. All but RollUp are model-structure
checks with no expression or variables.

Rules emitted:

  Per-taxonomy (always):
    UniqueQNameInTaxonomy  — XBRLTechnicalSyntaxRule

  Per-taxonomy (extend mode: parent_taxonomy_id set):
    LibraryOriginImmutability  — ReportLevelModelStructureRule

  Per-taxonomy (chart_of_accounts only):
    LeafHasClassification  — FundamentalAccountingConceptRelation

  Per-structure (all structures in the envelope):
    NoCycles         — ReportLevelModelStructureRule
    NoOrphanArcs     — ReportLevelModelStructureRule
    ParentBeforeChild — ReportLevelModelStructureRule

  Per-structure (``concept_arrangement='roll_up'`` with calculation arcs):
    RollUp (rule_pattern) — FundamentalAccountingConceptRelation, one per
    calc parent.
"""

from __future__ import annotations

import re
from collections import Counter

from sqlalchemy import delete, select
from sqlalchemy.orm import Session, aliased

from robosystems.models.api.taxonomy_block import UpdateTaxonomyBlockRequest
from robosystems.models.extensions import (
  Association,
  Element,
  Rule,
  Structure,
  Taxonomy,
  VerificationResult,
)
from robosystems.operations.information_block.rules.expressions import (
  build_rollup_expression,
)

_AUTO = "auto"
_STRUCTURE_RULES = "ReportLevelModelStructureRule"
_XBRL_RULES = "XBRLTechnicalSyntaxRule"
_FAC_RULES = "FundamentalAccountingConceptRelation"


def _identifier(token: str) -> str:
  """Sanitize a token into a ``$Variable`` identifier that parses as Python."""
  cleaned = re.sub(r"\W", "_", token)
  if not cleaned or cleaned[0].isdigit():
    cleaned = f"_{cleaned}"
  return cleaned


def _resolve_variable_names(
  entries: list[tuple[str | None, str | None, str]],
) -> list[str]:
  """Collision-free variable names for one rule's participants.

  ``entries`` is ``(qname, name, element_id)`` per participant, parent
  first. Uses the local name, the full qname when local names collide
  across namespaces, then a numeric suffix. Collided names would merge in
  the engine's name-keyed bindings.
  """
  sources = [qname or name or element_id for qname, name, element_id in entries]
  base_names = [_identifier(source.split(":")[-1]) for source in sources]
  counts = Counter(base_names)

  resolved: list[str] = []
  used: set[str] = set()
  for source, base in zip(sources, base_names, strict=False):
    candidate = _identifier(source) if counts[base] > 1 else base
    if candidate in used:
      suffix = 2
      while f"{candidate}_{suffix}" in used:
        suffix += 1
      candidate = f"{candidate}_{suffix}"
    used.add(candidate)
    resolved.append(candidate)
  return resolved


def emit_auto_rules(
  session: Session,
  taxonomy: Taxonomy,
  structures: list[Structure],
  *,
  created_by: str,
) -> None:
  """Persist structural auto-rules for a newly created taxonomy block.

  Call after the atoms are flushed (ids must exist).
  """
  taxonomy_id = str(taxonomy.id)
  is_extend = taxonomy.parent_taxonomy_id is not None
  is_coa = str(taxonomy.taxonomy_type) == "chart_of_accounts"

  _add(
    session,
    taxonomy_id=taxonomy_id,
    rule_category=_XBRL_RULES,
    rule_check_kind="UniqueQNameInTaxonomy",
    rule_message="Every element qname must be unique within this taxonomy.",
    target_kind="taxonomy",
    target_taxonomy_id=taxonomy_id,
    created_by=created_by,
  )

  if is_extend:
    _add(
      session,
      taxonomy_id=taxonomy_id,
      rule_category=_STRUCTURE_RULES,
      rule_check_kind="LibraryOriginImmutability",
      rule_message="Library-origin elements in the parent taxonomy cannot be mutated.",
      target_kind="taxonomy",
      target_taxonomy_id=taxonomy_id,
      created_by=created_by,
    )

  if is_coa:
    _add(
      session,
      taxonomy_id=taxonomy_id,
      rule_category=_FAC_RULES,
      rule_check_kind="LeafHasClassification",
      rule_message="Every leaf element must have an EFS classification.",
      target_kind="taxonomy",
      target_taxonomy_id=taxonomy_id,
      created_by=created_by,
    )

  for structure in structures:
    structure_id = str(structure.id)
    for check_kind, message in (
      ("NoCycles", "The structure must contain no cycles."),
      ("NoOrphanArcs", "Every arc endpoint must reference a declared element."),
      ("ParentBeforeChild", "Parent elements must precede their children in ordering."),
    ):
      _add(
        session,
        taxonomy_id=taxonomy_id,
        rule_category=_STRUCTURE_RULES,
        rule_check_kind=check_kind,
        rule_message=message,
        target_kind="structure",
        target_structure_id=structure_id,
        created_by=created_by,
      )
    if structure.concept_arrangement == "roll_up":
      _add_rollup_rules(
        session,
        taxonomy_id=taxonomy_id,
        structure=structure,
        created_by=created_by,
      )

  session.flush()


def _has_structural_deltas(payload: UpdateTaxonomyBlockRequest) -> bool:
  """True when the update touches what emission reads: structures, their
  ``concept_arrangement``, or calc arcs (element removal cascades arcs)."""
  return bool(
    payload.structures_to_add
    or payload.structures_to_update
    or payload.structures_to_remove
    or payload.associations_to_add
    or payload.associations_to_remove
    or payload.elements_to_remove
  )


def refresh_auto_rules(
  session: Session,
  taxonomy: Taxonomy,
  payload: UpdateTaxonomyBlockRequest,
  *,
  updated_by: str,
) -> None:
  """Re-derive the taxonomy's auto rules after a structural update.

  Auto rules have no natural upsert key, so this deletes and re-emits them
  (their verification results first: FK with no cascade). Skipped when the
  update has no structural delta, to avoid rule-id churn and losing
  verification history.
  """
  if not _has_structural_deltas(payload):
    return

  # autoflush is off and not every apply_* helper flushes.
  session.flush()

  auto_rule_ids = select(Rule.id).where(
    Rule.taxonomy_id == taxonomy.id,
    Rule.rule_origin == _AUTO,
  )
  session.execute(
    delete(VerificationResult).where(VerificationResult.rule_id.in_(auto_rule_ids))
  )
  session.execute(
    delete(Rule).where(
      Rule.taxonomy_id == taxonomy.id,
      Rule.rule_origin == _AUTO,
    )
  )

  structures = list(
    session.execute(select(Structure).where(Structure.taxonomy_id == taxonomy.id))
    .scalars()
    .all()
  )
  emit_auto_rules(session, taxonomy, structures, created_by=updated_by)


def _add_rollup_rules(
  session: Session,
  *,
  taxonomy_id: str,
  structure: Structure,
  created_by: str,
) -> None:
  """Emit one structure-scoped RollUp rule per calculation parent.

  Same shape as the seeded ``rs-gaap-rollup-rules`` (parent-first
  variables). Variables carry ``variable_element_id`` because tenant
  elements may have no qname.
  """
  parent_el = aliased(Element)
  child_el = aliased(Element)
  rows = session.execute(
    select(
      Association.from_element_id,
      Association.to_element_id,
      Association.weight,
      Association.order_value,
      parent_el.qname.label("parent_qname"),
      parent_el.name.label("parent_name"),
      child_el.qname.label("child_qname"),
      child_el.name.label("child_name"),
    )
    .join(parent_el, parent_el.id == Association.from_element_id)
    .join(child_el, child_el.id == Association.to_element_id)
    .where(
      Association.structure_id == str(structure.id),
      Association.association_type == "calculation",
    )
    .order_by(Association.from_element_id, Association.order_value.asc().nulls_last())
  ).fetchall()
  if not rows:
    return

  by_parent: dict[str, list] = {}
  for row in rows:
    by_parent.setdefault(row.from_element_id, []).append(row)

  for parent_id, children in by_parent.items():
    first = children[0]
    names = _resolve_variable_names(
      [(first.parent_qname, first.parent_name, parent_id)]
      + [(c.child_qname, c.child_name, c.to_element_id) for c in children]
    )
    variables: list[dict[str, str]] = [
      {
        "variable_name": names[0],
        "variable_qname": first.parent_qname or "",
        "variable_element_id": parent_id,
      }
    ]
    for i, child in enumerate(children):
      variables.append(
        {
          "variable_name": names[i + 1],
          "variable_qname": child.child_qname or "",
          "variable_element_id": child.to_element_id,
        }
      )
    expression = build_rollup_expression(
      names[0],
      [
        (names[i + 1], c.weight if c.weight is not None else 1.0)
        for i, c in enumerate(children)
      ],
    )

    session.add(
      Rule(
        taxonomy_id=taxonomy_id,
        rule_category=_FAC_RULES,
        rule_pattern="RollUp",
        rule_check_kind=None,
        rule_expression=expression,
        rule_message=(
          f"Calculation rollup: {names[0]} must equal the calculation "
          f"sum of its children."
        ),
        rule_severity="error",
        rule_origin=_AUTO,
        target_kind="structure",
        target_structure_id=str(structure.id),
        rule_variables=variables,
        metadata_={},
        created_by=created_by,
      )
    )


def _add(
  session: Session,
  *,
  taxonomy_id: str,
  rule_category: str,
  rule_check_kind: str,
  rule_message: str,
  target_kind: str,
  target_taxonomy_id: str | None = None,
  target_structure_id: str | None = None,
  created_by: str,
) -> None:
  """Persist a model-structure auto-rule (``rule_check_kind``, no pattern)."""
  session.add(
    Rule(
      taxonomy_id=taxonomy_id,
      rule_category=rule_category,
      rule_pattern=None,
      rule_check_kind=rule_check_kind,
      rule_expression="",
      rule_message=rule_message,
      rule_severity="error",
      rule_origin=_AUTO,
      target_kind=target_kind,
      target_taxonomy_id=target_taxonomy_id,
      target_structure_id=target_structure_id,
      rule_variables=[],
      metadata_={},
      created_by=created_by,
    )
  )
