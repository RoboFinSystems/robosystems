"""Structural auto-rule emission for Taxonomy Block envelopes.

At create time, each taxonomy block gets a set of ``rule_origin='auto'``
Rule rows that encode its structural invariants — the same invariants the
create-envelope validator checks pre-write, persisted so ``evaluate-rules``
can re-check them post-write on demand.

Auto-rules are tied to the taxonomy via ``taxonomy_id`` (ownership) and
``target_taxonomy_id`` / ``target_structure_id`` (polymorphic target).
They carry no arithmetic expression (``rule_expression=''``) and no
variables. The ``rule_origin='auto'`` discriminator prevents them from
being removed via ``rules_to_remove`` update deltas.

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
    calc parent. The engine's arc-derived evaluator foots the parent
    against its live calc children (structure-local arcs overlaid onto
    the global DAG), so an authored disclosure note gets footing
    validation the moment its facts land — no hand-authored rule needed.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session, aliased

from robosystems.models.extensions import (
  Association,
  Element,
  Rule,
  Structure,
  Taxonomy,
)

_AUTO = "auto"
_STRUCTURE_RULES = "ReportLevelModelStructureRule"
_XBRL_RULES = "XBRLTechnicalSyntaxRule"
_FAC_RULES = "FundamentalAccountingConceptRelation"


def emit_auto_rules(
  session: Session,
  taxonomy: Taxonomy,
  structures: list[Structure],
  *,
  created_by: str,
) -> None:
  """Persist structural auto-rules for a newly created taxonomy block.

  Called after all atoms are flushed so ``taxonomy.id`` and
  ``structure.id`` are available. Does nothing when ``structures`` is
  empty (schedule_container has no atoms).
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


def _add_rollup_rules(
  session: Session,
  *,
  taxonomy_id: str,
  structure: Structure,
  created_by: str,
) -> None:
  """Emit one structure-scoped RollUp rule per calculation parent.

  Mirrors the seeded ``rs-gaap-rollup-rules`` shape
  (``generate_rollup_rules.py``: parent-first variables,
  FundamentalAccountingConceptRelation, pattern ``RollUp``) so the
  engine's arc-derived evaluator picks it up unchanged. Variables carry
  ``variable_element_id`` explicitly — authored structures may reference
  tenant elements whose qname resolution shouldn't be load-bearing.

  No calculation arcs → no rules (a presentation-only roll_up renders
  but has nothing to foot).
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
    parent_label = (first.parent_qname or first.parent_name or parent_id).split(":")[-1]
    variables: list[dict[str, str]] = [
      {
        "variable_name": parent_label,
        "variable_qname": first.parent_qname or "",
        "variable_element_id": parent_id,
      }
    ]
    terms: list[str] = []
    for child in children:
      child_label = (
        child.child_qname or child.child_name or child.to_element_id
      ).split(":")[-1]
      variables.append(
        {
          "variable_name": child_label,
          "variable_qname": child.child_qname or "",
          "variable_element_id": child.to_element_id,
        }
      )
      weight = child.weight if child.weight is not None else 1.0
      sign = "-" if weight < 0 else "+"
      terms.append(f"{sign} ${child_label}")
    expression = f"${parent_label} = {' '.join(terms).lstrip('+ ')}"

    session.add(
      Rule(
        taxonomy_id=taxonomy_id,
        rule_category=_FAC_RULES,
        rule_pattern="RollUp",
        rule_check_kind=None,
        rule_expression=expression,
        rule_message=(
          f"Calculation rollup: {parent_label} must equal the calculation "
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
  """Persist a structural auto-rule. All auto-rules are model-structure
  checks (no fact-value arithmetic), so they populate rule_check_kind
  rather than rule_pattern."""
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
