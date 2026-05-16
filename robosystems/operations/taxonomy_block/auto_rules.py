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
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from robosystems.models.extensions import Rule, Structure, Taxonomy

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

  session.flush()


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
