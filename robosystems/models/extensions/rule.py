"""Verification rules evaluated against fact sets (Seattle Method taxonomy).

``rule_category`` is the governance axis, ``rule_pattern`` the mechanism,
and ``rule_expression`` an XPath-flavored predicate whose ``$Variables``
bind to concept qnames via ``rule_variables``. ``target_kind`` selects which
single target column is set; no target means a report-wide rule.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  CheckConstraint,
  Column,
  DateTime,
  ForeignKey,
  Index,
  String,
  Text,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class Rule(ExtensionsBase):
  __tablename__ = "rules"
  __table_args__ = (
    Index("idx_rules_taxonomy", "taxonomy_id"),
    Index(
      "idx_rules_target_structure",
      "target_structure_id",
      postgresql_where=Column("target_structure_id").isnot(None),
    ),
    Index(
      "idx_rules_target_element",
      "target_element_id",
      postgresql_where=Column("target_element_id").isnot(None),
    ),
    Index(
      "idx_rules_target_association",
      "target_association_id",
      postgresql_where=Column("target_association_id").isnot(None),
    ),
    Index(
      "idx_rules_target_taxonomy",
      "target_taxonomy_id",
      postgresql_where=Column("target_taxonomy_id").isnot(None),
    ),
    Index("idx_rules_category", "rule_category"),
    CheckConstraint(
      "rule_category IN ("
      "'AutomatedAccountingAndReportingChecks', "
      "'DisclosureMechanicsRule', "
      "'FundamentalAccountingConceptRelation', "
      "'PeerConsistencyRule', "
      "'PriorPeriodConsistencyRule', "
      "'ReportLevelModelStructureRule', "
      "'ReportingSystemSpecificRule', "
      "'ToDoManualTask', "
      "'XBRLTechnicalSyntaxRule'"
      ")",
      name="check_rule_category",
    ),
    # Arithmetic / logical patterns over fact values.
    CheckConstraint(
      # 'Derive' computes the LHS (compute-metrics) rather than verifying it.
      "rule_pattern IS NULL OR rule_pattern IN ("
      "'Adjustment', 'CoExists', 'Derive', 'EqualTo', 'Exists', 'GreaterThan', "
      "'GreaterThanOrEqualToZero', 'LessThan', 'RollForward', 'RollUp', "
      "'SumEquals', 'Variance'"
      ")",
      name="check_rule_pattern",
    ),
    # Model-structure checks over associations/classifications, not facts.
    CheckConstraint(
      "rule_check_kind IS NULL OR rule_check_kind IN ("
      "'LeafHasClassification', 'LibraryOriginImmutability', "
      "'NoCycles', 'NoOrphanArcs', 'ParentBeforeChild', "
      "'UniqueQNameInTaxonomy'"
      ")",
      name="check_rule_check_kind",
    ),
    CheckConstraint(
      "(rule_pattern IS NOT NULL AND rule_check_kind IS NULL) "
      "OR (rule_pattern IS NULL AND rule_check_kind IS NOT NULL)",
      name="check_rule_pattern_kind_xor",
    ),
    CheckConstraint(
      "rule_severity IN ('info', 'warning', 'error')",
      name="check_rule_severity",
    ),
    CheckConstraint(
      "rule_origin IN ('forked', 'native', 'auto')",
      name="check_rule_origin",
    ),
    CheckConstraint(
      "("
      "(target_kind IS NULL AND target_structure_id IS NULL "
      "AND target_element_id IS NULL AND target_association_id IS NULL "
      "AND target_taxonomy_id IS NULL) "
      "OR (target_kind = 'structure' AND target_structure_id IS NOT NULL "
      "AND target_element_id IS NULL AND target_association_id IS NULL "
      "AND target_taxonomy_id IS NULL) "
      "OR (target_kind = 'element' AND target_element_id IS NOT NULL "
      "AND target_structure_id IS NULL AND target_association_id IS NULL "
      "AND target_taxonomy_id IS NULL) "
      "OR (target_kind = 'association' AND target_association_id IS NOT NULL "
      "AND target_structure_id IS NULL AND target_element_id IS NULL "
      "AND target_taxonomy_id IS NULL) "
      "OR (target_kind = 'taxonomy' AND target_taxonomy_id IS NOT NULL "
      "AND target_structure_id IS NULL AND target_element_id IS NULL "
      "AND target_association_id IS NULL)"
      ")",
      name="check_rule_target_polymorphism",
    ),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("rule"))
  taxonomy_id = Column(String, ForeignKey("taxonomies.id"), nullable=False)

  rule_category = Column(String, nullable=False)
  # Exactly one of these two is set (check_rule_pattern_kind_xor).
  rule_pattern = Column(String, nullable=True)
  rule_check_kind = Column(String, nullable=True)
  rule_expression = Column(Text, nullable=False)
  rule_message = Column(Text, nullable=True)
  rule_severity = Column(String, nullable=False, default="error")
  rule_origin = Column(String, nullable=False, default="native")

  target_kind = Column(String, nullable=True)
  target_structure_id = Column(String, ForeignKey("structures.id"), nullable=True)
  target_element_id = Column(String, ForeignKey("elements.id"), nullable=True)
  target_association_id = Column(String, ForeignKey("associations.id"), nullable=True)
  target_taxonomy_id = Column(
    String,
    ForeignKey("taxonomies.id", use_alter=True, ondelete="SET NULL"),
    nullable=True,
  )

  rule_variables = Column(JSONB, nullable=False, default=list)
  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)

  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  updated_at = Column(
    DateTime,
    nullable=False,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
  )
  created_by = Column(String, nullable=False, default="system")

  def __repr__(self) -> str:
    kind = self.rule_pattern or self.rule_check_kind
    return f"<Rule {self.rule_category}/{kind} id={self.id}>"
