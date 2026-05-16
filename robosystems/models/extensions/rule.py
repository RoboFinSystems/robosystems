"""Verification rules: pattern- and expression-based validations evaluated against fact sets.

The initial rule corpus is sourced from the Seattle Method linkbases.

Tenant-scoped table. Each row is one rule in the canonical Seattle
Method taxonomy: ``rule_category`` names the governance axis
(``FundamentalAccountingConceptRelation``, ``ReportLevelModelStructureRule``,
etc.) and ``rule_pattern`` names the mechanism (``EqualTo``,
``RollForward``, ``Exists``, …). ``rule_expression`` carries an
XPath-flavored predicate whose ``$Variables`` bind to concept qnames via
``rule_variables``.

The polymorphic target (nullable FKs to ``structures`` / ``elements`` /
``associations`` + ``target_kind`` discriminator) scopes the rule to an
atom in the Information Block construct; null target = global report-wide
rule. CHECK constraints in the migration enforce that exactly one of the
three target columns is populated when ``target_kind`` is set.
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
    # Arithmetic / logical rule patterns. Evaluated by the AST evaluator
    # over fact values. Exactly one of rule_pattern / rule_check_kind is
    # populated (see check_rule_pattern_kind_xor below).
    CheckConstraint(
      "rule_pattern IS NULL OR rule_pattern IN ("
      "'Adjustment', 'CoExists', 'EqualTo', 'Exists', 'GreaterThan', "
      "'GreaterThanOrEqualToZero', 'LessThan', 'RollForward', 'RollUp', "
      "'SumEquals', 'Variance'"
      ")",
      name="check_rule_pattern",
    ),
    # Model-structure check kinds. Walk associations / classifications
    # rather than facts; need a separate evaluator path.
    CheckConstraint(
      "rule_check_kind IS NULL OR rule_check_kind IN ("
      "'LeafHasClassification', 'LibraryOriginImmutability', "
      "'NoCycles', 'NoOrphanArcs', 'ParentBeforeChild', "
      "'UniqueQNameInTaxonomy'"
      ")",
      name="check_rule_check_kind",
    ),
    # XOR: exactly one of rule_pattern / rule_check_kind is non-null.
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
  # Exactly one of rule_pattern (arithmetic) or rule_check_kind
  # (structural) is populated per row; the other is NULL. See the
  # check_rule_pattern_kind_xor CHECK constraint above.
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
