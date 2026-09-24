"""Tenant-scoped structures: named groupings of element associations within a
taxonomy (the OLTP side of graph Structure nodes).

Writers populate both ``artifact_mechanics`` (typed) and ``metadata_``
(untyped); readers prefer the former and fall back to the latter.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  Boolean,
  CheckConstraint,
  Column,
  DateTime,
  ForeignKey,
  Index,
  String,
)
from sqlalchemy import text as sqlalchemy_text
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid

# Concept Arrangement Pattern (CAP) vocabulary: the source for the CHECK below,
# the API request Literal in ``models/api/taxonomy_block.py`` (a drift test
# ties them), and seed validation.
CONCEPT_ARRANGEMENT_VALUES: tuple[str, ...] = (
  # 8 canonical CAPs
  "set",
  "roll_up",
  "roll_forward",
  "roll_forward_info",
  "adjustment",
  "variance",
  "arithmetic",
  "text_block",
  # 5 cm.xsd text-block / detail specializations (level is the CAP itself)
  "level1_textblock",
  "level2_textblock",
  "level3_textblock",
  "level4_detail",
  "table_equivalent_textblock",
  # 2 pseudo-patterns
  "grid",
  "compound_fact",
)

# CAPs whose facts are narrative text blocks (Nonnumeric facts bound from a
# platform Document) rather than numeric grids. ``level4_detail`` is excluded:
# cm.xsd level 4 is the numeric detail table, not narrative.
TEXT_BLOCK_CAPS: frozenset[str] = frozenset(
  {
    "text_block",
    "level1_textblock",
    "level2_textblock",
    "level3_textblock",
    "table_equivalent_textblock",
  }
)


# `structures.block_type` vocabulary: the source for the CHECK and for the
# tenant-provisioning widen step (a tenant CHECK narrower than public's breaks
# graph creation when the library adds a value).
BLOCK_TYPE_VALUES: tuple[str, ...] = (
  # Renderable financial-statement presentations (the user-facing forms)
  "income_statement",
  "balance_sheet",
  "cash_flow_statement",
  "equity_statement",
  "comprehensive_income",
  # Domain-specific working-paper / schedule patterns
  "schedule",
  "rollforward",
  "reconciliation",
  "policy",
  "metric",
  # Authored FP&A scenario container; derived forward facts land in the
  # statement/metric types stamped with fact_sets.scenario_id.
  "forecast",
  # Chart-of-accounts and CoA→GAAP mapping
  "chart_of_accounts",
  "coa_mapping",
  # Reference-taxonomy network roles (rules, regulatory disclosures,
  # crosswalks); never rendered in report packages.
  "validation_rules",
  "regulatory_disclosure",
  "taxonomy_mapping",
  # Pinned per entity via entities.reporting_style_id.
  "reporting_style",
  # Escape hatch
  "custom",
)


class Structure(ExtensionsBase):
  __tablename__ = "structures"
  __table_args__ = (
    Index("idx_structures_taxonomy", "taxonomy_id"),
    Index("idx_structures_type", "block_type"),
    # Serves the role_uri lookup in ``load_disclosure_id_for_structure``.
    Index(
      "idx_structures_role_uri",
      sqlalchemy_text("(metadata->>'role_uri')"),
    ),
    CheckConstraint(
      "block_type IN (" + ", ".join(f"'{v}'" for v in BLOCK_TYPE_VALUES) + ")",
      name="check_block_type",
    ),
    CheckConstraint(
      "concept_arrangement IS NULL OR concept_arrangement IN ("
      + ", ".join(f"'{v}'" for v in CONCEPT_ARRANGEMENT_VALUES)
      + ")",
      name="check_concept_arrangement",
    ),
    # Member Arrangement Pattern (MAP), non-aggregating to fully aggregating.
    CheckConstraint(
      "member_arrangement IS NULL OR member_arrangement IN ("
      "'is_a', 'whole_part', 'nested_whole_part', "
      "'two_dimension_aggregation', 'complex_aggregating_whole_part'"
      ")",
      name="check_member_arrangement",
    ),
  )

  id = Column(
    String, primary_key=True, default=lambda: generate_prefixed_ulid("struct")
  )
  name = Column(String, nullable=False)
  description = Column(String, nullable=True)

  block_type = Column(String, nullable=False)

  taxonomy_id = Column(String, ForeignKey("taxonomies.id"), nullable=False)

  graph_structure_id = Column(String, nullable=True)

  is_active = Column(Boolean, nullable=False, default=True)

  # Seattle Method information-model axes. NULL where a block type declares
  # no default; MAP is NULL for non-hypercube block types.
  concept_arrangement = Column(String, nullable=True)
  member_arrangement = Column(String, nullable=True)

  # ``ArtifactMechanics`` (models/api/information_block.py) as JSONB.
  artifact_mechanics = Column(JSONB, nullable=True)

  # Structure-level renderer caveat, e.g. "(in thousands, except per share)";
  # not an XBRL parenthetical, which is fact-level.
  renderer_note = Column(String, nullable=True)

  # References ``structure_templates.id``; deliberately no FK constraint.
  template_id = Column(String, nullable=True)

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
    return f"<Structure {self.name} ({self.block_type})>"
