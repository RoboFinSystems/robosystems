"""Structure model — organizes elements into named taxonomic structures.

Tenant-scoped table. The OLTP representation of Structure nodes in the graph.
Each structure belongs to a taxonomy and contains element associations.

During the metadata_ → artifact_mechanics transition, both columns are
populated on write and the read paths prefer ``artifact_mechanics``.
``metadata_`` remains for backward compatibility with rows written before
the typed mechanics column landed.
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


class Structure(ExtensionsBase):
  __tablename__ = "structures"
  __table_args__ = (
    Index("idx_structures_taxonomy", "taxonomy_id"),
    Index("idx_structures_type", "block_type"),
    # Expression index over metadata->>'role_uri' to support the
    # ``load_disclosure_id_for_structure`` LIKE lookup. ``role_uri`` is
    # stored inside the metadata JSONB blob, not as a top-level column.
    Index(
      "idx_structures_role_uri",
      sqlalchemy_text("(metadata->>'role_uri')"),
    ),
    CheckConstraint(
      "block_type IN ("
      # Renderable financial-statement presentations (the user-facing forms)
      "'income_statement', 'balance_sheet', "
      "'cash_flow_statement', 'equity_statement', "
      "'comprehensive_income', "
      # Domain-specific working-paper / schedule patterns
      "'schedule', 'rollforward', 'reconciliation', 'policy', 'metric', "
      # Chart-of-accounts and CoA→GAAP mapping
      "'chart_of_accounts', 'coa_mapping', "
      # Reference-taxonomy structure kinds (XBRL network roles distinct
      # from presentation): formal calculation/business rules expressed
      # as a network (FAC's "Assets = L + E"), named SEC/regulatory
      # disclosures (type-subtype's 991xxx schedules), and crosswalks
      # between taxonomies (fac-to-rs-gaap concordances). Filtered out
      # of the report-package render path; surfaced through their own
      # consumption paths (rule engine, disclosure registry, mapping
      # resolver).
      "'validation_rules', 'regulatory_disclosure', 'taxonomy_mapping', "
      # Reporting Style — the bundle a company picks (Charlie Hoffman's
      # term). One per entity graph, set at provision via
      # graphs.reporting_style_id; composes Networks per statement_type
      # via the reporting_style_networks table.
      "'reporting_style', "
      # Escape hatch for everything that doesn't fit the above
      "'custom'"
      ")",
      name="check_block_type",
    ),
    # Concept Arrangement Pattern (CAP). 8 canonical + 2 pseudo. See
    # information-block.md §3.2.1. NULL allowed for block types that
    # don't yet declare a default.
    CheckConstraint(
      "concept_arrangement IS NULL OR concept_arrangement IN ("
      "'set', 'roll_up', 'roll_forward', 'roll_forward_info', "
      "'adjustment', 'variance', 'arithmetic', 'text_block', "
      "'grid', 'compound_fact'"
      ")",
      name="check_concept_arrangement",
    ),
    # Member Arrangement Pattern (MAP). 5 canonical, from non-aggregating
    # to fully aggregating. See information-block.md §3.2.2. NULL allowed
    # for non-hypercube block types.
    CheckConstraint(
      "member_arrangement IS NULL OR member_arrangement IN ("
      "'is_a', 'whole_part', 'nested_whole_part', "
      "'two_dimension_aggregation', 'complex_aggregating_whole_part'"
      ")",
      name="check_member_arrangement",
    ),
  )

  # Identity
  id = Column(
    String, primary_key=True, default=lambda: generate_prefixed_ulid("struct")
  )
  name = Column(String, nullable=False)
  description = Column(String, nullable=True)

  # Type
  block_type = Column(String, nullable=False)

  # Taxonomy membership
  taxonomy_id = Column(String, ForeignKey("taxonomies.id"), nullable=False)

  # Graph reference
  graph_structure_id = Column(String, nullable=True)

  # State
  is_active = Column(Boolean, nullable=False, default=True)

  # Information Model axis columns. See information-block.md §3.2.1-§3.2.2
  # for the canonical Concept Arrangement Pattern and Member Arrangement
  # Pattern enumerations from Charlie Hoffman's Seattle Method.
  #
  # concept_arrangement — 8 canonical CAPs plus 2 pseudo-patterns:
  #   set | roll_up | roll_forward | roll_forward_info | adjustment |
  #   variance | arithmetic | text_block | grid | compound_fact.
  # Nullable until every block type declares a default. CHECK constraint
  # below enforces the closed vocabulary.
  concept_arrangement = Column(String, nullable=True)
  # member_arrangement — 5 canonical MAPs along the aggregation spectrum
  # from non-aggregating to fully aggregating:
  #   is_a | whole_part | nested_whole_part |
  #   two_dimension_aggregation | complex_aggregating_whole_part.
  # Null for non-hypercube block types.
  member_arrangement = Column(String, nullable=True)

  # Typed Artifact Mechanics. Pydantic discriminated union (see
  # ``models/api/information_block.py::ArtifactMechanics``) persisted as
  # JSONB. Read paths validate shape on envelope build; writes stamp this
  # column alongside ``metadata_``.
  artifact_mechanics = Column(JSONB, nullable=True)

  # Renderer caveat, e.g. "(in thousands, except per share)". NOT XBRL
  # parenthetical explanation — Charlie's parenthetical is fact-level via
  # XBRL footnotes (Framework p.8). This is a Structure-level renderer
  # hint, our extension; see information-block.md §1.7.
  renderer_note = Column(String, nullable=True)

  # Nullable FK to ``structure_templates``. No FK constraint yet so
  # writes that pin a template don't require coordinated lifecycle with
  # the templates table.
  template_id = Column(String, nullable=True)

  # Metadata
  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)

  # Timestamps
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
