"""Structure model — organizes elements into named taxonomic structures.

Tenant-scoped table. The OLTP representation of Structure nodes in the graph.
Each structure belongs to a taxonomy and contains element associations.
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
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class Structure(ExtensionsBase):
  __tablename__ = "structures"
  __table_args__ = (
    Index("idx_structures_taxonomy", "taxonomy_id"),
    Index("idx_structures_type", "structure_type"),
    CheckConstraint(
      "structure_type IN ("
      "'chart_of_accounts', 'income_statement', 'balance_sheet', "
      "'cash_flow_statement', 'equity_statement', 'coa_mapping', 'custom', "
      "'schedule', 'rollforward', 'reconciliation', 'policy', 'metric'"
      ")",
      name="check_structure_type",
    ),
  )

  # Identity
  id = Column(
    String, primary_key=True, default=lambda: generate_prefixed_ulid("struct")
  )
  name = Column(String, nullable=False)
  description = Column(String, nullable=True)

  # Type
  structure_type = Column(String, nullable=False)

  # Taxonomy membership
  taxonomy_id = Column(String, ForeignKey("taxonomies.id"), nullable=False)

  # Graph reference
  graph_structure_id = Column(String, nullable=True)

  # State
  is_active = Column(Boolean, nullable=False, default=True)

  # Information Model axis columns (Phase δ)
  # concept_arrangement: roll_up | roll_forward | variance | adjustment |
  # set | arithmetic | textblock. Nullable until every block type
  # declares a default; CHECK constraint deferred to a later phase so
  # the vocabulary can expand without a migration round-trip.
  concept_arrangement = Column(String, nullable=True)
  # member_arrangement: aggregation | nonaggregation. Null for
  # non-hypercube block types.
  member_arrangement = Column(String, nullable=True)

  # Typed Artifact Mechanics (Phase δ). Pydantic discriminated union
  # (see models/api/information_block.py::ArtifactMechanics) persisted
  # as JSONB. Read-path validates shape on envelope build; Schedule
  # writes stamp this column alongside metadata_ during the migration
  # window, then metadata_'s entry_template + schedule_metadata keys
  # are retired in a later phase.
  artifact_mechanics = Column(JSONB, nullable=True)

  # Renderer caveat, e.g. "(in thousands, except per share)". Phase δ.
  parenthetical_note = Column(String, nullable=True)

  # Nullable FK to structure_templates. The templates table lands in a
  # later phase; this column is added now so Phase δ writes that pin a
  # template don't require another round-trip. No FK constraint yet.
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
    return f"<Structure {self.name} ({self.structure_type})>"
