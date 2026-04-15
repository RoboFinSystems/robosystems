"""EntityTaxonomy join table — entity ↔ taxonomy adoption relationships.

Models the ENTITY_HAS_TAXONOMY graph edge as an OLTP many-to-many
junction. An entity can adopt multiple taxonomies across different bases
(reporting, chart_of_accounts, mapping, schedule). Within each basis, at
most one row is marked `is_primary=true` (enforced by a partial unique
index).

This is a base ontology concept — entities always have taxonomies,
regardless of which extension the entity lives in. See schemas/base.py
INVARIANT 1 (aspirational base).
"""

from datetime import UTC, datetime

from sqlalchemy import (
  Boolean,
  CheckConstraint,
  Column,
  Date,
  DateTime,
  ForeignKey,
  Index,
  String,
  UniqueConstraint,
)

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class EntityTaxonomy(ExtensionsBase):
  __tablename__ = "entity_taxonomies"
  __table_args__ = (
    Index("idx_entity_taxonomies_entity", "entity_id"),
    Index("idx_entity_taxonomies_taxonomy", "taxonomy_id"),
    # Exactly one row per (entity, taxonomy, basis) combination. If an entity
    # needs to change its adoption (e.g., switch primary reporting from
    # us-gaap-2023 to us-gaap-2024), update the existing row in place rather
    # than creating a second row. Historical adoption tracking, if needed,
    # belongs in an audit log, not in this current-state table.
    UniqueConstraint(
      "entity_id",
      "taxonomy_id",
      "basis",
      name="uq_entity_taxonomy_combo",
    ),
    # At most one is_primary=true row per (entity, basis). An entity can have
    # one primary reporting taxonomy, one primary CoA, etc.
    Index(
      "idx_entity_taxonomies_primary",
      "entity_id",
      "basis",
      unique=True,
      postgresql_where="is_primary = true",
    ),
    CheckConstraint(
      "basis IN ('reporting', 'chart_of_accounts', 'mapping', 'schedule')",
      name="check_entity_taxonomy_basis",
    ),
    CheckConstraint(
      "adoption_context IS NULL OR adoption_context IN "
      "('required_by_regulation', 'voluntary', 'contractual')",
      name="check_entity_taxonomy_adoption_context",
    ),
  )

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("et"))

  # Foreign keys
  entity_id = Column(
    String, ForeignKey("entities.id", ondelete="CASCADE"), nullable=False
  )
  taxonomy_id = Column(String, ForeignKey("taxonomies.id"), nullable=False)

  # Adoption metadata
  is_primary = Column(Boolean, nullable=False, default=False)
  basis = Column(
    String, nullable=False
  )  # reporting | chart_of_accounts | mapping | schedule
  effective_from = Column(Date, nullable=True)
  effective_to = Column(Date, nullable=True)
  adoption_context = Column(
    String, nullable=True
  )  # required_by_regulation | voluntary | contractual

  # Timestamps
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  updated_at = Column(
    DateTime,
    nullable=False,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
  )

  def __repr__(self) -> str:
    return (
      f"<EntityTaxonomy entity={self.entity_id} "
      f"taxonomy={self.taxonomy_id} basis={self.basis} primary={self.is_primary}>"
    )
