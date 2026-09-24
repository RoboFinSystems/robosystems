"""EntityTaxonomy: the entity ↔ taxonomy adoption junction (the
ENTITY_HAS_TAXONOMY graph edge), with at most one primary per basis."""

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
    # Current state only: change an adoption by updating its row in place.
    UniqueConstraint(
      "entity_id",
      "taxonomy_id",
      "basis",
      name="uq_entity_taxonomy_combo",
    ),
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

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("et"))

  entity_id = Column(
    String, ForeignKey("entities.id", ondelete="CASCADE"), nullable=False
  )
  # RESTRICT: taxonomies are shared, so deleting an adopted one must be
  # deliberate.
  taxonomy_id = Column(
    String, ForeignKey("taxonomies.id", ondelete="RESTRICT"), nullable=False
  )

  is_primary = Column(Boolean, nullable=False, default=False)
  basis = Column(String, nullable=False)
  effective_from = Column(Date, nullable=True)
  effective_to = Column(Date, nullable=True)
  adoption_context = Column(String, nullable=True)

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
