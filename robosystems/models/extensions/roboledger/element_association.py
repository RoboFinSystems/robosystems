"""ElementAssociation model — relationships between elements across taxonomies.

Tenant-scoped table. Defines parent-child hierarchies within taxonomies
and mapping associations between taxonomies (CoA → GAAP).
Materializes to Association nodes in the graph.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  CheckConstraint,
  Column,
  DateTime,
  Float,
  ForeignKey,
  Index,
  String,
  UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class ElementAssociation(ExtensionsBase):
  __tablename__ = "element_associations"
  __table_args__ = (
    UniqueConstraint(
      "structure_id",
      "from_element_id",
      "to_element_id",
      "association_type",
      name="uq_association_structure_elements_type",
    ),
    Index("idx_associations_structure", "structure_id"),
    Index("idx_associations_from", "from_element_id"),
    Index("idx_associations_to", "to_element_id"),
    Index("idx_associations_type", "association_type"),
    Index(
      "idx_associations_unapproved",
      "approved_by",
      postgresql_where="approved_by IS NULL AND suggested_by = 'ai'",
    ),
    CheckConstraint(
      "association_type IN ('presentation', 'calculation', 'mapping')",
      name="check_association_type",
    ),
  )

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("assoc"))

  # Structure membership
  structure_id = Column(String, ForeignKey("structures.id"), nullable=False)

  # Relationship
  from_element_id = Column(String, ForeignKey("elements.id"), nullable=False)
  to_element_id = Column(String, ForeignKey("elements.id"), nullable=False)

  # Association properties
  association_type = Column(String, nullable=False, default="presentation")
  arcrole = Column(String, nullable=True)
  order_value = Column(Float, nullable=True, default=0)
  weight = Column(Float, nullable=True)

  # AI provenance (for mapping associations)
  confidence = Column(Float, nullable=True)
  suggested_by = Column(String, nullable=True)
  approved_by = Column(String, nullable=True)
  approved_at = Column(DateTime, nullable=True)

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
    return f"<ElementAssociation {self.from_element_id} → {self.to_element_id} ({self.association_type})>"
