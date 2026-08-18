"""Association model — relationships between elements across taxonomies.

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

# The `associations.association_type` vocabulary — the single source for the
# model CHECK and the tenant-provisioning widen step.
# 'definition' arcs land from rs-gaap-disclosure-mechanics,
# rs-gaap-reporting-checklist and rs-gaap-reporting-styles.
# 'derivation' arcs map BS leaves to their CF default change tags.
# 'has-part' — Conceptual Model posting arcs: cm:Debit/cm:Credit ─has-part→ a
# CoA element, declaring the debit/credit legs of a schedule's posting
# template as first-class atoms.
ASSOCIATION_TYPE_VALUES: tuple[str, ...] = (
  "presentation",
  "calculation",
  "mapping",
  "equivalence",
  "general-special",
  "essence-alias",
  "definition",
  "derivation",
  "has-part",
)


class Association(ExtensionsBase):
  __tablename__ = "associations"
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
      "association_type IN ("
      + ", ".join(f"'{v}'" for v in ASSOCIATION_TYPE_VALUES)
      + ")",
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
    return f"<Association {self.from_element_id} → {self.to_element_id} ({self.association_type})>"
