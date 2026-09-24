"""Associations: element-to-element arcs, both hierarchies within a taxonomy
and mappings between taxonomies (CoA → GAAP)."""

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

# `associations.association_type` vocabulary for the CHECK and the tenant
# widen step. 'derivation' maps BS leaves to their CF change tags;
# 'has-part' links cm:Debit/cm:Credit to the CoA legs of a schedule's
# posting template.
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

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("assoc"))

  structure_id = Column(String, ForeignKey("structures.id"), nullable=False)

  from_element_id = Column(String, ForeignKey("elements.id"), nullable=False)
  to_element_id = Column(String, ForeignKey("elements.id"), nullable=False)

  association_type = Column(String, nullable=False, default="presentation")
  arcrole = Column(String, nullable=True)
  order_value = Column(Float, nullable=True, default=0)
  weight = Column(Float, nullable=True)

  # AI provenance, for mapping associations.
  confidence = Column(Float, nullable=True)
  suggested_by = Column(String, nullable=True)
  approved_by = Column(String, nullable=True)
  approved_at = Column(DateTime, nullable=True)

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
    return f"<Association {self.from_element_id} → {self.to_element_id} ({self.association_type})>"
