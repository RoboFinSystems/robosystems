"""AssociationClassification — junction between associations and classifications.

Mirrors :class:`ElementTrait`; ``is_primary`` picks the canonical row per
category. ``structures.concept_arrangement`` covers the common case where a
whole structure shares one pattern; this junction handles mixed ones (a
RollUp trunk with RollForward branches).
"""

from datetime import UTC, datetime

from sqlalchemy import (
  Boolean,
  Column,
  DateTime,
  Float,
  ForeignKey,
  Index,
  String,
)

from robosystems.db.extensions import ExtensionsBase


class AssociationClassification(ExtensionsBase):
  __tablename__ = "association_classifications"
  __table_args__ = (
    Index("idx_association_classifications_classification", "classification_id"),
    Index(
      "idx_association_classifications_primary",
      "association_id",
      "is_primary",
      postgresql_where="is_primary = true",
    ),
  )

  association_id = Column(
    String, ForeignKey("associations.id"), primary_key=True, nullable=False
  )
  classification_id = Column(
    String, ForeignKey("classifications.id"), primary_key=True, nullable=False
  )

  # Primary flag: the canonical classification per (association, category).
  is_primary = Column(Boolean, nullable=False, default=True)

  # Confidence (for AI-suggested rows) and provenance.
  confidence = Column(Float, nullable=True)
  source = Column(String, nullable=True)
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  updated_at = Column(
    DateTime,
    nullable=False,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
  )
  created_by = Column(String, nullable=False, default="system")

  def __repr__(self) -> str:
    return (
      f"<AssociationClassification association={self.association_id} "
      f"classification={self.classification_id} primary={self.is_primary}>"
    )
