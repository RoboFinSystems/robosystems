"""AssociationClassification — junction between associations and classifications.

An association can carry multiple classifications across categories —
``concept_arrangement`` (RollUp / RollForward / Adjustment / Variance / Set
/ MemberAggregation / Textblock / …), ``member_arrangement``
(aggregation / nonaggregation), and ``named_disclosure`` (SEC disclosure
mechanics catalog entries like ``AssetsRollUp``).

Mirrors the shape of :class:`ElementTrait`: composite PK on
``(association_id, classification_id)``, ``is_primary`` picks the
canonical row per category, ``confidence`` + ``source`` track AI/adapter
provenance. Structures carry an aggregate ``concept_arrangement`` column
as an optimized junction for the common case where every association in
the structure shares the same classifications; the per-row junction
enables the disaggregated view when they differ (e.g. a statement with a
RollUp trunk and RollForward branches).
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
    return (
      f"<AssociationClassification association={self.association_id} "
      f"classification={self.classification_id} primary={self.is_primary}>"
    )
