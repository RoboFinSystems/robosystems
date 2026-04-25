"""ElementTrait — junction between elements and traits.

An element can carry multiple traits across multiple categories
(e.g. ``elementsOfFinancialStatements=asset``, ``liquidity=current``,
``activityType=operatingActivity`` for NetCashFlowFromInvesting).

``is_primary`` picks the canonical row per category — used when a
single representative value per axis is needed for display or filtering.
``confidence`` and ``source`` track AI-suggested or adapter-derived
provenance.
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


class ElementTrait(ExtensionsBase):
  __tablename__ = "element_traits"
  __table_args__ = (
    Index("idx_element_traits_trait", "trait_id"),
    Index(
      "idx_element_traits_primary",
      "element_id",
      "is_primary",
      postgresql_where="is_primary = true",
    ),
  )

  element_id = Column(
    String, ForeignKey("elements.id"), primary_key=True, nullable=False
  )
  trait_id = Column(String, ForeignKey("traits.id"), primary_key=True, nullable=False)

  # Primary flag: the canonical trait per (element, category).
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
      f"<ElementTrait element={self.element_id} "
      f"trait={self.trait_id} primary={self.is_primary}>"
    )
