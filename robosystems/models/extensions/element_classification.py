"""ElementClassification — junction between elements and classifications.

An element can carry multiple classifications across multiple categories
(e.g. `economic_nature=asset`, `statement_context=cash_flow`,
`derivation_role=reconciliation` for NetCashFlowFromInvesting).

`is_primary` picks the canonical row per category — the same value that
lives on the denormalized column on `elements` (e.g.
`Element.statement_context` mirrors the `is_primary=true` row whose
classification has `category='statement_context'`). Denormalization
keeps the common case one-column-read; the junction preserves full
provenance (AI suggestions, alternates, history) when needed.
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


class ElementClassification(ExtensionsBase):
  __tablename__ = "element_classifications"
  __table_args__ = (
    Index("idx_element_classifications_classification", "classification_id"),
    Index(
      "idx_element_classifications_primary",
      "element_id",
      "is_primary",
      postgresql_where="is_primary = true",
    ),
  )

  element_id = Column(
    String, ForeignKey("elements.id"), primary_key=True, nullable=False
  )
  classification_id = Column(
    String, ForeignKey("classifications.id"), primary_key=True, nullable=False
  )

  # Primary flag: the canonical classification per (element, category).
  # Matches the denormalized `elements.<category>` column.
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
      f"<ElementClassification element={self.element_id} "
      f"classification={self.classification_id} primary={self.is_primary}>"
    )
