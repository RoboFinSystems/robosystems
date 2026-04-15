"""Dimension model — base ontology concept.

Dimensions are entity-tag axes (segment, geography, department, class,
project, etc.) used by XBRL facts, ledger line items, and any future
domain that needs dimensional segmentation. Maps to Dimension nodes in
the graph with `*_HAS_DIMENSION` relationships.

Dimension is a base ontology concept per schemas/base.py INVARIANT 1.
Junction tables that bind Dimension to roboledger-specific tables
(transactions, entries, line_items) live in
`roboledger/dimension_junctions.py` to preserve clean layering.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  Boolean,
  Column,
  DateTime,
  Index,
  String,
  UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class Dimension(ExtensionsBase):
  __tablename__ = "dimensions"
  __table_args__ = (
    UniqueConstraint("dimension_type", "value", name="uq_dimension_type_value"),
    Index("idx_dimensions_type", "dimension_type"),
  )

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("dim"))
  dimension_type = Column(String, nullable=False)
  name = Column(String, nullable=False)
  value = Column(String, nullable=False)

  # Metadata
  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)
  is_active = Column(Boolean, nullable=False, default=True)

  # Timestamps
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  updated_at = Column(
    DateTime,
    nullable=False,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
  )

  def __repr__(self) -> str:
    return f"<Dimension {self.dimension_type}={self.value}>"
