"""Dimension model and junction tables.

Dimensions are entity tags at any level (transaction, entry, line item).
Maps to Dimension nodes in the graph with *_HAS_DIMENSION relationships.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  Boolean,
  Column,
  DateTime,
  ForeignKey,
  Index,
  String,
  Table,
  UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.ledger import LedgerBase
from robosystems.utils.ulid import generate_prefixed_ulid

# Junction tables — pure join tables, no additional columns

transaction_dimensions = Table(
  "transaction_dimensions",
  LedgerBase.metadata,
  Column(
    "transaction_id",
    String,
    ForeignKey("transactions.id", ondelete="CASCADE"),
    primary_key=True,
  ),
  Column(
    "dimension_id",
    String,
    ForeignKey("dimensions.id"),
    primary_key=True,
  ),
)

entry_dimensions = Table(
  "entry_dimensions",
  LedgerBase.metadata,
  Column(
    "entry_id",
    String,
    ForeignKey("entries.id", ondelete="CASCADE"),
    primary_key=True,
  ),
  Column(
    "dimension_id",
    String,
    ForeignKey("dimensions.id"),
    primary_key=True,
  ),
)

line_item_dimensions = Table(
  "line_item_dimensions",
  LedgerBase.metadata,
  Column(
    "line_item_id",
    String,
    ForeignKey("line_items.id", ondelete="CASCADE"),
    primary_key=True,
  ),
  Column(
    "dimension_id",
    String,
    ForeignKey("dimensions.id"),
    primary_key=True,
  ),
)


class Dimension(LedgerBase):
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
