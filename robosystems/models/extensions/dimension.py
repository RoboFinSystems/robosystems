"""Dimension: a tag axis member (segment, geography, department, class,
project, scenario) for facts and ledger rows. The ledger junction tables live
in `roboledger/dimension_junctions.py`."""

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
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("dim"))
  dimension_type = Column(String, nullable=False)
  name = Column(String, nullable=False)
  value = Column(String, nullable=False)
  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)
  is_active = Column(Boolean, nullable=False, default=True)
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  updated_at = Column(
    DateTime,
    nullable=False,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
  )

  def __repr__(self) -> str:
    return f"<Dimension {self.dimension_type}={self.value}>"
