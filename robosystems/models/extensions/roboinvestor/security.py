"""Security model — ownership instruments issued by entities.

A Security represents a type of ownership in a private company: common stock,
preferred stock, warrants, convertible notes, LLC units, LP interests, etc.
One Entity can issue many Securities. The terms JSONB carries instrument-specific
details (liquidation preference, strike price, vesting schedule) for future
waterfall distribution modeling.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  BigInteger,
  Boolean,
  Column,
  DateTime,
  ForeignKey,
  Index,
  Integer,
  String,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class Security(ExtensionsBase):
  __tablename__ = "securities"
  __table_args__ = (
    Index("idx_securities_entity", "entity_id"),
    Index("idx_securities_type", "security_type"),
    Index(
      "idx_securities_active",
      "is_active",
      postgresql_where="is_active = true",
    ),
  )

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("sec"))
  entity_id = Column(String, ForeignKey("entities.id"), nullable=True)

  # Description
  name = Column(String, nullable=False)  # "Common Stock Class A", "Preferred Series A"
  security_type = Column(
    String, nullable=False
  )  # common_stock, preferred_stock, warrant, etc.
  security_subtype = Column(String, nullable=True)  # class_a, series_a, series_seed

  # Terms (structured for future waterfall modeling)
  terms = Column(JSONB, nullable=False, default=dict)

  # State
  is_active = Column(Boolean, nullable=False, default=True)
  authorized_shares = Column(BigInteger, nullable=True)
  outstanding_shares = Column(BigInteger, nullable=True)

  # Metadata
  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)
  version = Column(Integer, nullable=False, default=1)

  # Timestamps
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  updated_at = Column(
    DateTime,
    nullable=False,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
  )
  created_by = Column(String, nullable=False)

  def __repr__(self) -> str:
    return f"<Security {self.name} ({self.security_type})>"
