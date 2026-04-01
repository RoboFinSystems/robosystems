"""Position model — a holding of a Security within a Portfolio.

Positions track quantity, cost basis, and current valuation for each
security held in a portfolio. The unique partial index on
(portfolio_id, security_id) WHERE status = 'active' prevents duplicate
active positions for the same security in the same portfolio.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  BigInteger,
  Column,
  Date,
  DateTime,
  Float,
  ForeignKey,
  Index,
  Integer,
  String,
  text,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class Position(ExtensionsBase):
  __tablename__ = "positions"
  __table_args__ = (
    Index("idx_positions_portfolio", "portfolio_id"),
    Index("idx_positions_security", "security_id"),
    Index("idx_positions_status", "status"),
    Index(
      "uq_positions_portfolio_security_active",
      "portfolio_id",
      "security_id",
      unique=True,
      postgresql_where=text("status = 'active'"),
    ),
  )

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("pos"))
  portfolio_id = Column(String, ForeignKey("portfolios.id"), nullable=False)
  security_id = Column(String, ForeignKey("securities.id"), nullable=False)

  # Holding
  quantity = Column(Float, nullable=False)  # shares, units, or percentage
  quantity_type = Column(
    String, nullable=False, default="shares"
  )  # shares, units, percentage
  cost_basis = Column(BigInteger, nullable=False, default=0)  # total cost in cents
  currency = Column(String, nullable=False, default="USD")

  # Valuation
  current_value = Column(BigInteger, nullable=True)  # current value in cents
  valuation_date = Column(Date, nullable=True)
  valuation_source = Column(
    String, nullable=True
  )  # manual, report_derived, market, formula

  # Dates
  acquisition_date = Column(Date, nullable=True)
  disposition_date = Column(Date, nullable=True)

  # State
  status = Column(String, nullable=False, default="active")  # active, disposed, pending

  # Metadata
  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)
  notes = Column(String, nullable=True)
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
    return f"<Position {self.portfolio_id} → {self.security_id} ({self.quantity} {self.quantity_type})>"
