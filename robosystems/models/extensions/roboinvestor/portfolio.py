"""Portfolio model — investment portfolio for tracking private company holdings.

Portfolios organize positions across securities issued by entities.
Each portfolio belongs to a single graph tenant.
"""

from datetime import UTC, datetime

from sqlalchemy import Column, Date, DateTime, Index, Integer, String
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class Portfolio(ExtensionsBase):
  __tablename__ = "portfolios"
  __table_args__ = (Index("idx_portfolios_strategy", "strategy"),)

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("port"))
  name = Column(String, nullable=False)
  description = Column(String, nullable=True)

  # Strategy (informational)
  strategy = Column(
    String, nullable=True
  )  # growth, income, balanced, pe_fund, venture, family_office
  inception_date = Column(Date, nullable=True)
  base_currency = Column(String, nullable=False, default="USD")

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
    return f"<Portfolio {self.name} ({self.strategy})>"
