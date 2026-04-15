"""Portfolio model — investment portfolio for tracking private company holdings.

Portfolios organize positions across securities issued by entities.
Each portfolio belongs to a single graph tenant.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  CheckConstraint,
  Column,
  Date,
  DateTime,
  ForeignKey,
  Index,
  Integer,
  String,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class Portfolio(ExtensionsBase):
  __tablename__ = "portfolios"
  __table_args__ = (
    Index("idx_portfolios_strategy", "strategy"),
    Index(
      "idx_portfolios_entity",
      "entity_id",
      postgresql_where="entity_id IS NOT NULL",
    ),
    CheckConstraint(
      "ownership_type IS NULL OR ownership_type IN ('direct', 'managed', 'advised')",
      name="check_portfolio_ownership_type",
    ),
  )

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("port"))
  name = Column(String, nullable=False)
  description = Column(String, nullable=True)

  # Entity linkage (ENTITY_HAS_PORTFOLIO in graph) — the investing entity
  # that owns or manages this portfolio. Nullable because legacy portfolios
  # may not have an entity linked yet. ondelete='RESTRICT' blocks entity
  # deletion if portfolios reference it — portfolios should never silently
  # disappear because an entity was removed.
  entity_id = Column(
    String, ForeignKey("entities.id", ondelete="RESTRICT"), nullable=True
  )
  ownership_type = Column(String, nullable=True)  # direct | managed | advised

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
