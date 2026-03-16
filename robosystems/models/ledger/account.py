"""Account model — Chart of Accounts.

Maps to Element nodes in the graph. Each account row materializes to an
Element node during the postgres_scanner → DuckDB → LadybugDB pipeline.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  Boolean,
  CheckConstraint,
  Column,
  DateTime,
  ForeignKey,
  Index,
  Integer,
  String,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.ledger import LedgerBase
from robosystems.utils.ulid import generate_prefixed_ulid


class Account(LedgerBase):
  __tablename__ = "accounts"
  __table_args__ = (
    Index("idx_accounts_classification", "classification"),
    Index("idx_accounts_parent", "parent_id"),
    Index("idx_accounts_external", "external_id", "external_source"),
    Index(
      "idx_accounts_active",
      "is_active",
      postgresql_where="is_active = true",
    ),
    CheckConstraint(
      "classification IN ('asset', 'liability', 'equity', 'revenue', 'expense')",
      name="check_account_classification",
    ),
    CheckConstraint(
      "balance_type IN ('debit', 'credit')",
      name="check_balance_type",
    ),
  )

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("acct"))
  code = Column(String, unique=True, nullable=False)
  name = Column(String, nullable=False)
  description = Column(String, nullable=True)

  # Classification
  classification = Column(String, nullable=False)
  sub_classification = Column(String, nullable=True)
  balance_type = Column(String, nullable=False, default="debit")

  # Hierarchy
  parent_id = Column(String, ForeignKey("accounts.id"), nullable=True)
  depth = Column(Integer, nullable=False, default=0)
  path = Column(String, nullable=False, default="")

  # Currency
  currency = Column(String, nullable=False, default="USD")

  # State
  is_active = Column(Boolean, nullable=False, default=True)
  is_placeholder = Column(Boolean, nullable=False, default=False)

  # External mapping (QB, Xero, etc.)
  external_id = Column(String, nullable=True)
  external_source = Column(String, nullable=True)

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
    return f"<Account {self.code} {self.name}>"
