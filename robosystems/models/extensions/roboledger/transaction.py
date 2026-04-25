"""Transaction model — business events.

Maps to Transaction nodes in the graph. Represents what happened in the
real world (invoice, payment, deposit, etc.).
"""

from datetime import UTC, datetime

from sqlalchemy import (
  BigInteger,
  CheckConstraint,
  Column,
  Date,
  DateTime,
  Index,
  Integer,
  String,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class Transaction(ExtensionsBase):
  __tablename__ = "transactions"
  __table_args__ = (
    Index("idx_transactions_date", "date"),
    Index("idx_transactions_type", "type"),
    Index("idx_transactions_status", "status"),
    Index("idx_transactions_source", "source"),
    Index("idx_transactions_merchant", "merchant_name"),
    Index("idx_transactions_created", "created_at"),
    Index(
      "idx_transactions_triggered_by_event",
      "triggered_by_event_id",
      postgresql_where="triggered_by_event_id IS NOT NULL",
    ),
    CheckConstraint(
      "status IN ('pending', 'posted', 'void')",
      name="check_transaction_status",
    ),
    CheckConstraint("amount >= 0", name="check_transaction_amount"),
  )

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("txn"))
  number = Column(String, nullable=True)
  idempotency_key = Column(String, unique=True, nullable=True)

  # Classification
  type = Column(String, nullable=False)
  category = Column(String, nullable=True)

  # Financial (minor currency units — cents)
  amount = Column(BigInteger, nullable=False)
  currency = Column(String, nullable=False, default="USD")

  # Dates
  date = Column(Date, nullable=False)
  due_date = Column(Date, nullable=True)

  # Counterparty
  merchant_name = Column(String, nullable=True)
  reference_number = Column(String, nullable=True)

  # Description
  description = Column(String, nullable=True)

  # Source provenance
  source = Column(String, nullable=False, default="native")
  source_id = Column(String, nullable=True)
  connection_id = Column(String, nullable=True)

  # Event audit chain — links this transaction to the business event that caused it.
  # Null for legacy transactions and explicit journal entries (no upstream event).
  triggered_by_event_id = Column(String, nullable=True)

  # State
  status = Column(String, nullable=False, default="pending")
  posted_at = Column(DateTime, nullable=True)

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
    return f"<Transaction {self.id} {self.type} {self.amount}>"
