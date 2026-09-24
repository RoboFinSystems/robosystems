"""Transactions: source documents such as invoices, payments, and deposits."""

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
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("txn"))
  number = Column(String, nullable=True)
  idempotency_key = Column(String, unique=True, nullable=True)
  type = Column(String, nullable=False)
  category = Column(String, nullable=True)

  amount = Column(BigInteger, nullable=False)  # cents
  currency = Column(String, nullable=False, default="USD")
  date = Column(Date, nullable=False)
  due_date = Column(Date, nullable=True)

  merchant_name = Column(String, nullable=True)
  reference_number = Column(String, nullable=True)
  description = Column(String, nullable=True)

  source = Column(String, nullable=False, default="native")
  source_id = Column(String, nullable=True)
  connection_id = Column(String, nullable=True)

  # The business event that caused this transaction, if any.
  triggered_by_event_id = Column(String, nullable=True)
  status = Column(String, nullable=False, default="pending")
  posted_at = Column(DateTime, nullable=True)
  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)
  version = Column(Integer, nullable=False, default=1)
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
