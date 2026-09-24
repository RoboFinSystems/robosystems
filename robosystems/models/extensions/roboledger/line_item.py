"""Journal entry lines. Exactly one of debit_amount/credit_amount is positive;
the other is zero."""

from datetime import UTC, datetime

from sqlalchemy import (
  BigInteger,
  CheckConstraint,
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


class LineItem(ExtensionsBase):
  __tablename__ = "line_items"
  __table_args__ = (
    Index("idx_line_items_entry", "entry_id"),
    Index("idx_line_items_element", "element_id"),
    Index("idx_line_items_flow_element", "flow_element_id"),
    CheckConstraint("debit_amount >= 0", name="check_debit_positive"),
    CheckConstraint("credit_amount >= 0", name="check_credit_positive"),
    CheckConstraint(
      "debit_amount > 0 OR credit_amount > 0",
      name="check_at_least_one_amount",
    ),
    CheckConstraint(
      "NOT (debit_amount > 0 AND credit_amount > 0)",
      name="check_not_both_amounts",
    ),
  )
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("li"))
  entry_id = Column(
    String, ForeignKey("entries.id", ondelete="CASCADE"), nullable=False
  )
  element_id = Column(String, ForeignKey("elements.id"), nullable=False)

  # The economic flow this line represents (e.g.
  # rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment), driving rollforward
  # attribution and cash-flow/equity-flow rendering. NULL when the line
  # carries no flow. A valid flow element has an `activityType` trait.
  flow_element_id = Column(String, ForeignKey("elements.id"), nullable=True)

  # Cents.
  debit_amount = Column(BigInteger, nullable=False, default=0)
  credit_amount = Column(BigInteger, nullable=False, default=0)
  description = Column(String, nullable=True)
  line_order = Column(Integer, nullable=False, default=0)
  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  updated_at = Column(
    DateTime,
    nullable=False,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
  )

  def __repr__(self) -> str:
    return f"<LineItem {self.id} debit={self.debit_amount} credit={self.credit_amount}>"
