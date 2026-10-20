"""What a payment settles, document by document.

``Event.discharges_event_id`` holds one obligation, so a payment that settles
several invoices or bills names only the first. An allocation row says how
much of the payment went to each; a payment with rows is read through them,
one without through the column at its full magnitude.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  BigInteger,
  CheckConstraint,
  Column,
  DateTime,
  ForeignKey,
  Index,
  String,
)

from robosystems.db.extensions import ExtensionsBase


class EventDischarge(ExtensionsBase):
  __tablename__ = "event_discharges"
  __table_args__ = (
    CheckConstraint("amount > 0", name="check_event_discharges_amount_positive"),
    Index("idx_event_discharges_obligation", "obligation_event_id"),
  )

  payment_event_id = Column(
    String, ForeignKey("events.id", ondelete="CASCADE"), primary_key=True
  )
  obligation_event_id = Column(
    String, ForeignKey("events.id", ondelete="CASCADE"), primary_key=True
  )
  # Positive cents of the payment applied to the obligation.
  amount = Column(BigInteger, nullable=False)

  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  created_by = Column(String, nullable=False)

  def __repr__(self) -> str:
    return (
      f"<EventDischarge {self.payment_event_id} -> {self.obligation_event_id} "
      f"{self.amount}>"
    )
