"""Billing invoice models - consolidated invoicing for all resources."""

import secrets
from datetime import datetime, timezone, timedelta
from typing import Optional
from enum import Enum
from sqlalchemy import Column, String, Integer, DateTime, JSON, ForeignKey, Index
from sqlalchemy.orm import relationship, Session

from ...database import Base
from ...logger import get_logger

logger = get_logger(__name__)


class InvoiceStatus(str, Enum):
  """Invoice status states."""

  DRAFT = "draft"
  OPEN = "open"
  PAID = "paid"
  VOID = "void"
  UNCOLLECTIBLE = "uncollectible"


class BillingInvoice(Base):
  """Consolidated invoice for a billing customer.

  One invoice per customer per billing period.
  Contains line items for all billable resources (graphs, repositories, etc).
  """

  __tablename__ = "billing_invoices"

  id = Column(
    String, primary_key=True, default=lambda: f"binv_{secrets.token_urlsafe(16)}"
  )

  billing_customer_user_id = Column(String, ForeignKey("users.id"), nullable=False)

  invoice_number = Column(String, unique=True, nullable=False)

  period_start = Column(DateTime, nullable=False)
  period_end = Column(DateTime, nullable=False)

  subtotal_cents = Column(Integer, nullable=False)
  tax_cents = Column(Integer, default=0, nullable=False)
  discount_cents = Column(Integer, default=0, nullable=False)
  total_cents = Column(Integer, nullable=False)

  status = Column(String, default="draft", nullable=False)

  stripe_invoice_id = Column(String, unique=True, nullable=True)

  sent_at = Column(DateTime, nullable=True)
  due_date = Column(DateTime, nullable=True)
  paid_at = Column(DateTime, nullable=True)
  payment_method = Column(String, nullable=True)
  payment_reference = Column(String, nullable=True)

  notes = Column(String, nullable=True)

  created_at = Column(
    DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
  )
  updated_at = Column(
    DateTime,
    default=lambda: datetime.now(timezone.utc),
    onupdate=lambda: datetime.now(timezone.utc),
    nullable=False,
  )

  line_items = relationship(
    "BillingInvoiceLineItem", back_populates="invoice", cascade="all, delete-orphan"
  )

  __table_args__ = (
    Index("idx_billing_invoice_customer", "billing_customer_user_id"),
    Index("idx_billing_invoice_status", "status"),
    Index("idx_billing_invoice_period", "period_start", "period_end"),
    Index("idx_billing_invoice_due_date", "due_date"),
  )

  def __repr__(self) -> str:
    return f"<BillingInvoice {self.invoice_number} total=${self.total_cents / 100:.2f}>"

  @classmethod
  def create_invoice(
    cls,
    user_id: str,
    period_start: datetime,
    period_end: datetime,
    session: Session,
    payment_terms: str = "net_30",
  ) -> "BillingInvoice":
    """Create a new invoice for a billing period."""
    now = datetime.now(timezone.utc)

    invoice_number = cls._generate_invoice_number(session)

    terms_days = {
      "net_15": 15,
      "net_30": 30,
      "net_60": 60,
      "net_90": 90,
    }.get(payment_terms, 30)

    due_date = now + timedelta(days=terms_days)

    invoice = cls(
      billing_customer_user_id=user_id,
      invoice_number=invoice_number,
      period_start=period_start,
      period_end=period_end,
      subtotal_cents=0,
      total_cents=0,
      due_date=due_date,
      status=InvoiceStatus.DRAFT.value,
      created_at=now,
    )

    session.add(invoice)
    session.commit()
    session.refresh(invoice)

    logger.info(f"Created invoice {invoice_number} for user {user_id}")

    return invoice

  @classmethod
  def _generate_invoice_number(cls, session: Session) -> str:
    """Generate unique invoice number."""
    now = datetime.now(timezone.utc)
    year = now.year
    month = now.month

    count = (
      session.query(cls)
      .filter(cls.invoice_number.like(f"INV-{year}-{month:02d}-%"))
      .count()
      + 1
    )

    return f"INV-{year}-{month:02d}-{count:04d}"

  def add_line_item(
    self,
    subscription_id: str,
    resource_type: str,
    resource_id: str,
    description: str,
    amount_cents: int,
    session: Session,
    quantity: int = 1,
    line_metadata: Optional[dict] = None,
  ) -> "BillingInvoiceLineItem":
    """Add a line item to the invoice."""
    line_item = BillingInvoiceLineItem(
      invoice_id=self.id,
      subscription_id=subscription_id,
      resource_type=resource_type,
      resource_id=resource_id,
      description=description,
      quantity=quantity,
      unit_price_cents=amount_cents,
      amount_cents=amount_cents * quantity,
      period_start=self.period_start,
      period_end=self.period_end,
      line_metadata=line_metadata,
    )

    session.add(line_item)
    session.commit()

    self._recalculate_totals(session)

    logger.info(f"Added line item to invoice {self.invoice_number}: {description}")

    return line_item

  def _recalculate_totals(self, session: Session) -> None:
    """Recalculate invoice totals from line items."""
    total = sum(item.amount_cents for item in self.line_items)
    self.subtotal_cents = total
    self.total_cents = total + self.tax_cents - self.discount_cents
    self.updated_at = datetime.now(timezone.utc)
    session.commit()

  def finalize(self, session: Session) -> None:
    """Finalize the invoice and mark as open."""
    self.status = InvoiceStatus.OPEN.value
    self.sent_at = datetime.now(timezone.utc)
    self.updated_at = datetime.now(timezone.utc)

    session.commit()
    session.refresh(self)

    logger.info(f"Finalized invoice {self.invoice_number}")

  def mark_paid(
    self, session: Session, payment_method: str, payment_reference: Optional[str] = None
  ) -> None:
    """Mark invoice as paid."""
    self.status = InvoiceStatus.PAID.value
    self.paid_at = datetime.now(timezone.utc)
    self.payment_method = payment_method
    self.payment_reference = payment_reference
    self.updated_at = datetime.now(timezone.utc)

    session.commit()
    session.refresh(self)

    logger.info(f"Marked invoice {self.invoice_number} as paid")


class BillingInvoiceLineItem(Base):
  """Line item for a billing invoice."""

  __tablename__ = "billing_invoice_line_items"

  id = Column(
    String, primary_key=True, default=lambda: f"bli_{secrets.token_urlsafe(16)}"
  )

  invoice_id = Column(String, ForeignKey("billing_invoices.id"), nullable=False)

  subscription_id = Column(
    String, ForeignKey("billing_subscriptions.id"), nullable=True
  )

  resource_type = Column(String, nullable=False)
  resource_id = Column(String, nullable=False)
  description = Column(String, nullable=False)

  quantity = Column(Integer, default=1, nullable=False)
  unit_price_cents = Column(Integer, nullable=False)
  amount_cents = Column(Integer, nullable=False)

  period_start = Column(DateTime, nullable=False)
  period_end = Column(DateTime, nullable=False)

  line_metadata = Column(JSON, nullable=True)

  created_at = Column(
    DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
  )

  invoice = relationship("BillingInvoice", back_populates="line_items")

  __table_args__ = (
    Index("idx_billing_line_item_invoice", "invoice_id"),
    Index("idx_billing_line_item_subscription", "subscription_id"),
    Index("idx_billing_line_item_resource", "resource_type", "resource_id"),
  )

  def __repr__(self) -> str:
    return f"<BillingInvoiceLineItem {self.description} ${self.amount_cents / 100:.2f}>"
