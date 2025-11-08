"""Billing customer model - stores payment information for users."""

from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import Column, String, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import Session

from ...database import Base
from ...logger import get_logger

logger = get_logger(__name__)


class BillingCustomer(Base):
  """Billing information for a user.

  Separated from IAM User model to isolate billing concerns.
  Designed for eventual extraction to billing microservice.
  """

  __tablename__ = "billing_customers"

  user_id = Column(String, ForeignKey("users.id"), primary_key=True)

  stripe_customer_id = Column(String, unique=True, nullable=True)
  has_payment_method = Column(Boolean, default=False, nullable=False)
  default_payment_method_id = Column(String, nullable=True)

  invoice_billing_enabled = Column(Boolean, default=False, nullable=False)
  billing_email = Column(String, nullable=True)
  billing_contact_name = Column(String, nullable=True)
  payment_terms = Column(String, default="net_30", nullable=False)

  created_at = Column(
    DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
  )
  updated_at = Column(
    DateTime,
    default=lambda: datetime.now(timezone.utc),
    onupdate=lambda: datetime.now(timezone.utc),
    nullable=False,
  )

  def __repr__(self) -> str:
    return f"<BillingCustomer user_id={self.user_id} invoice_enabled={self.invoice_billing_enabled}>"

  @classmethod
  def get_or_create(cls, user_id: str, session: Session) -> "BillingCustomer":
    """Get or create billing customer for a user."""
    customer = session.query(cls).filter(cls.user_id == user_id).first()

    if not customer:
      customer = cls(user_id=user_id)
      session.add(customer)
      session.commit()
      session.refresh(customer)
      logger.info(f"Created billing customer for user {user_id}")

    return customer

  @classmethod
  def get_by_user_id(
    cls, user_id: str, session: Session
  ) -> Optional["BillingCustomer"]:
    """Get billing customer by user ID."""
    return session.query(cls).filter(cls.user_id == user_id).first()

  @classmethod
  def get_by_stripe_customer_id(
    cls, stripe_customer_id: str, session: Session
  ) -> Optional["BillingCustomer"]:
    """Get billing customer by Stripe customer ID."""
    return (
      session.query(cls).filter(cls.stripe_customer_id == stripe_customer_id).first()
    )

  def can_provision_resources(
    self, environment: str, billing_enabled: bool
  ) -> tuple[bool, Optional[str]]:
    """Check if customer can provision new resources.

    Returns:
        Tuple of (can_provision, error_message)
    """
    if not billing_enabled:
      return (True, None)

    if environment == "dev":
      return (True, None)

    if self.invoice_billing_enabled:
      return (True, None)

    if self.has_payment_method:
      return (True, None)

    return (
      False,
      "Payment method required. Please add a credit card or contact support for invoice billing options.",
    )

  def update_payment_method(
    self, stripe_payment_method_id: str, session: Session
  ) -> None:
    """Update default payment method."""
    self.default_payment_method_id = stripe_payment_method_id
    self.has_payment_method = True
    self.updated_at = datetime.now(timezone.utc)
    session.commit()
    session.refresh(self)
    logger.info(f"Updated payment method for billing customer {self.user_id}")

  def enable_invoice_billing(
    self,
    billing_email: str,
    billing_contact_name: str,
    payment_terms: str,
    session: Session,
  ) -> None:
    """Enable invoice billing for enterprise customers."""
    self.invoice_billing_enabled = True
    self.billing_email = billing_email
    self.billing_contact_name = billing_contact_name
    self.payment_terms = payment_terms
    self.updated_at = datetime.now(timezone.utc)
    session.commit()
    session.refresh(self)
    logger.info(f"Enabled invoice billing for customer {self.user_id}")
