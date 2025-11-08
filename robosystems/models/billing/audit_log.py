"""Billing audit log - consolidated audit trail for all billing events."""

import secrets
from datetime import datetime, timezone
from typing import Optional
from enum import Enum
from sqlalchemy import Column, String, DateTime, JSON, ForeignKey, Index
from sqlalchemy.orm import Session

from ...database import Base
from ...logger import get_logger

logger = get_logger(__name__)


class BillingEventType(str, Enum):
  """Types of billing audit events."""

  CUSTOMER_CREATED = "customer_created"
  PAYMENT_METHOD_ADDED = "payment_method_added"
  PAYMENT_METHOD_REMOVED = "payment_method_removed"
  PAYMENT_METHOD_UPDATED = "payment_method_updated"

  SUBSCRIPTION_CREATED = "subscription_created"
  SUBSCRIPTION_ACTIVATED = "subscription_activated"
  SUBSCRIPTION_PAUSED = "subscription_paused"
  SUBSCRIPTION_RESUMED = "subscription_resumed"
  SUBSCRIPTION_CANCELED = "subscription_canceled"
  SUBSCRIPTION_EXPIRED = "subscription_expired"

  PLAN_UPGRADED = "plan_upgraded"
  PLAN_DOWNGRADED = "plan_downgraded"

  INVOICE_GENERATED = "invoice_generated"
  INVOICE_SENT = "invoice_sent"
  INVOICE_PAID = "invoice_paid"
  INVOICE_OVERDUE = "invoice_overdue"
  INVOICE_VOIDED = "invoice_voided"

  PAYMENT_SUCCEEDED = "payment_succeeded"
  PAYMENT_FAILED = "payment_failed"
  REFUND_PROCESSED = "refund_processed"

  INVOICE_BILLING_ENABLED = "invoice_billing_enabled"
  INVOICE_BILLING_DISABLED = "invoice_billing_disabled"

  WEBHOOK_RECEIVED = "webhook_received"

  ADMIN_OVERRIDE = "admin_override"
  DISCOUNT_APPLIED = "discount_applied"


class BillingAuditLog(Base):
  """Consolidated audit log for all billing events.

  Tracks customer, subscription, invoice, and payment events
  for security, compliance, and debugging purposes.
  """

  __tablename__ = "billing_audit_logs"

  id = Column(
    String, primary_key=True, default=lambda: f"baud_{secrets.token_urlsafe(16)}"
  )

  event_type = Column(String, nullable=False)
  event_timestamp = Column(
    DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
  )

  org_id = Column(String, ForeignKey("orgs.id"), nullable=True)

  subscription_id = Column(
    String, ForeignKey("billing_subscriptions.id"), nullable=True
  )

  invoice_id = Column(String, ForeignKey("billing_invoices.id"), nullable=True)

  event_data = Column(JSON, nullable=True)
  description = Column(String, nullable=False)

  actor_user_id = Column(String, ForeignKey("users.id"), nullable=True)
  actor_type = Column(String, nullable=False)
  actor_ip = Column(String, nullable=True)

  created_at = Column(
    DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
  )

  __table_args__ = (
    Index("idx_billing_audit_org", "org_id"),
    Index("idx_billing_audit_subscription", "subscription_id"),
    Index("idx_billing_audit_invoice", "invoice_id"),
    Index("idx_billing_audit_event_type", "event_type"),
    Index("idx_billing_audit_timestamp", "event_timestamp"),
    Index("idx_billing_audit_actor", "actor_user_id"),
  )

  def __repr__(self) -> str:
    return f"<BillingAuditLog {self.event_type} at {self.event_timestamp}>"

  @classmethod
  def log_event(
    cls,
    session: Session,
    event_type: BillingEventType | str,
    description: str,
    actor_type: str = "system",
    org_id: Optional[str] = None,
    subscription_id: Optional[str] = None,
    invoice_id: Optional[str] = None,
    event_data: Optional[dict] = None,
    actor_user_id: Optional[str] = None,
    actor_ip: Optional[str] = None,
  ) -> "BillingAuditLog":
    """Create an audit log entry."""
    event_type_str = (
      event_type.value if isinstance(event_type, BillingEventType) else event_type
    )
    audit_log = cls(
      event_type=event_type_str,
      description=description,
      actor_type=actor_type,
      org_id=org_id,
      subscription_id=subscription_id,
      invoice_id=invoice_id,
      event_data=event_data,
      actor_user_id=actor_user_id,
      actor_ip=actor_ip,
    )

    session.add(audit_log)
    session.commit()

    logger.info(
      f"Billing audit log: {event_type_str}",
      extra={
        "event_type": event_type_str,
        "org_id": org_id,
        "subscription_id": subscription_id,
        "invoice_id": invoice_id,
        "actor_type": actor_type,
      },
    )

    return audit_log

  @classmethod
  def get_org_history(
    cls,
    session: Session,
    org_id: str,
    event_type: Optional[BillingEventType] = None,
    limit: int = 100,
  ) -> list["BillingAuditLog"]:
    """Get audit history for an organization."""
    query = session.query(cls).filter(cls.org_id == org_id)

    if event_type:
      query = query.filter(cls.event_type == event_type.value)

    return query.order_by(cls.event_timestamp.desc()).limit(limit).all()

  @classmethod
  def get_user_history(
    cls,
    session: Session,
    user_id: str,
    event_type: Optional[BillingEventType] = None,
    limit: int = 100,
  ) -> list["BillingAuditLog"]:
    """Get audit history for a user (looks up user's org first)."""
    from ..iam import OrgUser

    org_user = session.query(OrgUser).filter(OrgUser.user_id == user_id).first()

    if not org_user:
      return []

    return cls.get_org_history(session, org_user.org_id, event_type, limit)

  @classmethod
  def get_subscription_history(
    cls,
    session: Session,
    subscription_id: str,
    limit: int = 100,
  ) -> list["BillingAuditLog"]:
    """Get audit history for a subscription."""
    return (
      session.query(cls)
      .filter(cls.subscription_id == subscription_id)
      .order_by(cls.event_timestamp.desc())
      .limit(limit)
      .all()
    )

  @classmethod
  def get_invoice_history(
    cls,
    session: Session,
    invoice_id: str,
  ) -> list["BillingAuditLog"]:
    """Get audit history for an invoice."""
    return (
      session.query(cls)
      .filter(cls.invoice_id == invoice_id)
      .order_by(cls.event_timestamp.desc())
      .all()
    )

  @classmethod
  def is_webhook_processed(cls, event_id: str, provider: str, session: Session) -> bool:
    """Check if a webhook event has already been processed.

    Uses the audit log to track webhook events for idempotency.

    Args:
        event_id: The webhook event ID from the payment provider
        provider: Payment provider name (e.g., 'stripe')
        session: Database session

    Returns:
        True if event already processed, False otherwise
    """
    from sqlalchemy import and_

    return (
      session.query(cls)
      .filter(
        and_(
          cls.event_type == BillingEventType.WEBHOOK_RECEIVED.value,
          cls.event_data.op("->>")("provider") == provider,
          cls.event_data.op("->>")("event_id") == event_id,
        )
      )
      .first()
      is not None
    )

  @classmethod
  def mark_webhook_processed(
    cls,
    event_id: str,
    provider: str,
    event_type: str,
    event_data: dict,
    session: Session,
  ) -> "BillingAuditLog":
    """Mark a webhook event as processed in the audit log.

    Args:
        event_id: The webhook event ID from the payment provider
        provider: Payment provider name (e.g., 'stripe')
        event_type: The webhook event type (e.g., 'checkout.session.completed')
        event_data: Full event data from the webhook
        session: Database session

    Returns:
        The created audit log entry
    """
    return cls.log_event(
      session=session,
      event_type=BillingEventType.WEBHOOK_RECEIVED,
      description=f"{provider} webhook: {event_type}",
      actor_type=f"{provider}_webhook",
      event_data={
        "provider": provider,
        "event_id": event_id,
        "webhook_type": event_type,
        "data": event_data,
      },
    )
