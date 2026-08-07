"""Billing audit log - consolidated audit trail for all billing events."""

from datetime import UTC, datetime
from enum import Enum

from sqlalchemy import Column, DateTime, ForeignKey, Index, String, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session

from robosystems.database import Base
from robosystems.logger import get_logger
from robosystems.utils.ulid import generate_prefixed_ulid

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
  SUBSCRIPTION_RENEWED = "subscription_renewed"

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
  """Audit trail for customer, subscription, invoice, and payment events.

  Also serves as the webhook idempotency store: a partial unique index over
  ``event_data->>'event_id'`` and ``->>'provider'`` makes a re-delivered
  webhook a constraint violation rather than a second effect.
  """

  __tablename__ = "billing_audit_logs"

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("baud"))

  event_type = Column(String, nullable=False)
  event_timestamp = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

  org_id = Column(String, ForeignKey("orgs.id"), nullable=True)

  subscription_id = Column(
    String, ForeignKey("billing_subscriptions.id"), nullable=True
  )

  invoice_id = Column(String, ForeignKey("billing_invoices.id"), nullable=True)

  event_data = Column(JSONB, nullable=True)
  description = Column(String, nullable=False)

  actor_user_id = Column(String, ForeignKey("users.id"), nullable=True)
  actor_type = Column(String, nullable=False)
  actor_ip = Column(String, nullable=True)

  created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)

  __table_args__ = (
    Index("idx_billing_audit_org", "org_id"),
    Index("idx_billing_audit_subscription", "subscription_id"),
    Index("idx_billing_audit_invoice", "invoice_id"),
    Index("idx_billing_audit_event_type", "event_type"),
    Index("idx_billing_audit_timestamp", "event_timestamp"),
    Index("idx_billing_audit_actor", "actor_user_id"),
    Index(
      "idx_webhook_idempotency",
      text("(event_data->>'event_id')"),
      text("(event_data->>'provider')"),
      unique=True,
      postgresql_where=text("event_type = 'webhook_received'"),
    ),
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
    org_id: str | None = None,
    subscription_id: str | None = None,
    invoice_id: str | None = None,
    event_data: dict | None = None,
    actor_user_id: str | None = None,
    actor_ip: str | None = None,
  ) -> "BillingAuditLog":
    """Create an audit log entry.

    Requires at least one of ``org_id`` / ``subscription_id`` / ``invoice_id``
    / ``actor_user_id``; an entry referencing nothing is not traceable.
    """
    if not any([org_id, subscription_id, invoice_id, actor_user_id]):
      raise ValueError(
        "Audit log must reference at least one entity: "
        "org_id, subscription_id, invoice_id, or actor_user_id"
      )

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
    event_type: BillingEventType | None = None,
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
    event_type: BillingEventType | None = None,
    limit: int = 100,
  ) -> list["BillingAuditLog"]:
    """Get audit history for a user (looks up user's org first)."""
    from robosystems.models.core.org import OrgUser

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
    """Whether this provider event has already been recorded (idempotency)."""
    from sqlalchemy import and_

    return (
      session.query(cls)
      .filter(
        and_(
          cls.event_type == BillingEventType.WEBHOOK_RECEIVED.value,
          cls.event_data["provider"].astext == provider,
          cls.event_data["event_id"].astext == event_id,
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
    """Record a webhook event, making a redelivery detectable.

    ``event_type`` is the provider's own type string (e.g.
    ``checkout.session.completed``), stored under ``webhook_type``; the row's
    ``event_type`` column is always ``webhook_received``.
    """
    # Different provider events carry entity references in different places.
    metadata = event_data.get("metadata", {})
    subscription_id = metadata.get("subscription_id")
    actor_user_id = metadata.get("user_id")

    stored_event_data = {
      "provider": provider,
      "event_id": event_id,
      "webhook_type": event_type,
      "data": event_data,
    }

    # Many webhook events (invoice.created, invoice.paid) carry no entity
    # references at all. log_event() insists on one, and actor_user_id is a FK
    # to users so it cannot be synthesized — insert the row directly instead.
    if not any([subscription_id, actor_user_id]):
      audit_log = cls(
        event_type=BillingEventType.WEBHOOK_RECEIVED.value,
        description=f"{provider} webhook: {event_type}",
        actor_type=f"{provider}_webhook",
        event_data=stored_event_data,
      )
      session.add(audit_log)
      session.commit()

      logger.info(
        f"Billing audit log: {BillingEventType.WEBHOOK_RECEIVED.value}",
        extra={"event_type": BillingEventType.WEBHOOK_RECEIVED.value},
      )
      return audit_log

    return cls.log_event(
      session=session,
      event_type=BillingEventType.WEBHOOK_RECEIVED,
      description=f"{provider} webhook: {event_type}",
      actor_type=f"{provider}_webhook",
      subscription_id=subscription_id,
      actor_user_id=actor_user_id,
      event_data=stored_event_data,
    )
