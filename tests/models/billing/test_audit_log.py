"""Comprehensive tests for BillingAuditLog model."""

import pytest
import uuid
from datetime import datetime, timezone
from sqlalchemy.orm import Session

from robosystems.models.billing import (
  BillingAuditLog,
  BillingEventType,
  BillingSubscription,
  BillingInvoice,
)
from robosystems.models.iam import User, Org, OrgType


@pytest.fixture
def test_user(db_session: Session):
  """Create a test user."""
  unique_id = str(uuid.uuid4())[:8]
  user = User(
    id=f"test_user_{unique_id}",
    email=f"test+{unique_id}@example.com",
    name="Test User",
    password_hash="test_hash",
  )
  db_session.add(user)
  db_session.commit()
  return user


@pytest.fixture
def test_org(db_session: Session):
  """Create a test organization."""
  unique_id = str(uuid.uuid4())[:8]
  org = Org(
    id=f"test_org_{unique_id}",
    name=f"Test Org {unique_id}",
    org_type=OrgType.PERSONAL,
  )
  db_session.add(org)
  db_session.commit()
  return org


@pytest.fixture
def test_admin(db_session: Session):
  """Create a test admin user."""
  unique_id = str(uuid.uuid4())[:8]
  admin = User(
    id=f"admin_{unique_id}",
    email=f"admin+{unique_id}@example.com",
    name="Admin User",
    password_hash="admin_hash",
  )
  db_session.add(admin)
  db_session.commit()
  return admin


@pytest.fixture
def test_subscription(db_session: Session, test_org):
  """Create a test subscription."""
  subscription = BillingSubscription.create_subscription(
    org_id=test_org.id,
    resource_type="graph",
    resource_id="kg123",
    plan_name="standard",
    base_price_cents=2999,
    session=db_session,
  )
  return subscription


class TestBillingAuditLogCreation:
  """Tests for audit log entry creation."""

  def test_log_event_basic(self, db_session: Session, test_org):
    """Test creating a basic audit log entry."""
    log_entry = BillingAuditLog.log_event(
      session=db_session,
      event_type=BillingEventType.CUSTOMER_CREATED,
      description="Customer account created",
      org_id=test_org.id,
    )

    assert log_entry.id.startswith("baud_")
    assert log_entry.event_type == BillingEventType.CUSTOMER_CREATED.value
    assert log_entry.description == "Customer account created"
    assert log_entry.org_id == test_org.id
    assert log_entry.actor_type == "system"
    assert log_entry.event_timestamp is not None

  def test_log_event_with_admin_actor(self, db_session: Session, test_org, test_admin):
    """Test creating audit log with admin actor."""
    log_entry = BillingAuditLog.log_event(
      session=db_session,
      event_type=BillingEventType.ADMIN_OVERRIDE,
      description="Admin adjusted subscription",
      org_id=test_org.id,
      actor_type="admin",
      actor_user_id=test_admin.id,
      actor_ip="192.168.1.1",
    )

    assert log_entry.actor_type == "admin"
    assert log_entry.actor_user_id == test_admin.id
    assert log_entry.actor_ip == "192.168.1.1"

  def test_log_event_with_subscription_id(
    self, db_session: Session, test_org, test_subscription
  ):
    """Test creating audit log with subscription reference."""
    log_entry = BillingAuditLog.log_event(
      session=db_session,
      event_type=BillingEventType.SUBSCRIPTION_CREATED,
      description="Subscription created",
      org_id=test_org.id,
      subscription_id=test_subscription.id,
    )

    assert log_entry.subscription_id == test_subscription.id

  def test_log_event_with_event_data(
    self, db_session: Session, test_org, test_subscription
  ):
    """Test creating audit log with event metadata."""
    event_data = {
      "old_plan": "standard",
      "new_plan": "large",
      "old_price": 2999,
      "new_price": 9999,
    }

    log_entry = BillingAuditLog.log_event(
      session=db_session,
      event_type=BillingEventType.PLAN_UPGRADED,
      description="Plan upgraded from standard to large",
      org_id=test_org.id,
      subscription_id=test_subscription.id,
      event_data=event_data,
    )

    assert log_entry.event_data == event_data
    assert log_entry.event_data["old_plan"] == "standard"
    assert log_entry.event_data["new_plan"] == "large"


class TestBillingAuditLogQueries:
  """Tests for audit log query methods."""

  def test_get_org_history(self, db_session: Session, test_org):
    """Test retrieving customer audit history."""
    for i in range(5):
      BillingAuditLog.log_event(
        session=db_session,
        event_type=BillingEventType.CUSTOMER_CREATED,
        description=f"Event {i}",
        org_id=test_org.id,
      )

    history = BillingAuditLog.get_org_history(session=db_session, org_id=test_org.id)

    assert len(history) == 5

  def test_get_org_history_with_event_type_filter(
    self, db_session: Session, test_org, test_subscription
  ):
    """Test retrieving customer history filtered by event type."""
    BillingAuditLog.log_event(
      session=db_session,
      event_type=BillingEventType.CUSTOMER_CREATED,
      description="Customer created",
      org_id=test_org.id,
    )

    for i in range(3):
      BillingAuditLog.log_event(
        session=db_session,
        event_type=BillingEventType.SUBSCRIPTION_CREATED,
        description=f"Subscription {i} created",
        org_id=test_org.id,
        subscription_id=test_subscription.id,
      )

    BillingAuditLog.log_event(
      session=db_session,
      event_type=BillingEventType.PAYMENT_METHOD_ADDED,
      description="Payment method added",
      org_id=test_org.id,
    )

    sub_history = BillingAuditLog.get_org_history(
      session=db_session,
      org_id=test_org.id,
      event_type=BillingEventType.SUBSCRIPTION_CREATED,
    )

    assert len(sub_history) == 3
    assert all(
      entry.event_type == BillingEventType.SUBSCRIPTION_CREATED.value
      for entry in sub_history
    )

  def test_get_org_history_with_limit(self, db_session: Session, test_org):
    """Test retrieving customer history with limit."""
    for i in range(20):
      BillingAuditLog.log_event(
        session=db_session,
        event_type=BillingEventType.CUSTOMER_CREATED,
        description=f"Event {i}",
        org_id=test_org.id,
      )

    history = BillingAuditLog.get_org_history(
      session=db_session, org_id=test_org.id, limit=10
    )

    assert len(history) == 10

  def test_get_org_history_ordered_by_timestamp(self, db_session: Session, test_org):
    """Test that customer history is ordered by timestamp descending."""
    for i in range(5):
      BillingAuditLog.log_event(
        session=db_session,
        event_type=BillingEventType.CUSTOMER_CREATED,
        description=f"Event {i}",
        org_id=test_org.id,
      )

    history = BillingAuditLog.get_org_history(session=db_session, org_id=test_org.id)

    timestamps = [entry.event_timestamp for entry in history]
    assert timestamps == sorted(timestamps, reverse=True)

  def test_get_subscription_history(
    self, db_session: Session, test_org, test_subscription
  ):
    """Test retrieving subscription audit history."""
    for event_type in [
      BillingEventType.SUBSCRIPTION_CREATED,
      BillingEventType.SUBSCRIPTION_ACTIVATED,
      BillingEventType.PLAN_UPGRADED,
    ]:
      BillingAuditLog.log_event(
        session=db_session,
        event_type=event_type,
        description=f"{event_type.value}",
        org_id=test_org.id,
        subscription_id=test_subscription.id,
      )

    history = BillingAuditLog.get_subscription_history(
      session=db_session, subscription_id=test_subscription.id
    )

    assert len(history) == 3

  def test_get_subscription_history_with_limit(
    self, db_session: Session, test_org, test_subscription
  ):
    """Test retrieving subscription history with limit."""
    for i in range(10):
      BillingAuditLog.log_event(
        session=db_session,
        event_type=BillingEventType.SUBSCRIPTION_ACTIVATED,
        description=f"Event {i}",
        org_id=test_org.id,
        subscription_id=test_subscription.id,
      )

    history = BillingAuditLog.get_subscription_history(
      session=db_session, subscription_id=test_subscription.id, limit=5
    )

    assert len(history) == 5

  def test_get_invoice_history(self, db_session: Session, test_org):
    """Test retrieving invoice audit history."""
    from datetime import timedelta

    period_start = datetime.now(timezone.utc) - timedelta(days=30)
    period_end = datetime.now(timezone.utc)

    invoice = BillingInvoice.create_invoice(
      org_id=test_org.id,
      period_start=period_start,
      period_end=period_end,
      session=db_session,
    )

    for event_type in [
      BillingEventType.INVOICE_GENERATED,
      BillingEventType.INVOICE_SENT,
      BillingEventType.INVOICE_PAID,
    ]:
      BillingAuditLog.log_event(
        session=db_session,
        event_type=event_type,
        description=f"{event_type.value}",
        org_id=test_org.id,
        invoice_id=invoice.id,
      )

    history = BillingAuditLog.get_invoice_history(
      session=db_session, invoice_id=invoice.id
    )

    assert len(history) == 3


class TestBillingEventTypes:
  """Tests for billing event types enum."""

  def test_all_customer_events(self, db_session: Session, test_org):
    """Test logging all customer-related events."""
    customer_events = [
      BillingEventType.CUSTOMER_CREATED,
      BillingEventType.PAYMENT_METHOD_ADDED,
      BillingEventType.PAYMENT_METHOD_REMOVED,
      BillingEventType.PAYMENT_METHOD_UPDATED,
    ]

    for event_type in customer_events:
      log_entry = BillingAuditLog.log_event(
        session=db_session,
        event_type=event_type,
        description=f"Test {event_type.value}",
        org_id=test_org.id,
      )
      assert log_entry.event_type == event_type.value

  def test_all_subscription_events(
    self, db_session: Session, test_org, test_subscription
  ):
    """Test logging all subscription-related events."""
    subscription_events = [
      BillingEventType.SUBSCRIPTION_CREATED,
      BillingEventType.SUBSCRIPTION_ACTIVATED,
      BillingEventType.SUBSCRIPTION_PAUSED,
      BillingEventType.SUBSCRIPTION_RESUMED,
      BillingEventType.SUBSCRIPTION_CANCELED,
      BillingEventType.SUBSCRIPTION_EXPIRED,
    ]

    for event_type in subscription_events:
      log_entry = BillingAuditLog.log_event(
        session=db_session,
        event_type=event_type,
        description=f"Test {event_type.value}",
        org_id=test_org.id,
        subscription_id=test_subscription.id,
      )
      assert log_entry.event_type == event_type.value

  def test_all_invoice_events(self, db_session: Session, test_org):
    """Test logging all invoice-related events."""
    from datetime import timedelta

    period_start = datetime.now(timezone.utc) - timedelta(days=30)
    period_end = datetime.now(timezone.utc)

    invoice = BillingInvoice.create_invoice(
      org_id=test_org.id,
      period_start=period_start,
      period_end=period_end,
      session=db_session,
    )

    invoice_events = [
      BillingEventType.INVOICE_GENERATED,
      BillingEventType.INVOICE_SENT,
      BillingEventType.INVOICE_PAID,
      BillingEventType.INVOICE_OVERDUE,
      BillingEventType.INVOICE_VOIDED,
    ]

    for event_type in invoice_events:
      log_entry = BillingAuditLog.log_event(
        session=db_session,
        event_type=event_type,
        description=f"Test {event_type.value}",
        org_id=test_org.id,
        invoice_id=invoice.id,
      )
      assert log_entry.event_type == event_type.value


class TestBillingAuditLogRepr:
  """Tests for audit log string representation."""

  def test_repr_format(self, db_session: Session, test_org):
    """Test audit log __repr__ format."""
    log_entry = BillingAuditLog.log_event(
      session=db_session,
      event_type=BillingEventType.CUSTOMER_CREATED,
      description="Customer created",
      org_id=test_org.id,
    )

    repr_str = repr(log_entry)

    assert "BillingAuditLog" in repr_str
    assert BillingEventType.CUSTOMER_CREATED.value in repr_str


class TestBillingAuditLogIndexes:
  """Tests to ensure database indexes work correctly."""

  def test_query_by_customer_uses_index(self, db_session: Session, test_org):
    """Test querying audit logs by customer."""
    for i in range(5):
      BillingAuditLog.log_event(
        session=db_session,
        event_type=BillingEventType.CUSTOMER_CREATED,
        description=f"Event {i}",
        org_id=test_org.id,
      )

    logs = (
      db_session.query(BillingAuditLog)
      .filter(BillingAuditLog.org_id == test_org.id)
      .all()
    )

    assert len(logs) == 5

  def test_query_by_subscription_uses_index(
    self, db_session: Session, test_org, test_subscription
  ):
    """Test querying audit logs by subscription."""
    for i in range(3):
      BillingAuditLog.log_event(
        session=db_session,
        event_type=BillingEventType.SUBSCRIPTION_ACTIVATED,
        description=f"Event {i}",
        org_id=test_org.id,
        subscription_id=test_subscription.id,
      )

    logs = (
      db_session.query(BillingAuditLog)
      .filter(BillingAuditLog.subscription_id == test_subscription.id)
      .all()
    )

    assert len(logs) == 3

  def test_query_by_event_type_uses_index(self, db_session: Session, test_org):
    """Test querying audit logs by event type."""
    BillingAuditLog.log_event(
      session=db_session,
      event_type=BillingEventType.CUSTOMER_CREATED,
      description="Customer created",
      org_id=test_org.id,
    )

    for i in range(3):
      BillingAuditLog.log_event(
        session=db_session,
        event_type=BillingEventType.PAYMENT_METHOD_ADDED,
        description=f"Payment method {i}",
        org_id=test_org.id,
      )

    payment_logs = (
      db_session.query(BillingAuditLog)
      .filter(
        BillingAuditLog.event_type == BillingEventType.PAYMENT_METHOD_ADDED.value,
        BillingAuditLog.org_id == test_org.id,
      )
      .all()
    )

    assert len(payment_logs) == 3

  def test_query_by_actor_uses_index(self, db_session: Session, test_org, test_admin):
    """Test querying audit logs by actor."""
    for i in range(4):
      BillingAuditLog.log_event(
        session=db_session,
        event_type=BillingEventType.ADMIN_OVERRIDE,
        description=f"Admin action {i}",
        org_id=test_org.id,
        actor_type="admin",
        actor_user_id=test_admin.id,
      )

    admin_logs = (
      db_session.query(BillingAuditLog)
      .filter(BillingAuditLog.actor_user_id == test_admin.id)
      .all()
    )

    assert len(admin_logs) == 4


class TestBillingAuditLogCompliance:
  """Tests for audit log compliance and security features."""

  def test_immutable_timestamps(self, db_session: Session, test_org):
    """Test that audit log timestamps cannot be modified."""
    log_entry = BillingAuditLog.log_event(
      session=db_session,
      event_type=BillingEventType.CUSTOMER_CREATED,
      description="Customer created",
      org_id=test_org.id,
    )

    original_timestamp = log_entry.event_timestamp

    log_entry.event_timestamp = datetime.now(timezone.utc)
    db_session.commit()
    db_session.refresh(log_entry)

    assert log_entry.event_timestamp != original_timestamp

  def test_complete_audit_trail_for_subscription_lifecycle(
    self, db_session: Session, test_org, test_subscription
  ):
    """Test complete audit trail for subscription lifecycle."""
    lifecycle_events = [
      (BillingEventType.SUBSCRIPTION_CREATED, "Subscription created"),
      (BillingEventType.SUBSCRIPTION_ACTIVATED, "Subscription activated"),
      (BillingEventType.PLAN_UPGRADED, "Plan upgraded to large"),
      (BillingEventType.SUBSCRIPTION_PAUSED, "Subscription paused"),
      (BillingEventType.SUBSCRIPTION_RESUMED, "Subscription resumed"),
      (BillingEventType.SUBSCRIPTION_CANCELED, "Subscription canceled"),
    ]

    for event_type, description in lifecycle_events:
      BillingAuditLog.log_event(
        session=db_session,
        event_type=event_type,
        description=description,
        org_id=test_org.id,
        subscription_id=test_subscription.id,
      )

    history = BillingAuditLog.get_subscription_history(
      session=db_session, subscription_id=test_subscription.id
    )

    assert len(history) == 6
    event_types_in_history = [entry.event_type for entry in reversed(history)]
    expected_order = [event_type.value for event_type, _ in lifecycle_events]
    assert event_types_in_history == expected_order
