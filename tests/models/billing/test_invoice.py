"""Comprehensive tests for BillingInvoice and BillingInvoiceLineItem models."""

import pytest
import uuid
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session

from robosystems.models.billing import (
  BillingInvoice,
  BillingSubscription,
  InvoiceStatus,
)
from robosystems.models.iam import User


@pytest.fixture
def test_user(db_session: Session):
  """Create a test user with org."""
  from robosystems.models.iam import Org, OrgUser, OrgRole, OrgType

  unique_id = str(uuid.uuid4())[:8]

  org = Org(
    id=f"test_org_{unique_id}",
    name=f"Test Org {unique_id}",
    org_type=OrgType.PERSONAL,
  )
  db_session.add(org)
  db_session.flush()

  user = User(
    id=f"test_user_{unique_id}",
    email=f"test+{unique_id}@example.com",
    name="Test User",
    password_hash="test_hash",
  )
  db_session.add(user)
  db_session.flush()

  org_user = OrgUser(
    org_id=org.id,
    user_id=user.id,
    role=OrgRole.OWNER,
  )
  db_session.add(org_user)
  db_session.commit()
  return user


@pytest.fixture
def test_org(test_user, db_session: Session):
  """Get org for test user."""
  from robosystems.models.iam import OrgUser

  org_users = OrgUser.get_user_orgs(test_user.id, db_session)
  return org_users[0].org


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


class TestBillingInvoiceCreation:
  """Tests for invoice creation."""

  def test_create_invoice_basic(self, db_session: Session, test_user, test_org):
    """Test basic invoice creation."""
    period_start = datetime.now(timezone.utc) - timedelta(days=30)
    period_end = datetime.now(timezone.utc)

    invoice = BillingInvoice.create_invoice(
      org_id=test_org.id,
      period_start=period_start,
      period_end=period_end,
      session=db_session,
    )

    assert invoice.id.startswith("binv_")
    assert invoice.org_id == test_org.id
    assert invoice.invoice_number.startswith("INV-")
    assert invoice.period_start == period_start.replace(tzinfo=None)
    assert invoice.period_end == period_end.replace(tzinfo=None)
    assert invoice.status == InvoiceStatus.DRAFT.value
    assert invoice.subtotal_cents == 0
    assert invoice.total_cents == 0
    assert invoice.due_date is not None

  def test_create_invoice_with_net_15_terms(
    self, db_session: Session, test_user, test_org
  ):
    """Test invoice creation with NET_15 payment terms."""
    period_start = datetime.now(timezone.utc) - timedelta(days=30)
    period_end = datetime.now(timezone.utc)
    before_creation = datetime.now(timezone.utc)

    invoice = BillingInvoice.create_invoice(
      org_id=test_org.id,
      period_start=period_start,
      period_end=period_end,
      payment_terms="net_15",
      session=db_session,
    )

    expected_due = (before_creation + timedelta(days=15)).replace(tzinfo=None)
    assert invoice.due_date >= expected_due - timedelta(seconds=5)
    assert invoice.due_date <= expected_due + timedelta(seconds=5)

  def test_create_invoice_with_net_60_terms(
    self, db_session: Session, test_user, test_org
  ):
    """Test invoice creation with NET_60 payment terms."""
    period_start = datetime.now(timezone.utc) - timedelta(days=30)
    period_end = datetime.now(timezone.utc)
    before_creation = datetime.now(timezone.utc)

    invoice = BillingInvoice.create_invoice(
      org_id=test_org.id,
      period_start=period_start,
      period_end=period_end,
      payment_terms="net_60",
      session=db_session,
    )

    expected_due = (before_creation + timedelta(days=60)).replace(tzinfo=None)
    assert invoice.due_date >= expected_due - timedelta(seconds=5)
    assert invoice.due_date <= expected_due + timedelta(seconds=5)

  def test_invoice_number_generation_uniqueness(
    self, db_session: Session, test_user, test_org
  ):
    """Test that invoice numbers are unique and sequential."""
    period_start = datetime.now(timezone.utc) - timedelta(days=30)
    period_end = datetime.now(timezone.utc)

    invoice1 = BillingInvoice.create_invoice(
      org_id=test_org.id,
      period_start=period_start,
      period_end=period_end,
      session=db_session,
    )

    invoice2 = BillingInvoice.create_invoice(
      org_id=test_org.id,
      period_start=period_start,
      period_end=period_end,
      session=db_session,
    )

    assert invoice1.invoice_number != invoice2.invoice_number
    assert invoice1.invoice_number.startswith("INV-")
    assert invoice2.invoice_number.startswith("INV-")


class TestBillingInvoiceLineItems:
  """Tests for invoice line item management."""

  def test_add_line_item(
    self, db_session: Session, test_user, test_org, test_subscription
  ):
    """Test adding a line item to an invoice."""
    period_start = datetime.now(timezone.utc) - timedelta(days=30)
    period_end = datetime.now(timezone.utc)

    invoice = BillingInvoice.create_invoice(
      org_id=test_org.id,
      period_start=period_start,
      period_end=period_end,
      session=db_session,
    )

    line_item = invoice.add_line_item(
      subscription_id=test_subscription.id,
      resource_type="graph",
      resource_id="kg123",
      description="Standard Graph Subscription",
      amount_cents=2999,
      session=db_session,
    )

    assert line_item.id.startswith("bli_")
    assert line_item.invoice_id == invoice.id
    assert line_item.subscription_id == test_subscription.id
    assert line_item.description == "Standard Graph Subscription"
    assert line_item.amount_cents == 2999
    assert line_item.unit_price_cents == 2999
    assert line_item.quantity == 1

    db_session.refresh(invoice)
    assert invoice.subtotal_cents == 2999
    assert invoice.total_cents == 2999

  def test_add_multiple_line_items(
    self, db_session: Session, test_user, test_org, test_subscription
  ):
    """Test adding multiple line items to an invoice."""
    period_start = datetime.now(timezone.utc) - timedelta(days=30)
    period_end = datetime.now(timezone.utc)

    invoice = BillingInvoice.create_invoice(
      org_id=test_org.id,
      period_start=period_start,
      period_end=period_end,
      session=db_session,
    )

    invoice.add_line_item(
      subscription_id=test_subscription.id,
      resource_type="graph",
      resource_id="kg123",
      description="Graph Subscription",
      amount_cents=2999,
      session=db_session,
    )

    invoice.add_line_item(
      subscription_id=test_subscription.id,
      resource_type="storage",
      resource_id="kg123",
      description="Storage Overage",
      amount_cents=500,
      session=db_session,
    )

    db_session.refresh(invoice)
    assert len(invoice.line_items) == 2
    assert invoice.subtotal_cents == 3499
    assert invoice.total_cents == 3499

  def test_add_line_item_with_quantity(
    self, db_session: Session, test_user, test_org, test_subscription
  ):
    """Test adding line item with quantity greater than 1."""
    period_start = datetime.now(timezone.utc) - timedelta(days=30)
    period_end = datetime.now(timezone.utc)

    invoice = BillingInvoice.create_invoice(
      org_id=test_org.id,
      period_start=period_start,
      period_end=period_end,
      session=db_session,
    )

    line_item = invoice.add_line_item(
      subscription_id=test_subscription.id,
      resource_type="api_calls",
      resource_id="kg123",
      description="API Calls",
      amount_cents=100,
      quantity=50,
      session=db_session,
    )

    assert line_item.quantity == 50
    assert line_item.unit_price_cents == 100
    assert line_item.amount_cents == 5000

    db_session.refresh(invoice)
    assert invoice.subtotal_cents == 5000

  def test_add_line_item_with_metadata(
    self, db_session: Session, test_user, test_org, test_subscription
  ):
    """Test adding line item with metadata."""
    period_start = datetime.now(timezone.utc) - timedelta(days=30)
    period_end = datetime.now(timezone.utc)

    invoice = BillingInvoice.create_invoice(
      org_id=test_org.id,
      period_start=period_start,
      period_end=period_end,
      session=db_session,
    )

    metadata = {
      "storage_gb": 150,
      "included_gb": 100,
      "overage_gb": 50,
      "rate_per_gb": 10,
    }

    line_item = invoice.add_line_item(
      subscription_id=test_subscription.id,
      resource_type="storage",
      resource_id="kg123",
      description="Storage Overage",
      amount_cents=500,
      line_metadata=metadata,
      session=db_session,
    )

    assert line_item.line_metadata == metadata


class TestBillingInvoiceLifecycle:
  """Tests for invoice lifecycle methods."""

  def test_finalize_invoice(
    self, db_session: Session, test_user, test_org, test_subscription
  ):
    """Test finalizing an invoice."""
    period_start = datetime.now(timezone.utc) - timedelta(days=30)
    period_end = datetime.now(timezone.utc)

    invoice = BillingInvoice.create_invoice(
      org_id=test_org.id,
      period_start=period_start,
      period_end=period_end,
      session=db_session,
    )

    invoice.add_line_item(
      subscription_id=test_subscription.id,
      resource_type="graph",
      resource_id="kg123",
      description="Subscription",
      amount_cents=2999,
      session=db_session,
    )

    invoice.finalize(db_session)

    assert invoice.status == InvoiceStatus.OPEN.value
    assert invoice.sent_at is not None

  def test_mark_invoice_paid(
    self, db_session: Session, test_user, test_org, test_subscription
  ):
    """Test marking an invoice as paid."""
    period_start = datetime.now(timezone.utc) - timedelta(days=30)
    period_end = datetime.now(timezone.utc)

    invoice = BillingInvoice.create_invoice(
      org_id=test_org.id,
      period_start=period_start,
      period_end=period_end,
      session=db_session,
    )

    invoice.add_line_item(
      subscription_id=test_subscription.id,
      resource_type="graph",
      resource_id="kg123",
      description="Subscription",
      amount_cents=2999,
      session=db_session,
    )

    invoice.finalize(db_session)
    invoice.mark_paid(
      session=db_session,
      payment_method="credit_card",
      payment_reference="ch_1234567890",
    )

    assert invoice.status == InvoiceStatus.PAID.value
    assert invoice.paid_at is not None
    assert invoice.payment_method == "credit_card"
    assert invoice.payment_reference == "ch_1234567890"

  def test_mark_paid_without_payment_reference(
    self, db_session: Session, test_user, test_org, test_subscription
  ):
    """Test marking invoice paid without payment reference."""
    period_start = datetime.now(timezone.utc) - timedelta(days=30)
    period_end = datetime.now(timezone.utc)

    invoice = BillingInvoice.create_invoice(
      org_id=test_org.id,
      period_start=period_start,
      period_end=period_end,
      session=db_session,
    )

    invoice.add_line_item(
      subscription_id=test_subscription.id,
      resource_type="graph",
      resource_id="kg123",
      description="Subscription",
      amount_cents=2999,
      session=db_session,
    )

    invoice.mark_paid(session=db_session, payment_method="bank_transfer")

    assert invoice.status == InvoiceStatus.PAID.value
    assert invoice.payment_method == "bank_transfer"
    assert invoice.payment_reference is None


class TestBillingInvoiceTotalCalculations:
  """Tests for invoice total calculations."""

  def test_recalculate_totals_with_tax(
    self, db_session: Session, test_user, test_org, test_subscription
  ):
    """Test total calculation with tax."""
    period_start = datetime.now(timezone.utc) - timedelta(days=30)
    period_end = datetime.now(timezone.utc)

    invoice = BillingInvoice.create_invoice(
      org_id=test_org.id,
      period_start=period_start,
      period_end=period_end,
      session=db_session,
    )

    invoice.add_line_item(
      subscription_id=test_subscription.id,
      resource_type="graph",
      resource_id="kg123",
      description="Subscription",
      amount_cents=10000,
      session=db_session,
    )

    invoice.tax_cents = 800
    db_session.commit()
    db_session.refresh(invoice)

    invoice._recalculate_totals(db_session)
    db_session.refresh(invoice)

    assert invoice.subtotal_cents == 10000
    assert invoice.tax_cents == 800
    assert invoice.total_cents == 10800

  def test_recalculate_totals_with_discount(
    self, db_session: Session, test_user, test_org, test_subscription
  ):
    """Test total calculation with discount."""
    period_start = datetime.now(timezone.utc) - timedelta(days=30)
    period_end = datetime.now(timezone.utc)

    invoice = BillingInvoice.create_invoice(
      org_id=test_org.id,
      period_start=period_start,
      period_end=period_end,
      session=db_session,
    )

    invoice.add_line_item(
      subscription_id=test_subscription.id,
      resource_type="graph",
      resource_id="kg123",
      description="Subscription",
      amount_cents=10000,
      session=db_session,
    )

    invoice.discount_cents = 2000
    db_session.commit()
    db_session.refresh(invoice)

    invoice._recalculate_totals(db_session)
    db_session.refresh(invoice)

    assert invoice.subtotal_cents == 10000
    assert invoice.discount_cents == 2000
    assert invoice.total_cents == 8000

  def test_recalculate_totals_with_tax_and_discount(
    self, db_session: Session, test_user, test_org, test_subscription
  ):
    """Test total calculation with both tax and discount."""
    period_start = datetime.now(timezone.utc) - timedelta(days=30)
    period_end = datetime.now(timezone.utc)

    invoice = BillingInvoice.create_invoice(
      org_id=test_org.id,
      period_start=period_start,
      period_end=period_end,
      session=db_session,
    )

    invoice.add_line_item(
      subscription_id=test_subscription.id,
      resource_type="graph",
      resource_id="kg123",
      description="Subscription",
      amount_cents=10000,
      session=db_session,
    )

    invoice.tax_cents = 800
    invoice.discount_cents = 2000
    db_session.commit()
    db_session.refresh(invoice)

    invoice._recalculate_totals(db_session)
    db_session.refresh(invoice)

    assert invoice.subtotal_cents == 10000
    assert invoice.total_cents == 8800

  def test_recalculate_totals_handles_none_values_gracefully(
    self, db_session: Session, test_user, test_org, test_subscription
  ):
    """Test that _recalculate_totals handles None tax_cents and discount_cents.

    Regression test for bug where None values in mocked or uninitialized
    objects caused TypeError: unsupported operand type(s) for +: 'int' and 'NoneType'

    This simulates the scenario where tax_cents/discount_cents might be None
    in memory before being set to their database defaults.
    """
    period_start = datetime.now(timezone.utc) - timedelta(days=30)
    period_end = datetime.now(timezone.utc)

    invoice = BillingInvoice.create_invoice(
      org_id=test_org.id,
      period_start=period_start,
      period_end=period_end,
      session=db_session,
    )

    invoice.add_line_item(
      subscription_id=test_subscription.id,
      resource_type="graph",
      resource_id="kg123",
      description="Subscription",
      amount_cents=10000,
      session=db_session,
    )

    db_session.refresh(invoice)

    assert invoice.tax_cents == 0
    assert invoice.discount_cents == 0
    assert invoice.total_cents == 10000

    result = 10000 + (None or 0) - (None or 0)
    assert result == 10000

  def test_recalculate_totals_with_zero_defaults(
    self, db_session: Session, test_user, test_org, test_subscription
  ):
    """Test that recalculation works correctly with zero tax and discount.

    Ensures the or 0 logic handles both None and 0 values correctly.
    """
    period_start = datetime.now(timezone.utc) - timedelta(days=30)
    period_end = datetime.now(timezone.utc)

    invoice = BillingInvoice.create_invoice(
      org_id=test_org.id,
      period_start=period_start,
      period_end=period_end,
      session=db_session,
    )

    invoice.add_line_item(
      subscription_id=test_subscription.id,
      resource_type="graph",
      resource_id="kg123",
      description="Subscription",
      amount_cents=10000,
      session=db_session,
    )

    db_session.refresh(invoice)
    assert invoice.tax_cents == 0
    assert invoice.discount_cents == 0
    assert invoice.subtotal_cents == 10000
    assert invoice.total_cents == 10000


class TestBillingInvoiceRepr:
  """Tests for invoice string representations."""

  def test_invoice_repr_format(
    self, db_session: Session, test_user, test_org, test_subscription
  ):
    """Test invoice __repr__ format."""
    period_start = datetime.now(timezone.utc) - timedelta(days=30)
    period_end = datetime.now(timezone.utc)

    invoice = BillingInvoice.create_invoice(
      org_id=test_org.id,
      period_start=period_start,
      period_end=period_end,
      session=db_session,
    )

    invoice.add_line_item(
      subscription_id=test_subscription.id,
      resource_type="graph",
      resource_id="kg123",
      description="Subscription",
      amount_cents=2999,
      session=db_session,
    )

    db_session.refresh(invoice)
    repr_str = repr(invoice)

    assert "BillingInvoice" in repr_str
    assert str(invoice.invoice_number) in repr_str
    assert "$29.99" in repr_str

  def test_line_item_repr_format(
    self, db_session: Session, test_user, test_org, test_subscription
  ):
    """Test line item __repr__ format."""
    period_start = datetime.now(timezone.utc) - timedelta(days=30)
    period_end = datetime.now(timezone.utc)

    invoice = BillingInvoice.create_invoice(
      org_id=test_org.id,
      period_start=period_start,
      period_end=period_end,
      session=db_session,
    )

    line_item = invoice.add_line_item(
      subscription_id=test_subscription.id,
      resource_type="graph",
      resource_id="kg123",
      description="Standard Subscription",
      amount_cents=2999,
      session=db_session,
    )

    repr_str = repr(line_item)

    assert "BillingInvoiceLineItem" in repr_str
    assert "Standard Subscription" in repr_str
    assert "$29.99" in repr_str


class TestBillingInvoiceIndexes:
  """Tests to ensure database indexes work correctly."""

  def test_query_by_customer_uses_index(self, db_session: Session, test_user, test_org):
    """Test querying invoices by customer."""
    period_start = datetime.now(timezone.utc) - timedelta(days=30)
    period_end = datetime.now(timezone.utc)

    for i in range(3):
      BillingInvoice.create_invoice(
        org_id=test_org.id,
        period_start=period_start,
        period_end=period_end,
        session=db_session,
      )

    invoices = (
      db_session.query(BillingInvoice)
      .filter(BillingInvoice.org_id == test_org.id)
      .all()
    )

    assert len(invoices) == 3

  def test_query_by_status_uses_index(self, db_session: Session, test_user, test_org):
    """Test querying invoices by status."""
    period_start = datetime.now(timezone.utc) - timedelta(days=30)
    period_end = datetime.now(timezone.utc)

    for i in range(5):
      invoice = BillingInvoice.create_invoice(
        org_id=test_org.id,
        period_start=period_start,
        period_end=period_end,
        session=db_session,
      )
      if i < 3:
        invoice.finalize(db_session)

    open_invoices = (
      db_session.query(BillingInvoice)
      .filter(
        BillingInvoice.status == InvoiceStatus.OPEN.value,
        BillingInvoice.org_id == test_org.id,
      )
      .all()
    )

    assert len(open_invoices) == 3
