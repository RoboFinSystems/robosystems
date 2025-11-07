"""Comprehensive tests for BillingCustomer model."""

import pytest
import uuid
from sqlalchemy.orm import Session

from robosystems.models.billing import BillingCustomer
from robosystems.models.iam import User


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


class TestBillingCustomerCreation:
  """Tests for billing customer creation."""

  def test_get_or_create_new_customer(self, db_session: Session, test_user):
    """Test creating a new billing customer."""
    customer = BillingCustomer.get_or_create(user_id=test_user.id, session=db_session)

    assert customer.user_id == test_user.id
    assert customer.stripe_customer_id is None
    assert customer.has_payment_method is False
    assert customer.invoice_billing_enabled is False
    assert customer.payment_terms == "net_30"
    assert customer.created_at is not None

  def test_get_or_create_existing_customer(self, db_session: Session, test_user):
    """Test retrieving an existing billing customer."""
    customer1 = BillingCustomer.get_or_create(user_id=test_user.id, session=db_session)
    created_at = customer1.created_at

    customer2 = BillingCustomer.get_or_create(user_id=test_user.id, session=db_session)

    assert customer1.user_id == customer2.user_id
    assert customer2.created_at == created_at

  def test_get_by_user_id_found(self, db_session: Session, test_user):
    """Test getting customer by user ID when exists."""
    BillingCustomer.get_or_create(user_id=test_user.id, session=db_session)

    customer = BillingCustomer.get_by_user_id(user_id=test_user.id, session=db_session)

    assert customer is not None
    assert customer.user_id == test_user.id

  def test_get_by_user_id_not_found(self, db_session: Session):
    """Test getting customer by user ID when doesn't exist."""
    customer = BillingCustomer.get_by_user_id(
      user_id="nonexistent_user", session=db_session
    )

    assert customer is None


class TestBillingCustomerProvisioningChecks:
  """Tests for can_provision_resources method."""

  def test_can_provision_when_billing_disabled(self, db_session: Session, test_user):
    """Test provisioning allowed when billing is disabled."""
    customer = BillingCustomer.get_or_create(user_id=test_user.id, session=db_session)

    can_provision, error_msg = customer.can_provision_resources(
      environment="prod", billing_enabled=False
    )

    assert can_provision is True
    assert error_msg is None

  def test_can_provision_in_dev_environment(self, db_session: Session, test_user):
    """Test provisioning allowed in dev environment."""
    customer = BillingCustomer.get_or_create(user_id=test_user.id, session=db_session)

    can_provision, error_msg = customer.can_provision_resources(
      environment="dev", billing_enabled=True
    )

    assert can_provision is True
    assert error_msg is None

  def test_can_provision_with_invoice_billing(self, db_session: Session, test_user):
    """Test provisioning allowed for invoice billing customers."""
    customer = BillingCustomer.get_or_create(user_id=test_user.id, session=db_session)
    customer.enable_invoice_billing(
      billing_email="billing@example.com",
      billing_contact_name="Finance Team",
      payment_terms="net_30",
      session=db_session,
    )

    can_provision, error_msg = customer.can_provision_resources(
      environment="prod", billing_enabled=True
    )

    assert can_provision is True
    assert error_msg is None

  def test_can_provision_with_payment_method(self, db_session: Session, test_user):
    """Test provisioning allowed with payment method on file."""
    customer = BillingCustomer.get_or_create(user_id=test_user.id, session=db_session)
    customer.update_payment_method(
      stripe_payment_method_id="pm_123456", session=db_session
    )

    can_provision, error_msg = customer.can_provision_resources(
      environment="prod", billing_enabled=True
    )

    assert can_provision is True
    assert error_msg is None

  def test_cannot_provision_without_payment_method(
    self, db_session: Session, test_user
  ):
    """Test provisioning denied without payment method."""
    customer = BillingCustomer.get_or_create(user_id=test_user.id, session=db_session)

    can_provision, error_msg = customer.can_provision_resources(
      environment="prod", billing_enabled=True
    )

    assert can_provision is False
    assert error_msg is not None
    assert "Payment method required" in error_msg

  def test_can_provision_in_staging_environment(self, db_session: Session, test_user):
    """Test provisioning behavior in staging environment."""
    customer = BillingCustomer.get_or_create(user_id=test_user.id, session=db_session)

    can_provision, error_msg = customer.can_provision_resources(
      environment="staging", billing_enabled=True
    )

    assert can_provision is False
    assert error_msg is not None


class TestBillingCustomerPaymentMethods:
  """Tests for payment method management."""

  def test_update_payment_method(self, db_session: Session, test_user):
    """Test updating payment method."""
    customer = BillingCustomer.get_or_create(user_id=test_user.id, session=db_session)

    payment_method_id = "pm_1234567890"
    customer.update_payment_method(
      stripe_payment_method_id=payment_method_id, session=db_session
    )

    assert customer.default_payment_method_id == payment_method_id
    assert customer.has_payment_method is True

  def test_update_payment_method_overwrites_previous(
    self, db_session: Session, test_user
  ):
    """Test that updating payment method overwrites the previous one."""
    customer = BillingCustomer.get_or_create(user_id=test_user.id, session=db_session)

    customer.update_payment_method(
      stripe_payment_method_id="pm_old", session=db_session
    )
    customer.update_payment_method(
      stripe_payment_method_id="pm_new", session=db_session
    )

    assert customer.default_payment_method_id == "pm_new"
    assert customer.has_payment_method is True


class TestBillingCustomerInvoiceBilling:
  """Tests for invoice billing management."""

  def test_enable_invoice_billing(self, db_session: Session, test_user):
    """Test enabling invoice billing for a customer."""
    customer = BillingCustomer.get_or_create(user_id=test_user.id, session=db_session)

    customer.enable_invoice_billing(
      billing_email="billing@enterprise.com",
      billing_contact_name="John Doe",
      payment_terms="net_60",
      session=db_session,
    )

    assert customer.invoice_billing_enabled is True
    assert customer.billing_email == "billing@enterprise.com"
    assert customer.billing_contact_name == "John Doe"
    assert customer.payment_terms == "net_60"

  def test_enable_invoice_billing_with_different_terms(
    self, db_session: Session, test_user
  ):
    """Test invoice billing with various payment terms."""
    customer = BillingCustomer.get_or_create(user_id=test_user.id, session=db_session)

    for terms in ["net_15", "net_30", "net_60", "net_90"]:
      customer.enable_invoice_billing(
        billing_email="billing@example.com",
        billing_contact_name="Finance",
        payment_terms=terms,
        session=db_session,
      )
      assert customer.payment_terms == terms

  def test_invoice_billing_updates_timestamp(self, db_session: Session, test_user):
    """Test that enabling invoice billing updates the timestamp."""
    customer = BillingCustomer.get_or_create(user_id=test_user.id, session=db_session)
    original_updated_at = customer.updated_at

    customer.enable_invoice_billing(
      billing_email="billing@example.com",
      billing_contact_name="Finance",
      payment_terms="net_30",
      session=db_session,
    )

    assert customer.updated_at > original_updated_at


class TestBillingCustomerRepr:
  """Tests for billing customer string representation."""

  def test_repr_format_without_invoice_billing(self, db_session: Session, test_user):
    """Test customer __repr__ format without invoice billing."""
    customer = BillingCustomer.get_or_create(user_id=test_user.id, session=db_session)

    repr_str = repr(customer)

    assert "BillingCustomer" in repr_str
    assert test_user.id in repr_str
    assert "invoice_enabled=False" in repr_str

  def test_repr_format_with_invoice_billing(self, db_session: Session, test_user):
    """Test customer __repr__ format with invoice billing enabled."""
    customer = BillingCustomer.get_or_create(user_id=test_user.id, session=db_session)
    customer.enable_invoice_billing(
      billing_email="billing@example.com",
      billing_contact_name="Finance",
      payment_terms="net_30",
      session=db_session,
    )

    repr_str = repr(customer)

    assert "BillingCustomer" in repr_str
    assert "invoice_enabled=True" in repr_str


class TestBillingCustomerStripeIntegration:
  """Tests for Stripe integration fields."""

  def test_stripe_customer_id_assignment(self, db_session: Session, test_user):
    """Test that Stripe customer ID can be stored."""
    customer = BillingCustomer.get_or_create(user_id=test_user.id, session=db_session)

    customer.stripe_customer_id = "cus_1234567890"
    db_session.commit()
    db_session.refresh(customer)

    assert customer.stripe_customer_id == "cus_1234567890"

  def test_stripe_customer_id_uniqueness(self, db_session: Session):
    """Test that Stripe customer IDs must be unique."""
    unique_id1 = str(uuid.uuid4())[:8]
    unique_id2 = str(uuid.uuid4())[:8]
    user1 = User(
      id=f"user1_{unique_id1}",
      email=f"user1+{unique_id1}@example.com",
      name="User 1",
      password_hash="hash1",
    )
    user2 = User(
      id=f"user2_{unique_id2}",
      email=f"user2+{unique_id2}@example.com",
      name="User 2",
      password_hash="hash2",
    )
    db_session.add_all([user1, user2])
    db_session.commit()

    customer1 = BillingCustomer.get_or_create(user_id=user1.id, session=db_session)
    customer1.stripe_customer_id = "cus_same_id"
    db_session.commit()

    customer2 = BillingCustomer.get_or_create(user_id=user2.id, session=db_session)
    customer2.stripe_customer_id = "cus_same_id"

    with pytest.raises(Exception):
      db_session.commit()


class TestBillingCustomerEdgeCases:
  """Tests for edge cases and error conditions."""

  def test_multiple_payment_method_updates(self, db_session: Session, test_user):
    """Test multiple payment method updates."""
    customer = BillingCustomer.get_or_create(user_id=test_user.id, session=db_session)

    for i in range(5):
      customer.update_payment_method(
        stripe_payment_method_id=f"pm_{i}", session=db_session
      )

    assert customer.default_payment_method_id == "pm_4"
    assert customer.has_payment_method is True

  def test_enable_invoice_billing_multiple_times(self, db_session: Session, test_user):
    """Test enabling invoice billing multiple times with different settings."""
    customer = BillingCustomer.get_or_create(user_id=test_user.id, session=db_session)

    customer.enable_invoice_billing(
      billing_email="old@example.com",
      billing_contact_name="Old Contact",
      payment_terms="net_15",
      session=db_session,
    )

    customer.enable_invoice_billing(
      billing_email="new@example.com",
      billing_contact_name="New Contact",
      payment_terms="net_90",
      session=db_session,
    )

    assert customer.billing_email == "new@example.com"
    assert customer.billing_contact_name == "New Contact"
    assert customer.payment_terms == "net_90"
