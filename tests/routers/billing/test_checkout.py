"""Tests for billing checkout endpoints."""

import pytest
from unittest.mock import Mock, patch
from fastapi import HTTPException
from robosystems.routers.billing.checkout import (
  create_checkout_session,
  get_checkout_status,
)
from robosystems.models.billing import BillingCustomer, BillingSubscription
from robosystems.models.iam import User
from robosystems.models.api.billing.checkout import CreateCheckoutRequest


class TestCreateCheckoutSession:
  """Tests for create_checkout_session endpoint."""

  @pytest.fixture
  def mock_user(self):
    user = Mock(spec=User)
    user.id = "user_123"
    user.email = "test@example.com"
    return user

  @pytest.fixture
  def mock_db(self):
    return Mock()

  @pytest.fixture
  def checkout_request(self):
    return CreateCheckoutRequest(
      resource_type="graph",
      plan_name="standard",
      resource_config={"tier": "standard"},
    )

  @pytest.mark.asyncio
  @patch("robosystems.routers.billing.checkout.BillingCustomer.get_or_create")
  @patch("robosystems.routers.billing.checkout.BillingConfig.get_subscription_plan")
  @patch("robosystems.routers.billing.checkout.BillingSubscription.create_subscription")
  @patch("robosystems.routers.billing.checkout.get_payment_provider")
  async def test_create_checkout_session_success(
    self,
    mock_get_provider,
    mock_create_sub,
    mock_get_plan,
    mock_get_customer,
    mock_user,
    mock_db,
    checkout_request,
  ):
    """Test successful checkout session creation."""
    mock_customer = Mock(spec=BillingCustomer)
    mock_customer.invoice_billing_enabled = False
    mock_customer.has_payment_method = False
    mock_customer.stripe_customer_id = "cus_123"
    mock_get_customer.return_value = mock_customer

    mock_get_plan.return_value = {"base_price_cents": 2999}

    mock_subscription = Mock(spec=BillingSubscription)
    mock_subscription.id = "sub_456"
    mock_subscription.subscription_metadata = {}
    mock_create_sub.return_value = mock_subscription

    mock_provider = Mock()
    mock_provider.get_or_create_price.return_value = "price_789"
    mock_provider.create_checkout_session.return_value = {
      "checkout_url": "https://checkout.stripe.com/test",
      "session_id": "cs_test",
    }
    mock_get_provider.return_value = mock_provider

    result = await create_checkout_session(checkout_request, mock_user, mock_db, None)

    assert result.checkout_url == "https://checkout.stripe.com/test"
    assert result.session_id == "cs_test"
    assert result.subscription_id == "sub_456"
    assert result.requires_checkout is True

    mock_provider.create_checkout_session.assert_called_once()
    assert mock_subscription.status == "pending_payment"
    assert mock_subscription.payment_provider == "stripe"

  @pytest.mark.asyncio
  @patch("robosystems.routers.billing.checkout.BillingCustomer.get_or_create")
  async def test_create_checkout_session_enterprise_customer_rejected(
    self, mock_get_customer, mock_user, mock_db, checkout_request
  ):
    """Test that enterprise customers cannot use checkout."""
    mock_customer = Mock(spec=BillingCustomer)
    mock_customer.invoice_billing_enabled = True
    mock_get_customer.return_value = mock_customer

    with pytest.raises(HTTPException) as exc:
      await create_checkout_session(checkout_request, mock_user, mock_db, None)

    assert exc.value.status_code == 400
    assert "enterprise customers" in exc.value.detail.lower()

  @pytest.mark.asyncio
  @patch("robosystems.routers.billing.checkout.BillingCustomer.get_or_create")
  async def test_create_checkout_session_payment_method_already_exists(
    self, mock_get_customer, mock_user, mock_db, checkout_request
  ):
    """Test that customers with payment method shouldn't use checkout."""
    mock_customer = Mock(spec=BillingCustomer)
    mock_customer.invoice_billing_enabled = False
    mock_customer.has_payment_method = True
    mock_get_customer.return_value = mock_customer

    with pytest.raises(HTTPException) as exc:
      await create_checkout_session(checkout_request, mock_user, mock_db, None)

    assert exc.value.status_code == 400
    assert "already on file" in exc.value.detail.lower()

  @pytest.mark.asyncio
  @patch("robosystems.routers.billing.checkout.BillingCustomer.get_or_create")
  @patch("robosystems.routers.billing.checkout.BillingConfig.get_subscription_plan")
  async def test_create_checkout_session_invalid_plan(
    self, mock_get_plan, mock_get_customer, mock_user, mock_db, checkout_request
  ):
    """Test handling of invalid plan name."""
    mock_customer = Mock(spec=BillingCustomer)
    mock_customer.invoice_billing_enabled = False
    mock_customer.has_payment_method = False
    mock_get_customer.return_value = mock_customer

    mock_get_plan.return_value = None

    with pytest.raises(HTTPException) as exc:
      await create_checkout_session(checkout_request, mock_user, mock_db, None)

    assert exc.value.status_code == 400
    assert "Invalid plan" in exc.value.detail

  @pytest.mark.asyncio
  @patch("robosystems.routers.billing.checkout.BillingCustomer.get_or_create")
  @patch("robosystems.routers.billing.checkout.BillingConfig.get_subscription_plan")
  @patch("robosystems.routers.billing.checkout.BillingSubscription.create_subscription")
  @patch("robosystems.routers.billing.checkout.get_payment_provider")
  async def test_create_checkout_session_creates_stripe_customer(
    self,
    mock_get_provider,
    mock_create_sub,
    mock_get_plan,
    mock_get_customer,
    mock_user,
    mock_db,
    checkout_request,
  ):
    """Test that Stripe customer is created if not exists."""
    mock_customer = Mock(spec=BillingCustomer)
    mock_customer.invoice_billing_enabled = False
    mock_customer.has_payment_method = False
    mock_customer.stripe_customer_id = None
    mock_get_customer.return_value = mock_customer

    mock_get_plan.return_value = {"base_price_cents": 2999}

    mock_subscription = Mock(spec=BillingSubscription)
    mock_subscription.id = "sub_456"
    mock_subscription.subscription_metadata = {}
    mock_create_sub.return_value = mock_subscription

    mock_provider = Mock()
    mock_provider.create_customer.return_value = "cus_new_123"
    mock_provider.get_or_create_price.return_value = "price_789"
    mock_provider.create_checkout_session.return_value = {
      "checkout_url": "https://checkout.stripe.com/test",
      "session_id": "cs_test",
    }
    mock_get_provider.return_value = mock_provider

    await create_checkout_session(checkout_request, mock_user, mock_db, None)

    mock_provider.create_customer.assert_called_once_with(mock_user.id, mock_user.email)
    assert mock_customer.stripe_customer_id == "cus_new_123"

  @pytest.mark.asyncio
  @patch("robosystems.routers.billing.checkout.BillingCustomer.get_or_create")
  @patch("robosystems.routers.billing.checkout.BillingConfig.get_subscription_plan")
  @patch("robosystems.routers.billing.checkout.BillingSubscription.create_subscription")
  @patch("robosystems.routers.billing.checkout.get_payment_provider")
  async def test_create_checkout_session_price_error(
    self,
    mock_get_provider,
    mock_create_sub,
    mock_get_plan,
    mock_get_customer,
    mock_user,
    mock_db,
    checkout_request,
  ):
    """Test handling of price creation errors."""
    mock_customer = Mock(spec=BillingCustomer)
    mock_customer.invoice_billing_enabled = False
    mock_customer.has_payment_method = False
    mock_customer.stripe_customer_id = "cus_123"
    mock_get_customer.return_value = mock_customer

    mock_get_plan.return_value = {"base_price_cents": 2999}

    mock_subscription = Mock(spec=BillingSubscription)
    mock_subscription.id = "sub_456"
    mock_subscription.subscription_metadata = {}
    mock_create_sub.return_value = mock_subscription

    mock_provider = Mock()
    mock_provider.get_or_create_price.side_effect = ValueError("Price not found")
    mock_get_provider.return_value = mock_provider

    with pytest.raises(HTTPException) as exc:
      await create_checkout_session(checkout_request, mock_user, mock_db, None)

    assert exc.value.status_code == 500
    assert "Payment configuration error" in exc.value.detail

  @pytest.mark.asyncio
  @patch("robosystems.routers.billing.checkout.BillingCustomer.get_or_create")
  @patch("robosystems.routers.billing.checkout.BillingConfig.get_repository_plan")
  @patch("robosystems.routers.billing.checkout.BillingSubscription.create_subscription")
  @patch("robosystems.routers.billing.checkout.get_payment_provider")
  async def test_create_checkout_session_repository_type(
    self,
    mock_get_provider,
    mock_create_sub,
    mock_get_plan,
    mock_get_customer,
    mock_user,
    mock_db,
  ):
    """Test checkout session for repository subscriptions."""
    repo_request = CreateCheckoutRequest(
      resource_type="repository",
      plan_name="starter",
      resource_config={"repository_name": "sec"},
    )

    mock_customer = Mock(spec=BillingCustomer)
    mock_customer.invoice_billing_enabled = False
    mock_customer.has_payment_method = False
    mock_customer.stripe_customer_id = "cus_123"
    mock_get_customer.return_value = mock_customer

    mock_get_plan.return_value = {"price_cents": 999}

    mock_subscription = Mock(spec=BillingSubscription)
    mock_subscription.id = "sub_456"
    mock_subscription.subscription_metadata = {}
    mock_create_sub.return_value = mock_subscription

    mock_provider = Mock()
    mock_provider.get_or_create_price.return_value = "price_789"
    mock_provider.create_checkout_session.return_value = {
      "checkout_url": "https://checkout.stripe.com/test",
      "session_id": "cs_test",
    }
    mock_get_provider.return_value = mock_provider

    result = await create_checkout_session(repo_request, mock_user, mock_db, None)

    assert result.checkout_url == "https://checkout.stripe.com/test"
    mock_get_plan.assert_called_once_with("sec", "starter")


class TestGetCheckoutStatus:
  """Tests for get_checkout_status endpoint."""

  @pytest.fixture
  def mock_user(self):
    user = Mock(spec=User)
    user.id = "user_123"
    return user

  @pytest.fixture
  def mock_db(self):
    return Mock()

  @pytest.mark.asyncio
  @patch(
    "robosystems.routers.billing.checkout.BillingSubscription.get_by_provider_subscription_id"
  )
  async def test_get_checkout_status_success(self, mock_get_sub, mock_user, mock_db):
    """Test successful checkout status retrieval."""
    mock_subscription = Mock(spec=BillingSubscription)
    mock_subscription.id = "sub_456"
    mock_subscription.billing_customer_user_id = "user_123"
    mock_subscription.status = "provisioning"
    mock_subscription.resource_id = "kg_789"
    mock_subscription.subscription_metadata = {"operation_id": "op_123"}
    mock_get_sub.return_value = mock_subscription

    result = await get_checkout_status("cs_test", mock_user, mock_db, None)

    assert result.status == "provisioning"
    assert result.subscription_id == "sub_456"
    assert result.resource_id == "kg_789"
    assert result.operation_id == "op_123"

  @pytest.mark.asyncio
  @patch(
    "robosystems.routers.billing.checkout.BillingSubscription.get_by_provider_subscription_id"
  )
  async def test_get_checkout_status_not_found(self, mock_get_sub, mock_user, mock_db):
    """Test checkout status when session not found."""
    mock_get_sub.return_value = None

    with pytest.raises(HTTPException) as exc:
      await get_checkout_status("cs_invalid", mock_user, mock_db, None)

    assert exc.value.status_code == 404
    assert "not found" in exc.value.detail.lower()

  @pytest.mark.asyncio
  @patch(
    "robosystems.routers.billing.checkout.BillingSubscription.get_by_provider_subscription_id"
  )
  async def test_get_checkout_status_unauthorized(
    self, mock_get_sub, mock_user, mock_db
  ):
    """Test checkout status authorization check."""
    mock_subscription = Mock(spec=BillingSubscription)
    mock_subscription.billing_customer_user_id = "different_user"
    mock_get_sub.return_value = mock_subscription

    with pytest.raises(HTTPException) as exc:
      await get_checkout_status("cs_test", mock_user, mock_db, None)

    assert exc.value.status_code == 403
    assert "not authorized" in exc.value.detail.lower()

  @pytest.mark.asyncio
  @patch(
    "robosystems.routers.billing.checkout.BillingSubscription.get_by_provider_subscription_id"
  )
  async def test_get_checkout_status_with_error(self, mock_get_sub, mock_user, mock_db):
    """Test checkout status when subscription has error."""
    mock_subscription = Mock(spec=BillingSubscription)
    mock_subscription.id = "sub_456"
    mock_subscription.billing_customer_user_id = "user_123"
    mock_subscription.status = "failed"
    mock_subscription.resource_id = None
    mock_subscription.subscription_metadata = {
      "error": "Provisioning failed: insufficient resources"
    }
    mock_get_sub.return_value = mock_subscription

    result = await get_checkout_status("cs_test", mock_user, mock_db, None)

    assert result.status == "failed"
    assert result.error == "Provisioning failed: insufficient resources"
    assert result.resource_id is None

  @pytest.mark.asyncio
  @patch(
    "robosystems.routers.billing.checkout.BillingSubscription.get_by_provider_subscription_id"
  )
  async def test_get_checkout_status_pending_payment(
    self, mock_get_sub, mock_user, mock_db
  ):
    """Test checkout status for pending payment."""
    mock_subscription = Mock(spec=BillingSubscription)
    mock_subscription.id = "sub_456"
    mock_subscription.billing_customer_user_id = "user_123"
    mock_subscription.status = "pending_payment"
    mock_subscription.resource_id = None
    mock_subscription.subscription_metadata = {}
    mock_get_sub.return_value = mock_subscription

    result = await get_checkout_status("cs_test", mock_user, mock_db, None)

    assert result.status == "pending_payment"
    assert result.resource_id is None
    assert result.error is None
