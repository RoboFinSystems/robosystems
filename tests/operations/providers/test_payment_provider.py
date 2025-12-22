"""Comprehensive tests for payment provider abstraction layer."""

from unittest.mock import Mock, patch

import pytest

from robosystems.operations.providers.payment_provider import (
  PaymentProvider,
  StripePaymentProvider,
)


class TestPaymentProviderAbstractInterface:
  """Test the abstract PaymentProvider interface."""

  def test_payment_provider_cannot_be_instantiated(self):
    """Test that PaymentProvider abstract class cannot be instantiated directly."""
    with pytest.raises(TypeError):
      PaymentProvider()  # type: ignore[abstract]

  def test_payment_provider_requires_all_methods(self):
    """Test that concrete implementations must implement all abstract methods."""

    class IncompleteProvider(PaymentProvider):
      pass

    with pytest.raises(TypeError):
      IncompleteProvider()  # type: ignore[abstract]


class TestStripeCustomerOperations:
  """Tests for Stripe customer management."""

  @pytest.fixture
  def stripe_provider(self):
    """Create Stripe provider with mocked Stripe."""
    with patch("robosystems.operations.providers.payment_provider.env"):
      with patch.object(StripePaymentProvider, "__init__", lambda self: None):
        provider = StripePaymentProvider()
        provider.stripe = Mock()
        provider.stripe.Customer = Mock()
        provider._redis_client = None
        return provider

  def test_create_customer_success(self, stripe_provider):
    """Test successful customer creation in Stripe."""
    mock_customer = Mock()
    mock_customer.id = "cus_test123"
    stripe_provider.stripe.Customer.create.return_value = mock_customer

    result = stripe_provider.create_customer("user_123", "test@example.com")

    assert result == "cus_test123"
    stripe_provider.stripe.Customer.create.assert_called_once_with(
      email="test@example.com", metadata={"robosystems_user_id": "user_123"}
    )

  def test_create_customer_with_metadata(self, stripe_provider):
    """Test that customer metadata includes user ID."""
    mock_customer = Mock()
    mock_customer.id = "cus_test456"
    stripe_provider.stripe.Customer.create.return_value = mock_customer

    stripe_provider.create_customer("user_456", "user@example.com")

    call_args = stripe_provider.stripe.Customer.create.call_args
    assert call_args[1]["metadata"]["robosystems_user_id"] == "user_456"

  def test_create_customer_stripe_api_error(self, stripe_provider):
    """Test handling of Stripe API errors during customer creation."""
    from stripe.error import StripeError

    stripe_provider.stripe.Customer.create.side_effect = StripeError(
      "API error occurred"
    )

    with pytest.raises(StripeError):
      stripe_provider.create_customer("user_789", "error@example.com")


class TestStripeCheckoutSessions:
  """Tests for Stripe checkout session creation."""

  @pytest.fixture
  def stripe_provider(self):
    """Create Stripe provider with mocked Stripe."""
    with patch("robosystems.operations.providers.payment_provider.env") as mock_env:
      mock_env.ROBOSYSTEMS_URL = "https://robosystems.example.com"
      with patch.object(StripePaymentProvider, "__init__", lambda self: None):
        provider = StripePaymentProvider()
        provider.stripe = Mock()
        provider._redis_client = None
        provider.stripe.checkout = Mock()
        provider.stripe.checkout.Session = Mock()
        return provider

  def test_create_checkout_session_success(self, stripe_provider):
    """Test successful checkout session creation."""
    mock_session = Mock()
    mock_session.id = "cs_test_123"
    mock_session.url = "https://checkout.stripe.com/c/pay/cs_test_123"
    stripe_provider.stripe.checkout.Session.create.return_value = mock_session

    result = stripe_provider.create_checkout_session(
      customer_id="cus_123",
      price_id="price_456",
      metadata={"plan": "standard"},
    )

    assert result["checkout_url"] == "https://checkout.stripe.com/c/pay/cs_test_123"
    assert result["session_id"] == "cs_test_123"

  def test_checkout_session_includes_metadata(self, stripe_provider):
    """Test that checkout session includes custom metadata."""
    mock_session = Mock()
    mock_session.id = "cs_test_456"
    mock_session.url = "https://checkout.stripe.com/test"
    stripe_provider.stripe.checkout.Session.create.return_value = mock_session

    metadata = {
      "user_id": "user_123",
      "plan_name": "enterprise",
      "resource_id": "kg_789",
    }
    stripe_provider.create_checkout_session("cus_123", "price_456", metadata)

    call_args = stripe_provider.stripe.checkout.Session.create.call_args
    assert call_args[1]["metadata"] == metadata

  @patch("robosystems.operations.providers.payment_provider.env")
  def test_checkout_session_urls_use_environment(self, mock_env, stripe_provider):
    """Test that success/cancel URLs use environment configuration."""
    mock_env.ROBOSYSTEMS_URL = "https://robosystems.example.com"

    mock_session = Mock()
    mock_session.id = "cs_test"
    mock_session.url = "https://checkout.stripe.com/test"
    stripe_provider.stripe.checkout.Session.create.return_value = mock_session

    stripe_provider.create_checkout_session("cus_123", "price_456", {})

    call_args = stripe_provider.stripe.checkout.Session.create.call_args
    assert "robosystems.example.com/checkout" in call_args[1]["success_url"]
    assert "robosystems.example.com/billing" in call_args[1]["cancel_url"]

  def test_checkout_session_mode_is_subscription(self, stripe_provider):
    """Test that checkout mode is set to subscription."""
    mock_session = Mock()
    mock_session.id = "cs_test"
    mock_session.url = "https://checkout.stripe.com/test"
    stripe_provider.stripe.checkout.Session.create.return_value = mock_session

    stripe_provider.create_checkout_session("cus_123", "price_456", {})

    call_args = stripe_provider.stripe.checkout.Session.create.call_args
    assert call_args[1]["mode"] == "subscription"


class TestStripeSubscriptionOperations:
  """Tests for Stripe subscription management."""

  @pytest.fixture
  def stripe_provider(self):
    """Create Stripe provider with mocked Stripe."""
    with patch("robosystems.operations.providers.payment_provider.env"):
      with patch.object(StripePaymentProvider, "__init__", lambda self: None):
        provider = StripePaymentProvider()
        provider.stripe = Mock()
        provider._redis_client = None
        provider.stripe.Subscription = Mock()
        return provider

  def test_create_subscription_success(self, stripe_provider):
    """Test successful subscription creation."""
    mock_subscription = Mock()
    mock_subscription.id = "sub_test123"
    stripe_provider.stripe.Subscription.create.return_value = mock_subscription

    mock_pm = Mock()
    mock_pm.id = "pm_test123"
    mock_payment_methods = Mock()
    mock_payment_methods.data = [mock_pm]
    stripe_provider.stripe.PaymentMethod.list.return_value = mock_payment_methods

    mock_customer = {"invoice_settings": {"default_payment_method": None}}
    stripe_provider.stripe.Customer.retrieve.return_value = mock_customer

    result = stripe_provider.create_subscription(
      customer_id="cus_123", price_id="price_456", metadata={"plan": "standard"}
    )

    assert result == "sub_test123"
    stripe_provider.stripe.Subscription.create.assert_called_once_with(
      customer="cus_123",
      items=[{"price": "price_456"}],
      metadata={"plan": "standard"},
      default_payment_method="pm_test123",
    )

  def test_create_subscription_with_metadata(self, stripe_provider):
    """Test that subscription metadata is passed through."""
    mock_subscription = Mock()
    mock_subscription.id = "sub_test456"
    stripe_provider.stripe.Subscription.create.return_value = mock_subscription

    mock_pm = Mock()
    mock_pm.id = "pm_test456"
    mock_payment_methods = Mock()
    mock_payment_methods.data = [mock_pm]
    stripe_provider.stripe.PaymentMethod.list.return_value = mock_payment_methods

    mock_customer = {"invoice_settings": {"default_payment_method": None}}
    stripe_provider.stripe.Customer.retrieve.return_value = mock_customer

    metadata = {"user_id": "user_123", "graph_id": "kg_456", "tier": "enterprise"}
    stripe_provider.create_subscription("cus_123", "price_789", metadata)

    call_args = stripe_provider.stripe.Subscription.create.call_args
    assert call_args[1]["metadata"] == metadata


class TestStripeWebhookVerification:
  """Tests for Stripe webhook signature verification."""

  @pytest.fixture
  def stripe_provider(self):
    """Create Stripe provider with mocked Stripe."""

    class SignatureVerificationError(Exception):
      pass

    with patch("robosystems.operations.providers.payment_provider.env") as mock_env:
      mock_env.STRIPE_WEBHOOK_SECRET = "whsec_test123"
      with patch.object(StripePaymentProvider, "__init__", lambda self: None):
        provider = StripePaymentProvider()
        provider.stripe = Mock()
        provider._redis_client = None
        provider.stripe.Webhook = Mock()
        provider.stripe.error = Mock()
        provider.stripe.error.SignatureVerificationError = SignatureVerificationError
        return provider

  def test_verify_webhook_success(self, stripe_provider):
    """Test successful webhook verification."""
    mock_event = {
      "id": "evt_test123",
      "type": "payment_intent.succeeded",
      "data": {"object": {}},
    }
    stripe_provider.stripe.Webhook.construct_event.return_value = mock_event

    result = stripe_provider.verify_webhook(
      payload=b'{"test": "data"}', signature="sig_test"
    )

    assert result == mock_event
    stripe_provider.stripe.Webhook.construct_event.assert_called_once()

  def test_verify_webhook_invalid_signature(self, stripe_provider):
    """Test webhook verification with invalid signature."""

    class SignatureVerificationError(Exception):
      pass

    stripe_provider.stripe.error = Mock()
    stripe_provider.stripe.error.SignatureVerificationError = SignatureVerificationError

    stripe_provider.stripe.Webhook.construct_event.side_effect = (
      SignatureVerificationError("Invalid signature")
    )

    with pytest.raises(ValueError, match="Invalid webhook signature"):
      stripe_provider.verify_webhook(
        payload=b'{"test": "data"}', signature="invalid_sig"
      )

  def test_verify_webhook_malformed_payload(self, stripe_provider):
    """Test webhook verification with malformed payload."""
    stripe_provider.stripe.Webhook.construct_event.side_effect = ValueError(
      "Invalid JSON"
    )

    with pytest.raises(ValueError):
      stripe_provider.verify_webhook(payload=b"not json", signature="sig_test")


class TestStripePaymentMethods:
  """Tests for Stripe payment method management."""

  @pytest.fixture
  def stripe_provider(self):
    """Create Stripe provider with mocked Stripe."""
    with patch("robosystems.operations.providers.payment_provider.env"):
      with patch.object(StripePaymentProvider, "__init__", lambda self: None):
        provider = StripePaymentProvider()
        provider.stripe = Mock()
        provider._redis_client = None
        provider.stripe.PaymentMethod = Mock()
        provider.stripe.Customer = Mock()
        return provider

  def test_list_payment_methods_success(self, stripe_provider):
    """Test listing payment methods for a customer."""
    mock_pm1 = Mock()
    mock_pm1.id = "pm_1"
    mock_pm1.type = "card"
    mock_card1 = Mock()
    mock_card1.to_dict.return_value = {"brand": "visa", "last4": "4242"}
    mock_pm1.card = mock_card1

    mock_pm2 = Mock()
    mock_pm2.id = "pm_2"
    mock_pm2.type = "card"
    mock_card2 = Mock()
    mock_card2.to_dict.return_value = {"brand": "mastercard", "last4": "5555"}
    mock_pm2.card = mock_card2

    mock_list = Mock()
    mock_list.data = [mock_pm1, mock_pm2]
    stripe_provider.stripe.PaymentMethod.list.return_value = mock_list

    mock_customer = {"invoice_settings": {"default_payment_method": "pm_1"}}
    stripe_provider.stripe.Customer.retrieve.return_value = mock_customer

    result = stripe_provider.list_payment_methods("cus_123")

    assert len(result) == 2
    assert result[0]["id"] == "pm_1"
    assert result[0]["card"]["brand"] == "visa"
    assert result[0]["is_default"] is True
    assert result[1]["id"] == "pm_2"
    assert result[1]["is_default"] is False

  def test_list_payment_methods_empty(self, stripe_provider):
    """Test listing payment methods when customer has none."""
    mock_list = Mock()
    mock_list.data = []
    stripe_provider.stripe.PaymentMethod.list.return_value = mock_list

    result = stripe_provider.list_payment_methods("cus_123")

    assert result == []


class TestStripeInvoiceOperations:
  """Tests for Stripe invoice operations."""

  @pytest.fixture
  def stripe_provider(self):
    """Create Stripe provider with mocked Stripe."""
    with patch("robosystems.operations.providers.payment_provider.env"):
      with patch.object(StripePaymentProvider, "__init__", lambda self: None):
        provider = StripePaymentProvider()
        provider.stripe = Mock()
        provider._redis_client = None
        provider.stripe.Invoice = Mock()
        return provider

  def test_list_invoices_success(self, stripe_provider):
    """Test listing invoices for a customer."""
    mock_line1 = Mock()
    mock_line1.description = "Standard Plan - Monthly"
    mock_line1.amount = 2999
    mock_line1.quantity = 1
    mock_line1.period = Mock(start=1704067200, end=1706745600)

    mock_lines1 = Mock()
    mock_lines1.data = [mock_line1]

    mock_invoice1 = Mock()
    mock_invoice1.id = "in_1"
    mock_invoice1.number = "INV-001"
    mock_invoice1.amount_due = 2999
    mock_invoice1.amount_paid = 2999
    mock_invoice1.status = "paid"
    mock_invoice1.currency = "usd"
    mock_invoice1.created = 1704067200
    mock_invoice1.due_date = 1706745600
    mock_invoice1.status_transitions = Mock(paid_at=1704153600)
    mock_invoice1.invoice_pdf = "https://invoice.stripe.com/pdf"
    mock_invoice1.hosted_invoice_url = "https://invoice.stripe.com/hosted"
    mock_invoice1.subscription = "sub_123"
    mock_invoice1.lines = mock_lines1

    mock_lines2 = Mock()
    mock_lines2.data = []

    mock_invoice2 = Mock()
    mock_invoice2.id = "in_2"
    mock_invoice2.number = "INV-002"
    mock_invoice2.amount_due = 4999
    mock_invoice2.amount_paid = 0
    mock_invoice2.status = "open"
    mock_invoice2.currency = "usd"
    mock_invoice2.created = 1706745600
    mock_invoice2.due_date = None
    mock_invoice2.status_transitions = None
    mock_invoice2.invoice_pdf = None
    mock_invoice2.hosted_invoice_url = None
    mock_invoice2.subscription = "sub_123"
    mock_invoice2.lines = mock_lines2

    mock_list = Mock()
    mock_list.data = [mock_invoice1, mock_invoice2]
    mock_list.has_more = False
    stripe_provider.stripe.Invoice.list.return_value = mock_list

    result = stripe_provider.list_invoices("cus_123", limit=10)

    assert len(result["invoices"]) == 2
    assert result["has_more"] is False
    assert result["invoices"][0]["id"] == "in_1"
    assert result["invoices"][0]["amount_due"] == 2999
    assert len(result["invoices"][0]["lines"]) == 1
    assert result["invoices"][0]["lines"][0]["description"] == "Standard Plan - Monthly"

  def test_list_invoices_with_pagination(self, stripe_provider):
    """Test that pagination is handled correctly."""
    mock_list = Mock()
    mock_list.data = []
    mock_list.has_more = True
    stripe_provider.stripe.Invoice.list.return_value = mock_list

    result = stripe_provider.list_invoices("cus_123", limit=5)

    assert result["has_more"] is True
    stripe_provider.stripe.Invoice.list.assert_called_once_with(
      customer="cus_123", limit=5
    )

  def test_get_upcoming_invoice_success(self, stripe_provider):
    """Test getting upcoming invoice for a customer."""
    mock_line = Mock()
    mock_line.description = "Standard Plan - Monthly"
    mock_line.amount = 2999
    mock_line.quantity = 1
    mock_line.period = Mock(start=1234567890, end=1234599999)

    mock_lines = Mock()
    mock_lines.data = [mock_line]

    mock_invoice = Mock()
    mock_invoice.amount_due = 5999
    mock_invoice.currency = "usd"
    mock_invoice.period_start = 1234567890
    mock_invoice.period_end = 1234599999
    mock_invoice.subscription = "sub_123"
    mock_invoice.lines = mock_lines
    stripe_provider.stripe.Invoice.upcoming.return_value = mock_invoice

    result = stripe_provider.get_upcoming_invoice("cus_123")

    assert result["amount_due"] == 5999
    assert result["currency"] == "usd"
    assert result["subscription"] == "sub_123"
    assert len(result["lines"]) == 1
    assert result["lines"][0]["description"] == "Standard Plan - Monthly"
    stripe_provider.stripe.Invoice.upcoming.assert_called_once_with(customer="cus_123")

  def test_get_upcoming_invoice_none(self, stripe_provider):
    """Test getting upcoming invoice when none exists."""

    class MockStripeError(Exception):
      def __init__(self, message):
        super().__init__(message)
        self.code = "invoice_upcoming_none"

    stripe_provider.stripe.error = Mock()
    stripe_provider.stripe.error.StripeError = MockStripeError

    stripe_provider.stripe.Invoice.upcoming.side_effect = MockStripeError(
      "No upcoming invoice"
    )

    result = stripe_provider.get_upcoming_invoice("cus_123")

    assert result is None


class TestStripeCaching:
  """Tests for Redis caching of Stripe data."""

  @pytest.fixture
  def stripe_provider(self):
    """Create Stripe provider with mocked Redis."""
    with patch("robosystems.operations.providers.payment_provider.env"):
      with patch.object(StripePaymentProvider, "__init__", lambda self: None):
        provider = StripePaymentProvider()
        provider.stripe = Mock()
        provider._redis_client = Mock()
        return provider

  def test_redis_client_used_for_caching(self, stripe_provider):
    """Test that Redis client is available for caching."""
    assert stripe_provider._redis_client is not None
    assert hasattr(stripe_provider, "redis_client")
