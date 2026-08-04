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

  def test_checkout_session_stamps_metadata_on_the_subscription(self, stripe_provider):
    """Stripe does not copy session metadata onto the subscription it
    creates, and webhook resolution reads the subscription's metadata — so
    the session must stamp it there explicitly via subscription_data."""
    mock_session = Mock()
    mock_session.id = "cs_test_789"
    mock_session.url = "https://checkout.stripe.com/test"
    stripe_provider.stripe.checkout.Session.create.return_value = mock_session

    metadata = {
      "user_id": "user_123",
      "subscription_id": "sub_local_abc",
    }
    stripe_provider.create_checkout_session("cus_123", "price_456", metadata)

    call_args = stripe_provider.stripe.checkout.Session.create.call_args
    assert call_args[1]["subscription_data"] == {"metadata": metadata}

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
    assert (
      "robosystems.example.com/organization?tab=billing" in call_args[1]["cancel_url"]
    )

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
    import stripe as stripe_module

    with patch("robosystems.operations.providers.payment_provider.env"):
      with patch.object(StripePaymentProvider, "__init__", lambda self: None):
        provider = StripePaymentProvider()
        provider.stripe = Mock()
        provider.stripe.Subscription = Mock()
        # cancel_subscription's idempotency check catches
        # stripe.error.InvalidRequestError; except-clauses need the real
        # exception classes, not Mock attributes.
        provider.stripe.error = stripe_module.error
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

  def test_cancel_subscription_success(self, stripe_provider):
    """Test successful subscription cancellation."""
    stripe_provider.cancel_subscription("sub_test123")
    stripe_provider.stripe.Subscription.cancel.assert_called_once_with("sub_test123")

  def test_cancel_subscription_raises_on_error(self, stripe_provider):
    """Test that cancellation errors propagate."""
    stripe_provider.stripe.Subscription.cancel.side_effect = Exception("Not found")
    with pytest.raises(Exception, match="Not found"):
      stripe_provider.cancel_subscription("sub_nonexistent")


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
    stripe_provider.stripe.Invoice.create_preview.return_value = mock_invoice

    result = stripe_provider.get_upcoming_invoice("cus_123", "sub_123")

    assert result["amount_due"] == 5999
    assert result["currency"] == "usd"
    assert result["subscription"] == "sub_123"
    assert len(result["lines"]) == 1
    assert result["lines"][0]["description"] == "Standard Plan - Monthly"
    stripe_provider.stripe.Invoice.create_preview.assert_called_once_with(
      customer="cus_123", subscription="sub_123"
    )

  def test_get_upcoming_invoice_none(self, stripe_provider):
    """Test getting upcoming invoice when none exists."""

    class MockInvalidRequestError(Exception):
      def __init__(self, message):
        super().__init__(message)
        self.code = "invoice_upcoming_none"

    class MockStripeError(Exception):
      pass

    stripe_provider.stripe.error = Mock()
    stripe_provider.stripe.error.InvalidRequestError = MockInvalidRequestError
    stripe_provider.stripe.error.StripeError = MockStripeError

    stripe_provider.stripe.Invoice.create_preview.side_effect = MockInvalidRequestError(
      "No upcoming invoice"
    )

    result = stripe_provider.get_upcoming_invoice("cus_123", "sub_123")

    assert result is None


class TestStripeCaching:
  """Tests for in-memory caching of Stripe data."""

  @pytest.fixture
  def stripe_provider(self):
    """Create Stripe provider with mocked Stripe."""
    with patch("robosystems.operations.providers.payment_provider.env"):
      with patch.object(StripePaymentProvider, "__init__", lambda self: None):
        provider = StripePaymentProvider()
        provider.stripe = Mock()
        return provider

  def test_price_cache_is_available(self, stripe_provider):
    """Test that in-memory price cache is available."""
    assert hasattr(StripePaymentProvider, "_price_cache")
    assert hasattr(StripePaymentProvider, "_price_lock")


class TestGraphPriceResolution:
  """Graph prices must be matched by amount, not by 'first active price'.

  Regression guard for a repricing that could not reach Stripe: the graph path
  returned prices.data[0] without checking unit_amount, so changing
  base_price_cents left the old price active and still being handed out —
  checkout quoted the new amount from config and billed the old one. The
  repository path had always matched on amount; only the graph path did not.
  """

  @pytest.fixture
  def stripe_provider(self):
    with patch("robosystems.operations.providers.payment_provider.env") as mock_env:
      mock_env.ENVIRONMENT = "prod"
      with patch.object(StripePaymentProvider, "__init__", lambda self: None):
        provider = StripePaymentProvider()
        provider.stripe = Mock()
        yield provider

  @staticmethod
  def _price(price_id, unit_amount):
    price = Mock()
    price.id = price_id
    price.unit_amount = unit_amount
    price.recurring = {"interval": "month"}
    return price

  def _with_existing_prices(self, provider, prices):
    product = Mock()
    product.id = "prod_existing"
    provider.stripe.Product.search.return_value = Mock(data=[product])
    provider.stripe.Price.list.return_value = Mock(data=prices)
    return product

  def test_ignores_active_price_at_a_different_amount(self, stripe_provider):
    """A stale $149 price must not satisfy a request for $99."""
    self._with_existing_prices(stripe_provider, [self._price("price_old_149", 14900)])
    stripe_provider.stripe.Price.create.return_value = self._price("price_new_99", 9900)

    result = stripe_provider._get_or_create_graph_price(
      "ladybug-standard", {"display_name": "Standard"}, 9900
    )

    assert result == "price_new_99"
    assert stripe_provider.stripe.Price.create.call_args.kwargs["unit_amount"] == 9900

  def test_reuses_the_price_that_matches_the_target_amount(self, stripe_provider):
    """Matching price is reused, so repeated calls don't litter duplicates."""
    self._with_existing_prices(
      stripe_provider,
      [self._price("price_old_149", 14900), self._price("price_99", 9900)],
    )

    result = stripe_provider._get_or_create_graph_price(
      "ladybug-standard", {"display_name": "Standard"}, 9900
    )

    assert result == "price_99"
    stripe_provider.stripe.Price.create.assert_not_called()


class TestStripeCancelIdempotency:
  """Cancel must be idempotent so callers can gate local cancellation on it.

  The contract: a raise always means Stripe still has a live subscription
  that will keep billing. Already-canceled and missing subscriptions count
  as success — failing those cases would permanently block a local cancel
  that Stripe-side state has already satisfied.
  """

  @pytest.fixture
  def stripe_provider(self):
    import stripe as stripe_module

    with patch("robosystems.operations.providers.payment_provider.env"):
      with patch.object(StripePaymentProvider, "__init__", lambda self: None):
        provider = StripePaymentProvider()
        provider.stripe = Mock()
        # Except-clauses need the real exception classes, not Mock attributes.
        provider.stripe.error = stripe_module.error
        return provider

  def _invalid_request(self):
    from stripe.error import InvalidRequestError

    return InvalidRequestError("No such subscription", "id")

  def test_cancel_success(self, stripe_provider):
    stripe_provider.cancel_subscription("sub_1")
    stripe_provider.stripe.Subscription.cancel.assert_called_once_with("sub_1")

  def test_cancel_missing_subscription_counts_as_success(self, stripe_provider):
    stripe_provider.stripe.Subscription.cancel.side_effect = self._invalid_request()
    stripe_provider.stripe.Subscription.retrieve.side_effect = self._invalid_request()

    stripe_provider.cancel_subscription("sub_gone")

  def test_cancel_already_canceled_counts_as_success(self, stripe_provider):
    stripe_provider.stripe.Subscription.cancel.side_effect = self._invalid_request()
    stripe_provider.stripe.Subscription.retrieve.return_value = Mock(status="canceled")

    stripe_provider.cancel_subscription("sub_already")

  def test_cancel_failure_on_live_subscription_reraises(self, stripe_provider):
    from stripe.error import InvalidRequestError

    stripe_provider.stripe.Subscription.cancel.side_effect = self._invalid_request()
    stripe_provider.stripe.Subscription.retrieve.return_value = Mock(status="active")

    with pytest.raises(InvalidRequestError):
      stripe_provider.cancel_subscription("sub_live")

  def test_period_end_sets_flag(self, stripe_provider):
    stripe_provider.cancel_subscription_at_period_end("sub_1")
    stripe_provider.stripe.Subscription.modify.assert_called_once_with(
      "sub_1", cancel_at_period_end=True
    )

  def test_period_end_missing_subscription_counts_as_success(self, stripe_provider):
    stripe_provider.stripe.Subscription.modify.side_effect = self._invalid_request()
    stripe_provider.stripe.Subscription.retrieve.side_effect = self._invalid_request()

    stripe_provider.cancel_subscription_at_period_end("sub_gone")

  def test_period_end_failure_on_live_subscription_reraises(self, stripe_provider):
    from stripe.error import InvalidRequestError

    stripe_provider.stripe.Subscription.modify.side_effect = self._invalid_request()
    stripe_provider.stripe.Subscription.retrieve.return_value = Mock(status="active")

    with pytest.raises(InvalidRequestError):
      stripe_provider.cancel_subscription_at_period_end("sub_live")
