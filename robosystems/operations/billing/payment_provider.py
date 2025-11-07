"""Payment provider abstraction layer for extensibility.

This module provides an abstract interface for payment providers, making it easy
to support multiple processors (Stripe, Crossmint, etc.) without changing business logic.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
import time
from ...config import env
from ...config.valkey_registry import ValkeyDatabase, create_redis_client
from ...config.billing import BillingConfig
from ...logger import get_logger

logger = get_logger(__name__)


class PaymentProvider(ABC):
  """Abstract payment provider interface."""

  @abstractmethod
  def create_customer(self, user_id: str, email: str) -> str:
    """Create customer in payment system.

    Args:
        user_id: Internal user ID
        email: User email address

    Returns:
        provider_customer_id: Customer ID in payment provider system
    """
    pass

  @abstractmethod
  def create_checkout_session(
    self, customer_id: str, price_id: str, metadata: Dict[str, Any]
  ) -> Dict[str, Any]:
    """Create checkout session for collecting payment method.

    Args:
        customer_id: Provider customer ID
        price_id: Provider price/plan ID
        metadata: Custom metadata to attach to session

    Returns:
        Dict with keys: checkout_url, session_id
    """
    pass

  @abstractmethod
  def create_subscription(
    self, customer_id: str, price_id: str, metadata: Dict[str, Any]
  ) -> str:
    """Create subscription (for customers with payment method on file).

    Args:
        customer_id: Provider customer ID
        price_id: Provider price/plan ID
        metadata: Custom metadata

    Returns:
        provider_subscription_id: Subscription ID in payment provider
    """
    pass

  @abstractmethod
  def verify_webhook(self, payload: bytes, signature: str) -> Dict[str, Any]:
    """Verify and parse webhook event.

    Args:
        payload: Raw webhook payload
        signature: Webhook signature header

    Returns:
        Parsed webhook event

    Raises:
        ValueError: Invalid payload or signature
    """
    pass

  @abstractmethod
  def list_payment_methods(self, customer_id: str) -> List[Dict[str, Any]]:
    """List payment methods for a customer.

    Args:
        customer_id: Provider customer ID

    Returns:
        List of payment method dictionaries
    """
    pass

  @abstractmethod
  def update_default_payment_method(
    self, customer_id: str, payment_method_id: str
  ) -> Dict[str, Any]:
    """Update the default payment method for a customer.

    Args:
        customer_id: Provider customer ID
        payment_method_id: Payment method ID to set as default

    Returns:
        Updated payment method details
    """
    pass

  @abstractmethod
  def list_invoices(self, customer_id: str, limit: int = 10) -> Dict[str, Any]:
    """List invoices for a customer.

    Args:
        customer_id: Provider customer ID
        limit: Maximum number of invoices to return

    Returns:
        Dict with keys: invoices (list), has_more (bool)
    """
    pass

  @abstractmethod
  def get_upcoming_invoice(self, customer_id: str) -> Optional[Dict[str, Any]]:
    """Get the upcoming invoice for a customer.

    Args:
        customer_id: Provider customer ID

    Returns:
        Upcoming invoice details or None if no upcoming invoice
    """
    pass


class StripePaymentProvider(PaymentProvider):
  """Stripe implementation of payment provider."""

  def __init__(self):
    """Initialize Stripe with API key from environment."""
    import stripe

    stripe.api_key = env.STRIPE_SECRET_KEY
    stripe.api_version = "2024-11-20.acacia"
    self.stripe = stripe
    self._redis_client = None
    logger.info("Initialized Stripe payment provider")

  @property
  def redis_client(self):
    """Lazy-load Redis client for billing cache."""
    if self._redis_client is None:
      self._redis_client = create_redis_client(
        ValkeyDatabase.BILLING_CACHE, decode_responses=True
      )
    return self._redis_client

  def create_customer(self, user_id: str, email: str) -> str:
    """Create Stripe customer."""
    customer = self.stripe.Customer.create(
      email=email, metadata={"robosystems_user_id": user_id}
    )
    logger.info(
      f"Created Stripe customer {customer.id} for user {user_id}",
      extra={"user_id": user_id, "stripe_customer_id": customer.id},
    )
    return customer.id

  def create_checkout_session(
    self, customer_id: str, price_id: str, metadata: Dict[str, Any]
  ) -> Dict[str, Any]:
    """Create Stripe Checkout session."""
    session = self.stripe.checkout.Session.create(
      customer=customer_id,
      mode="subscription",
      line_items=[{"price": price_id, "quantity": 1}],
      success_url=f"{env.ROBOSYSTEMS_URL}/billing/success?session_id={{CHECKOUT_SESSION_ID}}",
      cancel_url=f"{env.ROBOSYSTEMS_URL}/billing/cancel",
      metadata=metadata,
      payment_method_types=["card"],
      billing_address_collection="auto",
    )

    logger.info(
      f"Created Stripe checkout session {session.id}",
      extra={
        "session_id": session.id,
        "customer_id": customer_id,
        "metadata": metadata,
      },
    )

    return {"checkout_url": session.url, "session_id": session.id}

  def create_subscription(
    self, customer_id: str, price_id: str, metadata: Dict[str, Any]
  ) -> str:
    """Create Stripe subscription (customer has payment method)."""
    subscription = self.stripe.Subscription.create(
      customer=customer_id, items=[{"price": price_id}], metadata=metadata
    )

    logger.info(
      f"Created Stripe subscription {subscription.id}",
      extra={
        "subscription_id": subscription.id,
        "customer_id": customer_id,
        "metadata": metadata,
      },
    )

    return subscription.id

  def verify_webhook(self, payload: bytes, signature: str) -> Dict[str, Any]:
    """Verify Stripe webhook signature and parse event."""
    try:
      event = self.stripe.Webhook.construct_event(
        payload, signature, env.STRIPE_WEBHOOK_SECRET
      )
      logger.debug(
        f"Verified Stripe webhook: {event['type']}",
        extra={"event_type": event["type"], "event_id": event["id"]},
      )
      return event
    except ValueError as e:
      logger.error(f"Invalid webhook payload: {e}")
      raise
    except self.stripe.error.SignatureVerificationError as e:
      logger.error(f"Invalid webhook signature: {e}")
      raise ValueError("Invalid webhook signature") from e

  def get_or_create_price(self, plan_name: str, resource_type: str = "graph") -> str:
    """Get or create Stripe price for a plan, with caching and auto-creation.

    This method implements the auto-create pattern:
    1. Check Redis cache for existing price ID
    2. If not cached, search Stripe for existing product
    3. If not in Stripe, create from billing config
    4. Cache the result with 24-hour TTL
    5. Use distributed locks to prevent race conditions

    Args:
        plan_name: Internal plan name (e.g., "kuzu-standard", "sec-starter")
        resource_type: "graph" or "repository"

    Returns:
        Stripe price ID

    Raises:
        ValueError: Plan not found in billing config
    """
    cache_key = f"stripe_price:{env.ENVIRONMENT}:{resource_type}:{plan_name}"
    lock_key = f"stripe_price_lock:{env.ENVIRONMENT}:{resource_type}:{plan_name}"

    cached_price_id = self.redis_client.get(cache_key)
    if cached_price_id:
      logger.debug(f"Using cached Stripe price ID for {plan_name}: {cached_price_id}")
      return cached_price_id

    lock_acquired = False
    try:
      lock_acquired = self.redis_client.set(lock_key, "1", nx=True, ex=30)

      if not lock_acquired:
        for _ in range(10):
          time.sleep(0.5)
          cached_price_id = self.redis_client.get(cache_key)
          if cached_price_id:
            logger.debug(f"Found price ID after waiting: {cached_price_id}")
            return cached_price_id

        logger.warning(f"Failed to acquire lock for {plan_name}, proceeding anyway")

      if resource_type == "graph":
        plan_config = BillingConfig.get_subscription_plan(plan_name)
      else:
        plan_config = BillingConfig.get_repository_plan(resource_type, plan_name)

      if not plan_config:
        raise ValueError(f"Plan '{plan_name}' not found in billing config")

      search_query = f'metadata["plan_name"]:"{plan_name}" AND metadata["environment"]:"{env.ENVIRONMENT}"'
      products = self.stripe.Product.search(query=search_query, limit=1)

      if products.data:
        product = products.data[0]
        logger.info(f"Found existing Stripe product for {plan_name}: {product.id}")

        prices = self.stripe.Price.list(product=product.id, active=True, limit=1)

        if prices.data:
          price_id = prices.data[0].id
          logger.info(f"Found existing Stripe price for {plan_name}: {price_id}")
        else:
          logger.warning(
            f"No active price for product {product.id}, creating new price"
          )
          price = self.stripe.Price.create(
            product=product.id,
            unit_amount=plan_config.get(
              "base_price_cents", plan_config.get("price_cents")
            ),
            currency="usd",
            recurring={"interval": "month"},
          )
          price_id = price.id
          logger.info(f"Created new Stripe price for existing product: {price_id}")
      else:
        product_name = plan_config.get("display_name", plan_config.get("name"))
        if env.ENVIRONMENT != "prod":
          product_name = f"{product_name} ({env.ENVIRONMENT})"

        logger.info(f"Creating new Stripe product for {plan_name}")
        product = self.stripe.Product.create(
          name=product_name,
          description=plan_config.get("description", ""),
          metadata={
            "plan_name": plan_name,
            "resource_type": resource_type,
            "environment": env.ENVIRONMENT,
          },
        )

        price = self.stripe.Price.create(
          product=product.id,
          unit_amount=plan_config.get(
            "base_price_cents", plan_config.get("price_cents")
          ),
          currency="usd",
          recurring={"interval": "month"},
          metadata={
            "plan_name": plan_name,
            "resource_type": resource_type,
            "environment": env.ENVIRONMENT,
          },
        )
        price_id = price.id

        logger.info(
          f"Created Stripe product and price for {plan_name}",
          extra={
            "plan_name": plan_name,
            "product_id": product.id,
            "price_id": price_id,
            "amount": plan_config.get(
              "base_price_cents", plan_config.get("price_cents")
            ),
          },
        )

      self.redis_client.setex(cache_key, 86400, price_id)

      return price_id

    finally:
      if lock_acquired:
        self.redis_client.delete(lock_key)

  def list_payment_methods(self, customer_id: str) -> List[Dict[str, Any]]:
    """List payment methods for a Stripe customer."""
    try:
      payment_methods = self.stripe.PaymentMethod.list(
        customer=customer_id, type="card"
      )

      customer = self.stripe.Customer.retrieve(customer_id)
      default_payment_method = customer.get("invoice_settings", {}).get(
        "default_payment_method"
      )

      result = []
      for pm in payment_methods.data:
        result.append(
          {
            "id": pm.id,
            "type": pm.type,
            "card": pm.card.to_dict() if pm.card else {},
            "is_default": pm.id == default_payment_method,
          }
        )

      logger.debug(f"Listed {len(result)} payment methods for customer {customer_id}")
      return result

    except Exception as e:
      logger.error(f"Failed to list payment methods: {e}", exc_info=True)
      raise

  def update_default_payment_method(
    self, customer_id: str, payment_method_id: str
  ) -> Dict[str, Any]:
    """Update the default payment method for a Stripe customer."""
    try:
      self.stripe.Customer.modify(
        customer_id,
        invoice_settings={"default_payment_method": payment_method_id},
      )

      payment_method = self.stripe.PaymentMethod.retrieve(payment_method_id)

      logger.info(
        f"Updated default payment method for customer {customer_id}",
        extra={"customer_id": customer_id, "payment_method_id": payment_method_id},
      )

      return {
        "id": payment_method.id,
        "type": payment_method.type,
        "card": payment_method.card.to_dict() if payment_method.card else {},
        "is_default": True,
      }

    except Exception as e:
      logger.error(f"Failed to update payment method: {e}", exc_info=True)
      raise

  def list_invoices(self, customer_id: str, limit: int = 10) -> Dict[str, Any]:
    """List invoices for a Stripe customer."""
    try:
      invoices = self.stripe.Invoice.list(customer=customer_id, limit=limit)

      result = {
        "invoices": [
          {
            "id": inv.id,
            "number": inv.number,
            "status": inv.status,
            "amount_due": inv.amount_due,
            "amount_paid": inv.amount_paid,
            "currency": inv.currency,
            "created": inv.created,
            "due_date": inv.due_date,
            "paid_at": inv.status_transitions.paid_at
            if inv.status_transitions
            else None,
            "invoice_pdf": inv.invoice_pdf,
            "hosted_invoice_url": inv.hosted_invoice_url,
            "subscription": inv.subscription,
            "lines": [
              {
                "description": line.description,
                "amount": line.amount,
                "quantity": line.quantity,
                "period_start": line.period.start if line.period else None,
                "period_end": line.period.end if line.period else None,
              }
              for line in inv.lines.data
            ],
          }
          for inv in invoices.data
        ],
        "has_more": invoices.has_more,
      }

      logger.debug(
        f"Listed {len(result['invoices'])} invoices for customer {customer_id}"
      )
      return result

    except Exception as e:
      logger.error(f"Failed to list invoices: {e}", exc_info=True)
      raise

  def get_upcoming_invoice(self, customer_id: str) -> Optional[Dict[str, Any]]:
    """Get the upcoming invoice for a Stripe customer."""
    try:
      invoice = self.stripe.Invoice.upcoming(customer=customer_id)

      if not invoice:
        return None

      return {
        "amount_due": invoice.amount_due,
        "currency": invoice.currency,
        "period_start": invoice.period_start,
        "period_end": invoice.period_end,
        "subscription": invoice.subscription,
        "lines": [
          {
            "description": line.description,
            "amount": line.amount,
            "quantity": line.quantity,
            "period_start": line.period.start if line.period else None,
            "period_end": line.period.end if line.period else None,
          }
          for line in invoice.lines.data
        ],
      }

    except self.stripe.error.StripeError as e:
      if e.code == "invoice_upcoming_none":
        logger.debug(f"No upcoming invoice for customer {customer_id}")
        return None
      logger.error(f"Failed to get upcoming invoice: {e}", exc_info=True)
      raise
    except Exception as e:
      logger.error(f"Failed to get upcoming invoice: {e}", exc_info=True)
      raise


def get_payment_provider(provider_name: str = "stripe") -> PaymentProvider:
  """Factory function to get payment provider instance.

  Args:
      provider_name: Name of payment provider (default: "stripe")

  Returns:
      PaymentProvider implementation

  Raises:
      ValueError: Unknown provider name
      NotImplementedError: Provider not yet implemented
  """
  if provider_name == "stripe":
    return StripePaymentProvider()
  elif provider_name == "crossmint":
    raise NotImplementedError("Crossmint provider not yet implemented")
  else:
    raise ValueError(f"Unknown payment provider: {provider_name}")
