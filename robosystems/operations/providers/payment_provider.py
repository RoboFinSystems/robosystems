"""Payment provider abstraction layer for extensibility.

This module provides an abstract interface for payment providers, making it easy
to support multiple processors without changing business logic.
"""

import threading
import time
from abc import ABC, abstractmethod
from typing import Any

from ...config import env
from ...config.billing import BillingConfig
from ...config.constants import STRIPE_API_VERSION
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
    self, customer_id: str, price_id: str, metadata: dict[str, Any]
  ) -> dict[str, Any]:
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
    self,
    customer_id: str,
    price_id: str,
    metadata: dict[str, Any],
    payment_method_id: str | None = None,
  ) -> str:
    """Create subscription (for customers with payment method on file).

    Args:
        customer_id: Provider customer ID
        price_id: Provider price/plan ID
        metadata: Custom metadata
        payment_method_id: Optional payment method ID to use

    Returns:
        provider_subscription_id: Subscription ID in payment provider
    """
    pass

  @abstractmethod
  def verify_webhook(self, payload: bytes, signature: str) -> dict[str, Any]:
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
  def list_payment_methods(self, customer_id: str) -> list[dict[str, Any]]:
    """List payment methods for a customer.

    Args:
        customer_id: Provider customer ID

    Returns:
        List of payment method dictionaries
    """
    pass

  @abstractmethod
  def list_invoices(self, customer_id: str, limit: int = 10) -> dict[str, Any]:
    """List invoices for a customer.

    Args:
        customer_id: Provider customer ID
        limit: Maximum number of invoices to return

    Returns:
        Dict with keys: invoices (list), has_more (bool)
    """
    pass

  @abstractmethod
  def get_upcoming_invoice(
    self, customer_id: str, subscription_id: str
  ) -> dict[str, Any] | None:
    """Get the upcoming invoice for a customer's subscription.

    Args:
        customer_id: Provider customer ID
        subscription_id: Provider subscription ID to preview. Required —
            an upcoming invoice is a property of a subscription, not of a
            customer, so there is nothing to preview without one.

    Returns:
        Upcoming invoice details or None if no upcoming invoice
    """
    pass

  @abstractmethod
  def cancel_subscription(self, subscription_id: str) -> None:
    """Cancel a subscription immediately.

    Args:
        subscription_id: Provider subscription ID to cancel
    """
    pass

  @abstractmethod
  def change_subscription_price(
    self,
    subscription_id: str,
    new_price_id: str,
    proration_behavior: str = "create_prorations",
  ) -> dict[str, Any]:
    """Change the price on an existing subscription (upgrade/downgrade).

    Args:
        subscription_id: Provider subscription ID
        new_price_id: New price ID to switch to
        proration_behavior: How to handle prorations

    Returns:
        Updated subscription data
    """
    pass

  @abstractmethod
  def create_portal_session(self, customer_id: str, return_url: str) -> str:
    """Create a customer portal session for managing payment methods.

    Args:
        customer_id: Provider customer ID
        return_url: URL to redirect customer after portal session

    Returns:
        Portal session URL where customer can manage payment methods
    """
    pass


class StripePaymentProvider(PaymentProvider):
  """Stripe implementation of payment provider."""

  # In-memory price cache: key -> (price_id, expires_at)
  _price_cache: dict[str, tuple[str, float]] = {}
  _price_lock = threading.Lock()

  def __init__(self):
    """Initialize Stripe with API key from environment."""
    import stripe

    stripe.api_key = env.STRIPE_SECRET_KEY
    stripe.api_version = STRIPE_API_VERSION
    self.stripe = stripe
    logger.info("Initialized Stripe payment provider")

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
    self, customer_id: str, price_id: str, metadata: dict[str, Any]
  ) -> dict[str, Any]:
    """Create Stripe Checkout session."""
    session = self.stripe.checkout.Session.create(
      customer=customer_id,
      mode="subscription",
      line_items=[{"price": price_id, "quantity": 1}],
      success_url=f"{env.ROBOSYSTEMS_URL}/checkout/{{CHECKOUT_SESSION_ID}}",
      cancel_url=f"{env.ROBOSYSTEMS_URL}/billing",
      metadata=metadata,
      payment_method_types=["card"],
      billing_address_collection="auto",
      allow_promotion_codes=True,
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
    self,
    customer_id: str,
    price_id: str,
    metadata: dict[str, Any],
    payment_method_id: str | None = None,
  ) -> str:
    """Create Stripe subscription (customer has payment method)."""
    if not payment_method_id:
      payment_methods = self.list_payment_methods(customer_id)
      if not payment_methods:
        raise ValueError(f"Customer {customer_id} has no payment methods attached")

      default_pm = next((pm for pm in payment_methods if pm.get("is_default")), None)
      payment_method_id = default_pm["id"] if default_pm else payment_methods[0]["id"]

      logger.info(
        f"Using payment method {payment_method_id} for subscription",
        extra={"customer_id": customer_id, "payment_method_id": payment_method_id},
      )

    subscription = self.stripe.Subscription.create(
      customer=customer_id,
      items=[{"price": price_id}],
      default_payment_method=payment_method_id,
      metadata=metadata,
    )

    logger.info(
      f"Created Stripe subscription {subscription.id}",
      extra={
        "subscription_id": subscription.id,
        "customer_id": customer_id,
        "payment_method_id": payment_method_id,
        "metadata": metadata,
      },
    )

    return subscription.id

  def verify_webhook(self, payload: bytes, signature: str) -> dict[str, Any]:
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

  def get_or_create_price(
    self,
    plan_name: str,
    resource_type: str = "graph",
    repository_id: str | None = None,
  ) -> str:
    """Get or create Stripe price for a plan, with caching and auto-creation.

    For graph plans: one Stripe product per plan (ladybug-standard, etc.)
    For repository plans: one Stripe product per repository with multiple prices.
      The product is found by the manifest's display name (e.g., "SEC EDGAR Filings")
      and prices are matched by unit_amount.

    Args:
        plan_name: Internal plan name (e.g., "ladybug-standard", "starter")
        resource_type: "graph" or "repository"
        repository_id: Required when resource_type is "repository" (e.g., "sec")

    Returns:
        Stripe price ID

    Raises:
        ValueError: Plan not found in billing config, or repository_id missing
    """
    if resource_type == "repository" and not repository_id:
      raise ValueError("repository_id is required when resource_type is 'repository'")

    # Include repository_id in cache key for repository plans to avoid collisions
    key_suffix = (
      f"{resource_type}:{repository_id}:{plan_name}"
      if repository_id
      else f"{resource_type}:{plan_name}"
    )
    cache_key = f"stripe_price:{env.ENVIRONMENT}:{key_suffix}"

    # Fast path: check cache without lock
    entry = self._price_cache.get(cache_key)
    if entry and entry[1] > time.time():
      logger.debug(f"Using cached Stripe price ID for {plan_name}: {entry[0]}")
      return entry[0]

    with self._price_lock:
      # Double-check after acquiring lock
      entry = self._price_cache.get(cache_key)
      if entry and entry[1] > time.time():
        logger.debug(f"Found price ID after lock acquire: {entry[0]}")
        return entry[0]

      if resource_type == "graph":
        plan_config = BillingConfig.get_subscription_plan(plan_name)
      else:
        plan_config = BillingConfig.get_repository_plan(repository_id, plan_name)

      if not plan_config:
        raise ValueError(f"Plan '{plan_name}' not found in billing config")

      target_amount = plan_config.get(
        "base_price_cents", plan_config.get("price_cents")
      )

      if resource_type == "repository":
        price_id = self._get_or_create_repository_price(
          repository_id, plan_name, plan_config, target_amount
        )
      else:
        price_id = self._get_or_create_graph_price(
          plan_name, plan_config, target_amount
        )

      self._price_cache[cache_key] = (price_id, time.time() + 86400)
      return price_id

  def _get_or_create_repository_price(
    self,
    repository_id: str,
    plan_name: str,
    plan_config: dict[str, Any],
    target_amount: int,
  ) -> str:
    """Find or create a price on a single-product-per-repository in Stripe.

    Repositories use one Stripe product (found by manifest name) with a separate
    price for each plan tier, matched by unit_amount. This enables upgrade/downgrade
    via Stripe subscription item swap.
    """
    from ...config.shared_repositories import get_manifest

    manifest = get_manifest(repository_id)
    if not manifest:
      raise ValueError(f"No manifest found for repository '{repository_id}'")

    product_name = manifest.name  # e.g., "SEC EDGAR Filings"

    # Search for existing product by name and environment
    search_query = (
      f'name:"{product_name}" AND metadata["environment"]:"{env.ENVIRONMENT}"'
    )
    products = self.stripe.Product.search(query=search_query, limit=1)

    if products.data:
      product = products.data[0]
      logger.info(f"Found existing Stripe product for {repository_id}: {product.id}")
    else:
      # Create new unified product for this repository
      logger.info(f"Creating new Stripe product for repository {repository_id}")
      product = self.stripe.Product.create(
        name=product_name,
        description=manifest.description,
        metadata={
          "repository_id": repository_id,
          "resource_type": "repository",
          "environment": env.ENVIRONMENT,
        },
      )
      logger.info(f"Created Stripe product {product.id} for {repository_id}")

    # Find existing price matching the target amount on this product
    prices = self.stripe.Price.list(product=product.id, active=True, limit=100)
    for price in prices.data:
      if price.unit_amount == target_amount and price.recurring:
        logger.info(
          f"Found existing price {price.id} ({target_amount} cents) "
          f"for {repository_id}/{plan_name}"
        )
        return price.id

    # No matching price found — create one
    price = self.stripe.Price.create(
      product=product.id,
      unit_amount=target_amount,
      currency="usd",
      recurring={"interval": "month"},
      metadata={
        "plan_name": plan_name,
        "repository_id": repository_id,
        "environment": env.ENVIRONMENT,
      },
    )
    logger.info(
      f"Created price {price.id} ({target_amount} cents) for {repository_id}/{plan_name}",
      extra={"product_id": product.id, "price_id": price.id, "amount": target_amount},
    )
    return price.id

  def _get_or_create_graph_price(
    self,
    plan_name: str,
    plan_config: dict[str, Any],
    target_amount: int,
  ) -> str:
    """Find or create a price for a graph plan (one product per plan)."""
    search_query = f'metadata["plan_name"]:"{plan_name}" AND metadata["environment"]:"{env.ENVIRONMENT}"'
    products = self.stripe.Product.search(query=search_query, limit=1)

    if products.data:
      product = products.data[0]
      logger.info(f"Found existing Stripe product for {plan_name}: {product.id}")

      prices = self.stripe.Price.list(product=product.id, active=True, limit=1)
      if prices.data:
        return prices.data[0].id

      logger.warning(f"No active price for product {product.id}, creating new price")
      price = self.stripe.Price.create(
        product=product.id,
        unit_amount=target_amount,
        currency="usd",
        recurring={"interval": "month"},
      )
      return price.id

    product_name = plan_config.get("display_name", plan_config.get("name"))
    logger.info(f"Creating new Stripe product for {plan_name}")
    product = self.stripe.Product.create(
      name=product_name,
      description=plan_config.get("description", ""),
      metadata={
        "plan_name": plan_name,
        "resource_type": "graph",
        "environment": env.ENVIRONMENT,
      },
    )

    price = self.stripe.Price.create(
      product=product.id,
      unit_amount=target_amount,
      currency="usd",
      recurring={"interval": "month"},
      metadata={
        "plan_name": plan_name,
        "resource_type": "graph",
        "environment": env.ENVIRONMENT,
      },
    )

    logger.info(
      f"Created Stripe product and price for {plan_name}",
      extra={
        "plan_name": plan_name,
        "product_id": product.id,
        "price_id": price.id,
        "amount": target_amount,
      },
    )
    return price.id

  def change_subscription_price(
    self,
    subscription_id: str,
    new_price_id: str,
    proration_behavior: str = "create_prorations",
  ) -> dict[str, Any]:
    """Change the price on an existing Stripe subscription (upgrade/downgrade).

    Swaps the subscription item to the new price. Both prices must be on the
    same product (single-product-per-repository pattern).

    Args:
        subscription_id: Stripe subscription ID
        new_price_id: New Stripe price ID to switch to
        proration_behavior: How to handle prorations (default: create_prorations)

    Returns:
        Updated subscription data
    """
    subscription = self.stripe.Subscription.retrieve(subscription_id)
    current_item = subscription["items"]["data"][0]

    updated = self.stripe.Subscription.modify(
      subscription_id,
      items=[
        {"id": current_item.id, "price": new_price_id},
      ],
      proration_behavior=proration_behavior,
    )

    logger.info(
      f"Changed subscription {subscription_id} to price {new_price_id}",
      extra={
        "subscription_id": subscription_id,
        "old_price": current_item.price.id,
        "new_price": new_price_id,
        "proration_behavior": proration_behavior,
      },
    )

    return {"subscription_id": updated.id, "status": updated.status}

  def list_payment_methods(self, customer_id: str) -> list[dict[str, Any]]:
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

  def list_invoices(self, customer_id: str, limit: int = 10) -> dict[str, Any]:
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
            "subscription": getattr(inv, "subscription", None),
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

  def get_upcoming_invoice(
    self, customer_id: str, subscription_id: str
  ) -> dict[str, Any] | None:
    """Get the upcoming invoice for a Stripe subscription.

    `Invoice.create_preview` previews a *subscription*, not a customer.
    Called with only `customer` it returns 400 every time — Stripe requires
    one of subscription / schedule / subscription_details.items /
    schedule_details.phases / invoice_items — so passing the subscription is
    what makes this work at all, not an optimization.
    """
    try:
      # v2026 API: Invoice.upcoming() replaced by Invoice.create_preview()
      invoice = self.stripe.Invoice.create_preview(
        customer=customer_id, subscription=subscription_id
      )

      if not invoice:
        return None

      return {
        "amount_due": invoice.amount_due,
        "currency": invoice.currency,
        "period_start": invoice.period_start,
        "period_end": invoice.period_end,
        "subscription": getattr(invoice, "subscription", None),
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

    except self.stripe.error.InvalidRequestError as e:
      # Reachable when the subscription is in a state with nothing to bill
      # (already canceled, fully credited). Logged at warning rather than
      # debug: with the subscription supplied this should be rare, and the
      # previous debug-level swallow is why a permanent 400 went unnoticed.
      logger.warning(
        f"No upcoming invoice for subscription {subscription_id}: {e}",
        extra={"customer_id": customer_id, "subscription_id": subscription_id},
      )
      return None
    except self.stripe.error.StripeError as e:
      logger.error(f"Failed to get upcoming invoice: {e}", exc_info=True)
      raise
    except Exception as e:
      logger.error(f"Failed to get upcoming invoice: {e}", exc_info=True)
      raise

  def cancel_subscription(self, subscription_id: str) -> None:
    """Cancel a Stripe subscription immediately."""
    try:
      self.stripe.Subscription.cancel(subscription_id)
      logger.info(
        f"Canceled Stripe subscription {subscription_id}",
        extra={"subscription_id": subscription_id},
      )
    except Exception as e:
      logger.error(
        f"Failed to cancel Stripe subscription {subscription_id}: {e}",
        exc_info=True,
      )
      raise

  def create_portal_session(self, customer_id: str, return_url: str) -> str:
    """Create a Stripe Customer Portal session for payment management."""
    try:
      session = self.stripe.billing_portal.Session.create(
        customer=customer_id,
        return_url=return_url,
      )

      logger.info(
        f"Created Stripe portal session for customer {customer_id}",
        extra={"customer_id": customer_id, "session_id": session.id},
      )

      return session.url

    except Exception as e:
      logger.error(f"Failed to create portal session: {e}", exc_info=True)
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
