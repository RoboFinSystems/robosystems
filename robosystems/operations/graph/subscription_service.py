"""Service for managing graph database subscriptions."""

import logging

from sqlalchemy.orm import Session

from ...config import BillingConfig, env
from ...config.graph_tier import GraphTier
from ...models.billing import (
  BillingAuditLog,
  BillingCustomer,
  BillingInvoice,
  BillingSubscription,
)
from ...models.billing.audit_log import BillingEventType

logger = logging.getLogger(__name__)

BILLING_ENABLED = env.BILLING_ENABLED
ENVIRONMENT = env.ENVIRONMENT


def get_available_plans() -> list[str]:
  """Get list of available billing plans from centralized config."""
  from ...config.billing.core import DEFAULT_GRAPH_BILLING_PLANS

  return [plan["name"] for plan in DEFAULT_GRAPH_BILLING_PLANS]


def get_max_plan_tier() -> str:
  """Get the maximum plan tier from centralized config."""
  from ...config.billing.core import DEFAULT_GRAPH_BILLING_PLANS

  if not DEFAULT_GRAPH_BILLING_PLANS:
    return "ladybug-standard"
  return DEFAULT_GRAPH_BILLING_PLANS[-1]["name"]


def generate_subscription_invoice(
  subscription: BillingSubscription,
  customer: BillingCustomer,
  description: str,
  session: Session,
) -> BillingInvoice:
  """Generate an invoice for a subscription.

  Creates an invoice in OPEN status immediately when a subscription is created.
  The invoice will be paid through Stripe webhook (credit card customers) or
  manually tracked (enterprise customers with invoice billing).

  Args:
      subscription: The subscription to invoice
      customer: The billing customer
      description: Line item description
      session: Database session

  Returns:
      BillingInvoice instance in OPEN status
  """
  invoice = BillingInvoice.create_invoice(
    org_id=subscription.org_id,
    period_start=subscription.current_period_start,
    period_end=subscription.current_period_end,
    payment_terms=customer.payment_terms,
    session=session,
  )

  invoice.add_line_item(
    subscription_id=subscription.id,
    resource_type=subscription.resource_type,
    resource_id=subscription.resource_id,
    description=description,
    amount_cents=subscription.base_price_cents,
    session=session,
  )

  invoice.finalize(session)

  BillingAuditLog.log_event(
    session=session,
    event_type=BillingEventType.INVOICE_GENERATED,
    org_id=subscription.org_id,
    subscription_id=subscription.id,
    invoice_id=invoice.id,
    description=f"Invoice {invoice.invoice_number} generated for subscription {subscription.id}",
    actor_type="system",
    event_data={
      "invoice_number": invoice.invoice_number,
      "amount_cents": subscription.base_price_cents,
      "due_date": invoice.due_date.isoformat() if invoice.due_date else None,
      "payment_terms": customer.payment_terms,
      "resource_type": subscription.resource_type,
      "resource_id": subscription.resource_id,
      "plan_name": subscription.plan_name,
    },
  )

  logger.info(
    f"Generated invoice {invoice.invoice_number} for subscription {subscription.id}",
    extra={
      "invoice_id": invoice.id,
      "invoice_number": invoice.invoice_number,
      "subscription_id": subscription.id,
      "org_id": subscription.org_id,
      "amount_cents": subscription.base_price_cents,
      "due_date": invoice.due_date.isoformat() if invoice.due_date else None,
      "payment_terms": customer.payment_terms,
      "invoice_billing_enabled": customer.invoice_billing_enabled,
    },
  )

  return invoice


class GraphSubscriptionService:
  """Service for managing graph database subscriptions and billing."""

  def __init__(self, session: Session):
    """Initialize subscription service with database session."""
    self.session = session

  def create_graph_subscription(
    self,
    user_id: str,
    graph_id: str,
    plan_name: str = "ladybug-standard",
    tier: GraphTier = GraphTier.LADYBUG_STANDARD,
  ) -> BillingSubscription:
    """Create a billing subscription for a graph database.

    Args:
        user_id: User ID (authenticated user creating subscription)
        graph_id: Graph database ID
        plan_name: Billing plan name
        tier: Graph tier

    Returns:
        BillingSubscription instance
    """
    from ...models.iam import OrgUser

    available_plans = get_available_plans()
    if plan_name not in available_plans:
      max_tier = get_max_plan_tier()
      logger.warning(f"Plan '{plan_name}' not available, using '{max_tier}' instead")
      plan_name = max_tier

    plan_config = BillingConfig.get_subscription_plan(plan_name)
    if not plan_config:
      raise ValueError(f"Billing plan '{plan_name}' not found")

    user_orgs = OrgUser.get_user_orgs(user_id, self.session)
    if not user_orgs:
      raise ValueError(f"User {user_id} has no organization")

    org_id = user_orgs[0].org_id
    customer = BillingCustomer.get_or_create(org_id, self.session)
    logger.info(f"Using billing customer for org {org_id}")

    existing = BillingSubscription.get_by_resource_and_org(
      resource_type="graph", resource_id=graph_id, org_id=org_id, session=self.session
    )

    if existing:
      logger.warning(f"Subscription already exists for graph {graph_id}")
      return existing

    subscription = BillingSubscription.create_subscription(
      org_id=org_id,
      resource_type="graph",
      resource_id=graph_id,
      plan_name=plan_name,
      base_price_cents=plan_config["base_price_cents"],
      session=self.session,
      billing_interval="monthly",
    )

    BillingAuditLog.log_event(
      session=self.session,
      event_type=BillingEventType.SUBSCRIPTION_CREATED,
      org_id=org_id,
      subscription_id=subscription.id,
      description=f"Created {plan_name} subscription for graph {graph_id}",
      actor_type="user",
      actor_user_id=user_id,
      event_data={
        "resource_type": "graph",
        "resource_id": graph_id,
        "plan_name": plan_name,
        "base_price_cents": plan_config["base_price_cents"],
        "graph_tier": tier.value,
      },
    )

    if BILLING_ENABLED and customer.has_payment_method and customer.stripe_customer_id:
      logger.info(
        "Customer has payment method on file, creating Stripe subscription automatically",
        extra={
          "user_id": user_id,
          "stripe_customer_id": f"{customer.stripe_customer_id[:8]}***{customer.stripe_customer_id[-4:]}"
          if customer.stripe_customer_id
          else None,
          "plan_name": plan_name,
        },
      )

      try:
        from ...operations.providers.payment_provider import get_payment_provider

        provider = get_payment_provider("stripe")
        stripe_price_id = provider.get_or_create_price(
          plan_name=plan_name, resource_type="graph"
        )

        stripe_subscription_id = provider.create_subscription(
          customer_id=customer.stripe_customer_id,
          price_id=stripe_price_id,
          metadata={
            "subscription_id": str(subscription.id),
            "user_id": user_id,
            "resource_type": "graph",
            "resource_id": graph_id,
          },
        )

        subscription.stripe_subscription_id = stripe_subscription_id
        subscription.provider_subscription_id = stripe_subscription_id
        subscription.provider_customer_id = customer.stripe_customer_id
        subscription.payment_provider = "stripe"

        redacted_stripe_id = (
          f"{stripe_subscription_id[:8]}***{stripe_subscription_id[-4:]}"
          if stripe_subscription_id
          else None
        )
        logger.info(
          f"Created Stripe subscription {redacted_stripe_id} for graph {graph_id}",
          extra={
            "user_id": user_id,
            "subscription_id": subscription.id,
            "stripe_subscription_id": redacted_stripe_id,
          },
        )

      except Exception as e:
        logger.error(
          f"Failed to create Stripe subscription for graph {graph_id}: {e}",
          extra={
            "user_id": user_id,
            "subscription_id": subscription.id,
            "plan_name": plan_name,
          },
          exc_info=True,
        )

    subscription.activate(self.session)

    BillingAuditLog.log_event(
      session=self.session,
      event_type=BillingEventType.SUBSCRIPTION_ACTIVATED,
      org_id=org_id,
      subscription_id=subscription.id,
      description=f"Activated subscription for graph {graph_id}",
      actor_type="system",
      event_data={
        "current_period_start": subscription.current_period_start.isoformat(),
        "current_period_end": subscription.current_period_end.isoformat(),
      },
    )

    if not subscription.stripe_subscription_id:
      generate_subscription_invoice(
        subscription=subscription,
        customer=customer,
        description=f"Graph Database Subscription - {plan_name}",
        session=self.session,
      )
    else:
      redacted_stripe_sub_id = (
        f"{subscription.stripe_subscription_id[:8]}***{subscription.stripe_subscription_id[-4:]}"
        if subscription.stripe_subscription_id
        else None
      )
      logger.info(
        f"Stripe will create invoice for subscription {subscription.id}",
        extra={
          "subscription_id": subscription.id,
          "stripe_subscription_id": redacted_stripe_sub_id,
        },
      )

    logger.info(
      f"Created billing subscription for graph {graph_id} with plan {plan_name}",
      extra={
        "user_id": user_id,
        "graph_id": graph_id,
        "plan_name": plan_name,
        "subscription_id": subscription.id,
      },
    )

    return subscription
