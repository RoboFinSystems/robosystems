"""Service for managing graph database subscriptions."""

import logging
from typing import List
from sqlalchemy.orm import Session

from ...models.billing import (
  BillingCustomer,
  BillingSubscription,
  BillingInvoice,
  BillingAuditLog,
)
from ...models.billing.audit_log import BillingEventType
from ...models.iam.graph_credits import GraphTier
from ...config import BillingConfig, env

logger = logging.getLogger(__name__)

BILLING_ENABLED = env.BILLING_ENABLED
ENVIRONMENT = env.ENVIRONMENT


def get_available_plans() -> List[str]:
  """Get list of available billing plans from centralized config."""
  from ...config.billing.core import DEFAULT_GRAPH_BILLING_PLANS

  return [plan["name"] for plan in DEFAULT_GRAPH_BILLING_PLANS]


def is_payment_required() -> bool:
  """Check if payment authentication is required."""
  if not BILLING_ENABLED:
    return False
  if ENVIRONMENT == "dev":
    return False
  return True


def get_max_plan_tier() -> str:
  """Get the maximum plan tier from centralized config."""
  from ...config.billing.core import DEFAULT_GRAPH_BILLING_PLANS

  if not DEFAULT_GRAPH_BILLING_PLANS:
    return "kuzu-standard"
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
    user_id=subscription.billing_customer_user_id,
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
    billing_customer_user_id=subscription.billing_customer_user_id,
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
      "user_id": subscription.billing_customer_user_id,
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
    plan_name: str = "kuzu-standard",
    tier: GraphTier = GraphTier.KUZU_STANDARD,
  ) -> BillingSubscription:
    """Create a billing subscription for a graph database.

    Args:
        user_id: User ID (billing owner)
        graph_id: Graph database ID
        plan_name: Billing plan name
        tier: Graph tier

    Returns:
        BillingSubscription instance
    """
    available_plans = get_available_plans()
    if plan_name not in available_plans:
      max_tier = get_max_plan_tier()
      logger.warning(f"Plan '{plan_name}' not available, using '{max_tier}' instead")
      plan_name = max_tier

    plan_config = BillingConfig.get_subscription_plan(plan_name)
    if not plan_config:
      raise ValueError(f"Billing plan '{plan_name}' not found")

    existing = BillingSubscription.get_by_resource(
      resource_type="graph", resource_id=graph_id, session=self.session
    )

    if existing:
      logger.warning(f"Subscription already exists for graph {graph_id}")
      return existing

    customer = BillingCustomer.get_or_create(user_id, self.session)

    subscription = BillingSubscription.create_subscription(
      user_id=user_id,
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
      billing_customer_user_id=user_id,
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

    subscription.activate(self.session)

    BillingAuditLog.log_event(
      session=self.session,
      event_type=BillingEventType.SUBSCRIPTION_ACTIVATED,
      billing_customer_user_id=user_id,
      subscription_id=subscription.id,
      description=f"Activated subscription for graph {graph_id}",
      actor_type="system",
      event_data={
        "current_period_start": subscription.current_period_start.isoformat(),
        "current_period_end": subscription.current_period_end.isoformat(),
      },
    )

    generate_subscription_invoice(
      subscription=subscription,
      customer=customer,
      description=f"Graph Database Subscription - {plan_name}",
      session=self.session,
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
