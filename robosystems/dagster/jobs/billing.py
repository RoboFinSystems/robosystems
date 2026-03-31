"""Dagster billing jobs.

These jobs handle credit allocation and usage reporting.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from dagster import (
  DefaultScheduleStatus,
  OpExecutionContext,
  ScheduleDefinition,
  job,
  op,
)

from robosystems.config import env
from robosystems.dagster.resources import DatabaseResource
from robosystems.models.core import (
  Graph,
  GraphCredits,
  GraphCreditTransaction,
)
from robosystems.models.core.graph.graph_credits import CreditTransactionType
from robosystems.operations.graph.credit_service import CreditService

# ============================================================================
# Environment-based Schedule Status
# ============================================================================

# Billing schedules require database access for credit/usage tracking.
# RUNNING in prod/staging, STOPPED in dev.
BILLING_SCHEDULE_STATUS = (
  DefaultScheduleStatus.RUNNING
  if env.ENVIRONMENT != "dev"
  else DefaultScheduleStatus.STOPPED
)


# ============================================================================
# Stripe Webhook Processing Job
# ============================================================================


async def _handle_checkout_completed(
  session_data: dict, db_session: Any, context: OpExecutionContext
) -> None:
  """Handle checkout.session.completed event."""
  from robosystems.models.core.billing import BillingCustomer, BillingSubscription

  session_id = session_data.get("id")
  customer_id = session_data.get("customer")
  payment_status = session_data.get("payment_status")
  stripe_subscription_id = session_data.get("subscription")
  metadata = session_data.get("metadata", {})

  context.log.info(
    f"Checkout completed: session_id={session_id}, status={payment_status}"
  )

  subscription = BillingSubscription.get_by_provider_subscription_id(
    session_id, db_session
  )

  if not subscription and metadata.get("subscription_id"):
    subscription = (
      db_session.query(BillingSubscription)
      .filter(BillingSubscription.id == metadata["subscription_id"])
      .first()
    )

  if not subscription:
    context.log.warning(f"Subscription not found for checkout session: {session_id}")
    return

  customer = (
    db_session.query(BillingCustomer)
    .filter(BillingCustomer.org_id == subscription.org_id)
    .first()
  )

  if not customer:
    context.log.error(f"Customer not found for subscription: {subscription.id}")
    return

  if payment_status == "paid":
    customer.has_payment_method = True

    if not customer.stripe_customer_id:
      customer.stripe_customer_id = customer_id

    if stripe_subscription_id:
      subscription.stripe_subscription_id = stripe_subscription_id
      # Preserve checkout session ID in metadata before overwriting
      # Must create a new dict — SQLAlchemy won't detect in-place JSONB mutation
      if subscription.provider_subscription_id:
        subscription.subscription_metadata = {
          **(subscription.subscription_metadata or {}),
          "checkout_session_id": subscription.provider_subscription_id,
        }
      subscription.provider_subscription_id = stripe_subscription_id

    subscription.status = "provisioning"
    subscription.provider_customer_id = customer_id

    db_session.commit()

    context.log.info(f"Payment collected for org {customer.org_id}")

    # Trigger provisioning via sensor (set status to provisioning)
    await _trigger_resource_provisioning(subscription, db_session, context)

  else:
    context.log.warning(f"Checkout completed but payment not paid: {payment_status}")


class SubscriptionNotFoundError(Exception):
  """Raised when a webhook event cannot be matched to a BillingSubscription.

  Caught separately from ValueError/Exception in the webhook handler so that
  "not found" triggers a Stripe retry, while unrelated errors are logged.
  """


def _extract_stripe_subscription_id(data: dict) -> str | None:
  """Extract subscription ID from any Stripe event payload.

  Stripe's payload structure varies across API versions and event types.
  Instead of assuming a single location, search all known paths.
  """
  # The object IS a subscription (customer.subscription.updated/deleted)
  obj_id = data.get("id", "")
  if isinstance(obj_id, str) and obj_id.startswith("sub_"):
    return obj_id

  # Direct field (older API versions)
  if sub_id := data.get("subscription"):
    if isinstance(sub_id, str):
      return sub_id

  # Nested under parent (newer API versions for invoices)
  if parent := data.get("parent"):
    if sub_details := parent.get("subscription_details"):
      if sub_id := sub_details.get("subscription"):
        return sub_id

  # Inside line items (invoices use "lines", subscriptions use "items")
  for key in ("lines", "items"):
    for item in data.get(key, {}).get("data", []):
      if parent := item.get("parent"):
        if sub_details := parent.get("subscription_item_details"):
          if sub_id := sub_details.get("subscription"):
            return sub_id
      # Older/simpler format
      if sub_id := item.get("subscription"):
        if isinstance(sub_id, str):
          return sub_id

  return None


def _resolve_subscription(event_data: dict, db_session: Any, context: Any) -> Any:
  """Resolve the BillingSubscription for any Stripe event.

  Tries subscription ID first (multiple payload locations), then falls
  back to customer ID. This is the single entry point for subscription
  lookup — all handlers should use this instead of rolling their own.

  Raises SubscriptionNotFoundError if the subscription cannot be found.
  """
  from robosystems.models.core.billing import BillingCustomer, BillingSubscription

  # --- Try by subscription ID (multiple locations) ---
  stripe_sub_id = _extract_stripe_subscription_id(event_data)
  if stripe_sub_id:
    subscription = BillingSubscription.get_by_provider_subscription_id(
      stripe_sub_id, db_session
    )
    if subscription:
      return subscription

    subscription = BillingSubscription.get_by_stripe_subscription_id(
      stripe_sub_id, db_session
    )
    if subscription:
      return subscription

  # --- Fallback: customer ID → org → subscription ---
  # Prefer active/provisioning over canceled to avoid matching a stale
  # subscription when the org has re-subscribed.
  customer_id = event_data.get("customer")
  if customer_id:
    customer = BillingCustomer.get_by_stripe_customer_id(customer_id, db_session)
    if customer:
      from sqlalchemy import case

      status_priority = case(
        {"active": 0, "provisioning": 1, "pending_payment": 2, "canceled": 3},
        value=BillingSubscription.status,
        else_=4,
      )
      subscription = (
        db_session.query(BillingSubscription)
        .filter(
          BillingSubscription.org_id == customer.org_id,
          BillingSubscription.status.in_(
            ["pending_payment", "provisioning", "active", "canceled"]
          ),
        )
        .order_by(status_priority, BillingSubscription.created_at.desc())
        .first()
      )
      if subscription:
        context.log.info(
          f"Resolved subscription {subscription.id} via customer {customer_id}"
        )
        return subscription

  raise SubscriptionNotFoundError(
    f"Subscription not found (stripe_sub={stripe_sub_id}, "
    f"customer={event_data.get('customer')})"
  )


def _create_invoice_from_stripe(
  invoice_data: dict,
  subscription: Any,
  db_session: Any,
  context: Any,
) -> Any:
  """Create a BillingInvoice and line items from Stripe invoice data.

  Returns the invoice (newly created or existing).
  """
  from robosystems.models.core.billing import BillingInvoice, BillingInvoiceLineItem

  stripe_invoice_id: str = invoice_data.get("id", "")
  now_ts = int(datetime.now(UTC).timestamp())
  period_start = invoice_data.get("period_start") or now_ts
  period_end = invoice_data.get("period_end") or now_ts
  due_date = invoice_data.get("due_date")
  status = invoice_data.get("status", "draft")

  existing_invoice = (
    db_session.query(BillingInvoice)
    .filter(BillingInvoice.stripe_invoice_id == stripe_invoice_id)
    .first()
  )

  if existing_invoice:
    context.log.info(f"Invoice already synced from Stripe: {stripe_invoice_id}")
    return existing_invoice

  stripe_number = invoice_data.get("number")
  invoice_number = stripe_number or f"STRIPE-{stripe_invoice_id[-8:]}"

  now = datetime.now(UTC)
  invoice = BillingInvoice(
    org_id=subscription.org_id,
    invoice_number=invoice_number,
    period_start=datetime.fromtimestamp(period_start, tz=UTC),
    period_end=datetime.fromtimestamp(period_end, tz=UTC),
    subtotal_cents=invoice_data.get("subtotal", 0),
    tax_cents=invoice_data.get("tax", 0) or 0,
    total_cents=invoice_data.get("total", 0),
    status=status,
    stripe_invoice_id=stripe_invoice_id,
    invoice_pdf=invoice_data.get("invoice_pdf"),
    hosted_invoice_url=invoice_data.get("hosted_invoice_url"),
    currency=invoice_data.get("currency", "usd"),
    due_date=datetime.fromtimestamp(due_date, tz=UTC) if due_date else None,
    created_at=now,
  )

  db_session.add(invoice)
  db_session.flush()

  if not subscription.resource_id:
    context.log.warning(
      f"Subscription {subscription.id} has no resource_id yet "
      f"(status={subscription.status}) — line items will use empty resource_id"
    )

  lines = invoice_data.get("lines", {}).get("data", [])
  for line in lines:
    line_period = line.get("period", {})
    line_item = BillingInvoiceLineItem(
      invoice_id=invoice.id,
      subscription_id=subscription.id,
      resource_type=subscription.resource_type or "unknown",
      resource_id=subscription.resource_id or "",
      description=line.get("description") or subscription.plan_name,
      quantity=line.get("quantity", 1),
      unit_price_cents=line.get("unit_amount_excluding_tax") or line.get("amount", 0),
      amount_cents=line.get("amount", 0),
      period_start=datetime.fromtimestamp(
        line_period.get("start", period_start), tz=UTC
      ),
      period_end=datetime.fromtimestamp(line_period.get("end", period_end), tz=UTC),
    )
    db_session.add(line_item)

  db_session.commit()

  context.log.info(
    f"Synced Stripe invoice {stripe_invoice_id} ({invoice_number}) "
    f"with {len(lines)} line items"
  )
  return invoice


async def _handle_invoice_created(
  invoice_data: dict, db_session: Any, context: OpExecutionContext
) -> None:
  """Handle invoice.created event from Stripe.

  Creates a BillingInvoice from Stripe data, using Stripe's invoice number
  and syncing all line items from the Stripe invoice.
  """
  subscription = _resolve_subscription(invoice_data, db_session, context)
  _create_invoice_from_stripe(invoice_data, subscription, db_session, context)


async def _handle_payment_succeeded(
  invoice_data: dict, db_session: Any, context: OpExecutionContext
) -> None:
  """Handle invoice.payment_succeeded event.

  If the invoice record doesn't exist yet (race condition: invoice.created
  fires before checkout.session.completed updates the subscription's
  provider_subscription_id), this handler creates it.
  """
  from robosystems.models.core.billing import (
    BillingCustomer,
    BillingInvoice,
  )

  stripe_invoice_id = invoice_data.get("id")
  customer_id = invoice_data.get("customer")

  subscription = _resolve_subscription(invoice_data, db_session, context)

  customer = BillingCustomer.get_by_stripe_customer_id(customer_id, db_session)

  if customer:
    customer.has_payment_method = True
    db_session.commit()

  invoice = (
    db_session.query(BillingInvoice)
    .filter(BillingInvoice.stripe_invoice_id == stripe_invoice_id)
    .first()
  )

  if invoice:
    invoice.status = "paid"
    invoice.paid_at = datetime.now(UTC)
    invoice.payment_method = "stripe"
    invoice.payment_reference = stripe_invoice_id
    invoice.invoice_pdf = invoice_data.get("invoice_pdf") or invoice.invoice_pdf
    invoice.hosted_invoice_url = (
      invoice_data.get("hosted_invoice_url") or invoice.hosted_invoice_url
    )
    db_session.commit()

    context.log.info(f"Marked invoice {invoice.invoice_number} as paid")
  else:
    # invoice.created was missed (race condition) — create it now
    context.log.info(
      f"Invoice not found for payment_succeeded: {stripe_invoice_id}, "
      "creating from payment data"
    )
    invoice = _create_invoice_from_stripe(
      invoice_data, subscription, db_session, context
    )
    if invoice:
      invoice.status = "paid"
      invoice.paid_at = datetime.now(UTC)
      invoice.payment_method = "stripe"
      invoice.payment_reference = stripe_invoice_id
      db_session.commit()
      context.log.info(f"Created and marked invoice {invoice.invoice_number} as paid")

  if subscription.status in ["pending_payment", "provisioning"]:
    await _trigger_resource_provisioning(subscription, db_session, context)

  context.log.info(f"Payment succeeded for subscription {subscription.id}")


async def _handle_payment_failed(
  invoice_data: dict, db_session: Any, context: OpExecutionContext
) -> None:
  """Handle invoice.payment_failed event."""
  subscription = _resolve_subscription(invoice_data, db_session, context)

  if subscription.status == "pending_payment":
    subscription.status = "unpaid"

    error_message = "Payment failed"
    metadata = dict(subscription.subscription_metadata or {})
    metadata["error"] = error_message
    subscription.subscription_metadata = metadata

    db_session.commit()

  context.log.warning(f"Payment failed for subscription {subscription.id}")


async def _handle_invoice_updated(
  invoice_data: dict, db_session: Any, context: OpExecutionContext
) -> None:
  """Handle invoice.updated event from Stripe.

  Updates mutable fields on an existing invoice: status, PDF URL, hosted URL.
  If the invoice transitioned to paid, sets paid_at and payment_method.
  """
  from robosystems.models.core.billing import BillingInvoice

  stripe_invoice_id = invoice_data.get("id")

  invoice = (
    db_session.query(BillingInvoice)
    .filter(BillingInvoice.stripe_invoice_id == stripe_invoice_id)
    .first()
  )

  if not invoice:
    context.log.info(f"Invoice not found for update: {stripe_invoice_id}")
    return

  new_status = invoice_data.get("status", invoice.status)
  old_status = invoice.status

  invoice.status = new_status
  invoice.invoice_pdf = invoice_data.get("invoice_pdf") or invoice.invoice_pdf
  invoice.hosted_invoice_url = (
    invoice_data.get("hosted_invoice_url") or invoice.hosted_invoice_url
  )

  # Update invoice number if Stripe assigned one (draft -> open transition)
  stripe_number = invoice_data.get("number")
  if stripe_number and invoice.invoice_number.startswith("STRIPE-"):
    invoice.invoice_number = stripe_number

  if new_status == "paid" and old_status != "paid":
    invoice.paid_at = datetime.now(UTC)
    invoice.payment_method = "stripe"

  db_session.commit()
  context.log.info(
    f"Updated invoice {invoice.invoice_number}: {old_status} -> {new_status}"
  )


async def _handle_invoice_voided(
  invoice_data: dict, db_session: Any, context: OpExecutionContext
) -> None:
  """Handle invoice.voided event from Stripe."""
  from robosystems.models.core.billing import BillingInvoice

  stripe_invoice_id = invoice_data.get("id")

  invoice = (
    db_session.query(BillingInvoice)
    .filter(BillingInvoice.stripe_invoice_id == stripe_invoice_id)
    .first()
  )

  if not invoice:
    context.log.info(f"Invoice not found for void: {stripe_invoice_id}")
    return

  invoice.status = "void"
  db_session.commit()
  context.log.info(f"Voided invoice {invoice.invoice_number}")


async def _handle_charge_refunded(
  charge_data: dict, db_session: Any, context: OpExecutionContext
) -> None:
  """Handle charge.refunded event from Stripe.

  Adds a negative line item to the invoice for the refunded amount,
  which naturally reduces the invoice total via _recalculate_totals.
  """
  from robosystems.models.core.billing import (
    BillingAuditLog,
    BillingEventType,
    BillingInvoice,
    BillingInvoiceLineItem,
  )

  stripe_invoice_id = charge_data.get("invoice")
  stripe_charge_id = charge_data.get("id")
  amount_refunded = charge_data.get("amount_refunded", 0)

  if not stripe_invoice_id:
    context.log.info(f"Charge {stripe_charge_id} refunded but no invoice associated")
    return

  invoice = (
    db_session.query(BillingInvoice)
    .filter(BillingInvoice.stripe_invoice_id == stripe_invoice_id)
    .first()
  )

  if not invoice:
    context.log.warning(f"Invoice not found for refunded charge: {stripe_invoice_id}")
    return

  # Add refund as a negative line item
  refund_item = BillingInvoiceLineItem(
    invoice_id=invoice.id,
    resource_type="refund",
    resource_id=stripe_charge_id,
    description=f"Refund - {stripe_charge_id}",
    quantity=1,
    unit_price_cents=-amount_refunded,
    amount_cents=-amount_refunded,
    period_start=invoice.period_start,
    period_end=invoice.period_end,
  )
  db_session.add(refund_item)
  invoice._recalculate_totals(db_session)

  BillingAuditLog.log_event(
    session=db_session,
    event_type=BillingEventType.REFUND_PROCESSED,
    description=f"Refund of {amount_refunded} cents on charge {stripe_charge_id}",
    invoice_id=invoice.id,
    org_id=invoice.org_id,
    event_data={
      "stripe_charge_id": stripe_charge_id,
      "stripe_invoice_id": stripe_invoice_id,
      "amount_refunded_cents": amount_refunded,
    },
  )

  context.log.info(
    f"Processed refund of {amount_refunded} cents on invoice {invoice.invoice_number}"
  )


async def _handle_subscription_updated(
  subscription_data: dict, db_session: Any, context: OpExecutionContext
) -> None:
  """Handle customer.subscription.updated event.

  Handles three key scenarios:
  1. Portal cancellation: cancel_at_period_end=true → cancel locally (access until period end)
  2. Portal reactivation: cancel_at_period_end=false on a canceled sub → reactivate
  3. Status transitions: past_due, unpaid, etc.
  """
  from datetime import UTC, datetime

  status = subscription_data.get("status")
  cancel_at_period_end = subscription_data.get("cancel_at_period_end", False)

  subscription = _resolve_subscription(subscription_data, db_session, context)

  # Sync billing period dates from Stripe
  # Newer Stripe API versions moved these from the subscription root
  # to items.data[].current_period_start/end
  period_start = subscription_data.get("current_period_start")
  period_end = subscription_data.get("current_period_end")

  if not period_start or not period_end:
    items = subscription_data.get("items", {}).get("data", [])
    if items:
      period_start = period_start or items[0].get("current_period_start")
      period_end = period_end or items[0].get("current_period_end")

  if period_start:
    subscription.current_period_start = datetime.fromtimestamp(period_start, tz=UTC)
  if period_end:
    subscription.current_period_end = datetime.fromtimestamp(period_end, tz=UTC)

  # --- Portal cancellation (cancel_at_period_end) ---
  # Mirrors the UI cancel: mark canceled, keep access until period end
  if cancel_at_period_end:
    if subscription.status != "canceled":
      subscription.cancel(db_session, immediate=False)
      context.log.info(
        f"Subscription {subscription.id} canceled via Stripe portal "
        f"(ends at period end: {subscription.ends_at})"
      )
    else:
      context.log.info(
        f"Subscription {subscription.id} already canceled locally, "
        f"ignoring cancel_at_period_end update from Stripe"
      )
    return

  # --- Portal reactivation (user removed pending cancellation) ---
  if (
    not cancel_at_period_end
    and subscription.status == "canceled"
    and subscription.ends_at
    and subscription.ends_at > datetime.now(UTC)
    and status == "active"
  ):
    subscription.status = "active"
    subscription.canceled_at = None
    subscription.ends_at = None
    subscription.updated_at = datetime.now(UTC)

    # Restore graph if it was suspended
    if subscription.resource_type == "graph" and subscription.resource_id:
      from robosystems.models.core.graph import Graph, GraphStatus

      graph = Graph.get_by_id(
        subscription.resource_id, db_session, include_deprovisioned=True
      )
      if graph and graph.status == GraphStatus.SUSPENDED.value:
        graph.transition_status(GraphStatus.ACTIVE, db_session)
        context.log.info(
          f"Restored graph {subscription.resource_id} from suspended to active"
        )

    db_session.commit()
    context.log.info(f"Subscription {subscription.id} reactivated via Stripe portal")
    return

  # --- Other status transitions ---
  status_mapping = {
    "active": "active",
    "past_due": "past_due",
    "unpaid": "unpaid",
    "canceled": "canceled",
    "incomplete": "pending_payment",
    "incomplete_expired": "canceled",
    "trialing": "active",
  }

  new_status = status_mapping.get(status, subscription.status)

  if new_status != subscription.status:
    old_status = subscription.status
    if new_status == "canceled":
      # Use cancel() to properly set canceled_at and ends_at
      subscription.cancel(db_session, immediate=True)
    else:
      subscription.status = new_status
      subscription.updated_at = datetime.now(UTC)
      db_session.commit()

    context.log.info(
      f"Subscription {subscription.id} status: {old_status} -> {new_status}"
    )
  else:
    # Commit period date sync even if status unchanged
    db_session.commit()


async def _handle_subscription_deleted(
  subscription_data: dict, db_session: Any, context: OpExecutionContext
) -> None:
  """Handle customer.subscription.deleted event.

  Fired when the Stripe subscription is fully terminated (e.g., period ended
  after a cancel_at_period_end cancellation, or immediate deletion).
  """
  from datetime import UTC, datetime

  subscription = _resolve_subscription(subscription_data, db_session, context)

  if subscription.status == "canceled":
    # Already canceled (via portal updated handler or UI cancel button).
    # Preserve original canceled_at timestamp. Use Stripe's period end if
    # the user paid for the current period, otherwise terminate now.
    now = datetime.now(UTC)
    # Period end may be at item level in newer Stripe API versions
    period_end_ts = subscription_data.get("current_period_end")
    if not period_end_ts:
      items = subscription_data.get("items", {}).get("data", [])
      if items:
        period_end_ts = items[0].get("current_period_end")
    if period_end_ts:
      period_end = datetime.fromtimestamp(period_end_ts, tz=UTC)
      subscription.ends_at = period_end if period_end > now else now
    else:
      subscription.ends_at = now
    subscription.updated_at = now
    db_session.commit()
    context.log.info(
      f"Subscription {subscription.id} fully terminated "
      f"(was canceled at {subscription.canceled_at})"
    )
  else:
    # Direct/unexpected deletion — cancel immediately
    subscription.cancel(db_session, immediate=True)
    context.log.info(f"Subscription {subscription.id} canceled via Stripe deletion")


async def _handle_setup_intent_succeeded(
  setup_intent_data: dict, db_session: Any, context: OpExecutionContext
) -> None:
  """Handle setup_intent.succeeded event.

  Fired when a customer adds a payment method via the Stripe portal.
  Updates has_payment_method on the BillingCustomer so direct subscription
  creation knows they can be charged.
  """
  from robosystems.models.core.billing import BillingCustomer

  customer_id = setup_intent_data.get("customer")
  if not customer_id:
    context.log.info("Setup intent succeeded but no customer ID")
    return

  customer = BillingCustomer.get_by_stripe_customer_id(customer_id, db_session)
  if not customer:
    context.log.warning(f"Customer not found for setup intent: {customer_id}")
    return

  if not customer.has_payment_method:
    customer.has_payment_method = True
    db_session.commit()
    context.log.info(
      f"Marked customer {customer.org_id} as having payment method via portal"
    )
  else:
    context.log.info(f"Customer {customer.org_id} already has payment method on file")


async def _trigger_resource_provisioning(
  subscription: Any, db_session: Any, context: OpExecutionContext
) -> None:
  """Trigger resource provisioning after payment confirmation.

  Directly provisions the resource, eliminating sensor polling delay and
  ECS cold start.
  """
  from robosystems.models.core import OrgRole, OrgUser

  resource_config = subscription.subscription_metadata.get("resource_config", {})
  resource_type = subscription.resource_type

  user_id = subscription.subscription_metadata.get("user_id")
  if not user_id:
    owner = (
      db_session.query(OrgUser)
      .filter(
        OrgUser.org_id == subscription.org_id,
        OrgUser.role == OrgRole.OWNER,
      )
      .first()
    )
    if not owner:
      context.log.error(f"No owner found for org {subscription.org_id}")
      subscription.status = "failed"
      subscription.subscription_metadata["error"] = "No org owner found"
      db_session.commit()
      return
    user_id = owner.user_id

  context.log.info(f"Triggering provisioning for {resource_type}")

  from robosystems.operations.graph.provisioning_service import (
    run_graph_provisioning,
    run_user_repository_provisioning,
  )

  if resource_type == "graph":
    if not subscription.subscription_metadata:
      subscription.subscription_metadata = {}
    subscription.subscription_metadata.update(resource_config)
    subscription.status = "provisioning"
    db_session.commit()

    tier = subscription.plan_name

    context.log.info(f"Provisioning graph for subscription {subscription.id}")

    try:
      result = await run_graph_provisioning(
        operation_id=None,  # No SSE tracking for webhook-triggered provisioning
        subscription_id=str(subscription.id),
        user_id=str(user_id),
        tier=tier,
      )
      context.log.info(
        f"Graph provisioning completed: graph_id={result.get('graph_id')}"
      )
    except Exception as e:
      context.log.error(f"Graph provisioning failed: {e}")
      raise

  elif resource_type == "repository":
    repository_name = resource_config.get("repository_name")

    if not subscription.subscription_metadata:
      subscription.subscription_metadata = {}
    subscription.subscription_metadata["repository_name"] = repository_name
    subscription.status = "provisioning"
    db_session.commit()

    context.log.info(
      f"Provisioning repository {repository_name} for subscription {subscription.id}"
    )

    try:
      result = await run_user_repository_provisioning(
        operation_id=None,
        subscription_id=str(subscription.id),
        user_id=str(user_id),
        repository_name=repository_name,
      )
      context.log.info(
        f"Repository provisioning completed: {result.get('repository_name')}"
      )
    except Exception as e:
      context.log.error(f"Repository provisioning failed: {e}")
      raise

  else:
    context.log.error(f"Unknown resource type: {resource_type}")
    subscription.status = "failed"
    subscription.subscription_metadata["error"] = (
      f"Unknown resource type: {resource_type}"
    )
    db_session.commit()


# ============================================================================
# Credit Allocation Jobs
# ============================================================================


@op
def get_graphs_with_negative_balance(
  context: OpExecutionContext, db: DatabaseResource
) -> list[dict[str, Any]]:
  """Get all graphs that have negative credit balances (overages)."""
  with db.get_session() as session:
    results = (
      session.query(
        GraphCredits.graph_id,
        GraphCredits.user_id,
        GraphCredits.billing_admin_id,
        GraphCredits.current_balance,
        GraphCredits.monthly_allocation,
        Graph.graph_tier,
      )
      .join(Graph, GraphCredits.graph_id == Graph.graph_id)
      .filter(GraphCredits.current_balance < 0)
      .all()
    )

    graphs = [
      {
        "graph_id": r.graph_id,
        "user_id": r.user_id,
        "billing_admin_id": r.billing_admin_id,
        "negative_balance": float(r.current_balance),
        "monthly_allocation": float(r.monthly_allocation),
        "graph_tier": r.graph_tier,
        "overage_amount": abs(float(r.current_balance)),
      }
      for r in results
    ]

    context.log.info(f"Found {len(graphs)} graphs with negative balances")
    return graphs


@op
def process_overage_invoices(
  context: OpExecutionContext,
  db: DatabaseResource,
  graphs_with_negative_balance: list[dict[str, Any]],
) -> list[dict[str, Any]]:
  """Process overage invoices for graphs with negative balances."""
  invoices = []

  with db.get_session() as session:
    for graph_info in graphs_with_negative_balance:
      try:
        overage_credits = abs(Decimal(str(graph_info["negative_balance"])))
        usd_amount = float(overage_credits) * 0.005

        credits_record = GraphCredits.get_by_graph_id(graph_info["graph_id"], session)

        if credits_record:
          GraphCreditTransaction.create_transaction(
            graph_credits_id=credits_record.id,
            transaction_type=CreditTransactionType.ALLOCATION,
            amount=Decimal("0"),
            description=f"Monthly overage invoice: {overage_credits} credits (${usd_amount:.2f})",
            metadata={
              "invoice_type": "overage",
              "overage_credits": str(overage_credits),
              "amount_usd": str(usd_amount),
              "billing_period_end": datetime.now(UTC).replace(day=1).isoformat(),
              "graph_tier": str(graph_info["graph_tier"]),
            },
            session=session,
          )

        invoice = {
          "graph_id": graph_info["graph_id"],
          "user_id": graph_info["user_id"],
          "overage_credits": float(overage_credits),
          "amount_usd": usd_amount,
          "invoice_date": datetime.now(UTC).isoformat(),
          "status": "pending_payment",
        }
        invoices.append(invoice)
        context.log.info(
          f"Generated overage invoice for {graph_info['graph_id']}: ${usd_amount:.2f}"
        )

      except Exception as e:
        context.log.error(
          f"Failed to process overage for {graph_info['graph_id']}: {e}"
        )

  return invoices


@op
def allocate_monthly_credits(
  context: OpExecutionContext,
  db: DatabaseResource,
  overage_invoices: list[dict[str, Any]],
) -> dict[str, Any]:
  """Allocate monthly credits to all graphs."""
  with db.get_session() as session:
    credit_service = CreditService(session)
    result = credit_service.bulk_allocate_monthly_credits()

    context.log.info(
      f"Allocated {result['total_credits_allocated']} credits "
      f"to {result['allocated_graphs']} graphs"
    )

    return {
      "allocation_result": result,
      "overage_invoices_count": len(overage_invoices),
      "timestamp": datetime.now(UTC).isoformat(),
    }


@op
def cleanup_old_credit_transactions(
  context: OpExecutionContext,
  db: DatabaseResource,
  allocation_result: dict[str, Any],
) -> dict[str, Any]:
  """Clean up old credit transaction records."""
  months_to_keep = 12
  cutoff_date = datetime.now(UTC) - timedelta(days=months_to_keep * 30)

  with db.get_session() as session:
    from sqlalchemy import and_

    count_query = session.query(GraphCreditTransaction).filter(
      and_(
        GraphCreditTransaction.created_at < cutoff_date,
        GraphCreditTransaction.transaction_type
        != CreditTransactionType.ALLOCATION.value,
      )
    )

    total_count = count_query.count()

    if total_count == 0:
      context.log.info("No old transactions to clean up")
      return {"deleted_transactions": 0, "allocation_result": allocation_result}

    deleted_count = count_query.delete()
    context.log.info(f"Deleted {deleted_count} old credit transactions")

    return {
      "deleted_transactions": deleted_count,
      "cutoff_date": cutoff_date.isoformat(),
      "allocation_result": allocation_result,
    }


@job(tags={"dagster/priority": "1", "dagster/max_retries": 3})
def monthly_credit_allocation_job():
  """Monthly credit allocation and overage processing job."""
  graphs = get_graphs_with_negative_balance()
  invoices = process_overage_invoices(graphs)
  result = allocate_monthly_credits(invoices)
  cleanup_old_credit_transactions(result)


@op
def generate_usage_report(
  context: OpExecutionContext, db: DatabaseResource
) -> dict[str, Any]:
  """Generate comprehensive monthly usage report."""
  last_month = datetime.now(UTC).replace(day=1) - timedelta(days=1)
  year = last_month.year
  month = last_month.month

  context.log.info(f"Generating usage report for {year}-{month:02d}")

  total_credits_consumed = Decimal("0")
  total_credits_allocated = Decimal("0")
  graphs_with_overage = 0
  graph_reports = []

  with db.get_session() as session:
    all_graphs = session.query(GraphCredits).all()

    month_start = datetime(year, month, 1, tzinfo=UTC)
    if month == 12:
      month_end = datetime(year + 1, 1, 1, tzinfo=UTC)
    else:
      month_end = datetime(year, month + 1, 1, tzinfo=UTC)

    for graph_credits in all_graphs:
      transactions = (
        session.query(GraphCreditTransaction)
        .filter(
          GraphCreditTransaction.graph_credits_id == graph_credits.id,
          GraphCreditTransaction.created_at >= month_start,
          GraphCreditTransaction.created_at < month_end,
        )
        .all()
      )

      consumption = sum(
        abs(t.amount)
        for t in transactions
        if t.transaction_type == CreditTransactionType.CONSUMPTION
      )
      allocation = sum(
        t.amount
        for t in transactions
        if t.transaction_type == CreditTransactionType.ALLOCATION
      )

      total_credits_consumed += consumption
      total_credits_allocated += allocation

      has_overage = graph_credits.current_balance < 0
      if has_overage:
        graphs_with_overage += 1

      graph_reports.append(
        {
          "graph_id": graph_credits.graph_id,
          "credits_consumed": float(consumption),
          "credits_allocated": float(allocation),
          "current_balance": float(graph_credits.current_balance),
          "has_overage": has_overage,
        }
      )

  context.log.info(
    f"Report complete: {len(graph_reports)} graphs, "
    f"{float(total_credits_consumed)} credits consumed"
  )

  return {
    "year": year,
    "month": month,
    "total_graphs": len(graph_reports),
    "total_credits_consumed": float(total_credits_consumed),
    "total_credits_allocated": float(total_credits_allocated),
    "graphs_with_overage": graphs_with_overage,
    "timestamp": datetime.now(UTC).isoformat(),
  }


@job(tags={"dagster/priority": "1", "dagster/max_retries": 3})
def monthly_usage_report_job():
  """Monthly usage report generation job."""
  generate_usage_report()


# ============================================================================
# Schedules
# ============================================================================

monthly_credit_allocation_schedule = ScheduleDefinition(
  job=monthly_credit_allocation_job,
  cron_schedule="0 0 1 * *",  # 1st of month at midnight UTC
  default_status=BILLING_SCHEDULE_STATUS,
)

monthly_usage_report_schedule = ScheduleDefinition(
  job=monthly_usage_report_job,
  cron_schedule="0 6 2 * *",  # 2nd of month at 6 AM UTC
  default_status=BILLING_SCHEDULE_STATUS,
)
