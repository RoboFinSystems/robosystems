"""Dagster billing jobs: Stripe webhook handling, credit allocation, usage reporting."""

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
  GraphCredits,
  GraphCreditTransaction,
  UserRepository,
  UserRepositoryCredits,
)
from robosystems.models.core.graph.graph_credits import CreditTransactionType
from robosystems.operations.graph.credit_service import CreditService

BILLING_SCHEDULE_STATUS = (
  DefaultScheduleStatus.RUNNING
  if env.ENVIRONMENT != "dev"
  else DefaultScheduleStatus.STOPPED
)


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
    from robosystems.models.core.billing import BillingAuditLog, BillingEventType
    from robosystems.models.core.billing.subscription import (
      TERMINAL_SUBSCRIPTION_STATUSES,
    )

    if (
      subscription.status in TERMINAL_SUBSCRIPTION_STATUSES
      and not subscription.resource_id
    ):
      # Retired locally (customer retried checkout) but this session was paid
      # before it expired at the provider. Money moved, so reopen the row for
      # the provisioning claim, which refuses terminal rows.
      previous_status = subscription.status
      subscription.status = "pending_payment"
      subscription.ends_at = None
      subscription.canceled_at = None
      subscription.subscription_metadata = {
        **(subscription.subscription_metadata or {}),
        "reclaimed_from_status": previous_status,
        "reclaimed_checkout_session_id": session_id,
      }
      db_session.commit()
      BillingAuditLog.log_event(
        session=db_session,
        event_type=BillingEventType.SUBSCRIPTION_RESUMED,
        description=(
          f"Reopened {previous_status} subscription {subscription.id}: its "
          f"checkout session {session_id} was paid after the row was retired"
        ),
        subscription_id=subscription.id,
        org_id=subscription.org_id,
        event_data={
          "checkout_session_id": session_id,
          "previous_status": previous_status,
          "stripe_subscription_id": stripe_subscription_id,
        },
      )
      context.log.warning(
        f"Checkout session {session_id} paid against {previous_status} "
        f"subscription {subscription.id}; reopened for provisioning"
      )

    customer.has_payment_method = True

    if not customer.stripe_customer_id:
      customer.stripe_customer_id = customer_id

    if stripe_subscription_id:
      subscription.stripe_subscription_id = stripe_subscription_id
      # Keep the cs_… id for the checkout-status lookup before
      # provider_subscription_id is overwritten. Take it from the event, not the
      # column (already the sub id on redelivery), and only if absent. New dict:
      # SQLAlchemy won't detect in-place JSONB mutation.
      existing_metadata = subscription.subscription_metadata or {}
      if "checkout_session_id" not in existing_metadata:
        subscription.subscription_metadata = {
          **existing_metadata,
          "checkout_session_id": session_id,
        }
      subscription.provider_subscription_id = stripe_subscription_id

    subscription.provider_customer_id = customer_id

    db_session.commit()

    context.log.info(f"Payment collected for org {customer.org_id}")

    # Don't set "provisioning" here: that transition is the claim, arbitrated
    # inside the sink so every trigger contends on equal terms.
    await _trigger_resource_provisioning(subscription, db_session, context)

  else:
    context.log.warning(f"Checkout completed but payment not paid: {payment_status}")


class SubscriptionNotFoundError(Exception):
  """No BillingSubscription matches the webhook event; the handler lets Stripe retry."""


def _as_utc(value: datetime) -> datetime:
  """Timestamp columns load naive; compare them as the UTC they were stored."""
  return value if value.tzinfo else value.replace(tzinfo=UTC)


def _stripe_confirms_reactivation(stripe_subscription_id: str | None, context) -> bool:
  """Whether Stripe's live subscription is active with no pending cancel.

  A payload emitted before a local cancel and redelivered after it looks
  exactly like a portal reactivation; only the live object tells them apart.
  """
  if not stripe_subscription_id:
    return False
  from robosystems.operations.providers.payment_provider import get_payment_provider

  live = get_payment_provider("stripe").get_subscription_state(stripe_subscription_id)
  confirmed = live.get("status") == "active" and not live.get("cancel_at_period_end")
  if not confirmed:
    context.log.warning(
      f"Ignoring stale reactivation for {stripe_subscription_id}: live state {live}"
    )
  return confirmed


def _extract_stripe_subscription_id(data: dict) -> str | None:
  """Stripe subscription id from any event payload; its location varies by API version."""
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


def _extract_local_subscription_id(data: dict) -> str | None:
  """Our own `BillingSubscription.id` from the Stripe payload's metadata.

  Stripe copies it onto derived objects such as invoices. It names one exact
  row, unlike the customer id, whose org may hold many subscriptions.
  """
  candidates = [data.get("metadata")]
  if parent := data.get("parent"):
    if isinstance(parent, dict) and (details := parent.get("subscription_details")):
      if isinstance(details, dict):
        candidates.append(details.get("metadata"))

  for metadata in candidates:
    if isinstance(metadata, dict):
      local_id = metadata.get("subscription_id")
      if isinstance(local_id, str) and local_id:
        return local_id
  return None


def _resolve_subscription(
  event_data: dict,
  db_session: Any,
  context: Any,
  *,
  allow_customer_fallback: bool = True,
) -> Any:
  """Resolve the BillingSubscription for any Stripe event.

  Order: Stripe subscription id, then our id from the payload metadata, then
  (if allowed and unambiguous) the billing customer's sole candidate.

  Handlers that mutate or cancel must pass `allow_customer_fallback=False`: a
  `customer.subscription.*` payload always carries its own id, so a miss means
  we don't have it, and another row from the same org would be the wrong one.
  The fallback exists for invoice events, which can arrive before
  `checkout.session.completed` writes the local row.

  Raises SubscriptionNotFoundError if the subscription cannot be resolved.
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

  # --- Try by our own subscription ID, carried in the Stripe metadata ---
  local_sub_id = _extract_local_subscription_id(event_data)
  if local_sub_id:
    subscription = (
      db_session.query(BillingSubscription)
      .filter(BillingSubscription.id == local_sub_id)
      .first()
    )
    if subscription:
      return subscription

  # --- Fallback: customer ID → org → subscription ---
  customer_id = event_data.get("customer")
  if customer_id and allow_customer_fallback:
    customer = BillingCustomer.get_by_stripe_customer_id(customer_id, db_session)
    if customer:
      candidates = (
        db_session.query(BillingSubscription)
        .filter(
          BillingSubscription.org_id == customer.org_id,
          BillingSubscription.status.in_(
            ["pending_payment", "provisioning", "active", "canceled"]
          ),
        )
        .limit(2)
        .all()
      )
      # Never guess between several: invoices dedupe on `stripe_invoice_id`,
      # so a misattribution here is permanent.
      if len(candidates) == 1:
        context.log.info(
          f"Resolved subscription {candidates[0].id} via customer {customer_id} "
          f"(sole candidate for org {customer.org_id})"
        )
        return candidates[0]
      if len(candidates) > 1:
        context.log.warning(
          f"Refusing to resolve via customer {customer_id}: org "
          f"{customer.org_id} has multiple candidate subscriptions and the "
          f"event carries no subscription id to disambiguate them"
        )

  raise SubscriptionNotFoundError(
    f"Subscription not found (stripe_sub={stripe_sub_id}, "
    f"local_sub={local_sub_id}, customer={event_data.get('customer')}, "
    f"customer_fallback={'allowed' if allow_customer_fallback else 'disallowed'})"
  )


def _create_invoice_from_stripe(
  invoice_data: dict,
  subscription: Any,
  db_session: Any,
  context: Any,
) -> Any:
  """Create a BillingInvoice and line items from Stripe data; idempotent on the Stripe id."""
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
  """Handle invoice.created event from Stripe."""
  subscription = _resolve_subscription(invoice_data, db_session, context)
  _create_invoice_from_stripe(invoice_data, subscription, db_session, context)


async def _handle_payment_succeeded(
  invoice_data: dict, db_session: Any, context: OpExecutionContext
) -> None:
  """Handle invoice.payment_succeeded event.

  Creates the invoice if invoice.created was missed (it can fire before
  checkout.session.completed links the subscription).
  """
  from robosystems.models.core.billing import (
    BillingAuditLog,
    BillingCustomer,
    BillingEventType,
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

  BillingAuditLog.log_event(
    session=db_session,
    event_type=BillingEventType.PAYMENT_SUCCEEDED,
    description=f"Payment succeeded for subscription {subscription.id}",
    subscription_id=subscription.id,
    org_id=subscription.org_id,
    invoice_id=invoice.id if invoice else None,
    event_data={
      "stripe_invoice_id": stripe_invoice_id,
      "amount_paid_cents": invoice_data.get("amount_paid"),
    },
  )

  if subscription.status in ["pending_payment", "provisioning"]:
    await _trigger_resource_provisioning(subscription, db_session, context)

  context.log.info(f"Payment succeeded for subscription {subscription.id}")


async def _handle_payment_failed(
  invoice_data: dict, db_session: Any, context: OpExecutionContext
) -> None:
  """Handle invoice.payment_failed event.

  A failed first invoice -> ``unpaid``; a failed renewal -> ``past_due``, so a
  missed ``customer.subscription.updated`` can't leave it ``active`` while
  Stripe duns. ``updated`` alone restores ``active`` after a successful retry.
  """
  from robosystems.models.core.billing import BillingAuditLog, BillingEventType

  subscription = _resolve_subscription(invoice_data, db_session, context)

  if subscription.status in ("pending_payment", "active"):
    subscription.status = (
      "unpaid" if subscription.status == "pending_payment" else "past_due"
    )

    error_message = "Payment failed"
    metadata = dict(subscription.subscription_metadata or {})
    metadata["error"] = error_message
    subscription.subscription_metadata = metadata

    db_session.commit()
    subscription._invalidate_access_cache()

  BillingAuditLog.log_event(
    session=db_session,
    event_type=BillingEventType.PAYMENT_FAILED,
    description=f"Payment failed for subscription {subscription.id}",
    subscription_id=subscription.id,
    org_id=subscription.org_id,
    event_data={
      "stripe_invoice_id": invoice_data.get("id"),
      "subscription_status": subscription.status,
      "amount_due_cents": invoice_data.get("amount_due"),
      "attempt_count": invoice_data.get("attempt_count"),
    },
  )

  context.log.warning(f"Payment failed for subscription {subscription.id}")


async def _handle_invoice_updated(
  invoice_data: dict, db_session: Any, context: OpExecutionContext
) -> None:
  """Handle invoice.updated event from Stripe."""
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
  # Events arrive out of order. Paid and void are final; uncollectible can
  # still be paid but never goes back to draft or open.
  if old_status in ("paid", "void") or (
    old_status == "uncollectible" and new_status in ("draft", "open")
  ):
    new_status = old_status

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
  """Handle charge.refunded: record the refund as a negative invoice line item."""
  from robosystems.models.core.billing import (
    BillingAuditLog,
    BillingEventType,
    BillingInvoice,
    BillingInvoiceLineItem,
  )
  from robosystems.operations.providers.payment_provider import get_payment_provider

  provider = get_payment_provider("stripe")
  stripe_charge_id = charge_data.get("id")
  stripe_invoice_id = charge_data.get("invoice")
  payment_intent_id = charge_data.get("payment_intent")
  if not stripe_invoice_id and payment_intent_id:
    stripe_invoice_id = provider.invoice_for_payment_intent(payment_intent_id)

  if not stripe_invoice_id:
    context.log.info(f"Charge {stripe_charge_id} refunded but no invoice associated")
    return

  # Locked so refunds of different charges on one invoice, delivered at once,
  # serialize on the line lookup below.
  invoice = (
    db_session.query(BillingInvoice)
    .filter(BillingInvoice.stripe_invoice_id == stripe_invoice_id)
    .with_for_update()
    .first()
  )

  if not invoice:
    context.log.warning(f"Invoice not found for refunded charge: {stripe_invoice_id}")
    return

  refund_item = (
    db_session.query(BillingInvoiceLineItem)
    .filter(
      BillingInvoiceLineItem.invoice_id == invoice.id,
      BillingInvoiceLineItem.resource_type == "refund",
      BillingInvoiceLineItem.resource_id == stripe_charge_id,
    )
    .first()
  )
  if refund_item is None:
    refund_item = BillingInvoiceLineItem(
      invoice_id=invoice.id,
      resource_type="refund",
      resource_id=stripe_charge_id,
      description=f"Refund - {stripe_charge_id}",
      quantity=1,
      period_start=invoice.period_start,
      period_end=invoice.period_end,
    )
    db_session.add(refund_item)
  # Deliveries arrive out of order and a failed refund lowers the total, so
  # the charge's live running total is the only reliable figure.
  amount_refunded = provider.charge_amount_refunded(stripe_charge_id)
  refund_item.unit_price_cents = -amount_refunded
  refund_item.amount_cents = -amount_refunded
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


async def _apply_subscription_updated(
  subscription_data: dict,
  db_session: Any,
  context: OpExecutionContext,
  subscription: Any,
) -> None:
  """Handle customer.subscription.updated: portal cancel/reactivate and status sync."""
  from datetime import UTC, datetime

  status = subscription_data.get("status")
  cancel_at_period_end = subscription_data.get("cancel_at_period_end", False)

  # Newer Stripe API versions moved these to items.data[].
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

  # A tier upgrade changes the price and Stripe reports active; the worker
  # owns the upgrading -> active transition.
  if subscription.status == "upgrading" and status == "active":
    context.log.info(
      f"Subscription {subscription.id} upgrading (infra migration), "
      f"ignoring Stripe active status"
    )
    db_session.commit()
    return

  # Portal cancel mirrors the UI cancel: access continues to period end.
  if cancel_at_period_end:
    if subscription.status == "failed":
      context.log.info(
        f"Subscription {subscription.id} is failed; ignoring cancel_at_period_end"
      )
      db_session.commit()
    elif subscription.status != "canceled":
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

  # Portal reactivation: the user removed a pending cancellation.
  if (
    not cancel_at_period_end
    and subscription.status == "canceled"
    and subscription.ends_at
    and _as_utc(subscription.ends_at) > datetime.now(UTC)
    and status == "active"
    and _stripe_confirms_reactivation(subscription_data.get("id"), context)
  ):
    subscription.status = "active"
    subscription.canceled_at = None
    subscription.ends_at = None
    subscription.updated_at = datetime.now(UTC)

    if subscription.restore_suspended_graph(db_session):
      context.log.info(
        f"Restored graph {subscription.resource_id} from suspended to active"
      )

    db_session.commit()
    subscription._invalidate_access_cache()
    context.log.info(f"Subscription {subscription.id} reactivated via Stripe portal")
    return

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

  # A late or out-of-order event must not revive a terminal row: the only
  # way back from canceled is the portal reactivation above.
  if subscription.status in ("canceled", "failed"):
    context.log.warning(
      f"Ignoring Stripe status {status!r} for terminal subscription "
      f"{subscription.id} ({subscription.status})"
    )
    db_session.commit()
    return

  if new_status != subscription.status:
    old_status = subscription.status
    if new_status == "canceled":
      # cancel() sets canceled_at/ends_at and invalidates the access cache.
      subscription.cancel(db_session, immediate=True)
    else:
      subscription.status = new_status
      subscription.updated_at = datetime.now(UTC)
      db_session.commit()
      subscription._invalidate_access_cache()

    context.log.info(
      f"Subscription {subscription.id} status: {old_status} -> {new_status}"
    )
  else:
    # Still commit the period-date sync.
    db_session.commit()


async def _apply_subscription_deleted(
  subscription_data: dict,
  db_session: Any,
  context: OpExecutionContext,
  subscription: Any,
) -> None:
  """Handle customer.subscription.deleted: the Stripe subscription fully terminated."""
  from datetime import UTC, datetime

  if subscription.status == "canceled":
    # Already canceled locally; keep the original canceled_at.
    from robosystems.models.core.billing.subscription import CancellationType

    now = datetime.now(UTC)
    if subscription.cancellation_type == CancellationType.IMMEDIATE.value:
      # Don't extend an immediate cancel to period end: the lifecycle sensors
      # gate teardown on ends_at < now.
      if subscription.ends_at is None or _as_utc(subscription.ends_at) > now:
        subscription.ends_at = now
    else:
      # Period-end cancel: the user paid through Stripe's period end (item-level
      # in newer API versions).
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
    subscription.cancel(db_session, immediate=True)
    context.log.info(f"Subscription {subscription.id} canceled via Stripe deletion")


async def _handle_subscription_updated(
  subscription_data: dict, db_session: Any, context: OpExecutionContext
) -> None:
  """Handle customer.subscription.updated, then bring a repository grant
  into line with the resulting status."""
  subscription = _resolve_subscription(
    subscription_data, db_session, context, allow_customer_fallback=False
  )
  await _apply_subscription_updated(
    subscription_data, db_session, context, subscription
  )
  _reconcile_repository_grant(subscription, db_session)


async def _handle_subscription_deleted(
  subscription_data: dict, db_session: Any, context: OpExecutionContext
) -> None:
  """Handle customer.subscription.deleted, then bring a repository grant
  into line with the resulting status."""
  subscription = _resolve_subscription(
    subscription_data, db_session, context, allow_customer_fallback=False
  )
  await _apply_subscription_deleted(
    subscription_data, db_session, context, subscription
  )
  _reconcile_repository_grant(subscription, db_session)


def _reconcile_repository_grant(subscription: Any, db_session: Any) -> None:
  from robosystems.operations.billing.repository_subscriptions import (
    reconcile_repository_grant,
  )

  db_session.refresh(subscription)
  reconcile_repository_grant(subscription, db_session)


async def _handle_setup_intent_succeeded(
  setup_intent_data: dict, db_session: Any, context: OpExecutionContext
) -> None:
  """Handle setup_intent.succeeded: a payment method was added via the portal."""
  from robosystems.models.core.billing import (
    BillingAuditLog,
    BillingCustomer,
    BillingEventType,
  )

  customer_id = setup_intent_data.get("customer")
  if not customer_id:
    context.log.info("Setup intent succeeded but no customer ID")
    return

  customer = BillingCustomer.get_by_stripe_customer_id(customer_id, db_session)
  if not customer:
    context.log.warning(f"Customer not found for setup intent: {customer_id}")
    return

  # Normally an id string (webhooks are unexpanded); tolerate an expanded object.
  payment_method = setup_intent_data.get("payment_method")
  if isinstance(payment_method, dict):
    payment_method = payment_method.get("id")
  payment_method_id = payment_method if isinstance(payment_method, str) else None

  changed = False

  if not customer.has_payment_method:
    customer.has_payment_method = True
    changed = True
    context.log.info(
      f"Marked customer {customer.org_id} as having payment method via portal"
    )
  else:
    context.log.info(f"Customer {customer.org_id} already has payment method on file")

  # A later setup intent means a newer default card.
  if payment_method_id and customer.default_payment_method_id != payment_method_id:
    customer.default_payment_method_id = payment_method_id
    changed = True
    context.log.info(f"Recorded default payment method for org {customer.org_id}")

  if changed:
    customer.updated_at = datetime.now(UTC)
    db_session.commit()

    BillingAuditLog.log_event(
      session=db_session,
      event_type=BillingEventType.PAYMENT_METHOD_ADDED,
      description=f"Payment method added via Stripe portal for org {customer.org_id}",
      org_id=customer.org_id,
      event_data={
        "stripe_customer_id": customer_id,
        "payment_method_id": payment_method_id,
        "setup_intent_id": setup_intent_data.get("id"),
      },
    )


def _merge_subscription_metadata(
  subscription: Any, updates: dict, db_session: Any
) -> None:
  """Merge into ``subscription_metadata`` via a new dict (in-place JSONB edits go unseen)."""
  subscription.subscription_metadata = {
    **(subscription.subscription_metadata or {}),
    **updates,
  }
  db_session.commit()


def _fail_subscription(subscription: Any, error: str, db_session: Any) -> None:
  """Mark a subscription failed with a reason an operator can read.

  Terminal: only for conditions no retry can fix. Setting ``ends_at`` starts the
  lifecycle sensors' retention clock so any infrastructure created is reclaimed.
  """
  now = datetime.now(UTC)
  subscription.status = "failed"
  subscription.subscription_metadata = {
    **(subscription.subscription_metadata or {}),
    "error": error,
    "failed_at": now.isoformat(),
  }
  if subscription.ends_at is None:
    subscription.ends_at = now
  db_session.commit()


def _record_provisioning_error(
  subscription: Any, error: Exception, db_session: Any
) -> None:
  """Record why a provisioning attempt failed, leaving the row retryable.

  Not marked failed: the customer paid and the provider redelivers past the
  claim's staleness window. The stalled-provisioning reaper writes off rows
  that never succeed.
  """
  subscription.subscription_metadata = {
    **(subscription.subscription_metadata or {}),
    "last_provisioning_error": str(error),
    "last_provisioning_error_at": datetime.now(UTC).isoformat(),
  }
  db_session.commit()


async def _trigger_resource_provisioning(
  subscription: Any, db_session: Any, context: OpExecutionContext
) -> None:
  """Provision the paid-for resource inline.

  Several provider events and redeliveries can reach this for one subscription;
  ``claim_for_provisioning`` is the single gate that arbitrates them.
  """
  from robosystems.models.core import OrgRole, OrgUser

  resource_config = subscription.subscription_metadata.get("resource_config", {})
  resource_type = subscription.resource_type

  if not subscription.claim_for_provisioning(db_session):
    context.log.info(
      f"Skipping provisioning for subscription {subscription.id}: "
      "claim held elsewhere or resource already provisioned"
    )
    return

  # Column is authoritative; metadata covers older rows. The org-owner fallback
  # can pick the wrong person in a multi-member org.
  user_id = subscription.user_id or subscription.subscription_metadata.get("user_id")
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
      _fail_subscription(subscription, "No org owner found", db_session)
      return
    user_id = owner.user_id

  if not subscription.user_id:
    subscription.user_id = user_id
    db_session.commit()

  context.log.info(f"Triggering provisioning for {resource_type}")

  from robosystems.operations.graph.provisioning_service import (
    run_graph_provisioning,
    run_user_repository_provisioning,
  )

  if resource_type == "graph":
    _merge_subscription_metadata(subscription, resource_config, db_session)

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
      _record_provisioning_error(subscription, e, db_session)
      raise

  elif resource_type == "repository":
    repository_name = resource_config.get("repository_name")

    _merge_subscription_metadata(
      subscription, {"repository_name": repository_name}, db_session
    )

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
      _record_provisioning_error(subscription, e, db_session)
      raise

  else:
    context.log.error(f"Unknown resource type: {resource_type}")
    _fail_subscription(
      subscription, f"Unknown resource type: {resource_type}", db_session
    )


# ============================================================================
# Credit Allocation Jobs
# ============================================================================


@op
def allocate_monthly_credits(
  context: OpExecutionContext, db: DatabaseResource
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


@op
def allocate_user_repository_credits(
  context: OpExecutionContext,
  db: DatabaseResource,
  allocation_result: dict[str, Any],
) -> dict[str, Any]:
  """Allocate monthly credits for user repository subscriptions that are due."""
  now = datetime.now(UTC)
  allocated_count = 0
  total_credits = Decimal("0")
  errors = []

  with db.get_session() as session:
    due_pools = (
      session.query(UserRepositoryCredits)
      .join(
        UserRepository, UserRepository.id == UserRepositoryCredits.user_repository_id
      )
      .filter(
        UserRepositoryCredits.is_active.is_(True),
        # A grant that ran out (a period-end cancel) earns no more credits.
        UserRepository.is_active.is_(True),
        (UserRepository.expires_at.is_(None)) | (UserRepository.expires_at > now),
        (UserRepositoryCredits.next_allocation_date.is_(None))
        | (UserRepositoryCredits.next_allocation_date <= now),
      )
      .all()
    )

    context.log.info(
      f"Found {len(due_pools)} user repository credit pools due for allocation"
    )

    for pool in due_pools:
      try:
        if pool.allocate_monthly_credits(session):
          allocated_count += 1
          total_credits += pool.monthly_allocation
      except Exception as e:
        session.expire(pool)
        errors.append(
          {
            "pool_id": pool.id,
            "user_repository_id": pool.user_repository_id,
            "error": str(e),
          }
        )
        context.log.error(f"Failed to allocate credits for pool {pool.id}: {e}")

  if errors:
    context.log.warning(
      f"User repository credit allocation completed with {len(errors)} errors"
    )

  context.log.info(
    f"Allocated {total_credits} user repository credits to {allocated_count} pools"
  )

  return {
    "allocated_pools": allocated_count,
    "total_credits_allocated": float(total_credits),
    "errors": errors,
    "graph_allocation_result": allocation_result,
    "timestamp": now.isoformat(),
  }


@op
def cleanup_old_usage_records(
  context: OpExecutionContext,
  db: DatabaseResource,
  cleanup_result: dict[str, Any],
) -> dict[str, Any]:
  """Apply twelve-month retention to ``graph_usage``.

  The default ``keep_monthly_summaries`` drops only per-call rows; storage
  snapshots and allocations survive so period rollups stay reproducible.
  """
  from robosystems.models.core.graph.graph_usage import GraphUsage

  with db.get_session() as session:
    result = GraphUsage.cleanup_old_records(session, older_than_days=365)

  context.log.info(
    f"Usage retention: deleted {result['deleted_records']} records, "
    f"preserved {result['preserved_summaries']} summaries"
  )

  return {"usage_cleanup": result, "credit_cleanup": cleanup_result}


@job(tags={"dagster/priority": "1", "dagster/max_retries": 3})
def monthly_credit_allocation_job():
  """Monthly credit allocation and retention job."""
  result = allocate_monthly_credits()
  repo_result = allocate_user_repository_credits(result)
  cleanup_result = cleanup_old_credit_transactions(repo_result)
  cleanup_old_usage_records(cleanup_result)


@op
def generate_usage_report(
  context: OpExecutionContext, db: DatabaseResource
) -> dict[str, Any]:
  """Generate the monthly usage report for the prior calendar month."""
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
