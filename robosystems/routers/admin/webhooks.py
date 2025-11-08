"""Admin webhook handlers for payment providers."""

from fastapi import APIRouter, Request, HTTPException, status, Depends
from sqlalchemy.orm import Session

from ...database import get_db_session
from ...models.billing import BillingCustomer, BillingSubscription, BillingAuditLog
from ...operations.billing.payment_provider import get_payment_provider
from ...logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/admin/v1/webhooks", tags=["admin"])


@router.post(
  "/stripe",
  status_code=status.HTTP_200_OK,
  summary="Stripe Webhook Handler",
  description="""Handle Stripe webhook events.

This endpoint receives and processes webhook events from Stripe including:
- checkout.session.completed - Payment method collected
- invoice.payment_succeeded - Payment successful, activate subscription
- invoice.payment_failed - Payment failed, mark subscription
- customer.subscription.updated - Subscription changes from Stripe
- customer.subscription.deleted - Subscription canceled in Stripe

**SECURITY**: This endpoint does NOT use @require_admin authentication because
Stripe webhooks cannot provide admin API keys. Instead, security is enforced
through Stripe's webhook signature verification (verify_webhook).

Webhooks are verified using Stripe signature before processing.""",
  operation_id="handleStripeWebhook",
)
async def handle_stripe_webhook(
  request: Request, db: Session = Depends(get_db_session)
):
  """Handle Stripe webhook events."""
  try:
    payload = await request.body()
    signature = request.headers.get("stripe-signature")

    if not signature:
      raise HTTPException(status_code=400, detail="Missing stripe-signature header")

    provider = get_payment_provider("stripe")

    try:
      event = provider.verify_webhook(payload, signature)
    except ValueError as e:
      logger.error(f"Invalid webhook signature: {e}")
      raise HTTPException(status_code=400, detail="Invalid webhook signature")

    event_type = event.get("type")
    event_data = event.get("data", {}).get("object", {})
    event_id = event.get("id")

    if BillingAuditLog.is_webhook_processed(event_id, "stripe", db):
      logger.info(
        f"Webhook event already processed: {event_id}",
        extra={"event_id": event_id, "event_type": event_type},
      )
      return {"status": "success", "message": "Event already processed"}

    logger.info(
      f"Processing Stripe webhook: {event_type}",
      extra={
        "event_type": event_type,
        "event_id": event_id,
      },
    )

    if event_type == "checkout.session.completed":
      await handle_checkout_completed(event_data, db)

    elif event_type == "invoice.payment_succeeded":
      await handle_payment_succeeded(event_data, db)

    elif event_type == "invoice.payment_failed":
      await handle_payment_failed(event_data, db)

    elif event_type == "customer.subscription.updated":
      await handle_subscription_updated(event_data, db)

    elif event_type == "customer.subscription.deleted":
      await handle_subscription_deleted(event_data, db)

    else:
      logger.info(f"Unhandled webhook event type: {event_type}")

    BillingAuditLog.mark_webhook_processed(
      event_id, "stripe", event_type, event_data, db
    )

    return {"status": "success"}

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to process webhook: {e}", exc_info=True)
    raise HTTPException(status_code=500, detail="Failed to process webhook")


async def handle_checkout_completed(session_data: dict, db: Session):
  """Handle checkout.session.completed event."""
  session_id = session_data.get("id")
  customer_id = session_data.get("customer")
  payment_status = session_data.get("payment_status")
  stripe_subscription_id = session_data.get("subscription")
  metadata = session_data.get("metadata", {})

  logger.info(
    f"Checkout completed: session_id={session_id}, payment_status={payment_status}"
  )

  subscription = BillingSubscription.get_by_provider_subscription_id(session_id, db)

  if not subscription and metadata.get("subscription_id"):
    subscription = (
      db.query(BillingSubscription)
      .filter(BillingSubscription.id == metadata["subscription_id"])
      .first()
    )

  if not subscription:
    logger.warning(f"Subscription not found for checkout session: {session_id}")
    return

  customer = BillingCustomer.get_by_user_id(subscription.billing_customer_user_id, db)

  if not customer:
    logger.error(f"Customer not found for subscription: {subscription.id}")
    return

  if payment_status == "paid":
    customer.has_payment_method = True

    if not customer.stripe_customer_id:
      customer.stripe_customer_id = customer_id

    if stripe_subscription_id:
      subscription.stripe_subscription_id = stripe_subscription_id
      subscription.provider_subscription_id = stripe_subscription_id

    subscription.status = "provisioning"
    subscription.provider_customer_id = customer_id

    db.commit()

    logger.info(
      f"Payment method collected for user {customer.user_id}",
      extra={
        "subscription_id": subscription.id,
        "session_id": session_id,
        "stripe_subscription_id": stripe_subscription_id,
      },
    )

    await trigger_resource_provisioning(subscription, db)

  else:
    logger.warning(f"Checkout completed but payment not paid: {payment_status}")


async def handle_payment_succeeded(invoice_data: dict, db: Session):
  """Handle invoice.payment_succeeded event."""
  subscription_id = invoice_data.get("subscription")
  customer_id = invoice_data.get("customer")

  if not subscription_id:
    logger.info("Payment succeeded but no subscription ID in invoice")
    return

  subscription = BillingSubscription.get_by_provider_subscription_id(
    subscription_id, db
  )

  if not subscription:
    logger.warning(f"Subscription not found for invoice: {subscription_id}")
    return

  customer = BillingCustomer.get_by_stripe_customer_id(customer_id, db)

  if customer:
    customer.has_payment_method = True
    db.commit()

  if subscription.status in ["pending_payment", "provisioning"]:
    await trigger_resource_provisioning(subscription, db)

  logger.info(
    f"Payment succeeded for subscription {subscription.id}",
    extra={
      "subscription_id": subscription.id,
      "stripe_subscription_id": subscription_id,
    },
  )


async def handle_payment_failed(invoice_data: dict, db: Session):
  """Handle invoice.payment_failed event."""
  subscription_id = invoice_data.get("subscription")

  if not subscription_id:
    return

  subscription = BillingSubscription.get_by_provider_subscription_id(
    subscription_id, db
  )

  if not subscription:
    logger.warning(f"Subscription not found for failed payment: {subscription_id}")
    return

  if subscription.status == "pending_payment":
    subscription.status = "unpaid"

    error_message = "Payment failed"
    if subscription.subscription_metadata:
      subscription.subscription_metadata["error"] = error_message  # type: ignore[index]
    else:
      subscription.subscription_metadata = {"error": error_message}

    db.commit()

  logger.warning(
    f"Payment failed for subscription {subscription.id}",
    extra={
      "subscription_id": subscription.id,
      "stripe_subscription_id": subscription_id,
    },
  )


async def handle_subscription_updated(subscription_data: dict, db: Session):
  """Handle customer.subscription.updated event."""
  subscription_id = subscription_data.get("id")
  status = subscription_data.get("status")

  subscription = BillingSubscription.get_by_provider_subscription_id(
    subscription_id, db
  )

  if not subscription:
    logger.warning(f"Subscription not found for update: {subscription_id}")
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

  new_status = status_mapping.get(status, subscription.status)  # type: ignore[arg-type]

  if new_status != subscription.status:
    subscription.status = new_status
    db.commit()

    logger.info(
      f"Subscription status updated: {subscription.id} -> {new_status}",
      extra={
        "subscription_id": subscription.id,
        "old_status": subscription.status,
        "new_status": new_status,
      },
    )


async def handle_subscription_deleted(subscription_data: dict, db: Session):
  """Handle customer.subscription.deleted event."""
  subscription_id = subscription_data.get("id")

  subscription = BillingSubscription.get_by_provider_subscription_id(
    subscription_id, db
  )

  if not subscription:
    logger.warning(f"Subscription not found for deletion: {subscription_id}")
    return

  subscription.cancel(db, immediate=True)

  logger.info(
    f"Subscription canceled via Stripe: {subscription.id}",
    extra={"subscription_id": subscription.id},
  )


async def trigger_resource_provisioning(subscription: BillingSubscription, db: Session):
  """Trigger resource provisioning after payment confirmation."""
  resource_config = subscription.subscription_metadata.get("resource_config", {})
  resource_type = subscription.resource_type

  logger.info(
    f"Triggering provisioning for {resource_type}",
    extra={
      "subscription_id": subscription.id,
      "resource_type": resource_type,
    },
  )

  try:
    if resource_type == "graph":
      from ...tasks.graph_operations.provision_graph import provision_graph_task

      result = provision_graph_task.delay(  # type: ignore[attr-defined]
        user_id=subscription.billing_customer_user_id,
        subscription_id=str(subscription.id),
        graph_config=resource_config,
      )

      if not subscription.subscription_metadata:
        subscription.subscription_metadata = {}
      subscription.subscription_metadata["operation_id"] = result.id  # type: ignore[index]
      subscription.status = "provisioning"
      db.commit()

    elif resource_type == "repository":
      from ...tasks.billing.provision_repository import provision_repository_access_task

      repository_name = resource_config.get("repository_name")

      result = provision_repository_access_task.delay(  # type: ignore[attr-defined]
        user_id=subscription.billing_customer_user_id,
        subscription_id=str(subscription.id),
        repository_name=repository_name,
      )

      if not subscription.subscription_metadata:
        subscription.subscription_metadata = {}
      subscription.subscription_metadata["operation_id"] = result.id  # type: ignore[index]
      subscription.resource_id = repository_name
      subscription.status = "active"
      db.commit()

    else:
      logger.error(f"Unknown resource type: {resource_type}")
      subscription.status = "failed"
      subscription.subscription_metadata["error"] = (  # type: ignore[index]
        f"Unknown resource type: {resource_type}"
      )
      db.commit()

  except Exception as e:
    logger.error(f"Failed to trigger provisioning: {e}", exc_info=True)
    subscription.status = "failed"
    if not subscription.subscription_metadata:
      subscription.subscription_metadata = {}
    subscription.subscription_metadata["error"] = str(e)  # type: ignore[index]
    db.commit()
