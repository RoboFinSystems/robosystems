"""Admin webhook handlers for payment providers."""

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from ...database import SessionFactory, get_db_session
from ...logger import get_logger
from ...models.billing import BillingAuditLog
from ...operations.providers.payment_provider import get_payment_provider
from ...security.audit_logger import SecurityAuditLogger, SecurityEventType

logger = get_logger(__name__)

router = APIRouter(prefix="/admin/v1/webhooks", tags=["admin"])


class _WebhookLogContext:
  """Lightweight stand-in for Dagster's OpExecutionContext.

  The billing handler functions use context.log.info/warning/error.
  This routes those calls to our standard logger.
  """

  def __init__(self) -> None:
    self.log = logger


async def _process_webhook_event(
  event_id: str,
  event_type: str,
  event_data: dict,
) -> None:
  """Process a Stripe webhook event directly (no Dagster).

  Creates its own database session since this runs as a background task
  after the request session has been closed.
  """
  from robosystems.dagster.jobs.billing import (
    SubscriptionNotFoundError,
    _handle_charge_refunded,
    _handle_checkout_completed,
    _handle_invoice_created,
    _handle_invoice_updated,
    _handle_invoice_voided,
    _handle_payment_failed,
    _handle_payment_succeeded,
    _handle_setup_intent_succeeded,
    _handle_subscription_deleted,
    _handle_subscription_updated,
  )

  ctx = _WebhookLogContext()
  db = SessionFactory()

  try:
    if event_type == "checkout.session.completed":
      await _handle_checkout_completed(event_data, db, ctx)

    elif event_type == "invoice.created":
      await _handle_invoice_created(event_data, db, ctx)

    elif event_type in ("invoice.payment_succeeded", "invoice.paid"):
      await _handle_payment_succeeded(event_data, db, ctx)

    elif event_type == "invoice.payment_failed":
      await _handle_payment_failed(event_data, db, ctx)

    elif event_type == "invoice.updated":
      await _handle_invoice_updated(event_data, db, ctx)

    elif event_type == "invoice.voided":
      await _handle_invoice_voided(event_data, db, ctx)

    elif event_type == "charge.refunded":
      await _handle_charge_refunded(event_data, db, ctx)

    elif event_type == "setup_intent.succeeded":
      await _handle_setup_intent_succeeded(event_data, db, ctx)

    elif event_type == "customer.subscription.updated":
      await _handle_subscription_updated(event_data, db, ctx)

    elif event_type == "customer.subscription.deleted":
      await _handle_subscription_deleted(event_data, db, ctx)

    else:
      logger.info(
        f"Unhandled webhook event type: {event_type}",
        extra={"event_id": event_id, "event_type": event_type},
      )
      # Still mark as processed to avoid reprocessing
      BillingAuditLog.mark_webhook_processed(
        event_id, "stripe", event_type, event_data, db
      )
      return

    # Mark as processed after successful handling
    BillingAuditLog.mark_webhook_processed(
      event_id, "stripe", event_type, event_data, db
    )

    logger.info(
      f"Webhook processed: {event_type}",
      extra={"event_id": event_id, "event_type": event_type},
    )

  except SubscriptionNotFoundError as e:
    # Subscription not found — do NOT mark as processed so Stripe retries.
    # This handles timing issues where checkout.session.completed hasn't
    # fired yet but invoice webhooks have already arrived.
    logger.warning(
      f"Webhook {event_type} deferred (will retry): {e}",
      extra={"event_id": event_id, "event_type": event_type},
    )
  except Exception as e:
    logger.error(
      f"Failed to process webhook {event_type}: {e}",
      exc_info=True,
      extra={"event_id": event_id, "event_type": event_type},
    )
  finally:
    db.close()


@router.post(
  "/stripe",
  status_code=status.HTTP_200_OK,
  summary="Stripe Webhook Handler",
  description="""Handle Stripe webhook events.

This endpoint receives and processes webhook events from Stripe including:
- charge.refunded - Refund processed, add negative line item to invoice
- checkout.session.completed - Payment method collected, trigger provisioning
- customer.subscription.deleted - Subscription canceled in Stripe
- customer.subscription.updated - Subscription changes from Stripe
- invoice.created - Sync Stripe invoice to database
- invoice.paid / invoice.payment_succeeded - Payment successful, mark invoice paid
- invoice.payment_failed - Payment failed, mark subscription
- invoice.updated - Invoice fields changed, sync to database
- invoice.voided - Invoice voided in Stripe
- setup_intent.succeeded - Payment method added via customer portal

**SECURITY**: This endpoint does NOT use @require_admin authentication because
Stripe webhooks cannot provide admin API keys. Instead, security is enforced
through Stripe's webhook signature verification (verify_webhook).

**Processing**: Webhooks are processed directly as background tasks for low
latency. Heavy operations (graph provisioning) are handled by the direct
provisioning system which manages its own async execution.

Webhooks are verified using Stripe signature before processing.""",
  operation_id="handleStripeWebhook",
)
async def handle_stripe_webhook(
  request: Request,
  background_tasks: BackgroundTasks,
  db: Session = Depends(get_db_session),
):
  """Handle Stripe webhook events."""
  try:
    payload = await request.body()
    signature = request.headers.get("stripe-signature")
    client_ip = request.client.host if request.client else "unknown"

    if not signature:
      # Log security event for missing signature
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTHORIZATION_DENIED,
        user_id=None,
        ip_address=client_ip,
        endpoint="/admin/v1/webhooks/stripe",
        details={
          "reason": "missing_webhook_signature",
          "payload_size_bytes": len(payload),
        },
        risk_level="high",
      )

      raise HTTPException(status_code=400, detail="Missing stripe-signature header")

    provider = get_payment_provider("stripe")

    try:
      event = provider.verify_webhook(payload, signature)
    except ValueError as e:
      logger.error(f"Invalid webhook signature: {e}")

      # Log security event for failed signature verification
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTHORIZATION_DENIED,
        user_id=None,
        ip_address=client_ip,
        endpoint="/admin/v1/webhooks/stripe",
        details={
          "reason": "invalid_webhook_signature",
          "error": str(e),
          "signature_present": bool(signature),
          "payload_size_bytes": len(payload),
        },
        risk_level="high",
      )

      raise HTTPException(status_code=400, detail="Invalid webhook signature")

    event_type = event.get("type")
    event_data = event.get("data", {}).get("object", {})
    event_id = event.get("id")

    # Check idempotency
    if BillingAuditLog.is_webhook_processed(event_id, "stripe", db):
      logger.info(
        f"Webhook event already processed: {event_id}",
        extra={"event_id": event_id, "event_type": event_type},
      )
      return {"status": "success", "message": "Event already processed"}

    logger.info(
      f"Processing Stripe webhook: {event_type}",
      extra={"event_type": event_type, "event_id": event_id},
    )

    # Process directly as a background task with its own DB session
    background_tasks.add_task(
      _process_webhook_event,
      event_id=event_id,
      event_type=event_type,
      event_data=event_data,
    )

    return {"status": "success", "message": "Webhook accepted for processing"}

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to handle webhook: {e}", exc_info=True)
    raise HTTPException(status_code=500, detail="Failed to process webhook")
