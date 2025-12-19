"""Admin webhook handlers for payment providers."""

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from ...database import get_db_session
from ...logger import get_logger
from ...middleware.sse import run_and_monitor_dagster_job
from ...models.billing import BillingAuditLog
from ...operations.providers.payment_provider import get_payment_provider
from ...security.audit_logger import SecurityAuditLogger, SecurityEventType

logger = get_logger(__name__)

router = APIRouter(prefix="/admin/v1/webhooks", tags=["admin"])


@router.post(
  "/stripe",
  status_code=status.HTTP_200_OK,
  summary="Stripe Webhook Handler",
  description="""Handle Stripe webhook events.

This endpoint receives and processes webhook events from Stripe including:
- checkout.session.completed - Payment method collected, trigger provisioning
- invoice.created - Sync Stripe invoice to database
- invoice.payment_succeeded - Payment successful, mark invoice paid
- invoice.payment_failed - Payment failed, mark subscription
- customer.subscription.updated - Subscription changes from Stripe
- customer.subscription.deleted - Subscription canceled in Stripe

**SECURITY**: This endpoint does NOT use @require_admin authentication because
Stripe webhooks cannot provide admin API keys. Instead, security is enforced
through Stripe's webhook signature verification (verify_webhook).

**Processing**: Webhooks are queued for processing via Dagster, providing:
- Retry logic with exponential backoff
- Full observability in Dagster UI
- Audit trail of all webhook processing

Webhooks are verified using Stripe signature before being queued.""",
  operation_id="handleStripeWebhook",
)
async def handle_stripe_webhook(
  request: Request,
  background_tasks: BackgroundTasks,
  db: Session = Depends(get_db_session),
):
  """Handle Stripe webhook events via Dagster."""
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

    # Check idempotency before queuing
    if BillingAuditLog.is_webhook_processed(event_id, "stripe", db):
      logger.info(
        f"Webhook event already processed: {event_id}",
        extra={"event_id": event_id, "event_type": event_type},
      )
      return {"status": "success", "message": "Event already processed"}

    logger.info(
      f"Queueing Stripe webhook for Dagster processing: {event_type}",
      extra={
        "event_type": event_type,
        "event_id": event_id,
      },
    )

    # Build Dagster job config
    from robosystems.dagster.jobs.billing import build_stripe_webhook_job_config

    run_config = build_stripe_webhook_job_config(
      event_id=event_id,
      event_type=event_type,
      event_data=event_data,
    )

    # Queue Dagster job for async processing
    background_tasks.add_task(
      run_and_monitor_dagster_job,
      job_name="process_stripe_webhook_job",
      operation_id=None,  # No SSE tracking needed for webhooks
      run_config=run_config,
    )

    logger.info(
      f"Queued Stripe webhook for processing: {event_id}",
      extra={"event_id": event_id, "event_type": event_type},
    )

    return {"status": "success", "message": "Webhook queued for processing"}

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to queue webhook: {e}", exc_info=True)
    raise HTTPException(status_code=500, detail="Failed to process webhook")


# NOTE: All webhook event handling is now done in Dagster (robosystems.dagster.jobs.billing)
# The process_stripe_webhook_job handles:
# - checkout.session.completed
# - invoice.created
# - invoice.payment_succeeded
# - invoice.payment_failed
# - customer.subscription.updated
# - customer.subscription.deleted
#
# This provides:
# - Retry logic with exponential backoff (3 retries)
# - Full observability in Dagster UI
# - Audit trail of all webhook processing
