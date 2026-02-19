"""Dagster sensor for invoice billing subscription renewals.

Monitors for invoice-billed subscriptions whose billing period has ended
and triggers renewal jobs to rotate periods and generate invoices.
"""

from dagster import (
  DefaultSensorStatus,
  RunRequest,
  SensorEvaluationContext,
  sensor,
)

from robosystems.dagster.jobs.invoice_billing import (
  invoice_subscription_renewal_job,
)
from robosystems.logger import get_logger

logger = get_logger(__name__)

# Always RUNNING: dev uses invoice billing by default, prod/staging covers
# any orgs with invoice_billing_enabled outside of Stripe.
_SENSOR_STATUS = DefaultSensorStatus.RUNNING


@sensor(
  job=invoice_subscription_renewal_job,
  minimum_interval_seconds=300,  # 5 minutes
  default_status=_SENSOR_STATUS,
  description="Renews invoice-billed subscriptions when their billing period ends",
)
def invoice_subscription_renewal_sensor(context: SensorEvaluationContext):
  """Find invoice-billed subscriptions due for renewal.

  Query: BillingSubscription where:
  - status = 'active'
  - stripe_subscription_id IS NULL (not Stripe-managed)
  - current_period_end IS NOT NULL AND current_period_end <= now()
  - BillingCustomer.invoice_billing_enabled = true
  """
  from datetime import UTC, datetime

  from robosystems.database import session as db_session_factory
  from robosystems.models.billing.customer import BillingCustomer
  from robosystems.models.billing.subscription import BillingSubscription

  db = db_session_factory()
  try:
    now = datetime.now(UTC)

    due_subs = (
      db.query(BillingSubscription)
      .join(BillingCustomer, BillingSubscription.org_id == BillingCustomer.org_id)
      .filter(
        BillingSubscription.status == "active",
        BillingSubscription.stripe_subscription_id.is_(None),
        BillingSubscription.current_period_end.isnot(None),
        BillingSubscription.current_period_end <= now,
        BillingCustomer.invoice_billing_enabled.is_(True),
      )
      .all()
    )

    if not due_subs:
      return

    subscription_ids = [sub.id for sub in due_subs]
    context.log.info(
      f"Found {len(subscription_ids)} invoice-billed subscriptions due for renewal"
    )

    yield RunRequest(
      run_key=f"invoice-renewal-{now.strftime('%Y%m%d%H%M')}",
      run_config={
        "ops": {
          "renew_invoice_subscriptions": {
            "config": {
              "subscription_ids": subscription_ids,
            }
          }
        }
      },
    )
  finally:
    db_session_factory.remove()
