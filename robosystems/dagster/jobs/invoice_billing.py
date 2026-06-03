"""Dagster job for invoice billing subscription renewals.

Handles period rotation and invoice generation for subscriptions
that use invoice billing (no Stripe subscription).
"""

import logging

from dagster import Config, OpExecutionContext, job, op
from sqlalchemy.orm import Session

from robosystems.dagster.resources import DatabaseResource

logger = logging.getLogger(__name__)


class RenewInvoiceSubscriptionsConfig(Config):
  """Configuration for renewing invoice-billed subscriptions."""

  subscription_ids: list[str]


def _renew_subscriptions(
  subscription_ids: list[str],
  session: Session,
  log: logging.Logger,
) -> dict:
  """Core renewal logic, testable without Dagster infrastructure.

  For each subscription ID:
  1. Load the subscription and its billing customer
  2. Re-verify eligibility (active, no stripe sub, invoice billing enabled)
  3. Rotate the billing period
  4. Generate a renewal invoice
  5. Log an audit event
  """
  from robosystems.config.billing import BillingConfig
  from robosystems.models.core.billing import (
    BillingAuditLog,
    BillingCustomer,
    BillingSubscription,
  )
  from robosystems.models.core.billing.audit_log import BillingEventType
  from robosystems.operations.graph.subscription_service import (
    generate_subscription_invoice,
  )

  renewed_count = 0
  skipped_count = 0
  errors: list[str] = []

  for sub_id in subscription_ids:
    try:
      subscription = (
        session.query(BillingSubscription)
        .filter(BillingSubscription.id == sub_id)
        .first()
      )

      if not subscription:
        log.warning(f"Subscription {sub_id} not found, skipping")
        skipped_count += 1
        continue

      customer = BillingCustomer.get_by_org_id(subscription.org_id, session)
      if not customer:
        log.warning(
          f"No billing customer for org {subscription.org_id}, skipping {sub_id}"
        )
        skipped_count += 1
        continue

      # Re-verify eligibility (may have changed since sensor ran)
      if not subscription.is_active():
        log.info(f"Subscription {sub_id} no longer active, skipping")
        skipped_count += 1
        continue

      if subscription.stripe_subscription_id is not None:
        log.info(f"Subscription {sub_id} has Stripe subscription, skipping")
        skipped_count += 1
        continue

      if not customer.invoice_billing_enabled:
        log.info(
          f"Invoice billing not enabled for org {subscription.org_id}, skipping {sub_id}"
        )
        skipped_count += 1
        continue

      if not subscription.current_period_end:
        log.warning(f"Subscription {sub_id} has no period end, skipping")
        skipped_count += 1
        continue

      # Use a savepoint so period rotation + invoice + audit are atomic.
      # If any step fails, the savepoint rolls back and the subscription
      # reappears on the next sensor tick.
      savepoint = session.begin_nested()
      try:
        old_period_end = subscription.current_period_end
        subscription.renew_period(session)

        plan_config = BillingConfig.get_subscription_plan(subscription.plan_name)
        tier_label = (
          plan_config["display_name"] if plan_config else subscription.plan_name
        )
        invoice = generate_subscription_invoice(
          subscription=subscription,
          customer=customer,
          description=f"Graph subscription renewal — {tier_label}",
          session=session,
        )

        BillingAuditLog.log_event(
          session=session,
          event_type=BillingEventType.SUBSCRIPTION_RENEWED,
          org_id=subscription.org_id,
          subscription_id=subscription.id,
          invoice_id=invoice.id,
          description=f"Subscription {subscription.id} renewed: period {old_period_end} -> {subscription.current_period_end}",
          actor_type="dagster",
          event_data={
            "old_period_end": old_period_end.isoformat() if old_period_end else None,
            "new_period_start": subscription.current_period_start.isoformat(),
            "new_period_end": subscription.current_period_end.isoformat(),
            "invoice_id": invoice.id,
            "plan_name": subscription.plan_name,
            "amount_cents": subscription.base_price_cents,
          },
        )

        savepoint.commit()
        renewed_count += 1
        log.info(
          f"Renewed subscription {sub_id}: "
          f"period {subscription.current_period_start} -> {subscription.current_period_end}, "
          f"invoice {invoice.id}"
        )
      except Exception:
        savepoint.rollback()
        raise

    except Exception as e:
      error_msg = f"Failed to renew subscription {sub_id}: {e}"
      errors.append(error_msg)
      log.error(error_msg)

  log.info(
    f"Invoice billing renewal complete: "
    f"{renewed_count} renewed, {skipped_count} skipped, {len(errors)} errors"
  )

  return {
    "renewed_count": renewed_count,
    "skipped_count": skipped_count,
    "errors": errors,
  }


@op
def renew_invoice_subscriptions(
  context: OpExecutionContext,
  db: DatabaseResource,
  config: RenewInvoiceSubscriptionsConfig,
) -> dict:
  """Renew invoice-billed subscriptions by rotating periods and generating invoices."""
  with db.get_session() as session:
    return _renew_subscriptions(config.subscription_ids, session, context.log)


@job(
  tags={
    "dagster/priority": "1",
  }
)
def invoice_subscription_renewal_job():
  """Renew invoice-billed subscriptions with period rotation and invoice generation."""
  renew_invoice_subscriptions()
