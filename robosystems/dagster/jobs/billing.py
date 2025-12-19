"""Dagster billing jobs.

These jobs handle credit allocation, storage billing, usage collection,
and Stripe webhook processing.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from dagster import (
  Backoff,
  Config,
  DefaultScheduleStatus,
  OpExecutionContext,
  RetryPolicy,
  ScheduleDefinition,
  job,
  op,
)

from robosystems.config import env
from robosystems.dagster.resources import DatabaseResource
from robosystems.logger import get_logger
from robosystems.models.iam import GraphCredits, GraphCreditTransaction, GraphUsage
from robosystems.models.iam.graph_credits import CreditTransactionType
from robosystems.models.iam.graph_usage import UsageEventType
from robosystems.operations.graph.credit_service import CreditService

logger = get_logger(__name__)

# ============================================================================
# Environment-based Schedule Status
# ============================================================================

# Billing schedules default to STOPPED. Enable via BILLING_SCHEDULES_ENABLED=true
# in AWS Secrets Manager or environment variables.
BILLING_SCHEDULE_STATUS = (
  DefaultScheduleStatus.RUNNING
  if env.BILLING_SCHEDULES_ENABLED
  else DefaultScheduleStatus.STOPPED
)


# ============================================================================
# Stripe Webhook Processing Job
# ============================================================================


class StripeWebhookConfig(Config):
  """Configuration for processing a Stripe webhook event."""

  event_id: str
  event_type: str
  event_data: dict
  operation_id: str | None = None


@op(
  retry_policy=RetryPolicy(
    max_retries=3,
    delay=10,
    backoff=Backoff.EXPONENTIAL,
  ),
  tags={"kind": "webhook", "category": "billing"},
)
def process_stripe_webhook_event(
  context: OpExecutionContext,
  db: DatabaseResource,
  config: StripeWebhookConfig,
) -> dict[str, Any]:
  """
  Process a Stripe webhook event with retry logic.

  Handles:
  - checkout.session.completed: Payment collected, trigger provisioning
  - invoice.created: Sync invoice to database
  - invoice.payment_succeeded: Mark invoice paid
  - invoice.payment_failed: Handle payment failure
  - customer.subscription.updated: Sync subscription status
  - customer.subscription.deleted: Cancel subscription

  Returns processing result for observability.
  """
  import asyncio

  context.log.info(
    f"Processing Stripe webhook: {config.event_type} (event_id: {config.event_id})"
  )

  with db.get_session() as session:
    # Check idempotency - may have been processed by another worker
    from robosystems.models.billing import BillingAuditLog

    if BillingAuditLog.is_webhook_processed(config.event_id, "stripe", session):
      context.log.info(f"Webhook {config.event_id} already processed, skipping")
      return {
        "event_id": config.event_id,
        "event_type": config.event_type,
        "status": "skipped",
        "reason": "already_processed",
      }

    # Process based on event type
    event_data = config.event_data
    result: dict[str, Any] = {
      "event_id": config.event_id,
      "event_type": config.event_type,
    }

    loop = asyncio.new_event_loop()
    try:
      if config.event_type == "checkout.session.completed":
        loop.run_until_complete(
          _handle_checkout_completed(event_data, session, context)
        )
        result["action"] = "checkout_processed"

      elif config.event_type == "invoice.created":
        loop.run_until_complete(_handle_invoice_created(event_data, session, context))
        result["action"] = "invoice_synced"

      elif config.event_type == "invoice.payment_succeeded":
        loop.run_until_complete(_handle_payment_succeeded(event_data, session, context))
        result["action"] = "payment_recorded"

      elif config.event_type == "invoice.payment_failed":
        loop.run_until_complete(_handle_payment_failed(event_data, session, context))
        result["action"] = "payment_failure_recorded"

      elif config.event_type == "customer.subscription.updated":
        loop.run_until_complete(
          _handle_subscription_updated(event_data, session, context)
        )
        result["action"] = "subscription_updated"

      elif config.event_type == "customer.subscription.deleted":
        loop.run_until_complete(
          _handle_subscription_deleted(event_data, session, context)
        )
        result["action"] = "subscription_canceled"

      else:
        context.log.info(f"Unhandled webhook event type: {config.event_type}")
        result["action"] = "ignored"
        result["reason"] = "unhandled_event_type"

    finally:
      loop.close()

    # Mark webhook as processed
    BillingAuditLog.mark_webhook_processed(
      config.event_id, "stripe", config.event_type, event_data, session
    )

    result["status"] = "completed"
    result["processed_at"] = datetime.now(UTC).isoformat()

    context.log.info(f"Webhook processing completed: {result}")

    # Emit SSE result if operation_id provided
    if config.operation_id:
      _emit_webhook_result_to_sse(context, config.operation_id, result)

    return result


async def _handle_checkout_completed(
  session_data: dict, db_session: Any, context: OpExecutionContext
) -> None:
  """Handle checkout.session.completed event."""
  from robosystems.models.billing import BillingCustomer, BillingSubscription

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

    subscription.status = "provisioning"
    subscription.provider_customer_id = customer_id

    db_session.commit()

    context.log.info(f"Payment collected for org {customer.org_id}")

    # Trigger provisioning via sensor (set status to provisioning)
    await _trigger_resource_provisioning(subscription, db_session, context)

  else:
    context.log.warning(f"Checkout completed but payment not paid: {payment_status}")


async def _handle_invoice_created(
  invoice_data: dict, db_session: Any, context: OpExecutionContext
) -> None:
  """Handle invoice.created event from Stripe."""
  from robosystems.models.billing import BillingInvoice, BillingSubscription

  stripe_invoice_id = invoice_data.get("id")
  subscription_id = invoice_data.get("subscription")
  amount_cents = invoice_data.get("amount_due")
  period_start = invoice_data.get("period_start")
  period_end = invoice_data.get("period_end")
  due_date = invoice_data.get("due_date")

  if not subscription_id:
    context.log.info("Invoice created but no subscription ID")
    return

  subscription = BillingSubscription.get_by_provider_subscription_id(
    subscription_id, db_session
  )

  if not subscription:
    context.log.warning(f"Subscription not found for Stripe invoice: {subscription_id}")
    return

  existing_invoice = (
    db_session.query(BillingInvoice)
    .filter(BillingInvoice.stripe_invoice_id == stripe_invoice_id)
    .first()
  )

  if existing_invoice:
    context.log.info(f"Invoice already synced from Stripe: {stripe_invoice_id}")
    return

  invoice = BillingInvoice.create_invoice(
    org_id=subscription.org_id,
    period_start=datetime.fromtimestamp(period_start, tz=UTC),
    period_end=datetime.fromtimestamp(period_end, tz=UTC),
    payment_terms="immediate",
    session=db_session,
  )

  invoice.stripe_invoice_id = stripe_invoice_id
  invoice.status = "open"

  if due_date:
    invoice.due_date = datetime.fromtimestamp(due_date, tz=UTC)

  invoice.add_line_item(
    subscription_id=subscription.id,
    resource_type=subscription.resource_type,
    resource_id=subscription.resource_id,
    description=f"Stripe Invoice - {subscription.plan_name}",
    amount_cents=amount_cents,
    session=db_session,
  )

  invoice.finalize(db_session)
  db_session.commit()

  context.log.info(f"Synced Stripe invoice {stripe_invoice_id} to database")


async def _handle_payment_succeeded(
  invoice_data: dict, db_session: Any, context: OpExecutionContext
) -> None:
  """Handle invoice.payment_succeeded event."""
  from robosystems.models.billing import (
    BillingCustomer,
    BillingInvoice,
    BillingSubscription,
  )

  stripe_invoice_id = invoice_data.get("id")
  subscription_id = invoice_data.get("subscription")
  customer_id = invoice_data.get("customer")

  if not subscription_id:
    context.log.info("Payment succeeded but no subscription ID in invoice")
    return

  subscription = BillingSubscription.get_by_provider_subscription_id(
    subscription_id, db_session
  )

  if not subscription:
    context.log.warning(f"Subscription not found for invoice: {subscription_id}")
    return

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
    db_session.commit()

    context.log.info(f"Marked invoice {invoice.invoice_number} as paid")

  if subscription.status in ["pending_payment", "provisioning"]:
    await _trigger_resource_provisioning(subscription, db_session, context)

  context.log.info(f"Payment succeeded for subscription {subscription.id}")


async def _handle_payment_failed(
  invoice_data: dict, db_session: Any, context: OpExecutionContext
) -> None:
  """Handle invoice.payment_failed event."""
  from robosystems.models.billing import BillingSubscription

  subscription_id = invoice_data.get("subscription")

  if not subscription_id:
    return

  subscription = BillingSubscription.get_by_provider_subscription_id(
    subscription_id, db_session
  )

  if not subscription:
    context.log.warning(f"Subscription not found for failed payment: {subscription_id}")
    return

  if subscription.status == "pending_payment":
    subscription.status = "unpaid"

    error_message = "Payment failed"
    metadata = dict(subscription.subscription_metadata or {})
    metadata["error"] = error_message
    subscription.subscription_metadata = metadata

    db_session.commit()

  context.log.warning(f"Payment failed for subscription {subscription.id}")


async def _handle_subscription_updated(
  subscription_data: dict, db_session: Any, context: OpExecutionContext
) -> None:
  """Handle customer.subscription.updated event."""
  from robosystems.models.billing import BillingSubscription

  subscription_id = subscription_data.get("id")
  status = subscription_data.get("status")

  subscription = BillingSubscription.get_by_provider_subscription_id(
    subscription_id, db_session
  )

  if not subscription:
    context.log.warning(f"Subscription not found for update: {subscription_id}")
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

  if new_status != subscription.status:
    old_status = subscription.status
    subscription.status = new_status
    db_session.commit()

    context.log.info(
      f"Subscription {subscription.id} status: {old_status} -> {new_status}"
    )


async def _handle_subscription_deleted(
  subscription_data: dict, db_session: Any, context: OpExecutionContext
) -> None:
  """Handle customer.subscription.deleted event."""
  from robosystems.models.billing import BillingSubscription

  subscription_id = subscription_data.get("id")

  subscription = BillingSubscription.get_by_provider_subscription_id(
    subscription_id, db_session
  )

  if not subscription:
    context.log.warning(f"Subscription not found for deletion: {subscription_id}")
    return

  subscription.cancel(db_session, immediate=True)

  context.log.info(f"Subscription canceled via Stripe: {subscription.id}")


async def _trigger_resource_provisioning(
  subscription: Any, db_session: Any, context: OpExecutionContext
) -> None:
  """Trigger resource provisioning after payment confirmation."""
  from robosystems.models.iam import OrgRole, OrgUser

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

  if resource_type == "graph":
    if not subscription.subscription_metadata:
      subscription.subscription_metadata = {}
    subscription.subscription_metadata.update(resource_config)
    subscription.status = "provisioning"
    db_session.commit()

    context.log.info(f"Subscription {subscription.id} set to provisioning")

  elif resource_type == "repository":
    repository_name = resource_config.get("repository_name")

    if not subscription.subscription_metadata:
      subscription.subscription_metadata = {}
    subscription.subscription_metadata["repository_name"] = repository_name
    subscription.status = "provisioning"
    db_session.commit()

    context.log.info(f"Repository subscription {subscription.id} set to provisioning")

  else:
    context.log.error(f"Unknown resource type: {resource_type}")
    subscription.status = "failed"
    subscription.subscription_metadata["error"] = (
      f"Unknown resource type: {resource_type}"
    )
    db_session.commit()


def _emit_webhook_result_to_sse(
  context: OpExecutionContext,
  operation_id: str,
  result: dict,
) -> None:
  """Update SSE operation metadata with the webhook result."""
  try:
    from robosystems.middleware.sse.event_storage import SSEEventStorage

    storage = SSEEventStorage()
    storage.update_operation_result_sync(operation_id, result)
    context.log.info(f"Updated SSE metadata for operation {operation_id}")
  except Exception as e:
    context.log.warning(f"Failed to update SSE operation metadata: {e}")


@job(
  tags={
    "dagster/max_runtime": 300,  # 5 minute max
    "category": "billing",
  },
)
def process_stripe_webhook_job():
  """
  Job for processing Stripe webhook events.

  This job provides:
  - Retry logic with exponential backoff (3 retries)
  - Full observability in Dagster UI
  - Idempotency checking
  - Audit trail of all webhook processing
  """
  process_stripe_webhook_event()


def build_stripe_webhook_job_config(
  event_id: str,
  event_type: str,
  event_data: dict,
  operation_id: str | None = None,
) -> dict:
  """
  Build run_config for process_stripe_webhook_job.

  Args:
    event_id: Stripe event ID
    event_type: Stripe event type (e.g., "checkout.session.completed")
    event_data: Event data object from Stripe
    operation_id: Optional SSE operation ID for progress tracking

  Returns:
    run_config dictionary for Dagster
  """
  config: dict[str, Any] = {
    "event_id": event_id,
    "event_type": event_type,
    "event_data": event_data,
  }

  if operation_id:
    config["operation_id"] = operation_id

  run_config: dict = {
    "ops": {
      "process_stripe_webhook_event": {"config": config},
    },
  }

  # In local development, use in_process executor
  if env.ENVIRONMENT == "dev":
    run_config["execution"] = {"config": {"in_process": {}}}

  return run_config


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
        GraphCredits.graph_tier,
      )
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
              "billing_period_end": datetime.now(UTC)
              .replace(day=1)
              .isoformat(),
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


@job
def monthly_credit_allocation_job():
  """Monthly credit allocation and overage processing job."""
  graphs = get_graphs_with_negative_balance()
  invoices = process_overage_invoices(graphs)
  result = allocate_monthly_credits(invoices)
  cleanup_old_credit_transactions(result)


@op
def get_graphs_with_storage_usage(
  context: OpExecutionContext, db: DatabaseResource
) -> list[dict[str, Any]]:
  """Get all graphs that have storage usage records for yesterday."""
  from sqlalchemy import func

  billing_date = (datetime.now(UTC) - timedelta(days=1)).date()
  context.log.info(f"Getting storage usage for date: {billing_date}")

  with db.get_session() as session:
    results = (
      session.query(
        GraphUsage.graph_id,
        GraphUsage.user_id,
        GraphUsage.graph_tier,
        func.count(GraphUsage.id).label("measurement_count"),
        func.avg(GraphUsage.storage_gb).label("avg_storage_gb"),
      )
      .filter(
        GraphUsage.event_type == UsageEventType.STORAGE_SNAPSHOT.value,
        GraphUsage.billing_year == billing_date.year,
        GraphUsage.billing_month == billing_date.month,
        GraphUsage.billing_day == billing_date.day,
      )
      .group_by(
        GraphUsage.graph_id,
        GraphUsage.user_id,
        GraphUsage.graph_tier,
      )
      .all()
    )

    graphs = [
      {
        "graph_id": r.graph_id,
        "user_id": r.user_id,
        "graph_tier": r.graph_tier,
        "measurement_count": r.measurement_count,
        "avg_storage_gb": float(r.avg_storage_gb) if r.avg_storage_gb else 0,
        "billing_date": billing_date.isoformat(),
      }
      for r in results
    ]

    context.log.info(f"Found {len(graphs)} graphs with storage usage")
    return graphs


@op
def bill_storage_credits(
  context: OpExecutionContext,
  db: DatabaseResource,
  graphs_with_usage: list[dict[str, Any]],
) -> dict[str, Any]:
  """Consume storage credits for all graphs."""
  total_processed = 0
  total_credits = Decimal("0")
  negative_balances = 0
  errors = 0

  with db.get_session() as session:
    credit_service = CreditService(session)

    for graph_info in graphs_with_usage:
      try:
        if graph_info["avg_storage_gb"] == 0:
          continue

        result = credit_service.consume_storage_credits(
          graph_id=graph_info["graph_id"],
          storage_gb=Decimal(str(graph_info["avg_storage_gb"])),
          metadata={
            "billing_date": graph_info["billing_date"],
            "user_id": graph_info["user_id"],
            "graph_tier": graph_info["graph_tier"],
            "source": "dagster_daily_billing",
          },
        )

        if result["success"]:
          total_processed += 1
          total_credits += Decimal(str(result["credits_consumed"]))

          if result.get("went_negative"):
            negative_balances += 1

      except Exception as e:
        errors += 1
        context.log.error(f"Error billing storage for {graph_info['graph_id']}: {e}")

  context.log.info(
    f"Storage billing complete: {total_processed} graphs, "
    f"{float(total_credits)} credits consumed, {negative_balances} negative"
  )

  return {
    "graphs_processed": total_processed,
    "total_credits_consumed": float(total_credits),
    "negative_balances": negative_balances,
    "errors": errors,
    "timestamp": datetime.now(UTC).isoformat(),
  }


@job
def daily_storage_billing_job():
  """Daily storage billing job."""
  graphs = get_graphs_with_storage_usage()
  bill_storage_credits(graphs)


@op
def collect_graph_usage(
  context: OpExecutionContext, db: DatabaseResource
) -> dict[str, Any]:
  """Collect storage usage snapshots for all active graphs."""
  from robosystems.models.iam import Graph
  from robosystems.operations.graph.storage_service import StorageCalculator

  collected = 0
  errors = 0

  with db.get_session() as session:
    # Get all active graphs with user_id and tier info
    # Join with Graph table since graph_tier is a property that reads from Graph
    active_graphs = (
      session.query(
        GraphCredits.graph_id,
        GraphCredits.user_id,
        Graph.graph_tier,
      )
      .join(Graph, GraphCredits.graph_id == Graph.graph_id)
      .all()
    )

    storage_calculator = StorageCalculator(session)

    for graph_id, user_id, graph_tier in active_graphs:
      try:
        # Calculate storage using StorageCalculator
        storage_data = storage_calculator.calculate_graph_storage(graph_id, user_id)

        # Record storage usage snapshot
        GraphUsage.record_storage_usage(
          user_id=user_id,
          graph_id=graph_id,
          graph_tier=graph_tier,
          storage_bytes=storage_data.get("total_bytes", 0),
          session=session,
          files_storage_gb=float(storage_data.get("files_gb", 0)),
          tables_storage_gb=float(storage_data.get("tables_gb", 0)),
          graphs_storage_gb=float(storage_data.get("graphs_gb", 0)),
          subgraphs_storage_gb=float(storage_data.get("subgraphs_gb", 0)),
          auto_commit=False,  # Commit at end
        )
        collected += 1
      except Exception as e:
        errors += 1
        context.log.warning(f"Failed to collect usage for {graph_id}: {e}")

    # Commit all at once
    session.commit()

  context.log.info(f"Collected usage for {collected} graphs, {errors} errors")

  return {
    "graphs_collected": collected,
    "errors": errors,
    "timestamp": datetime.now(UTC).isoformat(),
  }


@job
def hourly_usage_collection_job():
  """Hourly usage collection job."""
  collect_graph_usage()


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


@job
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

daily_storage_billing_schedule = ScheduleDefinition(
  job=daily_storage_billing_job,
  cron_schedule="0 2 * * *",  # Daily at 2 AM UTC
  default_status=BILLING_SCHEDULE_STATUS,
)

hourly_usage_collection_schedule = ScheduleDefinition(
  job=hourly_usage_collection_job,
  cron_schedule="5 * * * *",  # 5 minutes past every hour
  default_status=BILLING_SCHEDULE_STATUS,
)

monthly_usage_report_schedule = ScheduleDefinition(
  job=monthly_usage_report_job,
  cron_schedule="0 6 2 * *",  # 2nd of month at 6 AM UTC
  default_status=BILLING_SCHEDULE_STATUS,
)
