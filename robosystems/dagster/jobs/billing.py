"""Dagster billing jobs.

These jobs handle credit allocation, storage billing, and usage collection.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from dagster import (
  DefaultScheduleStatus,
  OpExecutionContext,
  ScheduleDefinition,
  job,
  op,
)

from robosystems.dagster.resources import DatabaseResource
from robosystems.models.iam import GraphCredits, GraphCreditTransaction, GraphUsage
from robosystems.models.iam.graph_credits import CreditTransactionType
from robosystems.models.iam.graph_usage import UsageEventType
from robosystems.operations.graph.credit_service import CreditService


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
              "billing_period_end": datetime.now(timezone.utc)
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
          "invoice_date": datetime.now(timezone.utc).isoformat(),
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
      "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@op
def cleanup_old_credit_transactions(
  context: OpExecutionContext,
  db: DatabaseResource,
  allocation_result: dict[str, Any],
) -> dict[str, Any]:
  """Clean up old credit transaction records."""
  months_to_keep = 12
  cutoff_date = datetime.now(timezone.utc) - timedelta(days=months_to_keep * 30)

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

  billing_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()
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
    "timestamp": datetime.now(timezone.utc).isoformat(),
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
    "timestamp": datetime.now(timezone.utc).isoformat(),
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
  last_month = datetime.now(timezone.utc).replace(day=1) - timedelta(days=1)
  year = last_month.year
  month = last_month.month

  context.log.info(f"Generating usage report for {year}-{month:02d}")

  total_credits_consumed = Decimal("0")
  total_credits_allocated = Decimal("0")
  graphs_with_overage = 0
  graph_reports = []

  with db.get_session() as session:
    all_graphs = session.query(GraphCredits).all()

    month_start = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
      month_end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
      month_end = datetime(year, month + 1, 1, tzinfo=timezone.utc)

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
    "timestamp": datetime.now(timezone.utc).isoformat(),
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
  default_status=DefaultScheduleStatus.RUNNING,
)

daily_storage_billing_schedule = ScheduleDefinition(
  job=daily_storage_billing_job,
  cron_schedule="0 2 * * *",  # Daily at 2 AM UTC
  default_status=DefaultScheduleStatus.RUNNING,
)

hourly_usage_collection_schedule = ScheduleDefinition(
  job=hourly_usage_collection_job,
  cron_schedule="5 * * * *",  # 5 minutes past every hour
  default_status=DefaultScheduleStatus.RUNNING,
)

monthly_usage_report_schedule = ScheduleDefinition(
  job=monthly_usage_report_job,
  cron_schedule="0 6 2 * *",  # 2nd of month at 6 AM UTC
  default_status=DefaultScheduleStatus.RUNNING,
)
