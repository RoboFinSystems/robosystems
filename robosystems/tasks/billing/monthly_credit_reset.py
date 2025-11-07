"""
Monthly Credit Reset and Overage Billing Task

This is the PRIMARY monthly billing task that runs on the 1st of each month.
It replaces the old allocate_monthly_graph_credits task with enhanced functionality.

Monthly Billing Flow:
--------------------
1. Identify graphs with negative balances (storage overages from daily billing)
2. Generate overage invoices documenting what users owe
3. Allocate fresh monthly credits via bulk_allocate_monthly_credits()
4. Clean up old transaction records (12 month retention)

Related Tasks:
--------------
- daily_storage_billing (storage_billing.py): Consumes credits for storage overages
- collect-storage-usage (usage_collector.py): Hourly storage snapshots with breakdown
- monthly_storage_summary (storage_billing.py): Monthly analytics on 2nd of month
- allocate_monthly_shared_credits (shared_credit_allocation.py): Shared repo credits

Key Difference from Old Task:
-----------------------------
The old allocate_monthly_graph_credits just allocated credits. This task:
- Processes overages BEFORE allocation (captures negative balances)
- Generates invoices for billing system integration
- Provides comprehensive monthly usage reporting
"""

import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Dict, List

from sqlalchemy.orm import Session
from sqlalchemy import and_

from ...celery import celery_app
from ...database import session as SessionLocal
from ...models.iam import GraphCredits, GraphCreditTransaction
from ...models.iam.graph_credits import CreditTransactionType
from ...operations.graph.credit_service import CreditService

logger = logging.getLogger(__name__)


def get_celery_db_session():
  """Get a database session for Celery tasks."""
  return SessionLocal()


@celery_app.task(name="robosystems.tasks.monthly_credit_reset", bind=True)
def monthly_credit_reset(self):
  """
  Run monthly credit reset and overage processing.

  This task:
  1. Identifies graphs with negative balances (overage situations)
  2. Generates overage invoices for billing
  3. Allocates new monthly credits to all graphs
  4. Cleans up old transaction records
  """
  logger.info("Starting monthly credit reset and overage processing")

  session = get_celery_db_session()
  try:
    credit_service = CreditService(session)

    # Track processing statistics
    total_graphs = 0
    graphs_with_overage = 0
    total_overage_amount = Decimal("0")
    allocation_errors = 0

    # Get all graphs with negative balances BEFORE reset
    negative_balance_graphs = get_graphs_with_negative_balance(session)
    graphs_with_overage = len(negative_balance_graphs)

    logger.info(f"Found {graphs_with_overage} graphs with negative balances (overages)")

    # Process overages and generate invoices
    overage_invoices = []
    for graph_info in negative_balance_graphs:
      try:
        invoice = process_overage_invoice(session, graph_info)
        overage_invoices.append(invoice)
        total_overage_amount += abs(Decimal(str(graph_info["negative_balance"])))

        logger.info(
          f"Generated overage invoice for graph {graph_info['graph_id']}: "
          f"{graph_info['negative_balance']} credits (${invoice['amount_usd']:.2f})"
        )

      except Exception as e:
        logger.error(
          f"Failed to process overage for graph {graph_info['graph_id']}: {e}"
        )
        allocation_errors += 1

    # Allocate monthly credits for all graphs
    logger.info("Allocating monthly credits to all graphs")
    allocation_result = credit_service.bulk_allocate_monthly_credits()

    total_graphs = allocation_result["allocated_graphs"]

    logger.info(
      f"Allocated {allocation_result['total_credits_allocated']} credits "
      f"to {total_graphs} graphs"
    )

    # Clean up old transaction records (keep last 12 months)
    cleanup_result = cleanup_old_transactions(session, months_to_keep=12)

    logger.info(
      f"Monthly credit reset completed: "
      f"{total_graphs} graphs processed, "
      f"{graphs_with_overage} overages, "
      f"{float(total_overage_amount)} total overage credits, "
      f"{allocation_errors} errors"
    )

    return {
      "status": "success",
      "timestamp": datetime.now(timezone.utc).isoformat(),
      "graphs_processed": total_graphs,
      "graphs_with_overage": graphs_with_overage,
      "total_overage_credits": float(total_overage_amount),
      "overage_invoices": overage_invoices,
      "allocation_result": allocation_result,
      "cleanup_result": cleanup_result,
      "processing_errors": allocation_errors,
    }

  except Exception as e:
    logger.error(f"Monthly credit reset failed: {e}")
    return {
      "status": "error",
      "error": str(e),
      "timestamp": datetime.now(timezone.utc).isoformat(),
    }
  finally:
    session.close()


def get_graphs_with_negative_balance(session: Session) -> List[Dict]:
  """Get all graphs that have negative credit balances (overages)."""
  results = (
    session.query(  # type: ignore[call-overload]
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

  return [
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


def process_overage_invoice(session: Session, graph_info: Dict) -> Dict:
  """
  Process overage invoice for a graph with negative balance.

  Creates a transaction record documenting the overage for billing purposes.
  In a real system, this would integrate with a payment processor.

  Args:
      session: Database session
      graph_info: Graph information including negative balance

  Returns:
      Dict with invoice details
  """
  overage_credits = abs(Decimal(str(graph_info["negative_balance"])))

  # Calculate USD amount (approximate: $0.01 per 2 credits = $0.005 per credit)
  # This is a rough conversion - actual pricing may vary
  usd_amount = float(overage_credits) * 0.005

  # Create a transaction record for the overage invoice
  # This serves as an audit trail for billing
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
        "billing_period_end": datetime.now(timezone.utc).replace(day=1).isoformat(),
        "graph_tier": str(graph_info["graph_tier"]),
        "monthly_allocation": str(graph_info["monthly_allocation"]),
        "negative_balance": str(graph_info["negative_balance"]),
      },
      session=session,
    )

    session.commit()

  return {
    "graph_id": graph_info["graph_id"],
    "user_id": graph_info["user_id"],
    "billing_admin_id": graph_info["billing_admin_id"],
    "overage_credits": float(overage_credits),
    "amount_usd": usd_amount,
    "invoice_date": datetime.now(timezone.utc).isoformat(),
    "status": "pending_payment",
  }


def cleanup_old_transactions(session: Session, months_to_keep: int = 12) -> Dict:
  """
  Clean up old credit transaction records to save database space.

  Keeps transactions for the specified number of months, deletes older ones.

  Args:
      session: Database session
      months_to_keep: Number of months of transactions to retain

  Returns:
      Dict with cleanup statistics
  """
  # Calculate cutoff date (keep last N months)
  cutoff_date = datetime.now(timezone.utc) - timedelta(days=months_to_keep * 30)

  # Count transactions to be deleted
  count_query = session.query(GraphCreditTransaction).filter(
    and_(
      GraphCreditTransaction.created_at < cutoff_date,
      # Don't delete ALLOCATION transactions (important for audit trail)
      GraphCreditTransaction.transaction_type != CreditTransactionType.ALLOCATION.value,
    )
  )

  total_count = count_query.count()

  if total_count == 0:
    return {"deleted_transactions": 0, "total_processed": 0}

  # Delete old consumption transactions
  deleted_count = count_query.delete()
  session.commit()

  logger.info(
    f"Cleaned up {deleted_count} old credit transactions (older than {months_to_keep} months)"
  )

  return {
    "deleted_transactions": deleted_count,
    "total_processed": total_count,
    "cutoff_date": cutoff_date.isoformat(),
    "months_kept": months_to_keep,
  }


@celery_app.task(name="robosystems.tasks.generate_monthly_usage_report", bind=True)
def generate_monthly_usage_report(self, year: int = None, month: int = None) -> Dict:
  """
  Generate comprehensive monthly usage report for all graphs.

  This report includes:
  - Credit consumption by graph
  - Storage usage statistics
  - Overage situations
  - Allocation history

  Args:
      year: Year for report (defaults to last month)
      month: Month for report (defaults to last month)

  Returns:
      Dict with comprehensive usage statistics
  """
  logger.info("Generating monthly usage report")

  # Use last month if not specified
  if not year or not month:
    last_month = datetime.now(timezone.utc).replace(day=1) - timedelta(days=1)
    year = last_month.year
    month = last_month.month

  logger.info(f"Generating usage report for {year}-{month:02d}")

  session = get_celery_db_session()
  try:
    # Get all graphs
    all_graphs = session.query(GraphCredits).all()

    total_graphs = len(all_graphs)
    total_credits_consumed = Decimal("0")
    total_credits_allocated = Decimal("0")
    graphs_with_overage_count = 0

    graph_reports = []

    for graph_credits in all_graphs:
      # Get transactions for the month
      month_start = datetime(year, month, 1, tzinfo=timezone.utc)
      if month == 12:
        month_end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
      else:
        month_end = datetime(year, month + 1, 1, tzinfo=timezone.utc)

      transactions = (
        session.query(GraphCreditTransaction)
        .filter(
          GraphCreditTransaction.graph_credits_id == graph_credits.id,
          GraphCreditTransaction.created_at >= month_start,
          GraphCreditTransaction.created_at < month_end,
        )
        .all()
      )

      # Calculate consumption and allocation
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
        graphs_with_overage_count += 1

      graph_reports.append(
        {
          "graph_id": graph_credits.graph_id,
          "user_id": graph_credits.user_id,
          "graph_tier": str(graph_credits.graph_tier),
          "monthly_allocation": float(graph_credits.monthly_allocation),
          "credits_consumed": float(consumption),
          "credits_allocated": float(allocation),
          "current_balance": float(graph_credits.current_balance),
          "has_overage": has_overage,
          "transaction_count": len(transactions),
        }
      )

    logger.info(
      f"Monthly usage report completed: "
      f"{total_graphs} graphs, "
      f"{float(total_credits_consumed)} credits consumed, "
      f"{graphs_with_overage_count} overages"
    )

    return {
      "status": "success",
      "year": year,
      "month": month,
      "timestamp": datetime.now(timezone.utc).isoformat(),
      "summary": {
        "total_graphs": total_graphs,
        "total_credits_consumed": float(total_credits_consumed),
        "total_credits_allocated": float(total_credits_allocated),
        "graphs_with_overage": graphs_with_overage_count,
      },
      "graph_reports": graph_reports,
    }

  except Exception as e:
    logger.error(f"Monthly usage report generation failed: {e}")
    return {
      "status": "error",
      "error": str(e),
      "year": year,
      "month": month,
      "timestamp": datetime.now(timezone.utc).isoformat(),
    }
  finally:
    session.close()
