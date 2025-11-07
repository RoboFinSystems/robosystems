"""
Daily Storage Credit Consumption Task

This task runs daily to calculate average storage usage and consume credits
for each graph database. Storage charges are always applied and can result in
negative credit balances (users cannot "turn off" storage).

Storage Billing Model:
---------------------
- Each subscription tier includes storage (100GB/500GB/2TB)
- Only OVERAGE above included limit consumes credits
- Rate: 10 credits/GB/day for storage above limit
- Charges applied even if balance goes negative
- Negative balances are invoiced at month-end by monthly_credit_reset task

Storage Data Source:
-------------------
- Hourly snapshots collected by usage_collector task
- Storage breakdown: files (S3), tables (S3), graphs (EBS), subgraphs (EBS)
- Daily billing uses average of all snapshots for the day
- Detailed breakdown enables storage analytics and optimization
"""

import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from ...celery import celery_app
from ...database import session as SessionLocal
from ...models.iam import GraphUsageTracking
from ...models.iam.graph_usage_tracking import UsageEventType
from ...operations.graph.credit_service import CreditService

logger = logging.getLogger(__name__)


def get_celery_db_session():
  """Get a database session for Celery tasks."""
  return SessionLocal()


@celery_app.task(name="robosystems.tasks.daily_storage_billing", bind=True)
def daily_storage_billing(self, target_date: Optional[str] = None):
  """
  Run daily storage billing for all graphs.

  Args:
      target_date: Optional date string (YYYY-MM-DD) to process.
                  If None, processes yesterday.
  """
  logger.info("Starting daily storage billing task")

  # Parse target date or use yesterday
  if target_date:
    try:
      billing_date = datetime.strptime(target_date, "%Y-%m-%d").date()
    except ValueError:
      logger.error(f"Invalid date format: {target_date}. Expected YYYY-MM-DD")
      return {"status": "error", "error": "Invalid date format"}
  else:
    # Process yesterday by default
    billing_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()

  logger.info(f"Processing storage billing for date: {billing_date}")

  session = get_celery_db_session()
  try:
    credit_service = CreditService(session)

    # Get all graphs with storage usage for the target date
    graphs_with_usage = get_graphs_with_storage_usage(
      session, billing_date.year, billing_date.month, billing_date.day
    )

    logger.info(f"Found {len(graphs_with_usage)} graphs with storage usage")

    # Track processing statistics
    total_processed = 0
    total_credits_consumed = Decimal("0")
    negative_balances = 0
    processing_errors = 0

    # Process each graph
    for graph_info in graphs_with_usage:
      try:
        # Calculate average storage for the day
        avg_storage_gb = calculate_daily_average_storage(
          session,
          graph_info["graph_id"],
          billing_date.year,
          billing_date.month,
          billing_date.day,
        )

        if avg_storage_gb is None or avg_storage_gb == 0:
          logger.warning(
            f"No storage data for graph {graph_info['graph_id']}, skipping"
          )
          continue

        # Consume storage credits
        result = credit_service.consume_storage_credits(
          graph_id=graph_info["graph_id"],
          storage_gb=Decimal(str(avg_storage_gb)),
          metadata={
            "billing_date": billing_date.isoformat(),
            "user_id": graph_info["user_id"],
            "graph_tier": graph_info["graph_tier"],
            "measurement_count": graph_info["measurement_count"],
            "task_id": str(self.request.id) if self.request else None,
          },
        )

        if result["success"]:
          total_processed += 1
          total_credits_consumed += Decimal(str(result["credits_consumed"]))

          if result["went_negative"]:
            negative_balances += 1
            logger.warning(
              f"Graph {graph_info['graph_id']} went negative: "
              f"{result['old_balance']} -> {result['remaining_balance']}"
            )

          logger.info(
            f"Processed {graph_info['graph_id']}: "
            f"{avg_storage_gb} GB -> {result['credits_consumed']} credits"
          )
        else:
          processing_errors += 1
          logger.error(
            f"Failed to process {graph_info['graph_id']}: {result.get('error')}"
          )

      except Exception as e:
        processing_errors += 1
        logger.error(f"Error processing graph {graph_info['graph_id']}: {e}")

    # Clean up old storage usage records to save space
    cleanup_result = cleanup_old_storage_records(session, days_to_keep=90)

    logger.info(
      f"Daily storage billing completed: "
      f"{total_processed} graphs processed, "
      f"{float(total_credits_consumed)} credits consumed, "
      f"{negative_balances} negative balances, "
      f"{processing_errors} errors"
    )

    return {
      "status": "success",
      "billing_date": billing_date.isoformat(),
      "timestamp": datetime.now(timezone.utc).isoformat(),
      "graphs_processed": total_processed,
      "total_credits_consumed": float(total_credits_consumed),
      "negative_balances": negative_balances,
      "processing_errors": processing_errors,
      "cleanup_result": cleanup_result,
    }

  except Exception as e:
    logger.error(f"Daily storage billing failed: {e}")
    return {
      "status": "error",
      "error": str(e),
      "billing_date": billing_date.isoformat(),
      "timestamp": datetime.now(timezone.utc).isoformat(),
    }
  finally:
    session.close()


def get_graphs_with_storage_usage(
  session: Session, year: int, month: int, day: int
) -> List[Dict]:
  """Get all graphs that have storage usage records for the specified date."""
  from sqlalchemy import func

  # Query for graphs with storage usage on the target date
  results = (
    session.query(
      GraphUsageTracking.graph_id,
      GraphUsageTracking.user_id,
      GraphUsageTracking.graph_tier,
      func.count(GraphUsageTracking.id).label("measurement_count"),
    )
    .filter(
      GraphUsageTracking.event_type == UsageEventType.STORAGE_SNAPSHOT.value,
      GraphUsageTracking.billing_year == year,
      GraphUsageTracking.billing_month == month,
      GraphUsageTracking.billing_day == day,
    )
    .group_by(
      GraphUsageTracking.graph_id,
      GraphUsageTracking.user_id,
      GraphUsageTracking.graph_tier,
    )
    .all()
  )

  return [
    {
      "graph_id": r.graph_id,
      "user_id": r.user_id,
      "graph_tier": r.graph_tier,
      "measurement_count": r.measurement_count,
    }
    for r in results
  ]


def calculate_daily_average_storage(
  session: Session, graph_id: str, year: int, month: int, day: int
) -> Optional[float]:
  """Calculate average storage usage for a graph on a specific day."""
  from sqlalchemy import func

  # Get all storage measurements for the day
  result = (
    session.query(func.avg(GraphUsageTracking.storage_gb))
    .filter(
      GraphUsageTracking.graph_id == graph_id,
      GraphUsageTracking.event_type == UsageEventType.STORAGE_SNAPSHOT.value,
      GraphUsageTracking.billing_year == year,
      GraphUsageTracking.billing_month == month,
      GraphUsageTracking.billing_day == day,
    )
    .scalar()
  )

  return float(result) if result else None


def cleanup_old_storage_records(session: Session, days_to_keep: int = 90) -> Dict:
  """Clean up old storage usage records to save database space."""
  cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)

  # Count records to be deleted
  count_query = session.query(GraphUsageTracking).filter(
    GraphUsageTracking.event_type == UsageEventType.STORAGE_SNAPSHOT.value,
    GraphUsageTracking.recorded_at < cutoff_date,
  )

  total_count = count_query.count()

  if total_count == 0:
    return {"deleted_records": 0, "total_processed": 0}

  # Delete old records
  deleted_count = count_query.delete()
  session.commit()

  logger.info(
    f"Cleaned up {deleted_count} old storage records (older than {days_to_keep} days)"
  )

  return {
    "deleted_records": deleted_count,
    "total_processed": total_count,
    "cutoff_date": cutoff_date.isoformat(),
  }


@celery_app.task(name="robosystems.tasks.monthly_storage_summary", bind=True)
def monthly_storage_summary(
  self, year: Optional[int] = None, month: Optional[int] = None
):
  """
  Generate monthly storage summary for all users.

  This task provides detailed storage analytics for the month and can be used
  for billing reconciliation and usage reporting.
  """
  logger.info("Starting monthly storage summary task")

  # Use last month if not specified
  if not year or not month:
    last_month = datetime.now(timezone.utc).replace(day=1) - timedelta(days=1)
    year = last_month.year
    month = last_month.month

  logger.info(f"Generating storage summary for {year}-{month:02d}")

  session = get_celery_db_session()
  try:
    # Get monthly storage summary for all users
    summaries = GraphUsageTracking.get_monthly_storage_summary(
      user_id=None,  # Get all users
      year=year,
      month=month,
      session=session,
    )

    total_graphs = len(summaries)
    total_gb_hours = sum(data["total_gb_hours"] for data in summaries.values())

    logger.info(
      f"Monthly storage summary completed: "
      f"{total_graphs} graphs, {total_gb_hours:.2f} total GB-hours"
    )

    return {
      "status": "success",
      "year": year,
      "month": month,
      "timestamp": datetime.now(timezone.utc).isoformat(),
      "total_graphs": total_graphs,
      "total_gb_hours": total_gb_hours,
      "summaries": summaries,
    }

  except Exception as e:
    logger.error(f"Monthly storage summary failed: {e}")
    return {
      "status": "error",
      "error": str(e),
      "year": year,
      "month": month,
      "timestamp": datetime.now(timezone.utc).isoformat(),
    }
  finally:
    session.close()
