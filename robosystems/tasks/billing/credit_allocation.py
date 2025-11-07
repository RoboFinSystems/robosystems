"""
Graph credits health check and utility tasks.

This module provides health monitoring and utility functions for the graph
credit system. Monthly credit allocation is now handled by monthly_credit_reset.py,
which includes overage processing before allocation.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any

from celery import shared_task
from sqlalchemy import func

from ...database import session as SessionLocal
from ...models.iam.graph_credits import GraphCredits

logger = logging.getLogger(__name__)


def get_celery_db_session():
  """Get a database session for Celery tasks."""
  return SessionLocal()


@shared_task(name="allocate_graph_credits_for_user")
def allocate_graph_credits_for_user(user_id: str) -> Dict[str, Any]:
  """
  Allocate monthly credits for all graphs owned by a specific user.

  This task can be used for targeted allocation, such as when a user
  upgrades their subscription or for customer support purposes.

  Args:
      user_id: User ID to allocate credits for

  Returns:
      Summary of allocation results for the user
  """
  logger.info(f"Starting graph credit allocation for user {user_id}")

  db = get_celery_db_session()
  try:
    # Get all graph credits for the user
    graph_credits = db.query(GraphCredits).filter(GraphCredits.user_id == user_id).all()

    allocated_count = 0
    total_credits = 0
    allocation_results = []

    for credits in graph_credits:
      if credits.allocate_monthly_credits(db):
        allocated_count += 1
        total_credits += credits.monthly_allocation
        allocation_results.append(
          {
            "graph_id": credits.graph_id,
            "graph_tier": credits.graph_tier,
            "credits_allocated": float(credits.monthly_allocation),
            "new_balance": float(credits.current_balance),
          }
        )

    db.commit()

    result = {
      "user_id": user_id,
      "graphs_allocated": allocated_count,
      "total_graphs": len(graph_credits),
      "total_credits_allocated": float(total_credits),
      "allocations": allocation_results,
    }

    logger.info(
      f"User {user_id} credit allocation completed: "
      f"{allocated_count}/{len(graph_credits)} graphs allocated"
    )

    return result

  except Exception as e:
    logger.error(f"Failed to allocate credits for user {user_id}: {e}")
    db.rollback()
    raise
  finally:
    db.close()


@shared_task(name="check_graph_credit_health")
def check_graph_credit_health() -> Dict[str, Any]:
  """
  Health check for graph credit system.

  Monitors for issues like:
  - Graphs without credit pools
  - Overdue allocations
  - Low balance warnings

  Returns:
      Health check results with any issues found
  """
  logger.info("Starting graph credit health check")

  db = get_celery_db_session()
  try:
    now = datetime.now(timezone.utc)
    issues = []

    # Check for overdue allocations (more than 35 days since last allocation)
    overdue_count = (
      db.query(func.count(GraphCredits.id))
      .filter(
        GraphCredits.last_allocation_date
        < func.date_trunc("day", now - timedelta(days=35))
      )
      .scalar()
    )

    if overdue_count > 0:
      issues.append(
        {
          "type": "overdue_allocations",
          "severity": "warning",
          "count": overdue_count,
          "message": f"{overdue_count} graphs have overdue credit allocations",
        }
      )

    # Check for very low balances (less than 10% of monthly allocation)
    low_balance_credits = (
      db.query(GraphCredits)
      .filter(GraphCredits.current_balance < GraphCredits.monthly_allocation * 0.1)
      .all()
    )

    if low_balance_credits:
      issues.append(
        {
          "type": "low_balances",
          "severity": "info",
          "count": len(low_balance_credits),
          "message": f"{len(low_balance_credits)} graphs have low credit balances",
          "details": [
            {
              "graph_id": c.graph_id,
              "balance": float(c.current_balance),
              "monthly_allocation": float(c.monthly_allocation),
            }
            for c in low_balance_credits[:10]  # Limit to first 10
          ],
        }
      )

    # Check for graphs with zero monthly allocation
    zero_allocation_count = (
      db.query(func.count(GraphCredits.id))
      .filter(GraphCredits.monthly_allocation == 0)
      .scalar()
    )

    if zero_allocation_count > 0:
      issues.append(
        {
          "type": "zero_allocations",
          "severity": "error",
          "count": zero_allocation_count,
          "message": f"{zero_allocation_count} graphs have zero monthly allocation",
        }
      )

    result = {
      "status": "healthy" if not issues else "issues_found",
      "checked_at": now.isoformat(),
      "total_graph_credit_pools": db.query(func.count(GraphCredits.id)).scalar(),
      "issues": issues,
    }

    if issues:
      logger.warning(f"Graph credit health check found {len(issues)} issues")
    else:
      logger.info("Graph credit health check passed")

    return result

  except Exception as e:
    logger.error(f"Failed to check graph credit health: {e}")
    raise
  finally:
    db.close()
