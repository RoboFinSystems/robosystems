"""Dagster infrastructure jobs.

These jobs are migrated from Celery tasks for system maintenance:
- Auth cleanup (expired API keys, tokens)
- Health checks (credit allocation, graph credits)
"""

from datetime import datetime, timezone

from dagster import (
  DefaultScheduleStatus,
  OpExecutionContext,
  ScheduleDefinition,
  job,
  op,
)

from robosystems.dagster.resources import DatabaseResource
from robosystems.models.iam import UserAPIKey


# ============================================================================
# Auth Cleanup Job
# Replaces: robosystems.tasks.infrastructure.auth_cleanup.cleanup_expired_api_keys_task
# ============================================================================


@op
def cleanup_expired_api_keys(context: OpExecutionContext, db: DatabaseResource) -> dict:
  """Clean up expired API keys from the database."""
  with db.get_session() as session:
    now = datetime.now(timezone.utc)

    # Find and delete expired API keys
    expired_keys = (
      session.query(UserAPIKey)
      .filter(
        UserAPIKey.expires_at.isnot(None),
        UserAPIKey.expires_at < now,
      )
      .all()
    )

    deleted_count = len(expired_keys)

    for key in expired_keys:
      session.delete(key)

    context.log.info(f"Cleaned up {deleted_count} expired API keys")

    return {
      "deleted_count": deleted_count,
      "timestamp": now.isoformat(),
    }


@job
def hourly_auth_cleanup_job():
  """Hourly auth cleanup job."""
  cleanup_expired_api_keys()


# ============================================================================
# Health Check Jobs
# Replaces: robosystems.tasks.billing.shared_credit_allocation.check_credit_allocation_health
# Replaces: robosystems.tasks.billing.credit_allocation.check_graph_credit_health
# ============================================================================


@op
def check_shared_credit_allocation_health(
  context: OpExecutionContext, db: DatabaseResource
) -> dict:
  """Check health of shared credit allocation system."""
  from robosystems.models.iam import UserRepositoryCredits

  issues = []

  with db.get_session() as session:
    # Check for repository subscriptions without credit records
    repos_without_credits = (
      session.query(UserRepositoryCredits)
      .filter(UserRepositoryCredits.current_balance.is_(None))
      .count()
    )

    if repos_without_credits > 0:
      issues.append(f"{repos_without_credits} repository subscriptions without credits")

    # Check for negative balances (shouldn't happen for shared repos)
    negative_balances = (
      session.query(UserRepositoryCredits)
      .filter(UserRepositoryCredits.current_balance < 0)
      .count()
    )

    if negative_balances > 0:
      issues.append(
        f"{negative_balances} repository subscriptions with negative balances"
      )

  health_status = "healthy" if len(issues) == 0 else "unhealthy"
  context.log.info(f"Shared credit allocation health: {health_status}")

  if issues:
    for issue in issues:
      context.log.warning(f"Health issue: {issue}")

  return {
    "status": health_status,
    "issues": issues,
    "timestamp": datetime.now(timezone.utc).isoformat(),
  }


@op
def check_graph_credit_health(
  context: OpExecutionContext, db: DatabaseResource
) -> dict:
  """Check health of graph credit system."""
  from robosystems.models.iam import GraphCredits

  issues = []

  with db.get_session() as session:
    # Check for graphs without monthly allocations
    no_allocation = (
      session.query(GraphCredits).filter(GraphCredits.monthly_allocation == 0).count()
    )

    if no_allocation > 0:
      issues.append(f"{no_allocation} graphs without monthly allocation")

    # Count graphs with negative balances (warning, not error)
    negative_count = (
      session.query(GraphCredits).filter(GraphCredits.current_balance < 0).count()
    )

    if negative_count > 0:
      context.log.info(f"{negative_count} graphs have negative balances (overages)")

  health_status = "healthy" if len(issues) == 0 else "warning"
  context.log.info(f"Graph credit health: {health_status}")

  return {
    "status": health_status,
    "issues": issues,
    "negative_balance_count": negative_count if "negative_count" in dir() else 0,
    "timestamp": datetime.now(timezone.utc).isoformat(),
  }


@op
def aggregate_health_results(
  context: OpExecutionContext,
  shared_health: dict,
  graph_health: dict,
) -> dict:
  """Aggregate health check results."""
  all_healthy = (
    shared_health["status"] == "healthy" and graph_health["status"] == "healthy"
  )

  overall_status = "healthy" if all_healthy else "warning"

  context.log.info(f"Overall credit system health: {overall_status}")

  return {
    "overall_status": overall_status,
    "shared_credits": shared_health,
    "graph_credits": graph_health,
    "timestamp": datetime.now(timezone.utc).isoformat(),
  }


@job
def weekly_health_check_job():
  """Weekly health check job for credit systems."""
  shared = check_shared_credit_allocation_health()
  graph = check_graph_credit_health()
  aggregate_health_results(shared, graph)


# ============================================================================
# Schedules
# ============================================================================

hourly_auth_cleanup_schedule = ScheduleDefinition(
  job=hourly_auth_cleanup_job,
  cron_schedule="0 * * * *",  # Every hour at :00
  default_status=DefaultScheduleStatus.RUNNING,
)

weekly_health_check_schedule = ScheduleDefinition(
  job=weekly_health_check_job,
  cron_schedule="0 3 * * 1",  # Mondays at 3 AM UTC
  default_status=DefaultScheduleStatus.RUNNING,
)
