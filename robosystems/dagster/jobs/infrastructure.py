"""Dagster infrastructure jobs.

These jobs handle system maintenance:
- Auth cleanup (expired API keys, tokens)
- Health checks (credit allocation, graph credits)
- Graph instance monitoring
"""

from datetime import datetime, timezone
from typing import Any

from dagster import (
  DefaultScheduleStatus,
  OpExecutionContext,
  ScheduleDefinition,
  job,
  op,
)

from robosystems.config import env
from robosystems.dagster.resources import DatabaseResource
from robosystems.models.iam import UserAPIKey


# ============================================================================
# Environment-based Schedule Status
# ============================================================================

# Instance infrastructure schedules require real AWS resources (DynamoDB, EC2, CloudWatch)
# Only enable automatically in production/staging environments
INSTANCE_SCHEDULE_STATUS = (
  DefaultScheduleStatus.RUNNING
  if env.ENVIRONMENT in ("prod", "staging")
  else DefaultScheduleStatus.STOPPED
)


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


# ============================================================================
# Instance Monitoring Jobs
# Replaces: bin/lambda/graph_instance_monitor.py
# ============================================================================


@op
def check_instance_health(context: OpExecutionContext) -> dict[str, Any]:
  """Check health of Graph EC2 instances and update registry.

  This operation:
  1. Queries all instances from DynamoDB registry
  2. Checks actual EC2 instance states
  3. Updates instance health status in registry
  4. Removes instances that have been terminated
  """
  from robosystems.operations.graph.infrastructure import InstanceMonitor

  monitor = InstanceMonitor()
  result = monitor.check_instance_health()

  context.log.info(
    f"Instance health check: {result.healthy} healthy, "
    f"{result.unhealthy} unhealthy, {result.removed} removed"
  )

  return {
    "timestamp": result.timestamp,
    "total_instances": result.total_instances,
    "healthy": result.healthy,
    "unhealthy": result.unhealthy,
    "terminated": result.terminated,
    "removed": result.removed,
    "errors": result.errors,
  }


@job
def instance_health_check_job():
  """Hourly health check for Graph EC2 instances."""
  check_instance_health()


@op
def collect_instance_metrics(context: OpExecutionContext) -> dict[str, Any]:
  """Collect and publish cluster metrics to CloudWatch.

  This operation:
  1. Queries instance and graph registries
  2. Calculates capacity, utilization, and health metrics
  3. Publishes metrics to CloudWatch for monitoring and auto-scaling
  """
  from robosystems.operations.graph.infrastructure import InstanceMonitor

  monitor = InstanceMonitor()
  result = monitor.collect_metrics()

  context.log.info(f"Published {result.metrics_published} metrics to CloudWatch")

  return {
    "timestamp": result.timestamp,
    "metrics_published": result.metrics_published,
    "errors": result.errors,
  }


@job
def instance_metrics_collection_job():
  """Collect cluster metrics every 5 minutes for auto-scaling."""
  collect_instance_metrics()


@op
def cleanup_stale_registry_entries(context: OpExecutionContext) -> dict[str, Any]:
  """Clean up stale entries from instance registry.

  Removes:
  - Entries marked as deleted older than 7 days
  - Entries with missing instance_id references
  """
  from robosystems.operations.graph.infrastructure import InstanceMonitor

  monitor = InstanceMonitor()
  result = monitor.cleanup_stale_graphs()

  context.log.info(f"Instance registry cleanup: {result.removed_count} entries removed")

  return {
    "timestamp": result.timestamp,
    "removed_count": result.removed_count,
    "errors": result.errors,
  }


@job
def instance_registry_cleanup_job():
  """Daily cleanup of stale instance registry entries."""
  cleanup_stale_registry_entries()


@op
def cleanup_stale_volume_entries(context: OpExecutionContext) -> dict[str, Any]:
  """Clean up stale entries from volume registry.

  Removes or updates:
  - Volumes stuck in 'attaching' state to non-existent instances
  - Volumes with missing instance references
  - Old unattached volumes (older than 30 days)
  """
  from robosystems.operations.graph.infrastructure import InstanceMonitor

  monitor = InstanceMonitor()
  result = monitor.cleanup_stale_volumes()

  context.log.info(
    f"Volume registry cleanup: {result.updated_count} updated, "
    f"{result.removed_count} removed"
  )

  return {
    "timestamp": result.timestamp,
    "updated_count": result.updated_count,
    "removed_count": result.removed_count,
    "errors": result.errors,
  }


@job
def volume_registry_cleanup_job():
  """Daily cleanup of stale volume registry entries."""
  cleanup_stale_volume_entries()


@op
def run_full_instance_maintenance(context: OpExecutionContext) -> dict[str, Any]:
  """Run all instance maintenance tasks in sequence.

  This combines:
  - Instance health check
  - Metrics collection
  - Instance registry cleanup
  - Volume registry cleanup
  """
  from robosystems.operations.graph.infrastructure import InstanceMonitor

  monitor = InstanceMonitor()

  results = {
    "health_check": {},
    "metrics": {},
    "instance_cleanup": {},
    "volume_cleanup": {},
  }

  # Health check
  health_result = monitor.check_instance_health()
  results["health_check"] = {
    "healthy": health_result.healthy,
    "unhealthy": health_result.unhealthy,
    "removed": health_result.removed,
  }
  context.log.info(f"Health check: {health_result.healthy} healthy instances")

  # Metrics collection
  metrics_result = monitor.collect_metrics()
  results["metrics"] = {
    "metrics_published": metrics_result.metrics_published,
  }
  context.log.info(f"Metrics: {metrics_result.metrics_published} published")

  # Instance registry cleanup
  instance_cleanup_result = monitor.cleanup_stale_graphs()
  results["instance_cleanup"] = {
    "removed_count": instance_cleanup_result.removed_count,
  }
  context.log.info(f"Instance cleanup: {instance_cleanup_result.removed_count} removed")

  # Volume cleanup
  volume_cleanup_result = monitor.cleanup_stale_volumes()
  results["volume_cleanup"] = {
    "updated_count": volume_cleanup_result.updated_count,
    "removed_count": volume_cleanup_result.removed_count,
  }
  context.log.info(
    f"Volume cleanup: {volume_cleanup_result.updated_count} updated, "
    f"{volume_cleanup_result.removed_count} removed"
  )

  return {
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "results": results,
  }


@job
def full_instance_maintenance_job():
  """Run all instance maintenance tasks (weekly)."""
  run_full_instance_maintenance()


# ============================================================================
# Instance Infrastructure Schedules
# ============================================================================

# Instance health check - every hour (matches Lambda: rate(1 hour))
# Auto-enabled in prod/staging only (requires AWS: DynamoDB, EC2)
instance_health_check_schedule = ScheduleDefinition(
  job=instance_health_check_job,
  cron_schedule="0 * * * *",  # Every hour at :00
  default_status=INSTANCE_SCHEDULE_STATUS,
)

# Metrics collection - every 5 minutes (matches Lambda: rate(5 minutes))
# Auto-enabled in prod/staging only (requires AWS: DynamoDB, EC2, CloudWatch)
# Critical for autoscaling
instance_metrics_collection_schedule = ScheduleDefinition(
  job=instance_metrics_collection_job,
  cron_schedule="*/5 * * * *",  # Every 5 minutes
  default_status=INSTANCE_SCHEDULE_STATUS,
)

# Instance registry cleanup - daily at 3 AM UTC (matches Lambda: cron(0 3 * * ? *))
# Auto-enabled in prod/staging only (requires AWS: DynamoDB)
instance_registry_cleanup_schedule = ScheduleDefinition(
  job=instance_registry_cleanup_job,
  cron_schedule="0 3 * * *",  # 3 AM UTC daily
  default_status=INSTANCE_SCHEDULE_STATUS,
)

# Volume registry cleanup - daily at 4 AM UTC (matches Lambda: cron(0 4 * * ? *))
# Auto-enabled in prod/staging only (requires AWS: DynamoDB)
volume_registry_cleanup_schedule = ScheduleDefinition(
  job=volume_registry_cleanup_job,
  cron_schedule="0 4 * * *",  # 4 AM UTC daily
  default_status=INSTANCE_SCHEDULE_STATUS,
)

# Full maintenance - weekly on Sundays at 2 AM UTC
# Auto-enabled in prod/staging only (requires AWS: DynamoDB, EC2, CloudWatch)
full_instance_maintenance_schedule = ScheduleDefinition(
  job=full_instance_maintenance_job,
  cron_schedule="0 2 * * 0",  # Sundays at 2 AM UTC
  default_status=INSTANCE_SCHEDULE_STATUS,
)
