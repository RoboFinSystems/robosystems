"""Daily retention for operation_mutation_audit.

Its own job, not a step of the billing jobs: those schedules are gated on
billing, and dedicated deployments run with billing off but still keep an
audit trail.
"""

from datetime import UTC, datetime, timedelta
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

_SCHEDULE_STATUS = (
  DefaultScheduleStatus.RUNNING
  if env.ENVIRONMENT != "dev"
  else DefaultScheduleStatus.STOPPED
)


@op
def prune_operation_mutation_audit(
  context: OpExecutionContext, db: DatabaseResource
) -> dict[str, Any]:
  """Delete audit rows older than OPERATION_AUDIT_RETENTION_DAYS."""
  from robosystems.models.core import OperationMutationAudit

  cutoff = datetime.now(UTC) - timedelta(days=env.OPERATION_AUDIT_RETENTION_DAYS)
  with db.get_session() as session:
    deleted = (
      session.query(OperationMutationAudit)
      .filter(OperationMutationAudit.occurred_at < cutoff)
      .delete(synchronize_session=False)
    )

  context.log.info(
    f"Operation mutation audit retention: deleted {deleted} rows older than "
    f"{env.OPERATION_AUDIT_RETENTION_DAYS} days"
  )
  return {"deleted": deleted, "cutoff": cutoff.isoformat()}


@job(tags={"dagster/priority": "3"})
def daily_audit_retention_job():
  prune_operation_mutation_audit()


daily_audit_retention_schedule = ScheduleDefinition(
  job=daily_audit_retention_job,
  cron_schedule="30 5 * * *",  # 5:30 AM UTC, after backup cleanup
  default_status=_SCHEDULE_STATUS,
)
