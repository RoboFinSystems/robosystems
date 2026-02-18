"""Dagster backup cleanup job.

Daily job to enforce backup retention policies:
- Op 1: Clean up tracked backups (GraphBackup records past their expires_at)
- Op 2: Clean up instance-level daemon backups older than 90 days

S3 lifecycle rules (cloudformation/s3.yaml) provide a 90-day safety net.
This job handles tier-specific shorter retention via the GraphBackup model.
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
from robosystems.dagster.resources import DatabaseResource, S3Resource

# Max retention across all tiers (XLarge = 90 days)
MAX_RETENTION_DAYS = 90

# Schedule status: RUNNING in prod/staging, STOPPED in dev
_SCHEDULE_STATUS = (
  DefaultScheduleStatus.RUNNING
  if env.ENVIRONMENT != "dev"
  else DefaultScheduleStatus.STOPPED
)


@op
def cleanup_tracked_backups(
  context: OpExecutionContext,
  db: DatabaseResource,
  s3: S3Resource,
) -> dict[str, Any]:
  """Delete S3 objects and mark GraphBackup records as EXPIRED.

  Covers both customer API backups and SEC pipeline backups,
  since both create GraphBackup records with expires_at.
  """
  from robosystems.models.iam import GraphBackup

  expired_count = 0
  error_count = 0

  with db.get_session() as session:
    expired_backups = GraphBackup.get_expired_backups(session)
    context.log.info(f"Found {len(expired_backups)} expired tracked backups")

    for backup in expired_backups:
      try:
        # Mark record as expired FIRST — if S3 deletion fails afterward,
        # the record is already expired and the S3 lifecycle rule (90 days)
        # will clean up the orphaned object as a safety net.
        backup.expire_backup(session)

        # Delete S3 backup object
        try:
          s3.client.delete_object(Bucket=backup.s3_bucket, Key=backup.s3_key)
        except Exception as e:
          context.log.warning(f"Failed to delete S3 object {backup.s3_key}: {e}")

        # Delete S3 metadata object if present
        if backup.s3_metadata_key:
          try:
            s3.client.delete_object(Bucket=backup.s3_bucket, Key=backup.s3_metadata_key)
          except Exception as e:
            context.log.warning(
              f"Failed to delete metadata {backup.s3_metadata_key}: {e}"
            )

        expired_count += 1

      except Exception as e:
        context.log.error(
          f"Failed to clean up backup {backup.id} for graph {backup.graph_id}: {e}"
        )
        error_count += 1

  context.log.info(
    f"Tracked backup cleanup: {expired_count} expired, {error_count} errors"
  )

  return {
    "expired_count": expired_count,
    "error_count": error_count,
    "timestamp": datetime.now(UTC).isoformat(),
  }


@op
def cleanup_instance_backups(
  context: OpExecutionContext,
  s3: S3Resource,
) -> dict[str, Any]:
  """Clean up old instance-level daemon backups from S3.

  These backups under graph-databases/{env}/ are created by the daemon
  on writer instances and aren't tracked in PostgreSQL.
  """
  from robosystems.config.storage.graph import get_instance_backup_prefix

  environment = env.ENVIRONMENT
  prefix = get_instance_backup_prefix(environment)
  cutoff = datetime.now(UTC) - timedelta(days=MAX_RETENTION_DAYS)

  context.log.info(
    f"Scanning instance backups under {prefix} older than {MAX_RETENTION_DAYS} days"
  )

  deleted_count = 0
  objects_to_delete: list[dict[str, str]] = []

  try:
    paginator = s3.client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=s3.bucket, Prefix=prefix)

    for page in pages:
      if "Contents" not in page:
        continue

      for obj in page["Contents"]:
        if obj["LastModified"].replace(tzinfo=UTC) < cutoff:
          objects_to_delete.append({"Key": obj["Key"]})

          # Batch delete in groups of 1000 (S3 API limit)
          if len(objects_to_delete) >= 1000:
            s3.client.delete_objects(
              Bucket=s3.bucket, Delete={"Objects": objects_to_delete}
            )
            deleted_count += len(objects_to_delete)
            objects_to_delete = []

    # Delete remaining
    if objects_to_delete:
      s3.client.delete_objects(Bucket=s3.bucket, Delete={"Objects": objects_to_delete})
      deleted_count += len(objects_to_delete)

  except Exception as e:
    context.log.error(f"Failed to clean up instance backups: {e}")

  context.log.info(f"Instance backup cleanup: {deleted_count} objects deleted")

  return {
    "deleted_count": deleted_count,
    "prefix": prefix,
    "cutoff_date": cutoff.isoformat(),
    "timestamp": datetime.now(UTC).isoformat(),
  }


@job(tags={"dagster/priority": "1", "dagster/max_retries": 3})
def daily_backup_cleanup_job():
  """Daily cleanup of expired backups across all storage layers."""
  cleanup_tracked_backups()
  cleanup_instance_backups()


daily_backup_cleanup_schedule = ScheduleDefinition(
  job=daily_backup_cleanup_job,
  cron_schedule="0 5 * * *",  # 5 AM UTC daily
  default_status=_SCHEDULE_STATUS,
)
