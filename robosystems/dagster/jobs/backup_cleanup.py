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
  """Delete S3 objects, then mark the GraphBackup records EXPIRED.

  Covers both customer API backups and SEC pipeline backups,
  since both create GraphBackup records with expires_at.

  Order matters, and it used to be the other way around. Marking the row
  first drops it out of ``get_expired_backups`` forever, and the orphan sweep
  skips every key a row still references — so a single transient S3 error
  meant nothing ever retried the delete and the object rode the 90-day
  lifecycle rule regardless of the graph's tier. A standard-tier backup
  outlived its 7-day retention by twelve weeks on one failed API call.

  Leaving the row un-expired is the retry: tomorrow's run picks it up again.
  Deleting an absent key is a success in S3, so a row whose object is already
  gone still settles on the next pass.
  """
  from robosystems.models.core import GraphBackup

  expired_count = 0
  deferred_count = 0

  with db.get_session() as session:
    expired_backups = GraphBackup.get_expired_backups(session)
    context.log.info(f"Found {len(expired_backups)} expired tracked backups")

    for backup in expired_backups:
      try:
        s3.client.delete_object(Bucket=backup.s3_bucket, Key=backup.s3_key)

        if backup.s3_metadata_key:
          s3.client.delete_object(Bucket=backup.s3_bucket, Key=backup.s3_metadata_key)

        backup.expire_backup(session)
        expired_count += 1

      except Exception as e:
        # Deliberately left un-expired so the next daily run retries it.
        context.log.warning(
          f"Deferred cleanup of backup {backup.id} for graph {backup.graph_id} "
          f"to the next run: {e}"
        )
        deferred_count += 1

  context.log.info(
    f"Tracked backup cleanup: {expired_count} expired, {deferred_count} deferred"
  )

  return {
    "expired_count": expired_count,
    "deferred_count": deferred_count,
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


@op
def cleanup_orphaned_backups(
  context: OpExecutionContext,
  db: DatabaseResource,
  s3: S3Resource,
) -> dict[str, Any]:
  """Delete application backup objects that no ``GraphBackup`` row references.

  ``cleanup_tracked_backups`` can only act on rows, so an object whose record
  was never written — or was removed — is invisible to it, and survives until
  the 90-day S3 lifecycle rule regardless of the graph's tier. A standard-tier
  orphan therefore outlives its 7-day retention by more than twelve weeks.
  Observed in prod: a 2026-06-06 object under ``kg19dcbe757481af06fc9b`` with
  no row at all.

  Retention comes from the owning graph's tier, parsed out of the key, so an
  orphan expires on the same clock its tracked siblings would have. Unknown or
  unparseable graphs fall back to the 90-day maximum rather than deleting
  early — this op removes data, so every ambiguity resolves toward keeping it.
  """
  from robosystems.config.graph_tier import GraphTierConfig
  from robosystems.config.storage.graph import get_backup_prefix
  from robosystems.models.core import Graph, GraphBackup

  prefix = get_backup_prefix()
  now = datetime.now(UTC)

  deleted_count = 0
  skipped_tracked = 0
  objects_to_delete: list[dict[str, str]] = []

  try:
    with db.get_session() as session:
      # Every key any row still points at, whatever its status — deleting one
      # here would race op 1. Op 1 no longer strands anything behind this
      # guard: a row is marked EXPIRED only once its objects are actually
      # gone, so a failed delete stays visible to tomorrow's tracked sweep
      # instead of becoming the lifecycle rule's problem.
      tracked_keys: set[str] = {
        key
        for (key,) in session.query(GraphBackup.s3_key).filter(
          GraphBackup.s3_key.isnot(None)
        )
      }
      tracked_keys |= {
        key
        for (key,) in session.query(GraphBackup.s3_metadata_key).filter(
          GraphBackup.s3_metadata_key.isnot(None)
        )
      }

      retention_by_graph: dict[str, int] = {}

      def _retention_days(graph_id: str) -> int:
        """Tier retention for a graph, defaulting to the 90-day maximum."""
        if graph_id not in retention_by_graph:
          graph = Graph.get_by_id(graph_id, session)
          tier = str(graph.graph_tier) if graph and graph.graph_tier else None
          retention_by_graph[graph_id] = (
            GraphTierConfig.get_backup_limits(tier).get(
              "backup_retention_days", MAX_RETENTION_DAYS
            )
            if tier
            else MAX_RETENTION_DAYS
          )
        return retention_by_graph[graph_id]

      paginator = s3.client.get_paginator("list_objects_v2")
      for page in paginator.paginate(Bucket=s3.bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
          key = obj["Key"]

          if key in tracked_keys:
            skipped_tracked += 1
            continue

          # graph-backups/databases/{graph_id}/{backup_type}/backup-{ts}{ext}
          parts = key[len(prefix) :].split("/")
          if len(parts) < 2 or not parts[0]:
            context.log.warning(f"Unparseable backup key, leaving in place: {key}")
            continue

          cutoff = now - timedelta(days=_retention_days(parts[0]))
          if obj["LastModified"].replace(tzinfo=UTC) >= cutoff:
            continue

          objects_to_delete.append({"Key": key})
          if len(objects_to_delete) >= 1000:
            s3.client.delete_objects(
              Bucket=s3.bucket, Delete={"Objects": objects_to_delete}
            )
            deleted_count += len(objects_to_delete)
            objects_to_delete = []

    if objects_to_delete:
      s3.client.delete_objects(Bucket=s3.bucket, Delete={"Objects": objects_to_delete})
      deleted_count += len(objects_to_delete)

  except Exception as e:
    context.log.error(f"Failed to clean up orphaned backups: {e}")

  context.log.info(
    f"Orphaned backup cleanup: {deleted_count} deleted, {skipped_tracked} tracked"
  )

  return {
    "deleted_count": deleted_count,
    "skipped_tracked": skipped_tracked,
    "prefix": prefix,
    "timestamp": now.isoformat(),
  }


@job(tags={"dagster/priority": "1", "dagster/max_retries": 3})
def daily_backup_cleanup_job():
  """Daily cleanup of expired backups across all storage layers."""
  cleanup_tracked_backups()
  cleanup_instance_backups()
  cleanup_orphaned_backups()


daily_backup_cleanup_schedule = ScheduleDefinition(
  job=daily_backup_cleanup_job,
  cron_schedule="0 5 * * *",  # 5 AM UTC daily
  default_status=_SCHEDULE_STATUS,
)
