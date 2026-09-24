"""Daily backup retention (tracked and orphaned objects) plus a coverage check.

The S3 lifecycle rule is a 90-day backstop; this job enforces the shorter
per-tier retention.
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
  """Delete expired backups' S3 objects, then mark their rows EXPIRED.

  Order matters: the un-expired row is the retry (the orphan sweep skips any
  key a row references), and deleting an absent key succeeds, so a failed pass
  settles tomorrow.
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
def cleanup_orphaned_backups(
  context: OpExecutionContext,
  db: DatabaseResource,
  s3: S3Resource,
) -> dict[str, Any]:
  """Delete application backup objects that no ``GraphBackup`` row references.

  Retention is the owning graph's tier, parsed from the key. Every ambiguity
  (unknown graph, unparseable key) resolves toward keeping the object.
  """
  from robosystems.config.graph_tier import GraphTierConfig
  from robosystems.config.storage.graph import (
    get_backup_metadata_prefix,
    get_backup_prefix,
  )
  from robosystems.models.core import Graph, GraphBackup

  payload_prefix = get_backup_prefix()
  metadata_prefix = get_backup_metadata_prefix()
  now = datetime.now(UTC)

  deleted_count = 0
  skipped_tracked = 0
  objects_to_delete: list[dict[str, str]] = []

  try:
    with db.get_session() as session:
      # Every key any row references, whatever its status; those belong to
      # cleanup_tracked_backups.
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
      # Payloads and metadata sidecars; both prefixes start with {graph_id}.
      for prefix in (payload_prefix, metadata_prefix):
        for page in paginator.paginate(Bucket=s3.bucket, Prefix=prefix):
          for obj in page.get("Contents", []):
            key = obj["Key"]

            if key in tracked_keys:
              skipped_tracked += 1
              continue

            # graph-backups/databases/{graph_id}/{backup_type}/backup-{ts}{ext}
            # graph-backups/metadata/{graph_id}/backup-{ts}.json
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
    "prefixes": [payload_prefix, metadata_prefix],
    "timestamp": now.isoformat(),
  }


def find_uncovered_graphs(session, staleness_cutoff: datetime) -> tuple[int, list[str]]:
  """Graphs that should have a recent scheduled backup and don't.

  Returns ``(checked, uncovered)``. Scope must mirror the nightly schedule's.
  """
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
  from robosystems.models.core import BackupInitiator, BackupStatus, Graph, GraphBackup

  graphs = (
    session.query(Graph.graph_id)
    .filter(
      Graph.graph_type.in_(("entity", "generic")),
      Graph.status == "active",
    )
    .all()
  )

  uncovered: list[str] = []
  checked = 0

  for (graph_id,) in graphs:
    if is_shared_repository_or_subgraph(graph_id):
      continue

    checked += 1
    recent = (
      session.query(GraphBackup.id)
      .filter(
        GraphBackup.graph_id == graph_id,
        GraphBackup.initiated_by == BackupInitiator.SCHEDULED.value,
        GraphBackup.status == BackupStatus.COMPLETED.value,
        GraphBackup.created_at >= staleness_cutoff,
      )
      .first()
    )
    if recent is None:
      uncovered.append(graph_id)

  return checked, uncovered


@op
def assert_backup_coverage(
  context: OpExecutionContext,
  db: DatabaseResource,
) -> dict[str, Any]:
  """Fail loudly when a graph that should have a recent backup doesn't.

  Checks the outcome, not the mechanism: a schedule that stops evaluating
  produces no failed run to notice. Logs CRITICAL (for a metric filter) rather
  than raising, so a coverage gap doesn't read as a retention failure.
  """
  # Two nightly cycles: one missed night is transient.
  staleness_cutoff = datetime.now(UTC) - timedelta(days=2)

  try:
    with db.get_session() as session:
      checked, uncovered = find_uncovered_graphs(session, staleness_cutoff)
  except Exception as e:
    context.log.error(f"Backup coverage check failed to run: {e}")
    return {"checked": 0, "uncovered": [], "error": str(e)}

  if uncovered:
    context.log.critical(
      f"BACKUP COVERAGE GAP: {len(uncovered)} of {checked} graphs have no "
      f"completed scheduled backup in the last 2 days: {', '.join(sorted(uncovered))}"
    )
  else:
    context.log.info(f"Backup coverage OK: {checked} graphs covered")

  return {
    "checked": checked,
    "uncovered": sorted(uncovered),
    "cutoff": staleness_cutoff.isoformat(),
  }


@job(tags={"dagster/priority": "1", "dagster/max_retries": 3})
def daily_backup_cleanup_job():
  """Daily cleanup of expired backups, plus a check that creation kept up."""
  cleanup_tracked_backups()
  cleanup_orphaned_backups()
  assert_backup_coverage()


daily_backup_cleanup_schedule = ScheduleDefinition(
  job=daily_backup_cleanup_job,
  cron_schedule="0 5 * * *",  # 5 AM UTC daily
  default_status=_SCHEDULE_STATUS,
)
