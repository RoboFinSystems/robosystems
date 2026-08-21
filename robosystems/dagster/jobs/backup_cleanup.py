"""Dagster backup cleanup job.

Daily job to enforce backup retention and verify creation kept up:
- Op 1: Expire tracked backups (GraphBackup records past their expires_at)
- Op 2: Sweep orphaned objects no row references
- Op 3: Assert every graph that should have a backup has a recent one

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

  Order matters: delete the object first, mark the row EXPIRED second. An
  un-expired row is what drives the retry. Marking first drops the row out of
  ``get_expired_backups`` for good, and the orphan sweep skips every key a row
  still references, so one transient S3 error would leave the object to the
  90-day lifecycle rule regardless of the graph's tier.

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

  Retention comes from the owning graph's tier, parsed out of the key, so an
  orphan expires on the same clock its tracked siblings would have. Unknown or
  unparseable graphs fall back to the 90-day maximum rather than deleting
  early — this op removes data, so every ambiguity resolves toward keeping it.
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
      # Both halves of a backup, not just the payload. The metadata sidecar
      # lives under a sibling prefix and was reachable only through the tracked
      # sweep, whose delete is guarded on the row knowing the key — which
      # nothing set until now. Untracked sidecars therefore outlived tier
      # retention entirely and rode the 90-day bucket lifecycle, including for
      # 7-day tiers. Both prefixes key on {graph_id} as their first segment, so
      # the same parse and the same retention clock apply to each.
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

  Returns ``(checked, uncovered)``. Scope mirrors the nightly schedule exactly —
  customer entity and generic graphs, subgraphs included, shared repositories
  excluded — because a coverage check against a different population than the
  producer would either cry wolf or stay quiet about real gaps.
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

  This checks the *outcome*, not the mechanism, and that distinction is the
  reason it exists. Dagster records a failed run, so a backup job that errors is
  visible in the run list — but a schedule that stops evaluating produces no run
  at all, and an absent run is precisely what a list of runs cannot show you.
  The defect that produced this whole surface was of exactly that shape:
  retention ran daily for months against a population nothing was replenishing,
  and no job was failing the entire time.

  So the question asked here is "does every graph that should have a backup have
  one", which is answerable from the rows regardless of why it might not.

  Deliberately a CRITICAL log rather than a raised failure: this op runs after
  the three cleanup ops, and aborting the run would make a coverage gap look
  like a retention malfunction. The signal belongs in the logs where a metric
  filter can find it, not in this job's exit status.
  """
  # Two nightly cycles. One missed night is a transient; two is a pattern, and
  # waiting for the second keeps a single slow evening off the alert path.
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
