"""Dagster storage reclaim job.

Daily sweep deleting the instance storage the breakdown labels reclaimable:
`transient` blue-green build artifacts stranded by crashed materializations,
and `orphan` estates left behind by deleted subgraphs. The same items are
itemized on ``/usage``; this job is what collects them.

Safety properties, in order of importance:

- The Graph API holds the base's materialization lock across a `-wip`/`-prev`
  delete, so an in-flight build cannot lose the copy it is writing into.
- Orphan labelling requires a successful registry read; if the registry is
  unreachable, items stay unlabelled and nothing is deleted (fail-safe).
- Deleting a stranded WIP costs nothing: builds begin by deleting any
  leftover WIP, so there are no resume semantics to preserve.
- ``STORAGE_RECLAIM_ENABLED`` (SSM, read per run) is the runtime kill switch.
"""

import asyncio
from datetime import UTC, datetime
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

_RECLAIM_ENABLED_FLAG = "STORAGE_RECLAIM_ENABLED"

# Schedule status: RUNNING in prod/staging, STOPPED in dev
_SCHEDULE_STATUS = (
  DefaultScheduleStatus.RUNNING
  if env.ENVIRONMENT != "dev"
  else DefaultScheduleStatus.STOPPED
)


@op
def reclaim_instance_storage_sweep(
  context: OpExecutionContext,
  db: DatabaseResource,
) -> dict[str, Any]:
  """Reclaim transient/orphan storage across all active parent user graphs."""
  from robosystems.config.parameter_store import get_parameter_value
  from robosystems.models.core.graph import Graph
  from robosystems.operations.graph.storage_reclaim import reclaim_instance_storage

  # Read per run rather than at import, so flipping the flag takes effect on
  # the next run instead of the next deploy.
  if get_parameter_value(_RECLAIM_ENABLED_FLAG, "true").lower() != "true":
    context.log.info(f"Storage reclaim disabled via {_RECLAIM_ENABLED_FLAG}")
    return {"skipped": True, "reason": f"{_RECLAIM_ENABLED_FLAG} is false"}

  graphs_swept = 0
  error_count = 0
  total_bytes_freed = 0
  total_reclaimed = 0
  total_skipped_items = 0

  with db.get_session() as session:
    # Same selection as the usage monitor: active parent user graphs with a
    # tier. Subgraphs share their parent's instance, and the breakdown's
    # `{id}_*` scan covers them from the parent id.
    parent_graphs = (
      session.query(Graph)
      .filter(
        Graph.is_repository.is_(False),
        Graph.parent_graph_id.is_(None),
        Graph.status == "active",
        Graph.graph_tier.isnot(None),
      )
      .all()
    )

    context.log.info(
      f"Sweeping {len(parent_graphs)} parent graphs for reclaimable storage"
    )

    for graph in parent_graphs:
      graph_id = str(graph.graph_id)
      try:
        result = asyncio.run(reclaim_instance_storage(graph_id, session, dry_run=False))
      except Exception as e:
        context.log.warning(f"Reclaim sweep failed for {graph_id}: {e}")
        error_count += 1
        continue

      graphs_swept += 1
      reclaimed = result.get("reclaimed", [])
      skipped = result.get("skipped", [])
      total_reclaimed += len(reclaimed)
      total_skipped_items += len(skipped)
      total_bytes_freed += result.get("bytes_freed", 0)

      if reclaimed or skipped:
        context.log.info(
          f"{graph_id}: reclaimed {len(reclaimed)} "
          f"({result.get('bytes_freed', 0)} bytes), skipped {len(skipped)}"
        )

  context.log.info(
    f"Storage reclaim sweep: {graphs_swept} graphs, "
    f"{total_reclaimed} items reclaimed ({total_bytes_freed} bytes), "
    f"{total_skipped_items} skipped, {error_count} errors"
  )

  return {
    "graphs_swept": graphs_swept,
    "items_reclaimed": total_reclaimed,
    "bytes_freed": total_bytes_freed,
    "items_skipped": total_skipped_items,
    "error_count": error_count,
    "timestamp": datetime.now(UTC).isoformat(),
  }


@job(tags={"dagster/priority": "1", "dagster/max_retries": 3})
def daily_storage_reclaim_job():
  """Daily reclaim of transient build artifacts and orphaned subgraph estates."""
  reclaim_instance_storage_sweep()


daily_storage_reclaim_schedule = ScheduleDefinition(
  job=daily_storage_reclaim_job,
  cron_schedule="30 5 * * *",  # 5:30 AM UTC daily, after backup cleanup
  default_status=_SCHEDULE_STATUS,
)
