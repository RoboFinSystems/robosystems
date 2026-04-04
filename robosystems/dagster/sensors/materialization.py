"""Dagster sensor for automatic graph rematerialization.

Polls for graphs marked as stale and submits materialization jobs.
Batches writes within a window to avoid excessive rebuilds
(e.g., 5 OLTP writes in 10 seconds don't trigger 5 materializations).
"""

from dagster import (
  DefaultSensorStatus,
  RunRequest,
  SensorEvaluationContext,
  sensor,
)

from robosystems.dagster.jobs.extensions import extensions_materialize_job
from robosystems.database import session as db_session_factory
from robosystems.logger import get_logger

logger = get_logger(__name__)

# Minimum staleness age before triggering (seconds).
# Prevents materializing immediately after every single OLTP write.
_MIN_STALE_AGE_SECONDS = 30


@sensor(
  job=extensions_materialize_job,
  minimum_interval_seconds=60,
  default_status=DefaultSensorStatus.STOPPED,
  description="Polls for stale graphs and submits materialization jobs",
)
def stale_graph_materialization_sensor(context: SensorEvaluationContext):
  """Find stale entity graphs and submit materialization jobs.

  Query: graphs where:
  - graph_stale = true
  - graph_stale_at older than _MIN_STALE_AGE_SECONDS (batch window)
  - graph_type = 'entity' (only entity graphs have extensions OLTP)
  - status = 'active'
  - is_repository = false (shared repos use their own pipeline)

  Skips graphs that are already materializing (checked via cursor).
  """
  from datetime import UTC, datetime, timedelta

  from robosystems.models.core.graph import Graph

  db = db_session_factory()
  try:
    now = datetime.now(UTC)
    cutoff = now - timedelta(seconds=_MIN_STALE_AGE_SECONDS)

    # Track which graphs we've already submitted in this tick
    # (cursor stores graph_ids of in-progress materializations)
    in_progress: set[str] = set()
    if context.cursor:
      in_progress = set(context.cursor.split(","))

    stale_graphs = (
      db.query(Graph)
      .filter(
        Graph.graph_stale.is_(True),
        Graph.graph_stale_at.isnot(None),
        Graph.graph_stale_at < cutoff,  # type: ignore[operator]
        Graph.graph_type == "entity",
        Graph.status == "active",
        Graph.is_repository.is_(False),
      )
      .all()
    )

    run_requests = []
    submitted_ids = set(in_progress)

    for graph in stale_graphs:
      graph_id = str(graph.graph_id)
      if graph_id in in_progress:
        logger.debug(f"Skipping {graph_id}: materialization already in progress")
        continue

      logger.info(
        f"Submitting materialization for stale graph {graph_id} "
        f"(stale since {graph.graph_stale_at}, reason: {graph.graph_stale_reason})"
      )

      run_requests.append(
        RunRequest(
          run_key=f"stale_materialize_{graph_id}_{now.isoformat()}",
          run_config={
            "ops": {
              "materialize_extensions_to_graph": {
                "config": {
                  "graph_id": graph_id,
                  "rebuild": True,
                }
              }
            }
          },
          tags={"graph_id": graph_id, "trigger": "stale_sensor"},
        )
      )
      submitted_ids.add(graph_id)

    # Update cursor with currently-in-progress graph IDs
    # Remove graph IDs that are no longer stale (materialization completed)
    still_stale = {str(g.graph_id) for g in stale_graphs}
    active_ids = submitted_ids & still_stale
    context.update_cursor(",".join(sorted(active_ids)) if active_ids else "")

    if run_requests:
      logger.info(
        f"Stale graph sensor: submitting {len(run_requests)} materialization(s)"
      )

    return run_requests

  except Exception as e:
    logger.error(f"Stale graph sensor failed: {e}")
    return []
  finally:
    db.close()
