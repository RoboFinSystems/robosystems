"""Dagster sensor for automatic graph rematerialization.

Polls for graphs marked as stale and submits materialization jobs.
Batches writes within a window to avoid excessive rebuilds
(e.g., 5 OLTP writes in 10 seconds don't trigger 5 materializations).
"""

import json

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

# How long a graph stays in the "in-progress" cursor before being eligible
# for re-submission (seconds). Prevents permanent blocking if a Dagster job
# fails without calling mark_fresh().
_CURSOR_EXPIRY_SECONDS = 7200  # 2 hours


@sensor(
  job=extensions_materialize_job,
  minimum_interval_seconds=60,
  default_status=DefaultSensorStatus.RUNNING,
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

  Skips graphs that are already materializing (checked via cursor with expiry).
  """
  from datetime import UTC, datetime, timedelta

  from robosystems.models.core.graph import Graph

  db = db_session_factory()
  try:
    now = datetime.now(UTC)
    cutoff = now - timedelta(seconds=_MIN_STALE_AGE_SECONDS)

    # Cursor stores {graph_id: submitted_at_iso} for in-progress materializations.
    # Entries expire after _CURSOR_EXPIRY_SECONDS to prevent permanent blocking.
    in_progress: dict[str, str] = {}
    if context.cursor:
      try:
        in_progress = json.loads(context.cursor)
      except (json.JSONDecodeError, TypeError):
        in_progress = {}

    # Expire stale cursor entries
    from dateutil import parser as date_parser

    active_in_progress: dict[str, str] = {}
    for gid, submitted_at_str in in_progress.items():
      try:
        submitted_at = date_parser.isoparse(submitted_at_str)
        if (now - submitted_at).total_seconds() < _CURSOR_EXPIRY_SECONDS:
          active_in_progress[gid] = submitted_at_str
        else:
          logger.info(f"Cursor entry expired for {gid} (submitted {submitted_at_str})")
      except Exception as exc:
        logger.debug(f"Dropping malformed cursor entry for {gid}: {exc}")

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
    new_cursor = dict(active_in_progress)

    for graph in stale_graphs:
      graph_id = str(graph.graph_id)
      if graph_id in active_in_progress:
        logger.debug(f"Skipping {graph_id}: materialization already in progress")
        continue

      # Use graph_stale_at as run_key so Dagster deduplicates on the same
      # staleness event (not on sensor tick time).
      stale_at_str = (
        graph.graph_stale_at.isoformat() if graph.graph_stale_at else "unknown"
      )

      logger.info(
        f"Submitting materialization for stale graph {graph_id} "
        f"(stale since {stale_at_str}, reason: {graph.graph_stale_reason})"
      )

      run_requests.append(
        RunRequest(
          run_key=f"stale_materialize_{graph_id}_{stale_at_str}",
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
      new_cursor[graph_id] = now.isoformat()

    # Remove cursor entries for graphs that are no longer stale
    still_stale = {str(g.graph_id) for g in stale_graphs}
    new_cursor = {gid: ts for gid, ts in new_cursor.items() if gid in still_stale}

    context.update_cursor(json.dumps(new_cursor) if new_cursor else "")

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
