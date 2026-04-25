"""Dagster sensor for the period-boundary obligation promoter (Stream 2.B).

Walks every active entity graph; for any graph that has matured `pending`
`schedule_entry_due` events, fires a RunRequest against
``extensions_promote_obligations_job``. Same fan-out shape as
``stale_graph_materialization_sensor`` — one RunRequest per graph with
work to do, deduplicated by Dagster on a stable `run_key`.

The check itself is a single-row count per graph. With hundreds of
graphs in production, sub-second per tick. The sensor opens its own
extensions session per graph (to set the schema search_path) and closes
it cleanly even if the count fails.

Cursor
------

We carry an `in_progress` cursor that maps `graph_id → submitted_at_iso`
for runs that have been queued but haven't been confirmed
materialized yet. Entries expire after ``_CURSOR_EXPIRY_SECONDS`` so a
sensor failure doesn't permanently block a graph.

Cadence
-------

``minimum_interval_seconds=300`` (5 min) — this is a clean default for
month-end / period-boundary work. The granularity of the obligation
register is daily at most (events have day-end ``occurred_at``), so
sub-minute polling buys nothing. Cadence is per-instance configurable
via the standard Dagster sensor controls.

Default status is ``STOPPED`` — the operator turns this on per
deployment, mirroring the materialization sensor's posture.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from dagster import (
  DefaultSensorStatus,
  RunRequest,
  SensorEvaluationContext,
  sensor,
)
from dateutil import parser as date_parser

from robosystems.config import env
from robosystems.dagster.jobs.extensions import extensions_promote_obligations_job
from robosystems.database import session as db_session_factory
from robosystems.db.extensions import extensions_session
from robosystems.logger import get_logger
from robosystems.models.extensions.roboledger.event import Event

logger = get_logger(__name__)

# How long a graph stays in the "in-progress" cursor before being eligible
# for re-submission. Prevents permanent blocking if the upstream job
# failed without touching the cursor.
_CURSOR_EXPIRY_SECONDS = 1800  # 30 min — promotion runs are short


def _has_matured_pending_obligations(graph_id: str, as_of: datetime) -> bool:
  """Tenant-scoped count check. Returns True iff at least one pending
  schedule_entry_due event has matured by `as_of`.
  """
  try:
    with extensions_session(graph_id) as session:
      count = (
        session.query(Event)
        .filter(
          Event.event_type == "schedule_entry_due",
          Event.status == "pending",
          Event.occurred_at <= as_of,
        )
        .count()
      )
      return count > 0
  except Exception as exc:
    # Schema may not exist yet for newly-provisioned graphs, or the
    # extensions DB may be down. Skip the graph and let the next tick
    # retry; logging is sufficient — this is not a sensor-level error.
    logger.debug(f"obligation_promoter: skipping {graph_id}: {exc}")
    return False


@sensor(
  job=extensions_promote_obligations_job,
  minimum_interval_seconds=300,
  default_status=DefaultSensorStatus.STOPPED,
  description="Promotes matured pending schedule obligations on each entity graph",
)
def scheduled_obligation_promotion_sensor(context: SensorEvaluationContext):
  """Fan out one RunRequest per entity graph that has matured pending obligations."""
  from robosystems.models.core.graph import Graph

  db = db_session_factory()
  try:
    now = datetime.now(UTC)

    in_progress: dict[str, str] = {}
    if context.cursor:
      try:
        in_progress = json.loads(context.cursor)
      except (json.JSONDecodeError, TypeError):
        in_progress = {}

    active_in_progress: dict[str, str] = {}
    for gid, submitted_at_str in in_progress.items():
      try:
        submitted_at = date_parser.isoparse(submitted_at_str)
        if (now - submitted_at).total_seconds() < _CURSOR_EXPIRY_SECONDS:
          active_in_progress[gid] = submitted_at_str
      except Exception as exc:
        logger.debug(f"Dropping malformed cursor entry for {gid}: {exc}")

    candidate_graphs = (
      db.query(Graph)
      .filter(
        Graph.graph_type == "entity",
        Graph.status == "active",
        Graph.is_repository.is_(False),
      )
      .all()
    )

    # Per-graph autopilot flag added in Stream 2.E. The env var stays
    # as the fallback default for legacy rows where the column is NULL,
    # so a deployment-wide override still works during the rollout
    # window.
    env_default_auto_dispatch = bool(env.EXTENSIONS_PROMOTION_AUTO_DISPATCH)
    as_of_iso = now.isoformat()

    run_requests = []
    new_cursor = dict(active_in_progress)

    for graph in candidate_graphs:
      graph_id = str(graph.graph_id)
      if graph_id in active_in_progress:
        continue
      if not _has_matured_pending_obligations(graph_id, now):
        continue

      auto_dispatch = (
        bool(graph.auto_dispatch_obligations)
        if graph.auto_dispatch_obligations is not None
        else env_default_auto_dispatch
      )

      logger.info(
        f"Submitting obligation promotion for {graph_id} "
        f"(as_of={as_of_iso}, dispatch={auto_dispatch})"
      )
      # Use the as_of timestamp + graph in the run_key so two ticks
      # within the same minute don't dedup against each other on
      # graphs that produced new pending events between them.
      run_requests.append(
        RunRequest(
          run_key=f"promote_obligations_{graph_id}_{as_of_iso}",
          run_config={
            "ops": {
              "promote_obligations_for_graph": {
                "config": {
                  "graph_id": graph_id,
                  "as_of_iso": as_of_iso,
                  "dispatch_handlers": auto_dispatch,
                }
              }
            }
          },
          tags={"graph_id": graph_id, "trigger": "obligation_promoter"},
        )
      )
      new_cursor[graph_id] = now.isoformat()

    # Drop cursor entries for graphs we didn't see this tick (they no
    # longer have pending obligations or are no longer active).
    seen = {str(g.graph_id) for g in candidate_graphs}
    new_cursor = {gid: ts for gid, ts in new_cursor.items() if gid in seen}

    context.update_cursor(json.dumps(new_cursor) if new_cursor else "")

    if run_requests:
      logger.info(f"obligation_promoter: submitting {len(run_requests)} promotion(s)")

    return run_requests

  except Exception as exc:
    logger.error(f"obligation_promoter sensor failed: {exc}")
    # Re-establish a clean cursor by leaving it untouched so we re-try
    # next tick from the last known good state.
    return []

  finally:
    # `db` (platform DB) is closed here. Per-graph extensions sessions
    # are managed by the helper above's context manager.
    db.close()
