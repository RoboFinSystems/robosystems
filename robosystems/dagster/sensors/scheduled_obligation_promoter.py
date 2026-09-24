"""Sensor that fans out one obligation-promotion run per entity graph with matured
pending ``schedule_entry_due`` events.

The in-progress cursor (``graph_id -> submitted_at``) stops resubmission on the
next tick; entries expire, and are dropped for graphs no longer active, so a
failed run never blocks a graph permanently. Obligations are day-granular, so a
5-minute cadence is plenty. Default STOPPED: operators enable it per deployment.
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

_CURSOR_EXPIRY_SECONDS = 1800  # promotion runs are short


def _has_matured_pending_obligations(graph_id: str, as_of: datetime) -> bool:
  """Whether any pending schedule_entry_due event has matured by `as_of`."""
  try:
    with extensions_session(graph_id, statement_timeout_ms=None) as session:
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
    # New graphs may lack a schema, or the DB may be down; retry next tick.
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

    # Default for graphs whose auto_dispatch_obligations is NULL.
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

    # Drop cursor entries for graphs no longer active entity graphs.
    seen = {str(g.graph_id) for g in candidate_graphs}
    new_cursor = {gid: ts for gid, ts in new_cursor.items() if gid in seen}

    context.update_cursor(json.dumps(new_cursor) if new_cursor else "")

    if run_requests:
      logger.info(f"obligation_promoter: submitting {len(run_requests)} promotion(s)")

    return run_requests

  except Exception as exc:
    logger.error(f"obligation_promoter sensor failed: {exc}")
    # Cursor untouched: retry next tick from the last good state.
    return []

  finally:
    db.close()
