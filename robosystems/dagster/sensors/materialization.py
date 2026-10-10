"""Sensor that rematerializes stale entity graphs, batching bursts of OLTP writes."""

import json
from datetime import UTC, datetime, timedelta

from dagster import (
  DefaultSensorStatus,
  RunRequest,
  SensorEvaluationContext,
  SkipReason,
  sensor,
)
from dateutil import parser as date_parser
from sqlalchemy import or_

from robosystems.config.tuning import TuningConfig
from robosystems.dagster.jobs.extensions import extensions_materialize_job
from robosystems.database import session as db_session_factory
from robosystems.logger import get_logger

logger = get_logger(__name__)

# A run submitted for a staleness event that left the graph stale (it failed)
# is retried after this long, unless a newer write comes first.
_FAILED_RUN_RETRY_SECONDS = 7200  # 2 hours


def _now() -> datetime:
  return datetime.now(UTC)


def _stale_windows() -> tuple[int, int]:
  """(min stale age, max stale wait) in seconds, read from SSM tuning each tick.

  The batch window keeps a burst of writes to one rebuild; the max wait still
  refreshes a graph written more often than the window. Both are raised on a
  dedicated deployment whose full rebuild is too long for the managed cadence.
  """
  return (
    TuningConfig.get_materialization_min_stale_age(),
    TuningConfig.get_materialization_max_stale_wait(),
  )


def _graphs_being_written(
  context: SensorEvaluationContext, graph_ids: list[str]
) -> set[str]:
  """The graphs a materialization is writing or about to write.

  Either its per-graph lock is held (a manual run, a sensor run past its
  start) or a Dagster run tagged for it is queued or running (a sensor run
  not yet at the lock). If the lock service cannot answer, every graph counts
  as busy: a materialize would refuse to run unlocked anyway.
  """
  if not graph_ids:
    return set()

  from dagster import DagsterRunStatus, RunsFilter

  from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
  from robosystems.graph_api.core.ladybug.materialization_lock import lock_key_for

  busy: set[str] = set()
  try:
    client = create_redis_client(ValkeyDatabase.LOCKS)
    try:
      pipe = client.pipeline()
      for graph_id in graph_ids:
        pipe.exists(lock_key_for(graph_id))
      held = pipe.execute()
      busy.update(g for g, h in zip(graph_ids, held, strict=True) if h)
    finally:
      client.close()
  except Exception as e:
    logger.warning(f"Stale graph sensor could not read materialization locks: {e}")
    return set(graph_ids)

  records = context.instance.get_run_records(
    RunsFilter(
      statuses=[
        DagsterRunStatus.QUEUED,
        DagsterRunStatus.NOT_STARTED,
        DagsterRunStatus.STARTING,
        DagsterRunStatus.STARTED,
        DagsterRunStatus.CANCELING,
      ],
      tags={"materialize_db": graph_ids},
    )
  )
  busy.update(
    r.dagster_run.tags["materialize_db"]
    for r in records
    if r.dagster_run.tags.get("materialize_db") in graph_ids
  )
  return busy


@sensor(
  job=extensions_materialize_job,
  minimum_interval_seconds=60,
  default_status=DefaultSensorStatus.RUNNING,
  description="Polls for stale graphs and submits materialization jobs",
)
def stale_graph_materialization_sensor(context: SensorEvaluationContext):
  """Submit materialization for active entity graphs stale past the batch window.

  Only entity graphs have extensions OLTP. A graph a materialization is already
  writing is skipped without touching the cursor, so it is picked up on the
  first tick after that run ends if the run left it stale.
  During a writer-roll maintenance pause the tick submits nothing and leaves the
  cursor alone, so stale graphs are picked up as soon as the pause ends.
  """
  from robosystems.middleware.graph.write_pause import graph_writes_paused_until
  from robosystems.models.core.graph import Graph

  paused_until = graph_writes_paused_until()
  if paused_until is not None:
    return SkipReason(
      f"Graph writes paused for maintenance until {paused_until.isoformat()}"
    )

  db = db_session_factory()
  try:
    now = _now()
    min_stale_age, max_stale_wait = _stale_windows()
    cutoff = now - timedelta(seconds=min_stale_age)

    # Cursor: {graph_id: {"stale_at", "submitted_at"}}, the staleness event
    # last submitted for each graph and when.
    submitted: dict[str, dict[str, str | None]] = {}
    if context.cursor:
      try:
        submitted = {
          gid: entry
          if isinstance(entry, dict)
          else {"stale_at": None, "submitted_at": entry}
          for gid, entry in json.loads(context.cursor).items()
        }
      except (json.JSONDecodeError, TypeError, AttributeError):
        submitted = {}

    stale_graphs = (
      db.query(Graph)
      .filter(
        Graph.graph_stale.is_(True),
        Graph.graph_stale_at.isnot(None),
        or_(
          Graph.graph_stale_at < cutoff,  # type: ignore[operator]
          Graph.graph_stale_since  # type: ignore[operator]
          < now - timedelta(seconds=max_stale_wait),
        ),
        Graph.graph_type == "entity",
        Graph.status == "active",
        Graph.is_repository.is_(False),
      )
      .all()
    )

    stale_ids = [str(g.graph_id) for g in stale_graphs]
    busy = _graphs_being_written(context, stale_ids)

    run_requests = []
    new_cursor = {gid: e for gid, e in submitted.items() if gid in stale_ids}

    for graph in stale_graphs:
      graph_id = str(graph.graph_id)
      if graph_id in busy:
        logger.debug(f"Skipping {graph_id}: materialization in progress")
        continue

      stale_at_str = (
        graph.graph_stale_at.isoformat() if graph.graph_stale_at else "unknown"
      )
      # A run for this exact staleness event already went and left the graph
      # stale: it failed. Wait out the retry window rather than hammer it; a
      # newer write (a new stale_at) resubmits at once.
      prior = submitted.get(graph_id)
      if prior and prior.get("stale_at") == stale_at_str:
        try:
          prior_at = date_parser.isoparse(str(prior.get("submitted_at")))
          if (now - prior_at).total_seconds() < _FAILED_RUN_RETRY_SECONDS:
            continue
        except (ValueError, TypeError):
          pass

      # The window index keeps a retry of the same event from deduping
      # against the failed run's run_key.
      window = int(now.timestamp() // _FAILED_RUN_RETRY_SECONDS)

      logger.info(
        f"Submitting materialization for stale graph {graph_id} "
        f"(stale since {stale_at_str}, reason: {graph.graph_stale_reason})"
      )

      run_requests.append(
        RunRequest(
          run_key=f"stale_materialize_{graph_id}_{stale_at_str}_{window}",
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
          # dagster.yaml limits materialize_db to 1 run per value: concurrent
          # COPYs into rel tables without a primary key would duplicate edges.
          tags={
            "graph_id": graph_id,
            "trigger": "stale_sensor",
            "materialize_db": graph_id,
          },
        )
      )
      new_cursor[graph_id] = {"stale_at": stale_at_str, "submitted_at": now.isoformat()}

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
