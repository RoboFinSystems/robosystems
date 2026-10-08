"""Unattended sync of every connection, on a timer.

Until now the only sync a tenant ever got was the one at connect time plus
whatever a person started by hand, and ``auto_sync_enabled`` was a setting
with no automation behind it. This sweep is the automation: every tick it
selects the live connections whose toggle is on and whose last sync is older
than the cadence, and starts each one through ``dispatch_connection_sync``,
the same kernel the Sync button and the MCP tool use — the per-connection
lock, the provider check and the routing are not reimplemented here.

A connection the schedule cannot help is left alone: one still waiting on
its sign-in, one whose bank revoked the login (``needs_reauth``), one
disconnected or severed. A sync attempted within the cadence is not tried
again, whatever became of it: a failed sync records its failure on the
connection without advancing ``last_sync``, a dispatch that failed records
the same, and the run store holds every run a dispatch started, so a run
that died before it could record anything, or one still running past its
lock, costs one attempt per cadence rather than one per tick. A sync a
person already started is the expected collision and is logged quietly.

A sweep that could dispatch none of its due connections fails, so the
run-failure alarm says so; one that could dispatch some logs the rest. Its
syncs are marked unattended: nothing that spends the tenant's credits is
started on their behalf.

- ``CONNECTION_SYNC_INTERVAL_HOURS`` is the cadence (default daily); the
  tick is hourly, so each connection syncs within an hour of coming due and
  the fleet's syncs spread across the day rather than landing at once.
- ``CONNECTION_SCHEDULED_SYNC_ENABLED`` (SSM, read per run) is the runtime
  kill switch.
"""

import asyncio
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

from dagster import (
  DagsterInstance,
  DefaultScheduleStatus,
  Failure,
  OpExecutionContext,
  RunsFilter,
  ScheduleDefinition,
  job,
  op,
)
from sqlalchemy import or_
from sqlalchemy.orm import Session

from robosystems.config import env
from robosystems.dagster.resources import DatabaseResource
from robosystems.logger import logger
from robosystems.models.core.connection.connection import Connection

_ENABLED_FLAG = "CONNECTION_SCHEDULED_SYNC_ENABLED"

# A connection the schedule can sync: one that holds credentials and a
# source. A failed last sync is retried once the failure is a cadence old; a
# revoked login is not retried, since only the customer can mend it.
SYNCABLE_STATUSES = ("connected", "error")

_SCHEDULE_STATUS = (
  DefaultScheduleStatus.RUNNING
  if env.ENVIRONMENT != "dev"
  else DefaultScheduleStatus.STOPPED
)


def due_connections(
  session: Session, *, now: datetime, interval: timedelta
) -> list[Connection]:
  """The live connections on active graphs whose toggle is on, whose status
  the schedule can act on, and whose last sync is older than ``interval``
  (or that never synced), oldest first. One whose last attempt failed more
  recently than ``interval`` waits, so a broken connection costs one run per
  cadence rather than one per tick. A provider the deployment has turned
  off is skipped here rather than refused per dispatch."""
  from robosystems.models.core.graph.graph import Graph, GraphStatus
  from robosystems.operations.providers.registry import provider_registry

  cutoff = now - interval
  rows = (
    session.query(Connection)
    .join(Graph, Graph.graph_id == Connection.graph_id)
    .filter(
      Connection.deleted_at.is_(None),
      Connection.auto_sync_enabled.is_(True),
      Connection.status.in_(SYNCABLE_STATUSES),
      Graph.status == GraphStatus.ACTIVE.value,
      or_(Connection.last_sync.is_(None), Connection.last_sync < cutoff),
    )
    .order_by(Connection.last_sync.asc().nulls_first(), Connection.created_at.asc())
    .all()
  )
  return [
    c
    for c in rows
    if provider_registry.is_enabled((c.provider or "").lower())
    and not _failed_since(c, cutoff)
  ]


def _failed_since(connection: Connection, cutoff: datetime) -> bool:
  """Whether the connection's last recorded attempt was a failure newer than
  ``cutoff``. A failed sync leaves ``last_sync`` alone and writes the outcome
  to ``last_sync_result`` with its ``synced_at``."""
  result = connection.last_sync_result or {}
  if result.get("status") != "failed":
    return False
  try:
    attempted = datetime.fromisoformat(str(result.get("synced_at") or ""))
  except ValueError:
    return False
  if attempted.tzinfo is None:
    attempted = attempted.replace(tzinfo=UTC)
  return attempted >= cutoff


def recently_attempted(
  instance: DagsterInstance, connection_ids: Iterable[str], *, cutoff: datetime
) -> set[str]:
  """The connections a sync run was started for since ``cutoff``, in any
  state. Every provider tags its sync runs with ``connection_id``, so the
  run store is the record of attempts, including the runs that died before
  they could record anything on the connection."""
  attempted: set[str] = set()
  for connection_id in connection_ids:
    records = instance.get_run_records(
      filters=RunsFilter(tags={"connection_id": connection_id}, created_after=cutoff),
      limit=1,
    )
    if records:
      attempted.add(connection_id)
  return attempted


def record_dispatch_failure(
  db: DatabaseResource, connection_id: str, exc: BaseException, *, now: datetime
) -> None:
  """A dispatch that failed started no run to record itself, so the sweep
  records it: the connection shows the failure, and it is the clock that
  holds the connection until the next cadence."""
  try:
    with db.get_session() as session:
      connection = session.get(Connection, connection_id)
      if connection is not None:
        connection.record_sync_result(
          session,
          {
            "status": "failed",
            "stage": "dispatch",
            "synced_at": now.isoformat(),
            "error": {
              "code": type(exc).__name__,
              "message": describe_failure(exc, limit=500),
            },
          },
        )
  except Exception as exc:
    logger.warning(
      "Could not record the failed dispatch on connection %s: %s", connection_id, exc
    )


def describe_failure(exc: BaseException, *, limit: int = 600) -> str:
  """The exception and what caused it, innermost last: a wrapped client error
  says which query failed, and only its cause says why."""
  parts: list[str] = []
  seen: set[int] = set()
  cursor: BaseException | None = exc
  while cursor is not None and id(cursor) not in seen:
    seen.add(id(cursor))
    text = " ".join(str(cursor).split())
    parts.append(
      f"{type(cursor).__name__}: {text[:200]}" if text else type(cursor).__name__
    )
    cursor = cursor.__cause__ or cursor.__context__
  return " <- ".join(parts)[:limit]


@op
def sweep_connection_syncs(
  context: OpExecutionContext, db: DatabaseResource
) -> dict[str, Any]:
  """Start a sync for every connection that is due. One connection's
  failure to dispatch never stops the sweep; a run already in progress is
  the expected collision."""
  from robosystems.config.parameter_store import get_parameter_value
  from robosystems.operations.connection_service import (
    SYSTEM_USER_ID,
    SyncInProgressError,
    dispatch_connection_sync,
  )

  if get_parameter_value(_ENABLED_FLAG, "true").lower() != "true":
    context.log.info(f"Scheduled sync disabled via {_ENABLED_FLAG}")
    return {"skipped": True, "reason": f"{_ENABLED_FLAG} is false"}

  interval = timedelta(hours=env.CONNECTION_SYNC_INTERVAL_HOURS)
  now = datetime.now(UTC)
  with db.get_session() as session:
    candidates = [
      (c.graph_id, c.id, c.provider)
      for c in due_connections(session, now=now, interval=interval)
    ]
  attempted = recently_attempted(
    context.instance, [cid for _, cid, _ in candidates], cutoff=now - interval
  )
  due = [row for row in candidates if row[1] not in attempted]

  counts = {
    "due": len(due),
    "attempted": len(attempted),
    "dispatched": 0,
    "in_progress": 0,
    "failed": 0,
  }

  async def dispatch_all() -> None:
    # One connection at a time: a dispatch only submits a run, so the sweep
    # stays well inside its hour, and the per-connection lock dedupes a
    # retried sweep.
    for graph_id, connection_id, provider in due:
      try:
        result = await dispatch_connection_sync(
          graph_id=graph_id,
          connection_id=connection_id,
          # The schedule acts for the platform; the run itself is stamped
          # with the connection's own user.
          user_id=SYSTEM_USER_ID,
          sync_options={"unattended": True},
        )
      except SyncInProgressError:
        counts["in_progress"] += 1
        context.log.debug(f"Connection {connection_id} is already syncing; left alone")
        continue
      except Exception as exc:
        counts["failed"] += 1
        reason = describe_failure(exc)
        context.log.warning(
          f"Scheduled sync of connection {connection_id} ({provider}) was not "
          f"dispatched: {reason}"
        )
        record_dispatch_failure(db, connection_id, exc, now=now)
        continue
      if result.get("dispatched"):
        counts["dispatched"] += 1
      else:
        context.log.info(
          f"Scheduled sync of connection {connection_id} ({provider}) was a "
          f"no-op: {result.get('message')}"
        )

  asyncio.run(dispatch_all())

  message = (
    f"Scheduled sync sweep: {counts['due']} due, {counts['dispatched']} "
    f"dispatched, {counts['in_progress']} already running, {counts['failed']} "
    f"not dispatched, {counts['attempted']} attempted within the cadence "
    f"(cadence {env.CONNECTION_SYNC_INTERVAL_HOURS} h)"
  )
  logger.info(message)
  context.log.info(message)
  if due and counts["failed"] == len(due):
    # Nothing could be dispatched: the cause is the platform's, not one
    # connection's, and the run-failure alarm is how it gets heard.
    raise Failure(description=message, metadata=counts)
  return counts


@job(
  tags={
    "dagster/priority": "2",
    # A dead worker is retried; a sweep that failed on purpose is not.
    "dagster/max_retries": 3,
    "dagster/retry_on_asset_or_op_failure": "false",
  }
)
def scheduled_connection_sync_job():
  """Start the syncs that are due."""
  sweep_connection_syncs()


scheduled_connection_sync_schedule = ScheduleDefinition(
  job=scheduled_connection_sync_job,
  # Hourly, a quarter past: the connections due in that hour are the ones
  # whose last sync is a cadence old, so the fleet stays spread out.
  cron_schedule="15 * * * *",
  default_status=_SCHEDULE_STATUS,
)
