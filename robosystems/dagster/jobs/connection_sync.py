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
disconnected or severed. A failed sync does not advance ``last_sync``, so
a connection that keeps failing is tried once per cadence, not every tick:
the failure it recorded is the clock until it is older than the cadence. A
sync a person already started is the expected collision and is logged
quietly.

- ``CONNECTION_SYNC_INTERVAL_HOURS`` is the cadence (default daily); the
  tick is hourly, so each connection syncs within an hour of coming due and
  the fleet's syncs spread across the day rather than landing at once.
- ``CONNECTION_SCHEDULED_SYNC_ENABLED`` (SSM, read per run) is the runtime
  kill switch.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from dagster import (
  DefaultScheduleStatus,
  OpExecutionContext,
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
  with db.get_session() as session:
    due = [
      (c.graph_id, c.id, c.provider)
      for c in due_connections(session, now=datetime.now(UTC), interval=interval)
    ]

  counts = {"due": len(due), "dispatched": 0, "in_progress": 0, "failed": 0}

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
        )
      except SyncInProgressError:
        counts["in_progress"] += 1
        context.log.debug(f"Connection {connection_id} is already syncing; left alone")
        continue
      except Exception as exc:
        counts["failed"] += 1
        context.log.warning(
          f"Scheduled sync of connection {connection_id} ({provider}) was not "
          f"dispatched: {type(exc).__name__}: {exc}"
        )
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
    f"not dispatched (cadence {env.CONNECTION_SYNC_INTERVAL_HOURS} h)"
  )
  logger.info(message)
  context.log.info(message)
  return counts


@job(tags={"dagster/priority": "2", "dagster/max_retries": 3})
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
