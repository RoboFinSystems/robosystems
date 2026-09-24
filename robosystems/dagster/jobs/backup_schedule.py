"""Nightly backups of customer graphs and their subgraphs, one run per graph.

Graphs with an upstream rebuild from it, so backups mainly give customers a
retrievable record and cover what has no upstream (entity subgraphs, the
semantic memory store). Shared repositories are excluded: large, re-ingestible,
and snapshotted by the instance lifecycle.
"""

from datetime import UTC, datetime
from typing import Any

from dagster import (
  DefaultScheduleStatus,
  RunRequest,
  ScheduleEvaluationContext,
  schedule,
)

from robosystems.config import env
from robosystems.config.graph_tier import GraphTierConfig
from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
from robosystems.dagster.jobs.graph import backup_graph_job
from robosystems.database import session as db_session_factory
from robosystems.logger import logger

# Matches daily_backup_cleanup_schedule, whose retention this balances.
_SCHEDULE_STATUS = (
  DefaultScheduleStatus.RUNNING
  if env.ENVIRONMENT != "dev"
  else DefaultScheduleStatus.STOPPED
)

# Graph types that belong to a customer and carry an expectation of backups.
_BACKED_UP_GRAPH_TYPES = ("entity", "generic")

_ACTIVE_GRAPH_STATUSES = ("active",)


def _graphs_to_back_up(session) -> list[str]:
  """Graph ids in scope, parents and subgraphs alike.

  Read from the ``graphs`` table: the DynamoDB volume registry lists only parent
  ids, and subgraphs need backups most.
  """
  from robosystems.models.core import Graph

  rows = (
    session.query(Graph.graph_id)
    .filter(
      Graph.graph_type.in_(_BACKED_UP_GRAPH_TYPES),
      Graph.status.in_(_ACTIVE_GRAPH_STATUSES),
    )
    .all()
  )

  return [
    graph_id
    for (graph_id,) in rows
    # Belt-and-braces over the graph_type filter.
    if not is_shared_repository_or_subgraph(graph_id)
  ]


def _run_config_for(graph_id: str, retention_days: int) -> dict[str, Any]:
  return {
    "ops": {
      "create_backup": {
        "config": {
          "graph_id": graph_id,
          "backup_type": "full",
          "backup_format": "full_dump",
          "retention_days": retention_days,
          "compression": True,
          # Outside the customer's daily allowance; badged in the listing.
          "initiated_by": "scheduled",
        }
      }
    }
  }


@schedule(
  job=backup_graph_job,
  # 3am local when the fleet is quietest; zone-pinned so DST doesn't shift it.
  cron_schedule="0 3 * * *",
  execution_timezone="America/New_York",
  default_status=_SCHEDULE_STATUS,
  name="nightly_graph_backup_schedule",
)
def nightly_graph_backup_schedule(context: ScheduleEvaluationContext):
  """One backup run per in-scope graph, nightly.

  ``daily_backup_cleanup_schedule`` (05:00 UTC, ~01:00 local) runs before this.
  Harmless while tier retention is well above the daily cadence; revisit if it
  ever shrinks toward it.
  """
  from robosystems.models.core import Graph

  # The same kill switch the manual backup paths honour.
  if not env.BACKUP_CREATION_ENABLED:
    context.log.info("Backup creation is disabled; skipping nightly backups")
    return []

  run_date = datetime.now(UTC).date().isoformat()
  requests: list[RunRequest] = []

  db = db_session_factory()
  try:
    for graph_id in _graphs_to_back_up(db):
      # Retention is the graph's tier maximum.
      graph = Graph.get_by_id(graph_id, db)
      tier = str(graph.graph_tier) if graph and graph.graph_tier else "ladybug-standard"
      retention_days = GraphTierConfig.get_backup_limits(tier).get(
        "backup_retention_days", 7
      )

      requests.append(
        RunRequest(
          # One run per graph per day; re-evaluations dedupe.
          run_key=f"nightly_backup_{graph_id}_{run_date}",
          run_config=_run_config_for(graph_id, retention_days),
          tags={"graph_id": graph_id, "trigger": "nightly_backup"},
        )
      )
  finally:
    db.close()

  logger.info(f"Nightly backup: submitting {len(requests)} graph backups")
  context.log.info(f"Submitting {len(requests)} nightly graph backups")

  return requests
