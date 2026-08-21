"""Nightly system backups for customer graphs.

Fans one ``backup_graph_job`` run out per graph rather than looping inside a
single run, so one graph failing does not take the rest of the fleet's backups
with it and each shows up separately in the Dagster UI.

Backups are a *download* capability: every graph type with an upstream rebuilds
from that upstream rather than from a snapshot, so this exists to give customers
a retrievable record of what their graph held — and to cover the two things that
have no upstream at all, entity subgraphs and the semantic memory store.

Scope is customer graphs and their subgraphs. Shared repositories are excluded:
they are large, re-ingestible from their public source, and already have a
last-known-good volume snapshot from the instance lifecycle.
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

# Schedule status: RUNNING in prod/staging, STOPPED in dev — matching
# daily_backup_cleanup_schedule, whose retention this creation side balances.
_SCHEDULE_STATUS = (
  DefaultScheduleStatus.RUNNING
  if env.ENVIRONMENT != "dev"
  else DefaultScheduleStatus.STOPPED
)

# Graph types that belong to a customer and carry an expectation of backups.
_BACKED_UP_GRAPH_TYPES = ("entity", "generic")

# Statuses that mean the graph is real and reachable. A graph still
# provisioning has nothing to back up, and one that failed provisioning may
# have no database at all.
_ACTIVE_GRAPH_STATUSES = ("active",)


def _graphs_to_back_up(session) -> list[str]:
  """Graph ids in scope, parents and subgraphs alike.

  Enumerated from the platform ``graphs`` table rather than the DynamoDB volume
  registry: the registry's ``databases`` list records only parent ids, so a
  subgraph — one of the two classes that has no upstream and therefore needs
  this most — would be invisible to a registry-driven sweep.
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
    # Belt to the graph_type filter: shared repositories are platform-managed
    # and re-ingestible, and backing up a 300 GB corpus nightly is the expensive
    # mistake this scope exists to avoid.
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
          # Keeps the run out of the customer's daily allowance and badges it
          # in the listing as taken on their behalf.
          "initiated_by": "scheduled",
        }
      }
    }
  }


@schedule(
  job=backup_graph_job,
  # Deep night in the platform's primary operating timezone: the fleet is
  # quietest and a backup
  # checkpoints and copies the whole database. Pinned to a zone rather than a
  # fixed UTC hour so it stays at 3am local across daylight-saving changes
  # instead of drifting an hour twice a year.
  cron_schedule="0 3 * * *",
  execution_timezone="America/New_York",
  default_status=_SCHEDULE_STATUS,
  name="nightly_graph_backup_schedule",
)
def nightly_graph_backup_schedule(context: ScheduleEvaluationContext):
  """One backup run per in-scope graph, nightly.

  Note the ordering against ``daily_backup_cleanup_schedule``, which runs at
  05:00 **UTC** — that is 01:00 local, so retention is enforced *before* the
  night's backup is taken rather than after. This is harmless only because
  retention is long relative to the cadence: at the entry tier's 7 days there
  are always six other backups standing when the sweep runs. Shortening tier
  retention toward the backup cadence would make the order matter, so revisit
  this comment if that ever changes.
  """
  from robosystems.models.core import Graph

  # The same kill switch both manual paths check. A flag honoured on two of
  # three paths that create backups is not a kill switch — and this is the one
  # path nobody is watching when they flip it during an incident.
  if not env.BACKUP_CREATION_ENABLED:
    context.log.info("Backup creation is disabled; skipping nightly backups")
    return []

  run_date = datetime.now(UTC).date().isoformat()
  requests: list[RunRequest] = []

  db = db_session_factory()
  try:
    for graph_id in _graphs_to_back_up(db):
      # Retention is the graph's own tier maximum. The nightly backup is the
      # baseline the tier advertises, so it should live exactly as long as the
      # tier says a backup does.
      graph = Graph.get_by_id(graph_id, db)
      tier = str(graph.graph_tier) if graph and graph.graph_tier else "ladybug-standard"
      retention_days = GraphTierConfig.get_backup_limits(tier).get(
        "backup_retention_days", 7
      )

      requests.append(
        RunRequest(
          # One run per graph per day. A re-evaluation within the same day
          # dedupes against this rather than taking a second backup and
          # eating into the tier's daily ceiling.
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
