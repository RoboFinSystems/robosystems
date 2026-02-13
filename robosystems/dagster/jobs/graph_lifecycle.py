"""Dagster jobs for graph lifecycle management.

Handles suspension and deprovisioning of graphs with expired subscriptions.
"""

from dagster import Config, OpExecutionContext, job, op

from robosystems.dagster.resources import DatabaseResource
from robosystems.logger import get_logger

logger = get_logger(__name__)


class SuspendGraphsConfig(Config):
  """Configuration for suspending expired graphs."""

  graph_ids: list[str]


@op
def suspend_expired_graphs(
  context: OpExecutionContext,
  db: DatabaseResource,
  config: SuspendGraphsConfig,
) -> dict:
  """Suspend graphs whose subscriptions have expired."""
  from robosystems.models.iam.graph import Graph, GraphStatus

  suspended_count = 0

  with db.get_session() as session:
    for graph_id in config.graph_ids:
      graph = Graph.get_by_id(graph_id, session)
      if not graph:
        context.log.warning(f"Graph {graph_id} not found, skipping")
        continue

      if graph.status != GraphStatus.ACTIVE.value:
        context.log.info(f"Graph {graph_id} is already {graph.status}, skipping")
        continue

      graph.transition_status(GraphStatus.SUSPENDED, session)
      suspended_count += 1
      context.log.info(f"Suspended graph {graph_id}")

  context.log.info(f"Suspended {suspended_count}/{len(config.graph_ids)} graphs")

  return {
    "suspended_count": suspended_count,
    "total_requested": len(config.graph_ids),
  }


@job(
  tags={
    "dagster/priority": "1",
  }
)
def suspend_expired_graphs_job():
  """Suspend graphs with expired subscriptions."""
  suspend_expired_graphs()


# ============================================================================
# Deprovisioning
# ============================================================================


class DeprovisionGraphsConfig(Config):
  """Configuration for deprovisioning suspended graphs."""

  graph_ids: list[str]


@op
def deprovision_suspended_graphs(
  context: OpExecutionContext,
  db: DatabaseResource,
  config: DeprovisionGraphsConfig,
) -> dict:
  """Deprovision graphs that have been suspended past the retention period."""
  import asyncio

  from robosystems.config import env
  from robosystems.operations.graph.deprovision_service import (
    GraphDeprovisionService,
  )

  service = GraphDeprovisionService(environment=env.ENVIRONMENT)
  deprovisioned_count = 0
  errors: list[str] = []

  with db.get_session() as session:
    for graph_id in config.graph_ids:
      try:
        result = asyncio.run(
          service.deprovision_graph(graph_id, session, create_backup=True)
        )
        if result.status in ("success", "partial"):
          deprovisioned_count += 1
          context.log.info(f"Deprovisioned graph {graph_id} (status={result.status})")
          if result.errors:
            for err in result.errors:
              context.log.warning(f"  {graph_id}: {err}")
        else:
          context.log.info(f"Skipped graph {graph_id} (status={result.status})")
      except Exception as e:
        error_msg = f"Failed to deprovision {graph_id}: {e}"
        errors.append(error_msg)
        context.log.error(error_msg)

  context.log.info(
    f"Deprovisioned {deprovisioned_count}/{len(config.graph_ids)} graphs"
  )

  return {
    "deprovisioned_count": deprovisioned_count,
    "total_requested": len(config.graph_ids),
    "errors": errors,
  }


@job(
  tags={
    "dagster/priority": "1",
  }
)
def deprovision_suspended_graphs_job():
  """Deprovision graphs that have been suspended past retention period."""
  deprovision_suspended_graphs()
