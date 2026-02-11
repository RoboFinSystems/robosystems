"""Dagster jobs for graph lifecycle management.

Handles suspension of graphs with expired subscriptions.
Infrastructure teardown (deprovisioning) is a future PR.
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
