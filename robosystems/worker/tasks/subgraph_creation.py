"""Worker task handler for subgraph creation (fork path).

Handles subgraph creation with data forking, which requires background
execution due to the time needed to copy data from parent to child.
Non-fork subgraph creation remains synchronous in the router.
"""

from __future__ import annotations

from typing import Any

from robosystems.logger import get_logger
from robosystems.worker.tasks import register_task
from robosystems.worker.tasks.base import BaseTask

logger = get_logger(__name__)


@register_task("subgraph_creation")
class SubgraphCreationTask(BaseTask):
  """Create a subgraph with optional data forking from parent."""

  async def execute(self) -> dict[str, Any]:
    from robosystems.database import get_db_session
    from robosystems.models.core import Graph, User
    from robosystems.operations.graph.subgraph_service import SubgraphService

    parent_graph_id = self.params["parent_graph_id"]
    subgraph_name = self.params["subgraph_name"]
    description = self.params.get("description")
    fork_data = self.params.get("fork_data", False)

    await self.report_progress("Initializing subgraph creation...", percent=0)

    db_gen = get_db_session()
    db = next(db_gen)

    try:
      parent_graph = db.query(Graph).filter(Graph.graph_id == parent_graph_id).first()
      if not parent_graph:
        raise ValueError(f"Parent graph {parent_graph_id} not found")

      user = db.query(User).filter(User.id == self.user_id).first()
      if not user:
        raise ValueError(f"User {self.user_id} not found")

      service = SubgraphService()

      fork_options = None
      if fork_data:
        fork_options = {"tables": []}  # Fork all tables

      await self.report_progress("Creating subgraph...", percent=20)

      result = await service.create_subgraph(
        parent_graph=parent_graph,
        user=user,
        name=subgraph_name,
        description=description,
        fork_parent=fork_data,
        fork_options=fork_options,
      )

      subgraph_id = result.get("graph_id", "unknown")
      logger.info(
        f"Subgraph created: {subgraph_id} (parent={parent_graph_id}, fork={fork_data})"
      )

      # Report to Dagster observable asset (non-blocking)
      from robosystems.dagster.reporting import report_asset_materialization

      await report_asset_materialization(
        asset_key="user_subgraph_creation",
        description=f"Subgraph {subgraph_id} created from {parent_graph_id}",
        metadata={
          "graph_id": subgraph_id,
          "parent_graph_id": parent_graph_id,
          "user_id": self.user_id,
          "fork_data": fork_data,
          "provisioning_method": "worker",
          "operation_id": self.task_id,
        },
      )

      return result

    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass
