"""Worker task handler for graph creation.

Replaces run_graph_creation() and run_entity_graph_creation() in
middleware/sse/direct_monitor.py. Handles both entity and generic
graph creation through the unified GraphCreationService.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
  from robosystems.operations.graph.graph_creation_service import GraphCreationConfig

from robosystems.logger import get_logger
from robosystems.worker.tasks import register_task
from robosystems.worker.tasks.base import BaseTask

logger = get_logger(__name__)


@register_task("graph_creation")
class GraphCreationTask(BaseTask):
  """Create a graph database (entity or generic) via the unified pipeline."""

  async def execute(self) -> dict[str, Any]:
    from robosystems.operations.graph.graph_creation_service import (
      GraphCreationConfig,
      GraphCreationService,
    )

    config = GraphCreationConfig(
      user_id=self.user_id,
      graph_id=self.params.get("graph_id"),
      graph_type=self.params.get("graph_type", "entity"),
      graph_name=self.params["graph_name"],
      tier=self.params["tier"],
      schema_extensions=self.params.get("schema_extensions", []),
      custom_schema=self.params.get("custom_schema"),
      entity_data=self.params.get("entity_data"),
      create_entity=self.params.get("create_entity", True),
      description=self.params.get("description"),
      tags=self.params.get("tags", []),
      progress=self._progress_adapter,
    )

    service = GraphCreationService()
    result = await service.create(config)

    # Billing subscription (non-blocking)
    self._create_billing_subscription(result.graph_id, config)

    # Report to Dagster observable asset
    from robosystems.dagster.reporting import report_asset_materialization

    await report_asset_materialization(
      asset_key="user_graph_creation",
      description=f"Graph {result.graph_id} created via worker",
      metadata={
        "graph_id": result.graph_id,
        "user_id": self.user_id,
        "graph_type": config.graph_type,
        "tier": config.tier,
        "provisioning_method": "worker",
        "operation_id": self.task_id,
      },
    )

    return result.to_dict()

  def _progress_adapter(self, message: str, percent: float) -> None:
    """Adapt the sync progress callback to the async BaseTask interface.

    GraphCreationService calls config.progress(message, percent) synchronously.
    We can't await here, so we fire-and-forget via create_task.
    """
    import asyncio

    try:
      loop = asyncio.get_running_loop()
      loop.create_task(self.report_progress(message, percent=percent))
    except RuntimeError:
      logger.debug(f"Skipping progress emit (no event loop): {message}")

  def _create_billing_subscription(
    self, graph_id: str, config: GraphCreationConfig
  ) -> None:
    """Create billing subscription. Non-blocking — failures logged."""
    try:
      from robosystems.config.graph_tier import GraphTier
      from robosystems.database import get_db_session
      from robosystems.operations.graph.subscription_service import (
        GraphSubscriptionService,
      )

      db_gen = get_db_session()
      db = next(db_gen)
      try:
        service = GraphSubscriptionService(db)
        service.create_graph_subscription(
          user_id=config.user_id,
          graph_id=graph_id,
          plan_name=config.tier,
          tier=GraphTier(config.tier),
        )
        logger.info(f"Billing subscription created for {graph_id}")
      finally:
        try:
          next(db_gen)
        except StopIteration:
          pass
    except Exception as e:
      logger.error(f"Failed to create billing subscription for {graph_id}: {e}")
