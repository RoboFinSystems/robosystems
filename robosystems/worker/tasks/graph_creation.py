"""Worker task handler for graph creation.


Replaces run_graph_creation() and run_entity_graph_creation() in
middleware/sse/direct_monitor.py. Handles both entity and generic
graph creation through the unified GraphCreationService.

On CapacityScalingTriggered, queues the graph for the Dagster
graph_creation_queue_sensor to pick up later.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from robosystems.logger import get_logger
from robosystems.worker.tasks import register_task
from robosystems.worker.tasks.base import BaseTask

if TYPE_CHECKING:
  from robosystems.operations.graph.graph_creation_service import GraphCreationConfig

logger = get_logger(__name__)


@register_task("graph_creation")
class GraphCreationTask(BaseTask):
  """Create a graph database (entity or generic) via the unified pipeline."""

  async def execute(self) -> dict[str, Any]:
    from robosystems.middleware.graph.allocation_manager import CapacityScalingTriggered
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

    try:
      result = await service.create(config)
    except CapacityScalingTriggered:
      return await self._handle_capacity_queue(config)

    # Billing subscription (non-blocking)
    self._create_billing_subscription(result.graph_id, config)

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
      pass

  async def _handle_capacity_queue(self, config: GraphCreationConfig) -> dict[str, Any]:
    """Queue graph for creation when no capacity is available.

    Creates a QUEUED graph record so the Dagster graph_creation_queue_sensor
    can pick it up once ASG scale-up completes.
    """
    from robosystems.config import env as app_env

    if not app_env.GRAPH_PROVISION_QUEUE_ENABLED:
      raise RuntimeError("No capacity currently available. Please try again later.")

    from robosystems.config.graph_tier import GraphTier
    from robosystems.database import get_db_session
    from robosystems.models.core.graph import Graph
    from robosystems.models.core.org.org_user import OrgUser
    from robosystems.utils.ulid import generate_ulid_hex

    graph_id = f"kg{generate_ulid_hex(16)}"

    db_gen = get_db_session()
    db = next(db_gen)
    try:
      org_user = db.query(OrgUser).filter(OrgUser.user_id == config.user_id).first()
      org_id = org_user.org_id if org_user else None

      Graph.create_queued(
        graph_id=graph_id,
        org_id=org_id,
        graph_name=config.graph_name,
        graph_type=config.graph_type,
        user_id=config.user_id,
        session=db,
        graph_tier=GraphTier(config.tier),
        schema_extensions=config.schema_extensions,
        graph_metadata={
          "created_by": config.user_id,
          "operation_id": self.task_id,
          "description": config.description or "",
          "tags": config.tags,
          "custom_schema": config.custom_schema,
          "entity_data": config.entity_data,
          "create_entity": config.create_entity,
        },
      )
      logger.info(f"Queued graph {graph_id} for user {config.user_id}")
    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass

    await self.report_progress(
      "Queued for provisioning — new infrastructure being started...", percent=5
    )

    return {
      "graph_id": graph_id,
      "operation_id": self.task_id,
      "status": "queued",
    }

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
