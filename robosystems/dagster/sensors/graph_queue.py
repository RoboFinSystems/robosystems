"""Dagster sensor for processing the graph creation queue.

When capacity is unavailable during graph creation, graphs are inserted with
status='queued'. This sensor processes the queue FIFO, one graph at a time
per tier, by transitioning to 'provisioning' and enqueuing a worker task
to attempt creation.

If the worker task fails (still no capacity), the graph stays queued and
the sensor will re-enqueue it on the next cycle.
"""

import json

from dagster import (
  DefaultSensorStatus,
  SensorEvaluationContext,
  SkipReason,
  sensor,
)

from robosystems.config.graph_tier import GraphTier
from robosystems.logger import get_logger

logger = get_logger(__name__)

# Tiers that can have queued graphs
QUEUE_TIERS = [
  GraphTier.LADYBUG_STANDARD.value,
  GraphTier.LADYBUG_LARGE.value,
  GraphTier.LADYBUG_XLARGE.value,
]


@sensor(
  minimum_interval_seconds=30,
  default_status=DefaultSensorStatus.RUNNING,
  description="Processes the graph creation queue (FIFO, one at a time per tier). Enqueues worker tasks for queued graphs. Controlled by GRAPH_PROVISION_QUEUE_ENABLED feature flag.",
)
def graph_creation_queue_sensor(context: SensorEvaluationContext):
  """Process the graph creation queue.

  For each tier:
  1. Skip if a graph is already status=provisioning for this tier
  2. Find the oldest status=queued graph (FIFO via created_at)
  3. Transition to status=provisioning
  4. Enqueue a worker task to create the graph
  """
  from robosystems.config import env

  if not env.GRAPH_PROVISION_QUEUE_ENABLED:
    return SkipReason("Graph provision queue disabled")

  from robosystems.database import session as db_session_factory
  from robosystems.models.core.graph import Graph, GraphStatus

  db = db_session_factory()
  try:
    enqueued = 0

    for tier in QUEUE_TIERS:
      # Guard: one provisioning job at a time per tier
      if Graph.has_provisioning_for_tier(tier, db):
        context.log.debug(f"Tier {tier}: already has a provisioning graph, skipping")
        continue

      # FIFO: get oldest queued graph for this tier
      queued_graph = Graph.get_oldest_queued_by_tier(tier, db)
      if not queued_graph:
        continue

      graph_id = queued_graph.graph_id
      graph_metadata = queued_graph.graph_metadata or {}

      # Transition to provisioning
      queued_graph.transition_status(GraphStatus.PROVISIONING, db)
      context.log.info(
        f"Tier {tier}: transitioning graph {graph_id} from queued to provisioning"
      )

      # Build worker task params from stored metadata
      schema_extensions = queued_graph.schema_extensions or []
      custom_schema = graph_metadata.get("custom_schema")
      entity_data = graph_metadata.get("entity_data")
      create_entity = graph_metadata.get("create_entity", True)

      _enqueue_graph_creation(
        graph_id=graph_id,
        user_id=graph_metadata.get("created_by", ""),
        graph_type=queued_graph.graph_type or "entity",
        graph_name=queued_graph.graph_name,
        tier=tier,
        schema_extensions=schema_extensions,
        custom_schema=custom_schema,
        entity_data=entity_data,
        create_entity=create_entity,
        description=graph_metadata.get("description", ""),
        tags=graph_metadata.get("tags", []),
      )
      enqueued += 1
      context.log.info(f"Enqueued worker task for queued graph {graph_id}")

    if enqueued == 0:
      return SkipReason("No queued graphs found")

    context.log.info(f"Enqueued {enqueued} graph creation worker tasks")
    return None

  finally:
    db_session_factory.remove()


def _enqueue_graph_creation(
  graph_id: str,
  user_id: str,
  graph_type: str,
  graph_name: str,
  tier: str,
  schema_extensions: list[str],
  custom_schema: dict | None,
  entity_data: dict | None,
  create_entity: bool,
  description: str,
  tags: list[str],
) -> None:
  """Enqueue a graph creation task to the worker via sync Redis.

  Uses sync Redis directly since Dagster sensors run in a sync context.
  """

  from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
  from robosystems.utils.ulid import generate_prefixed_ulid

  task_id = generate_prefixed_ulid("task")
  queue = create_redis_client(ValkeyDatabase.WORKER_QUEUE, decode_responses=True)

  try:
    task_payload = json.dumps(
      {
        "task_id": task_id,
        "task_type": "graph_creation",
        "graph_id": graph_id,
        "user_id": user_id,
        "attempt": 1,
        "params": {
          "graph_id": graph_id,
          "graph_type": graph_type,
          "graph_name": graph_name,
          "tier": tier,
          "schema_extensions": schema_extensions,
          "custom_schema": custom_schema,
          "entity_data": entity_data,
          "create_entity": create_entity,
          "description": description,
          "tags": tags,
        },
      }
    )
    queue.rpush("worker:tasks", task_payload)

    # Create SSE operation for the task so progress is trackable
    # (the original operation_id from the API request may have expired)
    _create_sse_operation_sync(task_id, graph_id, user_id)

  finally:
    queue.close()


def _create_sse_operation_sync(task_id: str, graph_id: str, user_id: str) -> None:
  """Create an SSE operation entry for the queued task (sync)."""

  from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client

  sse = create_redis_client(ValkeyDatabase.SSE, decode_responses=True)
  try:
    import json
    from datetime import UTC, datetime

    metadata = json.dumps(
      {
        "operation_id": task_id,
        "operation_type": "graph_creation",
        "user_id": user_id,
        "graph_id": graph_id,
        "status": "pending",
        "created_at": datetime.now(UTC).isoformat(),
      }
    )
    sse.setex(f"sse:operation:meta:{task_id}", 86400, metadata)
  finally:
    sse.close()
