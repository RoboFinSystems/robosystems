"""Dagster sensor for processing the graph creation queue.

When capacity is unavailable during graph creation, graphs are inserted with
status='queued'. This sensor processes the queue FIFO, one graph at a time
per tier, by transitioning to 'provisioning' and launching the
wait_and_create_graph_job.
"""

import json

from dagster import (
  DefaultSensorStatus,
  RunRequest,
  SensorEvaluationContext,
  sensor,
)

from robosystems.config.graph_tier import GraphTier
from robosystems.dagster.jobs.graph import wait_and_create_graph_job
from robosystems.logger import get_logger

logger = get_logger(__name__)

# Tiers that can have queued graphs
QUEUE_TIERS = [
  GraphTier.LADYBUG_STANDARD.value,
  GraphTier.LADYBUG_LARGE.value,
  GraphTier.LADYBUG_XLARGE.value,
]


@sensor(
  job=wait_and_create_graph_job,
  minimum_interval_seconds=30,
  default_status=DefaultSensorStatus.RUNNING,
  description="Processes the graph creation queue (FIFO, one at a time per tier)",
)
def graph_creation_queue_sensor(context: SensorEvaluationContext):
  """Process the graph creation queue.

  For each tier:
  1. Skip if a graph is already status=provisioning for this tier
  2. Find the oldest status=queued graph (FIFO via created_at)
  3. Transition to status=provisioning
  4. Yield RunRequest with graph_id + creation params
  """
  from robosystems.database import session as db_session_factory
  from robosystems.models.iam.graph import Graph, GraphStatus

  db = db_session_factory()
  try:
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
      operation_id = graph_metadata.get("operation_id", "")

      # Transition to provisioning
      queued_graph.transition_status(GraphStatus.PROVISIONING, db)
      context.log.info(
        f"Tier {tier}: transitioning graph {graph_id} from queued to provisioning"
      )

      # Build run config
      schema_extensions = queued_graph.schema_extensions or []
      custom_schema = graph_metadata.get("custom_schema")

      yield RunRequest(
        run_key=f"queue-{graph_id}-{int(queued_graph.updated_at.timestamp())}",
        run_config={
          "ops": {
            "wait_for_capacity_and_create_graph": {
              "config": {
                "operation_id": operation_id,
                "user_id": graph_metadata.get("created_by", ""),
                "graph_name": queued_graph.graph_name,
                "graph_id": graph_id,
                "tier": tier,
                "schema_extensions": ",".join(schema_extensions),
                "description": graph_metadata.get("description", ""),
                "tags": ",".join(graph_metadata.get("tags", [])),
                "custom_schema_json": json.dumps(custom_schema)
                if custom_schema
                else "",
              }
            }
          }
        },
        tags={"graph_id": graph_id, "tier": tier},
      )
  finally:
    db_session_factory.remove()
