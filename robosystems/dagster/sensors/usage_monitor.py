"""Dagster sensor for monitoring graph instance storage usage.

Periodically checks all active user graphs against their tier storage limits
and sends email alerts when approaching (80%) or exceeding (100%) capacity.
Uses Valkey for dedup to avoid spamming users with repeat alerts.
"""

import asyncio

from dagster import (
  DefaultSensorStatus,
  SensorEvaluationContext,
  SkipReason,
  sensor,
)

from robosystems.logger import get_logger

logger = get_logger(__name__)

# Valkey key TTL for dedup: re-alert after 7 days if still over threshold
_ALERT_DEDUP_TTL_SECONDS = 7 * 24 * 3600

# Thresholds that trigger alerts
_ALERT_STATUSES = ("approaching", "over_limit")


@sensor(
  minimum_interval_seconds=21600,  # 6 hours
  default_status=DefaultSensorStatus.STOPPED,
  description="Monitors graph instance storage and sends email alerts at 80%/100% thresholds",
)
def graph_usage_monitor_sensor(context: SensorEvaluationContext):
  """Check all active user graphs for storage usage and send alerts.

  Query: parent graphs where:
  - is_repository = false (skip shared repos)
  - parent_graph_id IS NULL (only parent graphs, not subgraphs)
  - status = 'active'

  For each graph over threshold:
  - Check Valkey dedup key to avoid repeat alerts
  - Look up graph owner's email
  - Send capacity warning email via SES
  """
  from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
  from robosystems.database import session as db_session_factory
  from robosystems.middleware.graph.ingestion_limits import IngestionLimitChecker
  from robosystems.models.core.graph import Graph
  from robosystems.models.core.graph.graph_user import GraphUser
  from robosystems.models.core.user import User

  db = db_session_factory()
  try:
    # Find all active parent user graphs
    parent_graphs = (
      db.query(Graph)
      .filter(
        Graph.is_repository.is_(False),
        Graph.parent_graph_id.is_(None),
        Graph.status == "active",
      )
      .all()
    )

    if not parent_graphs:
      return SkipReason("No active user graphs to monitor")

    context.log.info(f"Checking storage usage for {len(parent_graphs)} graphs")

    redis_client = create_redis_client(ValkeyDatabase.LOCKS)
    alerts_sent = 0

    for graph in parent_graphs:
      graph_tier = graph.graph_tier or "ladybug-standard"

      try:
        storage_check = asyncio.run(
          IngestionLimitChecker.check_instance_storage(
            db=db,
            graph_id=graph.graph_id,
            tier=graph_tier,
          )
        )
      except Exception as e:
        context.log.warning(f"Could not check storage for {graph.graph_id}: {e}")
        continue

      instance_status = storage_check["status"]

      # Only alert for approaching or over_limit
      if instance_status not in _ALERT_STATUSES:
        continue

      # Dedup: check if we already alerted for this graph at this status
      dedup_key = f"usage_alert:{graph.graph_id}:{instance_status}"
      if redis_client.exists(dedup_key):
        context.log.debug(
          f"Skipping alert for {graph.graph_id} ({instance_status}) — already alerted"
        )
        continue

      # Look up graph owner
      graph_user = (
        db.query(GraphUser)
        .filter(
          GraphUser.graph_id == graph.graph_id,
          GraphUser.role == "admin",
        )
        .first()
      )
      if not graph_user:
        continue

      user = db.query(User).filter(User.id == graph_user.user_id).first()
      if not user or not user.email:
        continue

      # Send capacity warning email
      try:
        from robosystems.operations.aws.ses import ses_service

        sent = asyncio.run(
          ses_service.send_capacity_warning_email(
            user_email=user.email,
            user_name=user.name or "there",
            graph_id=graph.graph_id,
            tier=graph_tier,
            usage_percentage=storage_check["usage_percentage"],
            used_gb=storage_check["total_storage_gb"],
            limit_gb=storage_check["limit_gb"],
            instance_status=instance_status,
            databases=storage_check["databases"],
          )
        )

        if sent:
          # Set dedup key with TTL
          redis_client.setex(dedup_key, _ALERT_DEDUP_TTL_SECONDS, "1")
          alerts_sent += 1
          context.log.info(
            f"Sent {instance_status} alert for {graph.graph_id} to {user.email} "
            f"({storage_check['usage_percentage']:.0f}%)"
          )
        else:
          context.log.warning(f"Failed to send capacity alert for {graph.graph_id}")
      except Exception as e:
        context.log.error(f"Error sending capacity alert for {graph.graph_id}: {e}")

    context.log.info(
      f"Usage monitor complete: checked {len(parent_graphs)} graphs, "
      f"sent {alerts_sent} alerts"
    )

    return None

  finally:
    db.close()
