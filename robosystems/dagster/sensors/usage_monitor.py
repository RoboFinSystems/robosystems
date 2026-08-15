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

# The storage cap is a hard block (materialize 413s above 100%), so the 80%
# warning this sensor sends is what gives customers runway before that hits.
#
# NOTE: this status only applies when the Dagster instance first sees the
# sensor. On/off state then persists in Dagster's Postgres storage, so a UI
# toggle outlives this value — which is why the runtime kill switch below is an
# SSM flag rather than a redeploy of this constant.
_SENSOR_STATUS = DefaultSensorStatus.RUNNING

# Runtime kill switch, read per tick (SSM, 5-min TTL cache) so alerting can be
# stopped immediately without a deploy or a Dagster UI login:
#   just ssm-set prod features/GRAPH_USAGE_ALERTS_ENABLED false
# Defaults on — an alerting sensor should fail toward telling you.
_ALERTS_ENABLED_FLAG = "GRAPH_USAGE_ALERTS_ENABLED"

# Storage is a property of the graph, not of whoever happened to be its first
# explicit admin: snapshots are recorded under this principal so the row is
# attributable to the measurement, not to a member who may later leave. Every
# reader of STORAGE_SNAPSHOT rows keys by graph_id; ``GraphUsage.user_id`` has
# no foreign key, so a non-user principal is safe.
USAGE_MONITOR_PRINCIPAL = "system:usage-monitor"


def _alert_dedup_key(graph_id: str, instance_status: str, user_id: str) -> str:
  """Valkey key recording that *this* admin was told about *this* graph at
  *this* status. Per recipient so a failed delivery is retried without
  re-emailing the admins who already received it."""
  return f"usage_alert:{graph_id}:{instance_status}:{user_id}"


def _capacity_alert_recipients(db, graph) -> list:
  """Everyone who administers the graph: explicit ``GraphUser`` admins plus
  the owning org's owners and admins, who hold implicit graph admin with no
  ``GraphUser`` row at all. Deduplicated; only active users with an email.

  The previous ``.filter(role == "admin").first()`` picked one arbitrary
  explicit admin, so an org-owned graph whose only admins were implicit got
  no capacity email — exactly where multi-member orgs live.
  """
  from robosystems.models.core.graph.graph_user import GraphUser
  from robosystems.models.core.org import OrgRole, OrgUser
  from robosystems.models.core.user import User

  user_ids: list[str] = [
    row.user_id
    for row in db.query(GraphUser.user_id)
    .filter(GraphUser.graph_id == graph.graph_id, GraphUser.role == "admin")
    .all()
  ]
  if graph.org_id:
    user_ids.extend(
      row.user_id
      for row in db.query(OrgUser.user_id)
      .filter(
        OrgUser.org_id == graph.org_id,
        OrgUser.role.in_([OrgRole.OWNER, OrgRole.ADMIN]),
      )
      .all()
    )
  if not user_ids:
    return []
  unique_ids = list(dict.fromkeys(user_ids))
  users = db.query(User).filter(User.id.in_(unique_ids), User.is_active.is_(True)).all()
  return [u for u in users if u.email]


@sensor(
  minimum_interval_seconds=21600,  # 6 hours
  default_status=_SENSOR_STATUS,
  description="Monitors graph instance storage and sends email alerts at 80%/100% thresholds",
)
def graph_usage_monitor_sensor(context: SensorEvaluationContext):
  """Check all active user graphs for storage usage and send alerts.

  Query: parent graphs where:
  - is_repository = false (skip shared repos)
  - parent_graph_id IS NULL (only parent graphs, not subgraphs)
  - status = 'active'
  - graph_tier IS NOT NULL (skip untiered internal/test graphs)

  For each graph over threshold:
  - Check Valkey dedup key to avoid repeat alerts
  - Look up graph owner's email
  - Send capacity warning email via SES

  Note: This sensor makes async Graph API calls per graph (1 + N subgraphs).
  With many active graphs, consider the cumulative latency. The 6-hour
  interval keeps this manageable.
  """
  from robosystems.config.parameter_store import get_parameter_value
  from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
  from robosystems.database import session as db_session_factory
  from robosystems.middleware.graph.ingestion_limits import IngestionLimitChecker
  from robosystems.models.core.graph import Graph

  # Read per tick rather than at import, so flipping the flag takes effect on the
  # next evaluation instead of the next deploy.
  if get_parameter_value(_ALERTS_ENABLED_FLAG, "true").lower() != "true":
    return SkipReason(f"Storage alerts disabled via {_ALERTS_ENABLED_FLAG}")

  db = db_session_factory()
  try:
    parent_graphs = (
      db.query(Graph)
      .filter(
        Graph.is_repository.is_(False),
        Graph.parent_graph_id.is_(None),
        Graph.status == "active",
        Graph.graph_tier.isnot(None),
      )
      .all()
    )

    if not parent_graphs:
      return SkipReason("No active user graphs to monitor")

    context.log.info(f"Checking storage usage for {len(parent_graphs)} graphs")

    # Use MCP_CACHE for dedup keys (TTL-based cache, not LOCKS which is for short-lived mutexes)
    redis_client = create_redis_client(ValkeyDatabase.MCP_CACHE)
    alerts_sent = 0

    for graph in parent_graphs:
      graph_tier = graph.graph_tier

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

      # Persist the measurement as a STORAGE_SNAPSHOT row — this sensor is the
      # only writer, and the admin and org usage surfaces read those rows.
      # Recorded for every measured status, not just alert-worthy ones; skipped
      # when the check returned "unknown" (nothing was measured). Attributed to
      # the monitor itself, never to a member (see USAGE_MONITOR_PRINCIPAL).
      if storage_check.get("total_storage_gb") is not None:
        try:
          from robosystems.models.core.graph.graph_usage import GraphUsage

          GraphUsage.record_storage_usage(
            user_id=USAGE_MONITOR_PRINCIPAL,
            graph_id=graph.graph_id,
            graph_tier=graph_tier,
            storage_bytes=storage_check["total_storage_gb"] * (1024**3),
            session=db,
          )
        except Exception as e:
          context.log.warning(
            f"Could not record storage snapshot for {graph.graph_id}: {e}"
          )

      # Only alert for approaching or over_limit
      if instance_status not in _ALERT_STATUSES:
        continue

      recipients = _capacity_alert_recipients(db, graph)
      if not recipients:
        context.log.warning(
          f"No admin with an email for {graph.graph_id}; capacity alert not sent"
        )
        continue

      # Dedup per recipient, not per graph: the key is set only for a user
      # whose delivery succeeded, so an admin whose send failed is retried on
      # the next tick while the ones already told are not emailed again. A
      # single graph-level key set on "any delivery succeeded" silenced the
      # failed recipients for the whole TTL.
      pending = [
        user
        for user in recipients
        if not redis_client.exists(
          _alert_dedup_key(graph.graph_id, instance_status, user.id)
        )
      ]
      if not pending:
        context.log.debug(
          f"Skipping alert for {graph.graph_id} ({instance_status}) — all "
          f"{len(recipients)} admin(s) already alerted"
        )
        continue

      delivered = 0
      for user in pending:
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
            delivered += 1
            redis_client.setex(
              _alert_dedup_key(graph.graph_id, instance_status, user.id),
              _ALERT_DEDUP_TTL_SECONDS,
              "1",
            )
          else:
            context.log.warning(
              f"Failed to send capacity alert for {graph.graph_id} to {user.email}"
            )
        except Exception as e:
          context.log.error(
            f"Error sending capacity alert for {graph.graph_id} to {user.email}: {e}"
          )

      if delivered:
        alerts_sent += 1
        context.log.info(
          f"Sent {instance_status} alert for {graph.graph_id} to {delivered} of "
          f"{len(pending)} pending admin(s) ({storage_check['usage_percentage']:.0f}%)"
        )

    context.log.info(
      f"Usage monitor complete: checked {len(parent_graphs)} graphs, "
      f"sent {alerts_sent} alerts"
    )

    return SkipReason(f"Checked {len(parent_graphs)} graphs, sent {alerts_sent} alerts")

  finally:
    db.close()
