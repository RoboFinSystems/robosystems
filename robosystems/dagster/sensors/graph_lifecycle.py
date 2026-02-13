"""Dagster sensors for graph lifecycle management.

Monitors for graphs with expired subscriptions and transitions them
from active to suspended, then from suspended to deprovisioned after
the retention period.
"""

from dagster import (
  DefaultSensorStatus,
  RunRequest,
  SensorEvaluationContext,
  sensor,
)

from robosystems.dagster.jobs.graph_lifecycle import (
  deprovision_suspended_graphs_job,
  suspend_expired_graphs_job,
)
from robosystems.logger import get_logger

logger = get_logger(__name__)


@sensor(
  job=suspend_expired_graphs_job,
  minimum_interval_seconds=300,  # 5 minutes
  default_status=DefaultSensorStatus.RUNNING,
  description="Suspends graphs whose subscriptions have expired",
)
def expired_graph_subscription_sensor(context: SensorEvaluationContext):
  """Find graphs with expired subscriptions and suspend them.

  Query: BillingSubscription where:
  - resource_type = "graph"
  - status = "canceled"
  - ends_at IS NOT NULL AND ends_at < now()
  - Linked graph status = "active" (not already suspended)
  """
  from datetime import UTC, datetime

  from robosystems.database import session as db_session_factory
  from robosystems.models.billing.subscription import BillingSubscription
  from robosystems.models.iam.graph import Graph, GraphStatus

  db = db_session_factory()
  try:
    now = datetime.now(UTC)

    # Find expired subscriptions linked to active graphs
    expired_subs = (
      db.query(BillingSubscription)
      .join(Graph, BillingSubscription.resource_id == Graph.graph_id)
      .filter(
        BillingSubscription.resource_type == "graph",
        BillingSubscription.status == "canceled",
        BillingSubscription.ends_at.isnot(None),
        BillingSubscription.ends_at < now,
        Graph.status == GraphStatus.ACTIVE.value,
      )
      .all()
    )

    if not expired_subs:
      return

    graph_ids = [sub.resource_id for sub in expired_subs if sub.resource_id]
    context.log.info(
      f"Found {len(graph_ids)} graphs with expired subscriptions to suspend"
    )

    yield RunRequest(
      run_key=f"suspend-{now.strftime('%Y%m%d%H%M')}",
      run_config={
        "ops": {
          "suspend_expired_graphs": {
            "config": {
              "graph_ids": graph_ids,
            }
          }
        }
      },
    )
  finally:
    db_session_factory.remove()


@sensor(
  job=deprovision_suspended_graphs_job,
  minimum_interval_seconds=300,  # 5 minutes
  default_status=DefaultSensorStatus.RUNNING,
  description="Deprovisions suspended graphs past the retention period",
)
def suspended_graph_deprovisioning_sensor(context: SensorEvaluationContext):
  """Find suspended graphs past retention and deprovision them.

  Query: Graph where:
  - status = "suspended"
  - deleted_at IS NULL (not already deprovisioned)
  - BillingSubscription.ends_at < (now - retention_days)
  """
  from datetime import UTC, datetime, timedelta

  from robosystems.config.deprovisioning import get_deprovisioning_config
  from robosystems.database import session as db_session_factory
  from robosystems.models.billing.subscription import BillingSubscription
  from robosystems.models.iam.graph import Graph, GraphStatus

  config = get_deprovisioning_config()
  db = db_session_factory()
  try:
    now = datetime.now(UTC)
    cutoff = now - timedelta(days=config.retention_days)

    # Find suspended user graphs past the retention period.
    # Shared repositories (is_repository=True) are platform-managed and
    # must never be auto-deprovisioned.
    ready_subs = (
      db.query(BillingSubscription)
      .join(Graph, BillingSubscription.resource_id == Graph.graph_id)
      .filter(
        BillingSubscription.resource_type == "graph",
        BillingSubscription.status == "canceled",
        BillingSubscription.ends_at.isnot(None),
        BillingSubscription.ends_at < cutoff,
        Graph.status == GraphStatus.SUSPENDED.value,
        Graph.deleted_at.is_(None),
        Graph.is_repository.is_(False),
      )
      .all()
    )

    if not ready_subs:
      return

    graph_ids = [sub.resource_id for sub in ready_subs if sub.resource_id]
    context.log.info(
      f"Found {len(graph_ids)} suspended graphs ready for deprovisioning"
    )

    yield RunRequest(
      run_key=f"deprovision-{now.strftime('%Y%m%d%H%M')}",
      run_config={
        "ops": {
          "deprovision_suspended_graphs": {
            "config": {
              "graph_ids": graph_ids,
            }
          }
        }
      },
    )
  finally:
    db_session_factory.remove()
