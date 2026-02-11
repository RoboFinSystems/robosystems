"""Dagster sensor for graph lifecycle management.

Monitors for graphs with expired subscriptions and transitions them
from active to suspended.
"""

from dagster import (
  DefaultSensorStatus,
  RunRequest,
  SensorEvaluationContext,
  sensor,
)

from robosystems.dagster.jobs.graph_lifecycle import suspend_expired_graphs_job
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
