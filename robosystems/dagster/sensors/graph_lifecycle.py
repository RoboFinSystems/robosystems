"""Sensors that suspend graphs whose subscriptions ended, deprovision them after
retention, and reap stalled provisioning."""

from dagster import (
  DefaultSensorStatus,
  RunRequest,
  SensorEvaluationContext,
  sensor,
)

from robosystems.dagster.jobs.graph_lifecycle import (
  deprovision_suspended_graphs_job,
  reap_stalled_provisioning_job,
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
  """Suspend active graphs whose subscription is terminal (canceled or failed)
  and past ends_at, or was canceled immediately."""
  from datetime import UTC, datetime

  from sqlalchemy import or_

  from robosystems.database import session as db_session_factory
  from robosystems.models.core.billing.subscription import (
    TERMINAL_SUBSCRIPTION_STATUSES,
    BillingSubscription,
    CancellationType,
  )
  from robosystems.models.core.graph import Graph, GraphStatus

  db = db_session_factory()
  try:
    now = datetime.now(UTC)

    # An IMMEDIATE cancel suspends regardless of ends_at, in case a later
    # event pushed ends_at into the future.
    expired_subs = (
      db.query(BillingSubscription)
      .join(Graph, BillingSubscription.resource_id == Graph.graph_id)
      .filter(
        BillingSubscription.resource_type == "graph",
        BillingSubscription.status.in_(TERMINAL_SUBSCRIPTION_STATUSES),
        BillingSubscription.ends_at.isnot(None),
        or_(
          BillingSubscription.ends_at < now,
          BillingSubscription.cancellation_type == CancellationType.IMMEDIATE.value,
        ),
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
  """Deprovision suspended graphs past retention (immediate cancels bypass it),
  and retry teardowns stranded part-way."""
  from datetime import UTC, datetime, timedelta

  from sqlalchemy import or_

  from robosystems.config.deprovisioning import get_deprovisioning_config
  from robosystems.database import session as db_session_factory
  from robosystems.models.core.billing.subscription import (
    TERMINAL_SUBSCRIPTION_STATUSES,
    BillingSubscription,
    CancellationType,
  )
  from robosystems.models.core.graph import Graph, GraphStatus
  from robosystems.operations.graph.deprovision_service import RESIDUAL_PENDING_KEY

  config = get_deprovisioning_config()
  db = db_session_factory()
  try:
    now = datetime.now(UTC)
    cutoff = now - timedelta(days=config.retention_days)

    # Shared repositories must never be auto-deprovisioned.
    ready_subs = (
      db.query(BillingSubscription)
      .join(Graph, BillingSubscription.resource_id == Graph.graph_id)
      .filter(
        BillingSubscription.resource_type == "graph",
        BillingSubscription.status.in_(TERMINAL_SUBSCRIPTION_STATUSES),
        BillingSubscription.ends_at.isnot(None),
        or_(
          BillingSubscription.ends_at < cutoff,
          BillingSubscription.cancellation_type == CancellationType.IMMEDIATE.value,
        ),
        Graph.status == GraphStatus.SUSPENDED.value,
        Graph.deleted_at.is_(None),
        Graph.is_repository.is_(False),
      )
      .all()
    )

    graph_ids = [sub.resource_id for sub in ready_subs if sub.resource_id]

    # Stranded mid-teardown: deleted_at stamped but never DEPROVISIONED (run
    # killed, or a delete failure left the registry intact). The query above
    # excludes them. The 1h gate avoids double-running an in-flight teardown;
    # deprovision_graph is idempotent.
    stranded_cutoff = now - timedelta(hours=1)
    stranded = (
      db.query(Graph.graph_id)
      .filter(
        Graph.deleted_at.isnot(None),
        Graph.deleted_at < stranded_cutoff,
        Graph.status != GraphStatus.DEPROVISIONED.value,
        Graph.is_repository.is_(False),
      )
      .all()
    )
    stranded_ids = [row[0] for row in stranded if row[0]]

    # Deprovisioned, but a data-disposal step (tenant schema, search entries,
    # report bundles) failed; deprovision_graph re-runs only those steps.
    residual = (
      db.query(Graph.graph_id)
      .filter(
        Graph.status == GraphStatus.DEPROVISIONED.value,
        Graph.deleted_at < stranded_cutoff,
        Graph.graph_metadata[RESIDUAL_PENDING_KEY].as_boolean().is_(True),
      )
      .all()
    )
    stranded_ids += [row[0] for row in residual if row[0]]
    if stranded_ids:
      context.log.info(
        f"Found {len(stranded_ids)} graphs stranded mid-teardown to retry"
      )
    # Dedup: a stranded graph could also match ready_subs.
    graph_ids = list(dict.fromkeys(graph_ids + stranded_ids))

    if not graph_ids:
      return

    context.log.info(
      f"Found {len(graph_ids)} graphs ready for deprovisioning "
      f"({len(stranded_ids)} stranded)"
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


@sensor(
  job=reap_stalled_provisioning_job,
  minimum_interval_seconds=300,  # 5 minutes
  default_status=DefaultSensorStatus.RUNNING,
  description="Writes off subscriptions stuck mid-provisioning",
)
def stalled_provisioning_sensor(context: SensorEvaluationContext):
  """Hand subscriptions stuck in `provisioning` to the reaper.

  Nothing else ends that state if an attempt dies. The window is the claim's own
  staleness constant, and the reaper re-checks status in its transaction.
  Stalled `upgrading` rows are out of scope: the graph is serving, so neither
  write-off nor forcing active is safe; that needs an operator.
  """
  from datetime import UTC, datetime, timedelta

  from robosystems.database import session as db_session_factory
  from robosystems.models.core.billing.subscription import (
    STALE_PROVISIONING_MINUTES,
    BillingSubscription,
  )

  db = db_session_factory()
  try:
    now = datetime.now(UTC)
    cutoff = now - timedelta(minutes=STALE_PROVISIONING_MINUTES)

    stalled = (
      db.query(BillingSubscription)
      .filter(
        BillingSubscription.status == "provisioning",
        BillingSubscription.updated_at < cutoff,
      )
      .all()
    )

    if not stalled:
      return

    subscription_ids = [sub.id for sub in stalled]
    context.log.warning(
      f"Found {len(subscription_ids)} subscriptions stalled in provisioning "
      f"for more than {STALE_PROVISIONING_MINUTES} minutes"
    )

    yield RunRequest(
      run_key=f"reap-stalled-{now.strftime('%Y%m%d%H%M')}",
      run_config={
        "ops": {
          "reap_stalled_provisioning": {
            "config": {
              "subscription_ids": subscription_ids,
            }
          }
        }
      },
    )
  finally:
    db_session_factory.remove()
