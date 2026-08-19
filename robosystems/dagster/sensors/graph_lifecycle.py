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
  """Find graphs with expired subscriptions and suspend them.

  Query: BillingSubscription where:
  - resource_type = "graph"
  - status IN ("canceled", "failed")
  - ends_at IS NOT NULL AND (ends_at < now() OR cancellation_type = "immediate")
  - Linked graph status = "active" (not already suspended)

  ``failed`` belongs here alongside ``canceled`` for the same reason: both are
  terminal, so neither is a subscription still in force, and a graph left
  running under one is infrastructure nobody is paying for.
  """
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

    # Find expired subscriptions linked to active graphs. An IMMEDIATE cancel
    # suspends regardless of ends_at: the user asked for teardown now, and this
    # is defense-in-depth against ends_at being pushed into the future by a
    # downstream event (e.g. the Stripe subscription.deleted handler). Mirrors
    # the immediate-bypass the deprovision sensor already applies.
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
  """Find suspended graphs past retention and deprovision them.

  Query: Graph where:
  - status = "suspended"
  - deleted_at IS NULL (not already deprovisioned)
  - BillingSubscription.status IN ("canceled", "failed")
  - BillingSubscription.ends_at < (now - retention_days), OR
  - BillingSubscription.cancellation_type == "immediate" (retention bypassed
    when the user explicitly requested immediate teardown)
  """
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

  config = get_deprovisioning_config()
  db = db_session_factory()
  try:
    now = datetime.now(UTC)
    cutoff = now - timedelta(days=config.retention_days)

    # Find suspended user graphs past the retention period, OR any suspended
    # graph whose subscription was canceled immediately (user explicitly
    # asked for fast teardown — retention window doesn't apply).
    # Shared repositories (is_repository=True) are platform-managed and
    # must never be auto-deprovisioned.
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

    # Re-select graphs stranded mid-teardown: deleted_at was stamped (teardown
    # started) but status never reached DEPROVISIONED — a run killed part-way
    # (a Dagster daemon restart on deploy; teardown includes a full backup), a
    # DBAPI error that failed the run, or a database-delete failure that
    # deliberately left the registry intact (deprovision_service). The query
    # above excludes them (deleted_at IS NULL), so nothing would ever retry
    # them and their .lbug / registry entry / tenant rows would sit forever.
    # Gate on deleted_at being older than a threshold so a normal in-flight
    # teardown (minutes) is not double-run; a teardown "leaving" for over an
    # hour is genuinely stuck. deprovision_graph is idempotent on re-run.
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
  """Find subscriptions stuck in `provisioning` and hand them to the reaper.

  Query: BillingSubscription where:
  - status = "provisioning"
  - updated_at < now() - STALE_PROVISIONING_MINUTES

  ``provisioning`` is the state a paid subscription sits in while its resource
  is being built. Nothing revisits it if the attempt dies, and because it is
  neither active nor terminal it is invisible to both lifecycle sensors — so a
  customer who paid can hold a subscription that no process will ever advance
  or clean up. This sensor is the only thing that ends that state.

  The window is the same constant the provisioning claim uses, so a row this
  sensor considers dead is exactly a row a redelivery would have been allowed
  to re-claim. The reaper re-checks status under its own transaction, which
  covers the case where a retry succeeds between this read and that write.

  Deliberately scoped to `provisioning` only. A stalled `upgrading` row is the
  same shape of problem with no safe automatic disposition — the graph exists
  and is serving reads, so writing the subscription off would tear down a
  paying customer's graph, while forcing it back to active would claim a
  migration succeeded when its volume may still be detached. That one wants an
  operator, and the tier task's own timeout is what keeps it rare.
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
