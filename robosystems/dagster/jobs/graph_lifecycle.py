"""Dagster jobs that suspend, deprovision, and reap stalled provisioning for graphs."""

from dagster import Config, OpExecutionContext, job, op

from robosystems.dagster.resources import DatabaseResource
from robosystems.logger import get_logger

logger = get_logger(__name__)


class SuspendGraphsConfig(Config):
  """Configuration for suspending expired graphs."""

  graph_ids: list[str]


@op
def suspend_expired_graphs(
  context: OpExecutionContext,
  db: DatabaseResource,
  config: SuspendGraphsConfig,
) -> dict:
  """Suspend graphs whose subscriptions have expired."""
  from robosystems.models.core.graph import Graph, GraphStatus

  suspended_count = 0

  with db.get_session() as session:
    for graph_id in config.graph_ids:
      graph = Graph.get_by_id(graph_id, session)
      if not graph:
        context.log.warning(f"Graph {graph_id} not found, skipping")
        continue

      if graph.status != GraphStatus.ACTIVE.value:
        context.log.info(f"Graph {graph_id} is already {graph.status}, skipping")
        continue

      graph.transition_status(GraphStatus.SUSPENDED, session)
      suspended_count += 1
      context.log.info(f"Suspended graph {graph_id}")

  context.log.info(f"Suspended {suspended_count}/{len(config.graph_ids)} graphs")

  return {
    "suspended_count": suspended_count,
    "total_requested": len(config.graph_ids),
  }


@job(
  tags={
    "dagster/priority": "1",
  }
)
def suspend_expired_graphs_job():
  """Suspend graphs with expired subscriptions."""
  suspend_expired_graphs()


# ============================================================================
# Deprovisioning
# ============================================================================


class DeprovisionGraphsConfig(Config):
  """Configuration for deprovisioning suspended graphs."""

  graph_ids: list[str]


@op
def deprovision_suspended_graphs(
  context: OpExecutionContext,
  db: DatabaseResource,
  config: DeprovisionGraphsConfig,
) -> dict:
  """Deprovision graphs that have been suspended past the retention period."""
  import asyncio

  from robosystems.config import env
  from robosystems.operations.graph.deprovision_service import (
    GraphDeprovisionService,
  )

  service = GraphDeprovisionService(environment=env.ENVIRONMENT)
  deprovisioned_count = 0
  errors: list[str] = []

  with db.get_session() as session:
    for graph_id in config.graph_ids:
      try:
        result = asyncio.run(
          service.deprovision_graph(graph_id, session, create_backup=True)
        )
        if result.status in ("success", "partial"):
          deprovisioned_count += 1
          context.log.info(f"Deprovisioned graph {graph_id} (status={result.status})")
          if result.errors:
            for err in result.errors:
              context.log.warning(f"  {graph_id}: {err}")
        else:
          context.log.info(f"Skipped graph {graph_id} (status={result.status})")
      except Exception as e:
        error_msg = f"Failed to deprovision {graph_id}: {e}"
        errors.append(error_msg)
        context.log.error(error_msg)
        # A DBAPI error poisons the shared session; roll back so the next
        # graph isn't hit by PendingRollbackError. The failed graph stays
        # stranded for the sensor to retry.
        try:
          session.rollback()
        except Exception:
          context.log.warning(f"Session rollback after {graph_id} failure failed")

  context.log.info(
    f"Deprovisioned {deprovisioned_count}/{len(config.graph_ids)} graphs"
  )

  return {
    "deprovisioned_count": deprovisioned_count,
    "total_requested": len(config.graph_ids),
    "errors": errors,
  }


@job(
  tags={
    "dagster/priority": "1",
  }
)
def deprovision_suspended_graphs_job():
  """Deprovision graphs that have been suspended past retention period."""
  deprovision_suspended_graphs()


# ============================================================================
# Stalled provisioning
# ============================================================================


class ReapStalledProvisioningConfig(Config):
  """Configuration for writing off stalled provisioning attempts."""

  subscription_ids: list[str]


@op
def reap_stalled_provisioning(
  context: OpExecutionContext,
  db: DatabaseResource,
  config: ReapStalledProvisioningConfig,
) -> dict:
  """Write off subscriptions stuck mid-provisioning.

  ``failed`` is terminal and visible to the lifecycle sensors, so any graph the
  attempt created is suspended and reclaimed on the normal retention schedule,
  and the customer can check out afresh.
  """
  from robosystems.models.core.billing.subscription import BillingSubscription

  reaped: list[str] = []

  with db.get_session() as session:
    for subscription_id in config.subscription_ids:
      subscription = (
        session.query(BillingSubscription)
        .filter(BillingSubscription.id == subscription_id)
        .first()
      )
      if not subscription:
        context.log.warning(f"Subscription {subscription_id} not found, skipping")
        continue

      if subscription.status != "provisioning":
        context.log.info(
          f"Subscription {subscription_id} is now {subscription.status}, skipping"
        )
        continue

      # Re-checks staleness atomically: a redelivery may have re-claimed the
      # row since the sensor's read.
      if not subscription.write_off_stalled_provisioning(session):
        context.log.info(
          f"Subscription {subscription_id} completed or was re-claimed since "
          "the sensor's read, skipping"
        )
        continue

      reaped.append(subscription_id)
      # Matched by the StalledProvisioningWriteOff metric filter; this is the
      # only page for a paid customer with no resource.
      context.log.error(
        f"STALLED PROVISIONING WRITTEN OFF: subscription {subscription_id} "
        f"(org={subscription.org_id}, resource_type={subscription.resource_type}, "
        f"resource_id={subscription.resource_id}). The customer paid and has no "
        "working resource — this needs an operator."
      )

  context.log.info(
    f"Wrote off {len(reaped)}/{len(config.subscription_ids)} stalled subscriptions"
  )

  return {
    "reaped_count": len(reaped),
    "total_requested": len(config.subscription_ids),
    "subscription_ids": reaped,
  }


@job(
  tags={
    "dagster/priority": "1",
  }
)
def reap_stalled_provisioning_job():
  """Write off subscriptions stuck mid-provisioning."""
  reap_stalled_provisioning()
