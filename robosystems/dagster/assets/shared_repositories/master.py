"""Dagster assets: park (wake / sleep) the shared master around a pipeline run.

The shared master is a single writer instance that hosts the platform's shared
repositories (SEC today). Adapter pipelines wake it before the first
master-dependent step and sleep it once every artifact is published to S3, so
the writer only runs while there is work for it.

- ``shared_master_awake`` scales the master ASG to 1 and blocks until it is
  genuinely healthy (EC2 running + data volume reattached + registered healthy +
  live /health). A wake that cannot reach healthy raises ``Failure`` — that
  failure IS the wake alarm, and it prevents any downstream master-dependent
  step from starting against a missing/half-ready master.
- ``shared_master_asleep`` clears scale-in protection and scales the ASG to 0.
  It also runs on the failure path so a broken run never strands the master
  awake. Idempotent — a clean no-op when no instance is running. Gated by
  ``SHARED_MASTER_PARKING_ENABLED``: when parking is disabled (e.g. a
  reserved-instance-backed master that should stay pinned awake and utilized),
  it no-ops instead of scaling to 0. The wake side stays ungated so staging
  still health-gates on the master before running.

The dev/test skip lives here in the asset layer; ``master_parking`` always
executes its logic so it stays unit-testable against mocked boto3.
"""

import asyncio

from dagster import AssetExecutionContext, Failure, MaterializeResult, asset

from robosystems.config import env

from .master_parking import MasterWakeTimeout, sleep_master, wake_master


@asset(
  group_name="shared_repositories",
  description="Scale the shared master to 1 and wait until it is healthy.",
  kinds={"aws"},
  metadata={"stage": "master_wake"},
)
def shared_master_awake(context: AssetExecutionContext) -> MaterializeResult:
  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping master wake in dev environment")
    return MaterializeResult(metadata={"status": "skipped"})

  try:
    result = asyncio.run(wake_master())
  except MasterWakeTimeout as exc:
    raise Failure(description=str(exc)) from exc

  context.log.info(f"Shared master awake: {result}")
  return MaterializeResult(
    metadata={
      "instance_id": result.get("instance_id", ""),
      "private_ip": result.get("private_ip", ""),
    }
  )


@asset(
  group_name="shared_repositories",
  description="Clear scale-in protection and scale the shared master to 0.",
  kinds={"aws"},
  metadata={"stage": "master_sleep"},
)
def shared_master_asleep(context: AssetExecutionContext) -> MaterializeResult:
  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping master sleep in dev environment")
    return MaterializeResult(
      metadata={"status": "skipped", "reason": "dev_environment"}
    )

  if not env.SHARED_MASTER_PARKING_ENABLED:
    context.log.info(
      "Master parking disabled (SHARED_MASTER_PARKING_ENABLED=false); "
      "leaving the shared master running"
    )
    return MaterializeResult(
      metadata={"status": "skipped", "reason": "parking_disabled"}
    )

  result = sleep_master()
  context.log.info(f"Shared master asleep: {result}")
  return MaterializeResult(metadata={"instance_id": result.get("instance_id") or ""})
