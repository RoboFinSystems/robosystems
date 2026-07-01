"""Dagster asset: wake the SEC shared master before staging.

First master-dependent step of the nightly incremental chain. Scales the shared
master ASG to 1 and blocks until it is genuinely healthy (EC2 running + SEC
volume reattached + registered healthy + live /health). A wake that cannot
reach healthy raises ``Failure`` — that failure IS the wake alarm, and it
prevents any downstream master-dependent step from starting against a
missing/half-ready master.
"""

import asyncio

from dagster import AssetExecutionContext, Failure, MaterializeResult, asset

from robosystems.config import env

from .master_parking import MasterWakeTimeout, wake_master


@asset(
  group_name="sec_pipeline",
  description="Scale the SEC shared master to 1 and wait until it is healthy.",
  kinds={"aws"},
  metadata={"pipeline": "sec", "stage": "master_wake"},
)
def sec_master_awake(context: AssetExecutionContext) -> MaterializeResult:
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
