"""Dagster asset: sleep (scale to 0) the SEC shared master after publish.

Terminal step of the nightly incremental chain. Once every artifact is in S3
the master is dead weight (replicas refresh from S3, never from it), so this
clears scale-in protection and scales the ASG to 0. It also runs on the failure
path (via ``sec_master_sleep_on_failure_sensor``) so a broken run never strands
the master awake. Idempotent — a clean no-op when no instance is running.

Gated by ``SHARED_MASTER_PARKING_ENABLED``: when parking is disabled (e.g. a
reserved-instance-backed master that should stay pinned awake and utilized),
this no-ops instead of scaling to 0. The wake side stays ungated so staging
still health-gates on the master before running. Both the happy-path and
failure-path sleep sensors route here, so this one gate covers both.
"""

from dagster import AssetExecutionContext, MaterializeResult, asset

from robosystems.config import env

from .master_parking import sleep_master


@asset(
  group_name="sec_pipeline",
  description="Clear scale-in protection and scale the SEC shared master to 0.",
  kinds={"aws"},
  metadata={"pipeline": "sec", "stage": "master_sleep"},
)
def sec_master_asleep(context: AssetExecutionContext) -> MaterializeResult:
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
