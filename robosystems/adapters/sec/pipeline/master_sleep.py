"""Dagster asset: sleep (scale to 0) the SEC shared master after publish.

Terminal step of the nightly incremental chain. Once every artifact is in S3
the master is dead weight (replicas refresh from S3, never from it), so this
clears scale-in protection and scales the ASG to 0. It also runs on the failure
path (via ``sec_master_sleep_on_failure_sensor``) so a broken run never strands
the master awake. Idempotent — a clean no-op when no instance is running.
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
    return MaterializeResult(metadata={"status": "skipped"})

  result = sleep_master()
  context.log.info(f"Shared master asleep: {result}")
  return MaterializeResult(metadata={"instance_id": result.get("instance_id") or ""})
