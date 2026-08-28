"""Dagster sensor that turns every failed run into a CloudWatch metric.

Every customer-visible failure that is not "the API is down" runs through
Dagster: QuickBooks sync, materialization, backups, invoicing, the SEC
pipeline. Before this sensor a failed run was a red row in the Dagster UI
and nothing else — a stage that failed two nights running went unnoticed
for four days because nothing watched for it.

One ``RunFailure`` data point per failed run goes to the
``RoboSystems/Dagster/{environment}`` namespace the stack's other detectors
already use, and ``DagsterRunFailureAlarm`` in ``cloudformation/dagster.yaml``
pages on any non-zero sum. A second copy of the point carries a ``Job``
dimension so the console can split failures by job; the alarm reads the
dimensionless copy, because CloudWatch never aggregates a custom metric
across dimensions.
"""

from typing import Any

from dagster import (
  DagsterRunStatus,
  DefaultSensorStatus,
  RunStatusSensorContext,
  run_status_sensor,
)

from robosystems.config import env
from robosystems.logger import get_logger

logger = get_logger(__name__)

METRIC_NAME = "RunFailure"


def metric_namespace() -> str:
  return f"RoboSystems/Dagster/{env.ENVIRONMENT}"


def run_failure_metric_data(job_name: str) -> list[dict[str, Any]]:
  """One dimensionless point for the alarm, one per-job point for triage."""
  return [
    {"MetricName": METRIC_NAME, "Value": 1, "Unit": "Count"},
    {
      "MetricName": METRIC_NAME,
      "Dimensions": [{"Name": "Job", "Value": job_name}],
      "Value": 1,
      "Unit": "Count",
    },
  ]


def publish_run_failure(job_name: str) -> None:
  import boto3

  boto3.client("cloudwatch").put_metric_data(
    Namespace=metric_namespace(),
    MetricData=run_failure_metric_data(job_name),
  )


@run_status_sensor(
  run_status=DagsterRunStatus.FAILURE,
  monitor_all_code_locations=True,
  default_status=DefaultSensorStatus.RUNNING,
  minimum_interval_seconds=60,
  description=(
    "Publishes a RunFailure CloudWatch metric for every failed run, in any "
    "job, so the stack's alarm pages on work not completing."
  ),
)
def run_failure_metric_sensor(context: RunStatusSensorContext) -> None:
  run = context.dagster_run
  job_name = run.job_name
  message = f"DAGSTER RUN FAILED job={job_name} run_id={run.run_id}"
  # The module logger reaches the daemon's CloudWatch stream; the tick log
  # is what the Dagster UI shows against this sensor.
  logger.error(message)
  context.log.error(message)

  if not env.is_aws_environment():
    return

  try:
    publish_run_failure(job_name)
  except Exception as e:
    # The alarm treats missing data as healthy, so a publish that fails is
    # silent downstream and the log line above is the only trace. Raising
    # here would only add the publisher's failure to the one being reported.
    logger.warning(f"Failed to publish {METRIC_NAME} for {job_name}: {e}")
