"""The run-failure sensor is the alarm behind "work not completing": one
RunFailure point per failed run, in the namespace the stack alarms on, for
every job — and its own errors never turn one failure into two."""

from unittest.mock import MagicMock, patch

import pytest
from dagster import (
  DagsterEvent,
  DagsterEventType,
  DagsterInstance,
  DagsterRun,
  DagsterRunStatus,
  DefaultSensorStatus,
  build_run_status_sensor_context,
)

from robosystems.config import EnvConfig
from robosystems.dagster.sensors.run_failure import (
  METRIC_NAME,
  run_failure_metric_data,
  run_failure_metric_sensor,
)


def _env(environment: str) -> type[EnvConfig]:
  """A stand-in for the sensor module's ``env`` with one ENVIRONMENT.

  Patched in as a class so ``ENVIRONMENT`` and the real
  ``is_aws_environment()`` classmethod resolve from the same place. Patching
  the shared instance is not enough: an earlier test's monkeypatch can leave
  an instance attribute shadowing the class one, and the two then disagree.
  """
  return type("StubEnv", (EnvConfig,), {"ENVIRONMENT": environment})


def _failed_run_context(job_name: str = "backup_graph_job"):
  run = DagsterRun(job_name=job_name, run_id="run-1", status=DagsterRunStatus.FAILURE)
  event = DagsterEvent(
    event_type_value=DagsterEventType.RUN_FAILURE.value, job_name=job_name
  )
  return build_run_status_sensor_context(
    sensor_name=run_failure_metric_sensor.name,
    dagster_event=event,
    dagster_instance=DagsterInstance.ephemeral(),
    dagster_run=run,
  )


@pytest.mark.unit
def test_registered_running_and_watching_every_job():
  from robosystems.dagster.definitions import all_sensors

  assert run_failure_metric_sensor in all_sensors
  assert run_failure_metric_sensor.default_status == DefaultSensorStatus.RUNNING
  assert run_failure_metric_sensor._monitor_all_code_locations is True  # pyright: ignore[reportPrivateUsage]


@pytest.mark.unit
def test_alarm_point_is_dimensionless_and_triage_point_names_the_job():
  points = run_failure_metric_data("sec_materialize")

  assert points[0] == {"MetricName": METRIC_NAME, "Value": 1, "Unit": "Count"}
  assert points[1]["Dimensions"] == [{"Name": "Job", "Value": "sec_materialize"}]
  assert points[1]["Value"] == 1


@pytest.mark.unit
def test_failed_run_publishes_to_the_dagster_namespace():
  cloudwatch = MagicMock()
  with (
    patch("robosystems.dagster.sensors.run_failure.env", _env("prod")),
    patch("boto3.client", return_value=cloudwatch) as client,
  ):
    run_failure_metric_sensor(_failed_run_context("extensions_materialize_job"))

  client.assert_called_once_with("cloudwatch")
  cloudwatch.put_metric_data.assert_called_once_with(
    Namespace="RoboSystems/Dagster/prod",
    MetricData=run_failure_metric_data("extensions_materialize_job"),
  )


@pytest.mark.unit
def test_no_cloudwatch_outside_aws():
  with (
    patch("robosystems.dagster.sensors.run_failure.env", _env("dev")),
    patch("boto3.client") as client,
  ):
    run_failure_metric_sensor(_failed_run_context())

  client.assert_not_called()


@pytest.mark.unit
def test_publish_error_never_fails_the_tick():
  cloudwatch = MagicMock()
  cloudwatch.put_metric_data.side_effect = RuntimeError("cloudwatch down")
  with (
    patch("robosystems.dagster.sensors.run_failure.env", _env("staging")),
    patch("boto3.client", return_value=cloudwatch),
  ):
    assert run_failure_metric_sensor(_failed_run_context()) is None
