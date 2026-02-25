"""Tests for SEC pipeline sensors (incremental pipeline chain).

The sec_processing_sensor is covered in test_sec.py.
This file covers the incremental pipeline chain sensors:
- sec_download_to_process_sensor
- sec_incremental_staging_sensor
- sec_stage_to_materialize_sensor
- sec_incremental_download_schedule
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from dagster import (
  DagsterRunStatus,
  RunRequest,
  SkipReason,
  build_schedule_context,
  build_sensor_context,
)

from robosystems.adapters.sec.pipeline.sensors import (
  _get_quarters_to_scan,
  sec_download_to_process_sensor,
  sec_incremental_download_schedule,
  sec_incremental_staging_sensor,
  sec_stage_to_materialize_sensor,
)


def _build_run_status_context(
  sensor_name,
  tags=None,
  run_id="run-123",
  job_name="sec_download",
  instance=None,
  get_runs_return=None,
):
  """Build a RunStatusSensorContext with a DagsterEvent for run_status_sensors."""
  from dagster import DagsterInstance, DagsterRun
  from dagster._core.events import DagsterEvent, DagsterEventType

  dagster_run = DagsterRun(
    job_name=job_name,
    run_id=run_id,
    tags=tags or {},
    status=DagsterRunStatus.SUCCESS,
  )

  dagster_event = DagsterEvent(
    event_type_value=DagsterEventType.RUN_SUCCESS.value,
    job_name=job_name,
  )

  from dagster import build_run_status_sensor_context

  if instance is None:
    instance = DagsterInstance.ephemeral()

  if get_runs_return is not None:
    instance.get_runs = lambda **kwargs: get_runs_return

  context = build_run_status_sensor_context(
    sensor_name=sensor_name,
    dagster_event=dagster_event,
    dagster_run=dagster_run,
    dagster_instance=instance,
  )
  return context


@pytest.mark.unit
class TestSecDownloadToProcessSensor:
  """Tests for sec_download_to_process_sensor."""

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_in_dev_environment(self, mock_env):
    """Test sensor skips in dev environment."""
    mock_env.ENVIRONMENT = "dev"

    context = _build_run_status_context(
      sensor_name="sec_download_to_process_sensor",
      tags={"mode": "incremental", "dagster/partition": "2025-Q1"},
    )

    result = list(sec_download_to_process_sensor(context))
    assert len(result) == 0  # Returns nothing (info log only)

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_non_incremental_runs(self, mock_env):
    """Test sensor skips non-incremental pipeline runs."""
    mock_env.ENVIRONMENT = "prod"

    context = _build_run_status_context(
      sensor_name="sec_download_to_process_sensor",
      tags={"mode": "backfill", "dagster/partition": "2025-Q1"},
    )

    result = list(sec_download_to_process_sensor(context))
    assert len(result) == 0

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_when_no_partition_key(self, mock_env):
    """Test sensor skips when no partition key in tags."""
    mock_env.ENVIRONMENT = "prod"

    context = _build_run_status_context(
      sensor_name="sec_download_to_process_sensor",
      tags={"mode": "incremental"},  # Missing dagster/partition
    )

    result = list(sec_download_to_process_sensor(context))
    assert len(result) == 0

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_yields_run_request_for_incremental(self, mock_env):
    """Test sensor yields RunRequest for incremental download success."""
    mock_env.ENVIRONMENT = "prod"

    from dagster import DagsterInstance

    with DagsterInstance.ephemeral() as instance:
      context = _build_run_status_context(
        sensor_name="sec_download_to_process_sensor",
        job_name="sec_download",
        run_id="run-abc123",
        tags={
          "mode": "incremental",
          "dagster/partition": "2025-Q1",
          "batch_id": "20250225-21",
        },
        instance=instance,
        get_runs_return=[],
      )

      result = list(sec_download_to_process_sensor(context))

    assert len(result) == 1
    assert isinstance(result[0], RunRequest)
    assert result[0].partition_key == "2025-Q1"
    assert result[0].tags["mode"] == "incremental"
    assert result[0].tags["pipeline"] == "sec"

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_when_process_already_running(self, mock_env):
    """Test sensor skips when process job already running for partition."""
    mock_env.ENVIRONMENT = "prod"

    from dagster import DagsterInstance

    active_run = MagicMock()
    active_run.run_id = "active-run"

    with DagsterInstance.ephemeral() as instance:
      context = _build_run_status_context(
        sensor_name="sec_download_to_process_sensor",
        job_name="sec_download",
        run_id="run-abc123",
        tags={
          "mode": "incremental",
          "dagster/partition": "2025-Q1",
        },
        instance=instance,
        get_runs_return=[active_run],
      )

      result = list(sec_download_to_process_sensor(context))

    assert len(result) == 0


@pytest.mark.unit
class TestSecIncrementalStagingSensor:
  """Tests for sec_incremental_staging_sensor."""

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_in_dev_environment(self, mock_env):
    """Test sensor skips in dev environment."""
    mock_env.ENVIRONMENT = "dev"

    context = build_sensor_context()
    result = list(sec_incremental_staging_sensor(context))

    assert len(result) == 1
    assert isinstance(result[0], SkipReason)
    assert "dev" in str(result[0]).lower()

  @patch("robosystems.database.session")
  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_when_pending_files_exist(self, mock_env, mock_session_factory):
    """Test sensor skips when pending files still exist."""
    mock_env.ENVIRONMENT = "prod"

    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_query.filter.return_value = mock_query
    mock_query.count.return_value = 42  # 42 pending files
    mock_session.query.return_value = mock_query
    mock_session_factory.return_value = mock_session

    context = build_sensor_context()
    result = list(sec_incremental_staging_sensor(context))

    assert len(result) == 1
    assert isinstance(result[0], SkipReason)
    assert "42" in str(result[0]) or "pending" in str(result[0]).lower()

  @patch("robosystems.database.session")
  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_when_no_recently_processed(self, mock_env, mock_session_factory):
    """Test sensor skips when no recently processed files."""
    mock_env.ENVIRONMENT = "prod"

    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_query.filter.return_value = mock_query
    # First count() = 0 pending files, second count() = 0 recent files
    mock_query.count.side_effect = [0, 0]
    mock_session.query.return_value = mock_query
    mock_session_factory.return_value = mock_session

    context = build_sensor_context()
    result = list(sec_incremental_staging_sensor(context))

    assert len(result) == 1
    assert isinstance(result[0], SkipReason)
    assert "recently processed" in str(result[0]).lower()

  @patch("robosystems.database.session")
  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_handles_database_error(self, mock_env, mock_session_factory):
    """Test sensor handles database errors gracefully."""
    mock_env.ENVIRONMENT = "prod"
    mock_session_factory.side_effect = Exception("DB connection refused")

    context = build_sensor_context()
    result = list(sec_incremental_staging_sensor(context))

    assert len(result) == 1
    assert isinstance(result[0], SkipReason)
    assert "Database error" in str(result[0])

  @patch("robosystems.database.session")
  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_yields_run_request_when_processing_complete(
    self, mock_env, mock_session_factory
  ):
    """Test sensor yields RunRequest when all pending files are processed."""
    from dagster import DagsterInstance

    mock_env.ENVIRONMENT = "prod"

    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_query.filter.return_value = mock_query
    # First count() = 0 pending, second count() = 100 recently processed
    mock_query.count.side_effect = [0, 100]
    mock_session.query.return_value = mock_query
    mock_session_factory.return_value = mock_session

    with DagsterInstance.ephemeral() as instance:
      with patch.object(instance, "get_runs", return_value=[]):
        context = build_sensor_context(instance=instance)
        result = list(sec_incremental_staging_sensor(context))

    assert len(result) == 1
    assert isinstance(result[0], RunRequest)
    assert result[0].tags["pipeline"] == "sec"
    assert result[0].tags["mode"] == "incremental"


@pytest.mark.unit
class TestSecStageToMaterializeSensor:
  """Tests for sec_stage_to_materialize_sensor."""

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_in_dev_environment(self, mock_env):
    """Test sensor skips in dev environment."""
    mock_env.ENVIRONMENT = "dev"

    context = _build_run_status_context(
      sensor_name="sec_stage_to_materialize_sensor",
      job_name="sec_incremental_stage",
      tags={"mode": "incremental"},
    )

    result = list(sec_stage_to_materialize_sensor(context))
    assert len(result) == 0  # Info log only

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_non_incremental_runs(self, mock_env):
    """Test sensor skips non-incremental runs."""
    mock_env.ENVIRONMENT = "prod"

    context = _build_run_status_context(
      sensor_name="sec_stage_to_materialize_sensor",
      job_name="sec_incremental_stage",
      tags={"mode": "full"},  # Not incremental
    )

    result = list(sec_stage_to_materialize_sensor(context))
    assert len(result) == 0

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_yields_run_request_after_incremental_stage(self, mock_env):
    """Test sensor yields RunRequest after incremental staging."""
    mock_env.ENVIRONMENT = "prod"

    from dagster import DagsterInstance

    with DagsterInstance.ephemeral() as instance:
      context = _build_run_status_context(
        sensor_name="sec_stage_to_materialize_sensor",
        job_name="sec_incremental_stage",
        run_id="run-stage-001",
        tags={"mode": "incremental"},
        instance=instance,
        get_runs_return=[],
      )

      result = list(sec_stage_to_materialize_sensor(context))

    assert len(result) == 1
    assert isinstance(result[0], RunRequest)
    assert result[0].tags["mode"] == "incremental"
    assert result[0].tags["phase"] == "materialize"

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_when_materialize_already_running(self, mock_env):
    """Test sensor skips when materialize job is already running."""
    mock_env.ENVIRONMENT = "prod"

    from dagster import DagsterInstance

    active_run = MagicMock()
    active_run.run_id = "active-mat-run"

    with DagsterInstance.ephemeral() as instance:
      context = _build_run_status_context(
        sensor_name="sec_stage_to_materialize_sensor",
        job_name="sec_incremental_stage",
        run_id="run-stage-001",
        tags={"mode": "incremental"},
        instance=instance,
        get_runs_return=[active_run],
      )

      result = list(sec_stage_to_materialize_sensor(context))

    assert len(result) == 0


@pytest.mark.unit
class TestSecIncrementalDownloadSchedule:
  """Tests for sec_incremental_download_schedule."""

  @patch("robosystems.adapters.sec.pipeline.sensors._get_quarters_to_scan")
  def test_yields_run_requests_per_quarter(self, mock_quarters):
    """Test schedule yields one RunRequest per quarter."""
    mock_quarters.return_value = ["2025-Q1"]

    context = build_schedule_context(
      scheduled_execution_time=datetime(2025, 2, 25, 21, 0, tzinfo=UTC)
    )

    result = list(sec_incremental_download_schedule(context))

    assert len(result) == 1
    assert isinstance(result[0], RunRequest)
    assert result[0].partition_key == "2025-Q1"
    assert result[0].tags["mode"] == "incremental"

  @patch("robosystems.adapters.sec.pipeline.sensors._get_quarters_to_scan")
  def test_yields_multiple_quarters_at_boundary(self, mock_quarters):
    """Test schedule yields two RunRequests at quarter boundary."""
    mock_quarters.return_value = ["2025-Q1", "2024-Q4"]

    context = build_schedule_context(
      scheduled_execution_time=datetime(2025, 1, 2, 21, 0, tzinfo=UTC)
    )

    result = list(sec_incremental_download_schedule(context))

    assert len(result) == 2
    partition_keys = {r.partition_key for r in result}
    assert partition_keys == {"2025-Q1", "2024-Q4"}

  @patch("robosystems.adapters.sec.pipeline.sensors._get_quarters_to_scan")
  def test_run_keys_include_batch_id(self, mock_quarters):
    """Test run keys include the batch_id for tracking."""
    mock_quarters.return_value = ["2025-Q1"]

    context = build_schedule_context(
      scheduled_execution_time=datetime(2025, 2, 25, 21, 0, tzinfo=UTC)
    )

    result = list(sec_incremental_download_schedule(context))

    assert result[0].run_key is not None
    assert "sec-incremental" in result[0].run_key
    assert result[0].tags.get("batch_id") is not None

  @patch("robosystems.adapters.sec.pipeline.sensors._get_quarters_to_scan")
  def test_includes_skip_existing_config(self, mock_quarters):
    """Test schedule includes skip_existing=True in config."""
    mock_quarters.return_value = ["2025-Q1"]

    context = build_schedule_context(
      scheduled_execution_time=datetime(2025, 2, 25, 21, 0, tzinfo=UTC)
    )

    result = list(sec_incremental_download_schedule(context))

    run_config = result[0].run_config
    filing_config = run_config["ops"]["sec_raw_filings"]["config"]
    assert filing_config["skip_existing"] is True


@pytest.mark.unit
class TestGetQuartersToScan:
  """Tests for _get_quarters_to_scan helper."""

  @patch("robosystems.adapters.sec.get_quarters_to_scan")
  def test_delegates_to_adapter(self, mock_get_quarters):
    """Test _get_quarters_to_scan delegates to the SEC adapter."""
    mock_get_quarters.return_value = ["2025-Q1"]

    result = _get_quarters_to_scan()

    assert result == ["2025-Q1"]
    mock_get_quarters.assert_called_once()

  @patch("robosystems.adapters.sec.get_quarters_to_scan")
  def test_returns_list_of_partition_keys(self, mock_get_quarters):
    """Test returns properly formatted partition keys."""
    mock_get_quarters.return_value = ["2025-Q1", "2024-Q4"]

    result = _get_quarters_to_scan()

    assert isinstance(result, list)
    assert len(result) == 2
    for key in result:
      parts = key.split("-Q")
      assert len(parts) == 2
