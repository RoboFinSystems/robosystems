"""Smoke tests for Dagster definitions.

Validates that all Dagster components load without import errors
and are properly registered.
"""

import pytest


class TestDefinitionsLoad:
  """Smoke tests to verify Dagster definitions load correctly."""

  @pytest.mark.unit
  def test_definitions_module_loads(self):
    """Test that the definitions module loads without import errors."""
    from robosystems.dagster.definitions import defs

    assert defs is not None

  @pytest.mark.unit
  def test_all_jobs_registered(self):
    """Test that all expected jobs are registered."""
    from robosystems.dagster.definitions import all_jobs, defs

    assert len(all_jobs) > 0
    assert defs.jobs is not None

    # Verify key jobs are present
    job_names = [job.name for job in all_jobs]
    expected_jobs = [
      "monthly_credit_allocation_job",
      "daily_storage_billing_job",
      "hourly_auth_cleanup_job",
      "provision_graph_job",
      "create_graph_job",
      "backup_graph_job",
      "sec_download_only",
      "sec_process",
      "sec_materialize",
    ]
    for expected in expected_jobs:
      assert expected in job_names, f"Missing job: {expected}"

  @pytest.mark.unit
  def test_all_schedules_registered(self):
    """Test that all expected schedules are registered."""
    from robosystems.dagster.definitions import all_schedules, defs

    assert len(all_schedules) > 0
    assert defs.schedules is not None

    # Verify key schedules are present
    schedule_names = [s.name for s in all_schedules]
    expected_schedules = [
      "monthly_credit_allocation_job_schedule",
      "daily_storage_billing_job_schedule",
      "hourly_auth_cleanup_job_schedule",
    ]
    for expected in expected_schedules:
      assert expected in schedule_names, f"Missing schedule: {expected}"

  @pytest.mark.unit
  def test_all_sensors_registered(self):
    """Test that all expected sensors are registered."""
    from robosystems.dagster.definitions import all_sensors, defs

    assert len(all_sensors) > 0
    assert defs.sensors is not None

    # Verify key sensors are present
    sensor_names = [s.name for s in all_sensors]
    expected_sensors = [
      "pending_subscription_sensor",
      "pending_repository_sensor",
    ]
    for expected in expected_sensors:
      assert expected in sensor_names, f"Missing sensor: {expected}"

  @pytest.mark.unit
  def test_all_assets_registered(self):
    """Test that all expected assets are registered."""
    from robosystems.dagster.definitions import all_assets, defs

    assert len(all_assets) > 0
    assert defs.assets is not None

  @pytest.mark.unit
  def test_resources_configured(self):
    """Test that resources are properly configured."""
    from robosystems.dagster.definitions import resources

    assert "db" in resources
    assert "s3" in resources
    assert "graph" in resources


class TestResourceClasses:
  """Tests for Dagster resource class instantiation."""

  @pytest.mark.unit
  def test_database_resource_class(self):
    """Test DatabaseResource can be instantiated."""
    from robosystems.dagster.resources import DatabaseResource

    resource = DatabaseResource(database_url="postgresql://test:test@localhost/test")
    assert resource is not None

  @pytest.mark.unit
  def test_graph_resource_class(self):
    """Test GraphResource can be instantiated."""
    from robosystems.dagster.resources import GraphResource

    resource = GraphResource(graph_api_url="http://localhost:8001")
    assert resource is not None

  @pytest.mark.unit
  def test_s3_resource_class(self):
    """Test S3Resource can be instantiated."""
    from robosystems.dagster.resources import S3Resource

    resource = S3Resource(bucket_name="test-bucket", region_name="us-east-1")
    assert resource is not None


class TestJobDefinitionsValid:
  """Tests that all job definitions are valid Dagster jobs."""

  @pytest.mark.unit
  def test_all_jobs_have_valid_graphs(self):
    """Test that all jobs have valid execution graphs."""
    from dagster import JobDefinition

    from robosystems.dagster.definitions import all_jobs

    for job in all_jobs:
      # Job name should be set
      assert job.name is not None
      assert len(job.name) > 0

      # Only check node_defs for regular JobDefinitions (not asset jobs)
      if isinstance(job, JobDefinition):
        assert len(job.all_node_defs) >= 1, f"Job {job.name} has no nodes"

  @pytest.mark.unit
  def test_all_schedules_have_valid_cron(self):
    """Test that all schedules have valid cron expressions."""
    from robosystems.dagster.definitions import all_schedules

    for schedule in all_schedules:
      assert schedule.cron_schedule is not None, f"Schedule {schedule.name} has no cron"
      # Cron should have 5 parts (minute hour day month weekday)
      parts = schedule.cron_schedule.split()
      assert len(parts) == 5, (
        f"Schedule {schedule.name} has invalid cron: {schedule.cron_schedule}"
      )
