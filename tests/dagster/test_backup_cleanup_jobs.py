"""Tests for the daily backup cleanup job."""

import pytest
from dagster import JobDefinition


class TestBackupCleanupJobDefinition:
  """Tests for backup cleanup job structure and registration."""

  @pytest.mark.unit
  def test_job_has_three_ops(self):
    """Test that the job graph contains exactly 3 ops."""
    from robosystems.dagster.jobs.backup_cleanup import daily_backup_cleanup_job

    assert isinstance(daily_backup_cleanup_job, JobDefinition)
    assert len(daily_backup_cleanup_job.all_node_defs) == 3

  @pytest.mark.unit
  def test_job_op_names(self):
    """Test that the job contains the expected ops."""
    from robosystems.dagster.jobs.backup_cleanup import daily_backup_cleanup_job

    op_names = {op_def.name for op_def in daily_backup_cleanup_job.all_node_defs}
    assert op_names == {
      "cleanup_tracked_backups",
      "cleanup_shared_repo_backups",
      "cleanup_instance_backups",
    }

  @pytest.mark.unit
  def test_job_has_correct_tags(self):
    """Test that the job has priority and retry tags."""
    from robosystems.dagster.jobs.backup_cleanup import daily_backup_cleanup_job

    assert daily_backup_cleanup_job.tags.get("dagster/priority") == "1"
    assert daily_backup_cleanup_job.tags.get("dagster/max_retries") == "3"

  @pytest.mark.unit
  def test_schedule_cron(self):
    """Test that the schedule runs daily at 5 AM UTC."""
    from robosystems.dagster.jobs.backup_cleanup import (
      daily_backup_cleanup_schedule,
    )

    assert daily_backup_cleanup_schedule.cron_schedule == "0 5 * * *"

  @pytest.mark.unit
  def test_schedule_targets_correct_job(self):
    """Test that the schedule targets the cleanup job."""
    from robosystems.dagster.jobs.backup_cleanup import (
      daily_backup_cleanup_job,
      daily_backup_cleanup_schedule,
    )

    assert daily_backup_cleanup_schedule.job_name == daily_backup_cleanup_job.name

  @pytest.mark.unit
  def test_job_registered_in_definitions(self):
    """Test that the job is registered in Dagster definitions."""
    from robosystems.dagster.definitions import all_jobs

    job_names = [j.name for j in all_jobs]
    assert "daily_backup_cleanup_job" in job_names

  @pytest.mark.unit
  def test_schedule_registered_in_definitions(self):
    """Test that the schedule is registered in Dagster definitions."""
    from robosystems.dagster.definitions import all_schedules

    schedule_names = [s.name for s in all_schedules]
    assert "daily_backup_cleanup_job_schedule" in schedule_names
