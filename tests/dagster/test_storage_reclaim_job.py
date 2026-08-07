"""Tests for the daily storage reclaim job."""

import pytest
from dagster import JobDefinition


class TestStorageReclaimJobDefinition:
  """Structure and registration of the storage reclaim job."""

  @pytest.mark.unit
  def test_job_has_one_op(self):
    from robosystems.dagster.jobs.storage_reclaim import daily_storage_reclaim_job

    assert isinstance(daily_storage_reclaim_job, JobDefinition)
    op_names = {op_def.name for op_def in daily_storage_reclaim_job.all_node_defs}
    assert op_names == {"reclaim_instance_storage_sweep"}

  @pytest.mark.unit
  def test_job_has_correct_tags(self):
    from robosystems.dagster.jobs.storage_reclaim import daily_storage_reclaim_job

    assert daily_storage_reclaim_job.tags.get("dagster/priority") == "1"
    assert daily_storage_reclaim_job.tags.get("dagster/max_retries") == "3"

  @pytest.mark.unit
  def test_schedule_cron(self):
    """Daily at 5:30 UTC — after backup cleanup, off business hours."""
    from robosystems.dagster.jobs.storage_reclaim import (
      daily_storage_reclaim_schedule,
    )

    assert daily_storage_reclaim_schedule.cron_schedule == "30 5 * * *"

  @pytest.mark.unit
  def test_schedule_targets_correct_job(self):
    from robosystems.dagster.jobs.storage_reclaim import (
      daily_storage_reclaim_job,
      daily_storage_reclaim_schedule,
    )

    assert daily_storage_reclaim_schedule.job_name == daily_storage_reclaim_job.name

  @pytest.mark.unit
  def test_job_and_schedule_registered_in_definitions(self):
    from robosystems.dagster.definitions import all_jobs, all_schedules
    from robosystems.dagster.jobs.storage_reclaim import (
      daily_storage_reclaim_job,
      daily_storage_reclaim_schedule,
    )

    assert daily_storage_reclaim_job in all_jobs
    assert daily_storage_reclaim_schedule in all_schedules
