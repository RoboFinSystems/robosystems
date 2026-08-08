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
      "cleanup_instance_backups",
      "cleanup_orphaned_backups",
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


class TestOrphanedBackupCleanup:
  """The orphan sweep removes S3 objects, so its guards matter more than its
  happy path: it must never touch a tracked key, and it must resolve every
  ambiguity toward keeping data."""

  @staticmethod
  def _run(objects, tracked, tier="ladybug-standard", graph_exists=True):
    from unittest.mock import MagicMock, patch

    from dagster import build_op_context

    from robosystems.dagster.jobs.backup_cleanup import cleanup_orphaned_backups

    session = MagicMock()

    def _query(col):
      q = MagicMock()
      q.filter.return_value = [(k,) for k in tracked]
      return q

    session.query.side_effect = _query
    db = MagicMock()
    db.get_session.return_value.__enter__ = lambda *_: session
    db.get_session.return_value.__exit__ = lambda *_: False

    s3 = MagicMock()
    s3.bucket = "test-bucket"
    s3.client.get_paginator.return_value.paginate.return_value = [{"Contents": objects}]

    graph = MagicMock()
    graph.graph_tier = tier

    with patch(
      "robosystems.models.core.Graph.get_by_id",
      return_value=graph if graph_exists else None,
    ):
      result = cleanup_orphaned_backups(build_op_context(), db, s3)

    deleted = [
      o["Key"]
      for call in s3.client.delete_objects.call_args_list
      for o in call.kwargs["Delete"]["Objects"]
    ]
    return result, deleted

  @staticmethod
  def _obj(key, days_old):
    from datetime import UTC, datetime, timedelta

    return {
      "Key": key,
      "LastModified": datetime.now(UTC) - timedelta(days=days_old),
    }

  @pytest.mark.unit
  def test_deletes_orphan_past_tier_retention(self):
    """A standard-tier orphan expires at 7 days, not at the 90-day lifecycle."""
    key = "graph-backups/databases/kg1/full/backup-20260101_000000.lbug.zip"
    result, deleted = self._run([self._obj(key, 30)], tracked=set())

    assert deleted == [key]
    assert result["deleted_count"] == 1

  @pytest.mark.unit
  def test_keeps_orphan_inside_tier_retention(self):
    key = "graph-backups/databases/kg1/full/backup-20260101_000000.lbug.zip"
    result, deleted = self._run([self._obj(key, 3)], tracked=set())

    assert deleted == []
    assert result["deleted_count"] == 0

  @pytest.mark.unit
  def test_never_touches_a_tracked_key(self):
    """Tracked objects belong to op 1 — deleting them here would race it."""
    key = "graph-backups/databases/kg1/full/backup-20260101_000000.lbug.zip"
    result, deleted = self._run([self._obj(key, 3650)], tracked={key})

    assert deleted == []
    assert result["skipped_tracked"] == 1

  @pytest.mark.unit
  def test_unknown_graph_falls_back_to_the_90_day_maximum(self):
    """An unresolvable graph must not be treated as short-retention."""
    key = "graph-backups/databases/ghost/full/backup-20260101_000000.lbug.zip"
    _, kept = self._run([self._obj(key, 30)], tracked=set(), graph_exists=False)
    assert kept == []

    _, swept = self._run([self._obj(key, 120)], tracked=set(), graph_exists=False)
    assert swept == [key]

  @pytest.mark.unit
  def test_unparseable_key_is_left_in_place(self):
    _, deleted = self._run([self._obj("graph-backups/databases/", 3650)], tracked=set())
    assert deleted == []
