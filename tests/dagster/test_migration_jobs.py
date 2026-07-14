"""Tests for the LadybugDB migration jobs — cleanup job + import cleanup gate."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from dagster import JobDefinition, build_op_context

from robosystems.dagster.jobs.migration import (
  MigrationCleanupConfig,
  MigrationImportConfig,
  _cleanup_instance,
  _import_instance,
  cleanup_all_instances,
  ladybug_migration_cleanup_job,
)

MIGRATION = "robosystems.dagster.jobs.migration"


def _fake_client(**methods):
  client = MagicMock()
  for name, retval in methods.items():
    setattr(client, name, AsyncMock(return_value=retval))
  return client


class TestCleanupJobDefinition:
  @pytest.mark.unit
  def test_job_is_defined_with_single_op(self):
    assert isinstance(ladybug_migration_cleanup_job, JobDefinition)
    op_names = {op.name for op in ladybug_migration_cleanup_job.all_node_defs}
    assert op_names == {"cleanup_all_instances"}

  @pytest.mark.unit
  def test_job_priority_tag(self):
    assert ladybug_migration_cleanup_job.tags.get("dagster/priority") == "1"

  @pytest.mark.unit
  def test_registered_in_definitions(self):
    from robosystems.dagster.definitions import all_jobs

    assert ladybug_migration_cleanup_job in all_jobs


class TestCleanupInstance:
  @pytest.mark.unit
  async def test_calls_migration_cleanup(self):
    client = _fake_client(migration_cleanup={"removed_files": 3})
    with patch(f"{MIGRATION}._get_client", new=AsyncMock(return_value=client)):
      result = await _cleanup_instance("i-1", "10.0.0.1")

    client.migration_cleanup.assert_awaited_once()
    assert result["instance_id"] == "i-1"
    assert result["status"] == "success"
    assert result["cleanup"] == {"removed_files": 3}


class TestImportCleanupGate:
  """The A-fix: cleanup=True must fire even when no migration is pending."""

  @pytest.mark.unit
  async def test_cleanup_runs_when_not_pending(self):
    client = _fake_client(
      migration_status={"migration_pending": False},
      migration_cleanup={"removed_files": 2},
    )
    with patch(f"{MIGRATION}._get_client", new=AsyncMock(return_value=client)):
      result = await _import_instance("i-1", "10.0.0.1", cleanup=True)

    client.migration_cleanup.assert_awaited_once()
    assert result["status"] == "no_migration_pending"
    assert result["cleanup"] == {"removed_files": 2}

  @pytest.mark.unit
  async def test_no_cleanup_when_not_pending_and_flag_off(self):
    client = _fake_client(
      migration_status={"migration_pending": False},
      migration_cleanup={"removed_files": 0},
    )
    with patch(f"{MIGRATION}._get_client", new=AsyncMock(return_value=client)):
      result = await _import_instance("i-1", "10.0.0.1", cleanup=False)

    client.migration_cleanup.assert_not_awaited()
    assert result["status"] == "no_migration_pending"
    assert "cleanup" not in result


class TestCleanupAllInstancesOp:
  @pytest.mark.unit
  def test_fans_out_over_writers(self):
    instances = [
      {"instance_id": "i-1", "private_ip": "10.0.0.1", "node_type": "shared_master"},
      {"instance_id": "i-2", "private_ip": "10.0.0.2", "node_type": "standard"},
    ]
    client = _fake_client(migration_cleanup={"removed_files": 1})
    with (
      patch(f"{MIGRATION}._get_writer_instances", return_value=instances),
      patch(f"{MIGRATION}._get_client", new=AsyncMock(return_value=client)),
    ):
      out = cleanup_all_instances(
        build_op_context(), MigrationCleanupConfig(include_shared_master=True)
      )

    assert out["summary"] == {"succeeded": 2, "failed": 0, "skipped": 0}
    assert client.migration_cleanup.await_count == 2

  @pytest.mark.unit
  def test_skips_listed_instances(self):
    instances = [
      {"instance_id": "i-1", "private_ip": "10.0.0.1", "node_type": "standard"},
      {"instance_id": "i-2", "private_ip": "10.0.0.2", "node_type": "standard"},
    ]
    client = _fake_client(migration_cleanup={"removed_files": 0})
    with (
      patch(f"{MIGRATION}._get_writer_instances", return_value=instances),
      patch(f"{MIGRATION}._get_client", new=AsyncMock(return_value=client)),
    ):
      out = cleanup_all_instances(
        build_op_context(), MigrationCleanupConfig(skip_instances=["i-1"])
      )

    assert out["summary"]["skipped"] == 1
    assert out["summary"]["succeeded"] == 1
    assert client.migration_cleanup.await_count == 1

  @pytest.mark.unit
  def test_raises_on_instance_failure(self):
    instances = [
      {"instance_id": "i-1", "private_ip": "10.0.0.1", "node_type": "standard"},
    ]
    client = MagicMock()
    client.migration_cleanup = AsyncMock(side_effect=RuntimeError("boom"))
    with (
      patch(f"{MIGRATION}._get_writer_instances", return_value=instances),
      patch(f"{MIGRATION}._get_client", new=AsyncMock(return_value=client)),
    ):
      with pytest.raises(RuntimeError, match="Cleanup failed on 1 instance"):
        cleanup_all_instances(build_op_context(), MigrationCleanupConfig())


@pytest.mark.unit
def test_import_config_still_has_cleanup_flag():
  """The import job keeps its cleanup flag (A-fix is behavioral, not schema)."""
  assert MigrationImportConfig().cleanup is False
