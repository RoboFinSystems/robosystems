"""Tests for SEC pipeline Dagster job definitions."""

import pytest

from robosystems.adapters.sec.pipeline.jobs import (
  sec_artifact_generation_job,
  sec_backup_job,
  sec_download_job,
  sec_duckdb_s3_publish_job,
  sec_entity_update_job,
  sec_historical_duckdb_s3_publish_job,
  sec_historical_lbug_s3_publish_job,
  sec_historical_materialize_job,
  sec_historical_stage_job,
  sec_historical_staged_materialize_job,
  sec_incremental_stage_job,
  sec_materialize_job,
  sec_process_job,
  sec_stage_job,
  sec_staged_materialize_job,
)


@pytest.mark.unit
class TestSECDownloadJob:
  """Tests for sec_download_job definition."""

  def test_job_name(self):
    """Test download job has correct name."""
    assert sec_download_job.name == "sec_download"

  def test_job_has_pipeline_tag(self):
    """Test download job has pipeline tag."""
    assert sec_download_job.tags.get("pipeline") == "sec"

  def test_job_has_phase_tag(self):
    """Test download job has phase tag."""
    assert sec_download_job.tags.get("phase") == "download"

  def test_job_has_partitions(self):
    """Test download job has partition definition."""
    assert sec_download_job.partitions_def is not None


@pytest.mark.unit
class TestSECProcessJob:
  """Tests for sec_process_job definition."""

  def test_job_name(self):
    """Test process job has correct name."""
    assert sec_process_job.name == "sec_process"

  def test_job_has_pipeline_tag(self):
    """Test process job has pipeline tag."""
    assert sec_process_job.tags.get("pipeline") == "sec"

  def test_job_has_phase_tag(self):
    """Test process job has phase tag."""
    assert sec_process_job.tags.get("phase") == "process"

  def test_job_has_partitions(self):
    """Test process job has partition definition."""
    assert sec_process_job.partitions_def is not None

  def test_job_has_ecs_tags(self):
    """Test process job has ECS resource tags for enhanced profile."""
    assert sec_process_job.tags.get("ecs/cpu") == "4096"
    assert sec_process_job.tags.get("ecs/memory") == "16384"


@pytest.mark.unit
class TestSECStageJob:
  """Tests for sec_stage_job definition."""

  def test_job_name(self):
    """Test stage job has correct name."""
    assert sec_stage_job.name == "sec_stage"

  def test_job_has_pipeline_tag(self):
    """Test stage job has pipeline tag."""
    assert sec_stage_job.tags.get("pipeline") == "sec"

  def test_job_has_minimal_ecs_profile(self):
    """Test stage job uses minimal ECS profile (orchestration only)."""
    assert sec_stage_job.tags.get("ecs/cpu") == "256"
    assert sec_stage_job.tags.get("ecs/memory") == "512"


@pytest.mark.unit
class TestSECMaterializeJob:
  """Tests for sec_materialize_job definition."""

  def test_job_name(self):
    """Test materialize job has correct name."""
    assert sec_materialize_job.name == "sec_materialize"

  def test_job_has_pipeline_tag(self):
    """Test materialize job has pipeline tag."""
    assert sec_materialize_job.tags.get("pipeline") == "sec"

  def test_job_has_phase_tag(self):
    """Test materialize job has phase tag."""
    assert sec_materialize_job.tags.get("phase") == "materialize"


@pytest.mark.unit
class TestSECStagedMaterializeJob:
  """Tests for sec_staged_materialize_job definition."""

  def test_job_name(self):
    """Test staged materialize job has correct name."""
    assert sec_staged_materialize_job.name == "sec_staged_materialize"

  def test_job_has_full_phase(self):
    """Test staged materialize job has full phase tag."""
    assert sec_staged_materialize_job.tags.get("phase") == "full"


@pytest.mark.unit
class TestSECIncrementalStageJob:
  """Tests for sec_incremental_stage_job definition."""

  def test_job_name(self):
    """Test incremental stage job has correct name."""
    assert sec_incremental_stage_job.name == "sec_incremental_stage"

  def test_job_has_incremental_mode(self):
    """Test incremental stage job has incremental mode tag."""
    assert sec_incremental_stage_job.tags.get("mode") == "incremental"


@pytest.mark.unit
class TestSECHistoricalJobs:
  """Tests for SEC historical pipeline job definitions."""

  def test_historical_stage_job_name(self):
    """Test historical stage job has correct name."""
    assert sec_historical_stage_job.name == "sec_historical_stage"

  def test_historical_materialize_job_name(self):
    """Test historical materialize job has correct name."""
    assert sec_historical_materialize_job.name == "sec_historical_materialize"

  def test_historical_staged_materialize_job_name(self):
    """Test historical staged materialize job has correct name."""
    assert (
      sec_historical_staged_materialize_job.name == "sec_historical_staged_materialize"
    )

  def test_historical_jobs_have_pipeline_tag(self):
    """Test all historical jobs have SEC pipeline tag."""
    for job in [
      sec_historical_stage_job,
      sec_historical_materialize_job,
      sec_historical_staged_materialize_job,
    ]:
      assert job.tags.get("pipeline") == "sec"


@pytest.mark.unit
class TestSECEntityUpdateJob:
  """Tests for sec_entity_update_job definition."""

  def test_job_name(self):
    """Test entity update job has correct name."""
    assert sec_entity_update_job.name == "sec_entity_update"

  def test_job_has_pipeline_tag(self):
    """Test entity update job has pipeline tag."""
    assert sec_entity_update_job.tags.get("pipeline") == "sec"


@pytest.mark.unit
class TestSECBackupJob:
  """Tests for sec_backup_job definition."""

  def test_job_name(self):
    """Test backup job has correct name."""
    assert sec_backup_job.name == "sec_create_backup"

  def test_job_has_pipeline_tag(self):
    """Test backup job has pipeline tag."""
    assert sec_backup_job.tags.get("pipeline") == "sec"

  def test_job_has_backup_phase(self):
    """Test backup job has backup phase tag."""
    assert sec_backup_job.tags.get("phase") == "backup"


@pytest.mark.unit
class TestSECS3PublishJobs:
  """Tests for S3 publish job definitions."""

  def test_duckdb_s3_publish_job_name(self):
    """Test DuckDB S3 publish job has correct name."""
    assert sec_duckdb_s3_publish_job.name == "sec_duckdb_s3_publish"

  def test_historical_duckdb_s3_publish_job_name(self):
    """Test historical DuckDB S3 publish job has correct name."""
    assert (
      sec_historical_duckdb_s3_publish_job.name == "sec_historical_duckdb_s3_publish"
    )

  def test_historical_lbug_s3_publish_job_name(self):
    """Test historical LadybugDB S3 publish job has correct name."""
    assert sec_historical_lbug_s3_publish_job.name == "sec_historical_lbug_s3_publish"


@pytest.mark.unit
class TestSECArtifactJob:
  """Tests for sec_artifact_generation_job definition."""

  def test_job_name(self):
    """Test artifact generation job has correct name."""
    assert sec_artifact_generation_job.name == "sec_artifact_generation"

  def test_job_has_pipeline_tag(self):
    """Test artifact generation job has pipeline tag."""
    assert sec_artifact_generation_job.tags.get("pipeline") == "sec"

  def test_job_has_artifact_phase(self):
    """Test artifact generation job has artifact phase tag."""
    assert sec_artifact_generation_job.tags.get("phase") == "artifact"

  def test_job_has_enhanced_ecs_profile(self):
    """Test artifact generation job has enhanced ECS profile for compute."""
    assert sec_artifact_generation_job.tags.get("ecs/cpu") == "4096"
    assert sec_artifact_generation_job.tags.get("ecs/memory") == "30720"


@pytest.mark.unit
class TestAllJobsConsistency:
  """Tests for consistency across all SEC pipeline jobs."""

  def test_all_jobs_have_pipeline_tag(self):
    """Test every SEC job has the pipeline=sec tag."""
    all_jobs = [
      sec_download_job,
      sec_process_job,
      sec_stage_job,
      sec_materialize_job,
      sec_staged_materialize_job,
      sec_incremental_stage_job,
      sec_historical_stage_job,
      sec_historical_materialize_job,
      sec_historical_staged_materialize_job,
      sec_entity_update_job,
      sec_backup_job,
      sec_duckdb_s3_publish_job,
      sec_historical_duckdb_s3_publish_job,
      sec_historical_lbug_s3_publish_job,
      sec_artifact_generation_job,
    ]
    for job in all_jobs:
      assert job.tags.get("pipeline") == "sec", (
        f"Job {job.name} missing pipeline=sec tag"
      )

  def test_all_job_names_are_unique(self):
    """Test all job names are unique."""
    all_jobs = [
      sec_download_job,
      sec_process_job,
      sec_stage_job,
      sec_materialize_job,
      sec_staged_materialize_job,
      sec_incremental_stage_job,
      sec_historical_stage_job,
      sec_historical_materialize_job,
      sec_historical_staged_materialize_job,
      sec_entity_update_job,
      sec_backup_job,
      sec_duckdb_s3_publish_job,
      sec_historical_duckdb_s3_publish_job,
      sec_historical_lbug_s3_publish_job,
      sec_artifact_generation_job,
    ]
    names = [job.name for job in all_jobs]
    assert len(names) == len(set(names)), "Duplicate job names found"
