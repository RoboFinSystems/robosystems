"""Tests for SEC pipeline __init__.py and get_dagster_components."""

import pytest

from robosystems.adapters.sec.pipeline import get_dagster_components


@pytest.mark.unit
class TestGetDagsterComponents:
  """Tests for the get_dagster_components function."""

  def test_returns_dict_with_expected_keys(self):
    """Test that get_dagster_components returns all expected keys."""
    components = get_dagster_components()

    assert isinstance(components, dict)
    assert "assets" in components
    assert "jobs" in components
    assert "sensors" in components
    assert "schedules" in components

  def test_assets_are_list(self):
    """Test that assets is a list."""
    components = get_dagster_components()
    assert isinstance(components["assets"], list)

  def test_jobs_are_list(self):
    """Test that jobs is a list."""
    components = get_dagster_components()
    assert isinstance(components["jobs"], list)

  def test_sensors_are_list(self):
    """Test that sensors is a list."""
    components = get_dagster_components()
    assert isinstance(components["sensors"], list)

  def test_schedules_are_list(self):
    """Test that schedules is a list."""
    components = get_dagster_components()
    assert isinstance(components["schedules"], list)

  def test_expected_number_of_assets(self):
    """Test that the expected number of assets are registered."""
    components = get_dagster_components()
    # 14 shared pipeline assets + 3 entity sync assets (extract, transform, load)
    assert len(components["assets"]) == 17

  def test_expected_number_of_jobs(self):
    """Test that the expected number of jobs are registered."""
    components = get_dagster_components()
    # 15 shared pipeline jobs + 1 entity sync job
    assert len(components["jobs"]) == 16

  def test_expected_number_of_sensors(self):
    """Test that the expected number of sensors are registered."""
    components = get_dagster_components()
    assert len(components["sensors"]) == 4

  def test_expected_number_of_schedules(self):
    """Test that the expected number of schedules are registered."""
    components = get_dagster_components()
    assert len(components["schedules"]) == 1

  def test_asset_names_include_core_pipeline(self):
    """Test that core pipeline assets are included."""
    components = get_dagster_components()
    asset_names = {
      asset.op.name if hasattr(asset, "op") else asset.key.path[-1]
      for asset in components["assets"]
    }

    expected_assets = {
      "sec_raw_filings",
      "sec_processed_filings",
      "sec_duckdb_staged",
      "sec_graph_materialized",
      "sec_backup",
      "sec_lbug_s3_published",
      "sec_knowledge_artifacts",
      "sec_entity_extract",
      "sec_entity_transform",
      "sec_entity_load",
    }

    for expected in expected_assets:
      assert expected in asset_names, f"Missing asset: {expected}"

  def test_job_names_include_core_pipeline(self):
    """Test that core pipeline jobs are included."""
    components = get_dagster_components()
    job_names = {job.name for job in components["jobs"]}

    expected_jobs = {
      "sec_download",
      "sec_process",
      "sec_stage",
      "sec_materialize",
      "sec_staged_materialize",
      "sec_create_backup",
      "sec_entity_sync",
    }

    for expected in expected_jobs:
      assert expected in job_names, f"Missing job: {expected}"
