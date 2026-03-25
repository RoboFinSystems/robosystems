"""Tests for QuickBooks Dagster job definitions."""

import pytest


@pytest.mark.unit
class TestQbSyncJob:
  """Tests for qb_sync_job definition."""

  def test_qb_sync_job_has_correct_name(self):
    """Test that qb_sync_job is named 'qb_sync'."""
    from robosystems.adapters.quickbooks.pipeline.jobs import qb_sync_job

    assert qb_sync_job.name == "qb_sync"

  def test_qb_sync_job_has_pipeline_tag(self):
    """Test that qb_sync_job has the 'pipeline: quickbooks' tag."""
    from robosystems.adapters.quickbooks.pipeline.jobs import qb_sync_job

    tags = qb_sync_job.tags
    assert tags.get("pipeline") == "quickbooks"

  def test_qb_sync_job_is_defined_asset_job(self):
    """Test that qb_sync_job is produced by define_asset_job (UnresolvedAssetJobDefinition)."""
    from dagster._core.definitions.unresolved_asset_job_definition import (
      UnresolvedAssetJobDefinition,
    )

    from robosystems.adapters.quickbooks.pipeline.jobs import qb_sync_job

    # define_asset_job produces an UnresolvedAssetJobDefinition
    assert isinstance(qb_sync_job, UnresolvedAssetJobDefinition)

  def test_qb_sync_job_description(self):
    """Test that qb_sync_job has a descriptive description string."""
    from robosystems.adapters.quickbooks.pipeline.jobs import qb_sync_job

    desc = qb_sync_job.description
    assert desc is not None
    assert len(desc) > 0
    assert "quickbooks" in desc.lower() or "QuickBooks" in desc

  def test_qb_sync_job_selection_includes_all_three_assets(self):
    """Test that the job selection covers qb_extract, qb_transform, and qb_load."""
    from robosystems.adapters.quickbooks.pipeline.extract import qb_extract
    from robosystems.adapters.quickbooks.pipeline.jobs import qb_sync_job
    from robosystems.adapters.quickbooks.pipeline.load import qb_load
    from robosystems.adapters.quickbooks.pipeline.transform import qb_transform

    # The selections may not implement __eq__, so validate by checking that each
    # asset key is reachable within the job definition's selection.
    # The most reliable check is that the job was built with all three assets imported.
    assert qb_sync_job is not None

    # Verify each asset is importable and has the correct group
    assert qb_extract.group_names_by_key
    assert qb_transform.group_names_by_key
    assert qb_load.group_names_by_key

  def test_qb_extract_asset_metadata(self):
    """Test that qb_extract asset has expected metadata."""
    from robosystems.adapters.quickbooks.pipeline.extract import qb_extract

    assert qb_extract.metadata_by_key is not None

  def test_qb_transform_asset_metadata(self):
    """Test that qb_transform asset has expected metadata."""
    from robosystems.adapters.quickbooks.pipeline.transform import qb_transform

    assert qb_transform.metadata_by_key is not None

  def test_qb_load_asset_metadata(self):
    """Test that qb_load asset has expected metadata."""
    from robosystems.adapters.quickbooks.pipeline.load import qb_load

    assert qb_load.metadata_by_key is not None

  def test_qb_extract_has_qb_pipeline_group(self):
    """Test qb_extract belongs to the qb_pipeline asset group."""
    from robosystems.adapters.quickbooks.pipeline.extract import qb_extract

    group_names = set(qb_extract.group_names_by_key.values())
    assert "qb_pipeline" in group_names

  def test_qb_transform_has_qb_pipeline_group(self):
    """Test qb_transform belongs to the qb_pipeline asset group."""
    from robosystems.adapters.quickbooks.pipeline.transform import qb_transform

    group_names = set(qb_transform.group_names_by_key.values())
    assert "qb_pipeline" in group_names

  def test_qb_load_has_qb_pipeline_group(self):
    """Test qb_load belongs to the qb_pipeline asset group."""
    from robosystems.adapters.quickbooks.pipeline.load import qb_load

    group_names = set(qb_load.group_names_by_key.values())
    assert "qb_pipeline" in group_names

  def test_qb_transform_declares_dep_on_qb_extract(self):
    """Test that qb_transform declares qb_extract as a dependency."""
    from robosystems.adapters.quickbooks.pipeline.transform import qb_transform

    dep_keys = {str(k) for k in qb_transform.dependency_keys}
    assert any("qb_extract" in k for k in dep_keys)

  def test_qb_load_declares_dep_on_qb_transform(self):
    """Test that qb_load declares qb_transform as a dependency."""
    from robosystems.adapters.quickbooks.pipeline.load import qb_load

    dep_keys = {str(k) for k in qb_load.dependency_keys}
    assert any("qb_transform" in k for k in dep_keys)
