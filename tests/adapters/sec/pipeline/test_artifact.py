"""Tests for SEC knowledge artifact generation asset."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from dagster import MaterializeResult, build_asset_context

from robosystems.adapters.sec.pipeline.artifact import (
  SECArtifactConfig,
  sec_knowledge_artifacts,
)


@pytest.mark.unit
class TestSECArtifactConfig:
  """Tests for SECArtifactConfig."""

  def test_default_values(self):
    """Test default configuration values."""
    config = SECArtifactConfig()
    assert config.duckdb_source == "sec"
    assert config.memory_limit == "8GB"

  def test_custom_values(self):
    """Test custom configuration values."""
    config = SECArtifactConfig(duckdb_source="sec_historical", memory_limit="16GB")
    assert config.duckdb_source == "sec_historical"
    assert config.memory_limit == "16GB"


@pytest.mark.unit
class TestSecKnowledgeArtifacts:
  """Tests for sec_knowledge_artifacts asset."""

  @patch("robosystems.adapters.sec.knowledge.framework.DuckDBAnalyticsContext")
  @patch("robosystems.adapters.sec.knowledge.artifact.ElementKnowledgeBuilder")
  @patch("robosystems.adapters.sec.knowledge.artifact.StructureKnowledgeBuilder")
  @patch("robosystems.adapters.sec.knowledge.artifact.DisclosureProfileBuilder")
  @patch("robosystems.config.env")
  def test_successful_artifact_generation_dev(
    self,
    mock_env,
    mock_disclosure_builder_cls,
    mock_structure_builder_cls,
    mock_element_builder_cls,
    mock_analytics_ctx_cls,
  ):
    """Test successful artifact generation in dev environment."""
    mock_env.ENVIRONMENT = "dev"

    # Mock DuckDBAnalyticsContext
    mock_ctx = MagicMock()
    mock_ctx.db_path = Path("/tmp/sec.duckdb")
    mock_ctx.__enter__ = MagicMock(return_value=mock_ctx)
    mock_ctx.__exit__ = MagicMock(return_value=False)
    mock_analytics_ctx_cls.return_value = mock_ctx

    # Mock element builder
    mock_element_builder = MagicMock()
    element_path = Path("/tmp/element_knowledge.parquet")
    mock_element_builder.build.return_value = element_path
    mock_element_builder_cls.return_value = mock_element_builder

    # Mock structure builder
    mock_structure_builder = MagicMock()
    profiles_path = Path("/tmp/structure_profiles.parquet")
    consensus_path = Path("/tmp/structure_consensus.parquet")
    mock_structure_builder.build.return_value = (profiles_path, consensus_path)
    mock_structure_builder_cls.return_value = mock_structure_builder

    # Mock disclosure builder
    mock_disclosure_builder = MagicMock()
    disc_profiles_path = Path("/tmp/disclosure_profiles.parquet")
    disc_consensus_path = Path("/tmp/disclosure_consensus.parquet")
    mock_disclosure_builder.build.return_value = (
      disc_profiles_path,
      disc_consensus_path,
    )
    mock_disclosure_builder_cls.return_value = mock_disclosure_builder

    config = SECArtifactConfig()
    context = build_asset_context()

    result = sec_knowledge_artifacts(context, config)

    assert isinstance(result, MaterializeResult)
    assert result.metadata["element_knowledge_path"] == str(element_path)
    assert result.metadata["structure_profiles_path"] == str(profiles_path)
    assert result.metadata["structure_consensus_path"] == str(consensus_path)
    assert result.metadata["disclosure_profiles_path"] == str(disc_profiles_path)
    assert result.metadata["disclosure_consensus_path"] == str(disc_consensus_path)

  @patch("robosystems.adapters.sec.knowledge.framework.DuckDBAnalyticsContext")
  @patch("robosystems.adapters.sec.knowledge.artifact.ElementKnowledgeBuilder")
  @patch("robosystems.adapters.sec.knowledge.artifact.StructureKnowledgeBuilder")
  @patch("robosystems.adapters.sec.knowledge.artifact.DisclosureProfileBuilder")
  @patch("robosystems.config.env")
  def test_artifact_generation_skips_upload_in_dev(
    self,
    mock_env,
    mock_disclosure_builder_cls,
    mock_structure_builder_cls,
    mock_element_builder_cls,
    mock_analytics_ctx_cls,
  ):
    """Test that artifacts are NOT uploaded to S3 in dev environment."""
    mock_env.ENVIRONMENT = "dev"

    mock_ctx = MagicMock()
    mock_ctx.db_path = Path("/tmp/sec.duckdb")
    mock_ctx.__enter__ = MagicMock(return_value=mock_ctx)
    mock_ctx.__exit__ = MagicMock(return_value=False)
    mock_analytics_ctx_cls.return_value = mock_ctx

    mock_element_builder = MagicMock()
    mock_element_builder.build.return_value = Path("/tmp/element.parquet")
    mock_element_builder_cls.return_value = mock_element_builder

    mock_structure_builder = MagicMock()
    mock_structure_builder.build.return_value = (
      Path("/tmp/profiles.parquet"),
      Path("/tmp/consensus.parquet"),
    )
    mock_structure_builder_cls.return_value = mock_structure_builder

    mock_disclosure_builder = MagicMock()
    mock_disclosure_builder.build.return_value = (
      Path("/tmp/disc_profiles.parquet"),
      Path("/tmp/disc_consensus.parquet"),
    )
    mock_disclosure_builder_cls.return_value = mock_disclosure_builder

    config = SECArtifactConfig()
    context = build_asset_context()

    # Should complete without trying to upload
    result = sec_knowledge_artifacts(context, config)
    assert isinstance(result, MaterializeResult)

  @patch("robosystems.adapters.sec.knowledge.framework.DuckDBAnalyticsContext")
  @patch("robosystems.adapters.sec.knowledge.artifact.ElementKnowledgeBuilder")
  @patch("robosystems.adapters.sec.knowledge.artifact.StructureKnowledgeBuilder")
  @patch("robosystems.adapters.sec.knowledge.artifact.DisclosureProfileBuilder")
  @patch("robosystems.config.env")
  def test_artifact_uses_custom_memory_limit(
    self,
    mock_env,
    mock_disclosure_builder_cls,
    mock_structure_builder_cls,
    mock_element_builder_cls,
    mock_analytics_ctx_cls,
  ):
    """Test that custom memory limit is passed to builders."""
    mock_env.ENVIRONMENT = "dev"

    mock_ctx = MagicMock()
    mock_ctx.db_path = Path("/tmp/sec.duckdb")
    mock_ctx.__enter__ = MagicMock(return_value=mock_ctx)
    mock_ctx.__exit__ = MagicMock(return_value=False)
    mock_analytics_ctx_cls.return_value = mock_ctx

    mock_element_builder = MagicMock()
    mock_element_builder.build.return_value = Path("/tmp/element.parquet")
    mock_element_builder_cls.return_value = mock_element_builder

    mock_structure_builder = MagicMock()
    mock_structure_builder.build.return_value = (
      Path("/tmp/profiles.parquet"),
      Path("/tmp/consensus.parquet"),
    )
    mock_structure_builder_cls.return_value = mock_structure_builder

    mock_disclosure_builder = MagicMock()
    mock_disclosure_builder.build.return_value = (
      Path("/tmp/disc_profiles.parquet"),
      Path("/tmp/disc_consensus.parquet"),
    )
    mock_disclosure_builder_cls.return_value = mock_disclosure_builder

    config = SECArtifactConfig(memory_limit="16GB")
    context = build_asset_context()

    sec_knowledge_artifacts(context, config)

    # Verify memory limit passed to builders
    mock_element_builder_cls.assert_called_once_with(memory_limit="16GB")
    mock_structure_builder_cls.assert_called_once_with(memory_limit="16GB")
    mock_disclosure_builder_cls.assert_called_once_with(memory_limit="16GB")

  @patch("robosystems.adapters.sec.knowledge.framework.DuckDBAnalyticsContext")
  @patch("robosystems.adapters.sec.knowledge.artifact.ElementKnowledgeBuilder")
  @patch("robosystems.adapters.sec.knowledge.artifact.StructureKnowledgeBuilder")
  @patch("robosystems.adapters.sec.knowledge.artifact.DisclosureProfileBuilder")
  @patch("robosystems.config.env")
  def test_artifact_uses_duckdb_source(
    self,
    mock_env,
    mock_disclosure_builder_cls,
    mock_structure_builder_cls,
    mock_element_builder_cls,
    mock_analytics_ctx_cls,
  ):
    """Test that custom duckdb_source is passed to analytics context."""
    mock_env.ENVIRONMENT = "dev"

    mock_ctx = MagicMock()
    mock_ctx.db_path = Path("/tmp/sec_historical.duckdb")
    mock_ctx.__enter__ = MagicMock(return_value=mock_ctx)
    mock_ctx.__exit__ = MagicMock(return_value=False)
    mock_analytics_ctx_cls.return_value = mock_ctx

    mock_element_builder = MagicMock()
    mock_element_builder.build.return_value = Path("/tmp/element.parquet")
    mock_element_builder_cls.return_value = mock_element_builder

    mock_structure_builder = MagicMock()
    mock_structure_builder.build.return_value = (
      Path("/tmp/profiles.parquet"),
      Path("/tmp/consensus.parquet"),
    )
    mock_structure_builder_cls.return_value = mock_structure_builder

    mock_disclosure_builder = MagicMock()
    mock_disclosure_builder.build.return_value = (
      Path("/tmp/disc_profiles.parquet"),
      Path("/tmp/disc_consensus.parquet"),
    )
    mock_disclosure_builder_cls.return_value = mock_disclosure_builder

    config = SECArtifactConfig(duckdb_source="sec_historical")
    context = build_asset_context()

    sec_knowledge_artifacts(context, config)

    mock_analytics_ctx_cls.assert_called_once_with(
      duckdb_source="sec_historical",
      memory_limit="256MB",
    )
