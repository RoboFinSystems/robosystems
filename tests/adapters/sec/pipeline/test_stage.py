"""Tests for SEC DuckDB staging assets."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from dagster import MaterializeResult, build_asset_context

from robosystems.adapters.sec.pipeline.configs import (
  SECHistoricalStageConfig,
  SECIncrementalStageConfig,
  SECStageConfig,
)
from robosystems.adapters.sec.pipeline.stage import (
  sec_duckdb_incremental_staged,
  sec_duckdb_staged,
  sec_historical_duckdb_staged,
)


def _make_staging_result(
  status="success",
  error=None,
  table_names=None,
  total_files=10,
  total_rows=1000,
  duckdb_path="/tmp/sec.duckdb",
  duration_ms=5000,
):
  """Create a mock staging result."""
  result = MagicMock()
  result.status = status
  result.error = error
  result.table_names = table_names or ["Entity", "Report", "Fact"]
  result.total_files = total_files
  result.total_rows = total_rows
  result.duckdb_path = duckdb_path
  result.duration_ms = duration_ms
  return result


@pytest.mark.unit
class TestSecDuckdbStaged:
  """Tests for sec_duckdb_staged asset."""

  @patch("robosystems.config.env")
  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  @patch(
    "robosystems.operations.graph.shared_repository_service.ensure_shared_repository_exists"
  )
  @patch("robosystems.graph_api.client.factory.boost_graph_memory")
  @patch("robosystems.graph_api.client.factory.release_graph_memory")
  def test_successful_staging(
    self, mock_release, mock_boost, mock_ensure_repo, mock_processor_cls, mock_env
  ):
    """Test successful full DuckDB staging."""
    mock_env.ENVIRONMENT = "dev"

    staging_result = _make_staging_result()
    mock_processor = MagicMock()
    mock_processor.stage_to_duckdb = AsyncMock(return_value=staging_result)
    mock_processor_cls.return_value = mock_processor

    mock_ensure_repo.return_value = {"status": "exists"}
    mock_boost.return_value = {"message": "boosted"}
    mock_release.return_value = {"message": "released"}

    config = SECStageConfig()
    context = build_asset_context()

    result = sec_duckdb_staged(context, config)

    assert isinstance(result, MaterializeResult)
    assert result.metadata["status"] == "success"
    assert result.metadata["tables_staged"] == 3
    assert result.metadata["graph_id"] == "sec"

  @patch("robosystems.config.env")
  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  @patch(
    "robosystems.operations.graph.shared_repository_service.ensure_shared_repository_exists"
  )
  @patch("robosystems.graph_api.client.factory.boost_graph_memory")
  def test_staging_error(
    self, mock_boost, mock_ensure_repo, mock_processor_cls, mock_env
  ):
    """Test staging returns error metadata on failure."""
    mock_env.ENVIRONMENT = "dev"

    staging_result = _make_staging_result(status="error", error="DuckDB out of memory")
    mock_processor = MagicMock()
    mock_processor.stage_to_duckdb = AsyncMock(return_value=staging_result)
    mock_processor_cls.return_value = mock_processor

    mock_ensure_repo.return_value = {"status": "exists"}
    mock_boost.return_value = {"message": "boosted"}

    config = SECStageConfig()
    context = build_asset_context()

    result = sec_duckdb_staged(context, config)

    assert isinstance(result, MaterializeResult)
    assert result.metadata["status"] == "error"
    assert "DuckDB out of memory" in str(result.metadata["error"])

  @patch("robosystems.config.env")
  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  @patch(
    "robosystems.operations.graph.shared_repository_service.ensure_shared_repository_exists"
  )
  @patch("robosystems.graph_api.client.factory.boost_graph_memory")
  @patch("robosystems.graph_api.client.factory.release_graph_memory")
  def test_staging_with_year_filter(
    self, mock_release, mock_boost, mock_ensure_repo, mock_processor_cls, mock_env
  ):
    """Test staging with a specific year filter."""
    mock_env.ENVIRONMENT = "dev"

    staging_result = _make_staging_result()
    mock_processor = MagicMock()
    mock_processor.stage_to_duckdb = AsyncMock(return_value=staging_result)
    mock_processor_cls.return_value = mock_processor

    mock_ensure_repo.return_value = {"status": "exists"}
    mock_boost.return_value = {"message": "boosted"}
    mock_release.return_value = {"message": "released"}

    config = SECStageConfig(year=2024)
    context = build_asset_context()

    sec_duckdb_staged(context, config)

    # Verify the year parameter was passed
    call_kwargs = mock_processor.stage_to_duckdb.call_args
    assert call_kwargs.kwargs["year"] == 2024

  @patch("robosystems.config.env")
  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  @patch(
    "robosystems.operations.graph.shared_repository_service.ensure_shared_repository_exists"
  )
  @patch("robosystems.graph_api.client.factory.boost_graph_memory")
  @patch("robosystems.graph_api.client.factory.release_graph_memory")
  def test_memory_boost_failure_non_fatal(
    self, mock_release, mock_boost, mock_ensure_repo, mock_processor_cls, mock_env
  ):
    """Test that memory boost failure does not fail the asset."""
    mock_env.ENVIRONMENT = "dev"

    staging_result = _make_staging_result()
    mock_processor = MagicMock()
    mock_processor.stage_to_duckdb = AsyncMock(return_value=staging_result)
    mock_processor_cls.return_value = mock_processor

    mock_ensure_repo.return_value = {"status": "exists"}
    mock_boost.side_effect = Exception("Boost failed")
    mock_release.return_value = {"message": "released"}

    config = SECStageConfig()
    context = build_asset_context()

    # Should not raise
    result = sec_duckdb_staged(context, config)
    assert isinstance(result, MaterializeResult)
    assert result.metadata["status"] == "success"

  @patch("robosystems.config.env")
  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  @patch(
    "robosystems.operations.graph.shared_repository_service.ensure_shared_repository_exists"
  )
  @patch("robosystems.graph_api.client.factory.boost_graph_memory")
  @patch("robosystems.graph_api.client.factory.release_graph_memory")
  def test_staging_with_reset(
    self, mock_release, mock_boost, mock_ensure_repo, mock_processor_cls, mock_env
  ):
    """Test staging with reset_staging flag."""
    mock_env.ENVIRONMENT = "dev"

    staging_result = _make_staging_result()
    mock_processor = MagicMock()
    mock_processor.stage_to_duckdb = AsyncMock(return_value=staging_result)
    mock_processor_cls.return_value = mock_processor

    mock_ensure_repo.return_value = {"status": "exists"}
    mock_boost.return_value = {"message": "boosted"}
    mock_release.return_value = {"message": "released"}

    config = SECStageConfig(reset_staging=True)
    context = build_asset_context()

    sec_duckdb_staged(context, config)

    call_kwargs = mock_processor.stage_to_duckdb.call_args
    assert call_kwargs.kwargs["reset_staging"] is True


@pytest.mark.unit
class TestSecHistoricalDuckdbStaged:
  """Tests for sec_historical_duckdb_staged asset."""

  @patch("robosystems.config.env")
  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  @patch(
    "robosystems.operations.graph.shared_repository_service.ensure_shared_subgraph_exists"
  )
  @patch("robosystems.graph_api.client.factory.boost_graph_memory")
  @patch("robosystems.graph_api.client.factory.release_graph_memory")
  def test_historical_staging_success(
    self, mock_release, mock_boost, mock_ensure_subgraph, mock_processor_cls, mock_env
  ):
    """Test successful historical staging."""
    mock_env.ENVIRONMENT = "dev"

    staging_result = _make_staging_result()
    mock_processor = MagicMock()
    mock_processor.stage_to_duckdb = AsyncMock(return_value=staging_result)
    mock_processor_cls.return_value = mock_processor

    mock_ensure_subgraph.return_value = {"status": "exists"}
    mock_boost.return_value = {"message": "boosted"}
    mock_release.return_value = {"message": "released"}

    config = SECHistoricalStageConfig()
    context = build_asset_context()

    result = sec_historical_duckdb_staged(context, config)

    assert isinstance(result, MaterializeResult)
    assert result.metadata["graph_id"] == "sec_historical"
    assert result.metadata["start_year"] == 2009
    assert result.metadata["end_year"] == 2023

  @patch("robosystems.config.env")
  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  @patch(
    "robosystems.operations.graph.shared_repository_service.ensure_shared_subgraph_exists"
  )
  @patch("robosystems.graph_api.client.factory.boost_graph_memory")
  def test_historical_staging_error(
    self, mock_boost, mock_ensure_subgraph, mock_processor_cls, mock_env
  ):
    """Test historical staging error handling."""
    mock_env.ENVIRONMENT = "dev"

    staging_result = _make_staging_result(status="error", error="Staging failed")
    mock_processor = MagicMock()
    mock_processor.stage_to_duckdb = AsyncMock(return_value=staging_result)
    mock_processor_cls.return_value = mock_processor

    mock_ensure_subgraph.return_value = {"status": "exists"}
    mock_boost.return_value = {"message": "boosted"}

    config = SECHistoricalStageConfig()
    context = build_asset_context()

    result = sec_historical_duckdb_staged(context, config)

    assert result.metadata["status"] == "error"

  @patch("robosystems.config.env")
  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  @patch(
    "robosystems.operations.graph.shared_repository_service.ensure_shared_subgraph_exists"
  )
  @patch("robosystems.graph_api.client.factory.boost_graph_memory")
  @patch("robosystems.graph_api.client.factory.release_graph_memory")
  def test_historical_staging_passes_year_range(
    self, mock_release, mock_boost, mock_ensure_subgraph, mock_processor_cls, mock_env
  ):
    """Test historical staging passes correct year range."""
    mock_env.ENVIRONMENT = "dev"

    staging_result = _make_staging_result()
    mock_processor = MagicMock()
    mock_processor.stage_to_duckdb = AsyncMock(return_value=staging_result)
    mock_processor_cls.return_value = mock_processor

    mock_ensure_subgraph.return_value = {"status": "exists"}
    mock_boost.return_value = {"message": "boosted"}
    mock_release.return_value = {"message": "released"}

    config = SECHistoricalStageConfig(start_year=2015, end_year=2020)
    context = build_asset_context()

    sec_historical_duckdb_staged(context, config)

    call_kwargs = mock_processor.stage_to_duckdb.call_args
    assert call_kwargs.kwargs["start_year"] == 2015
    assert call_kwargs.kwargs["end_year"] == 2020


@pytest.mark.unit
class TestSecDuckdbIncrementalStaged:
  """Tests for sec_duckdb_incremental_staged asset."""

  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  @patch("robosystems.graph_api.client.factory.boost_graph_memory")
  @patch("robosystems.graph_api.client.factory.release_graph_memory")
  def test_incremental_staging_success(
    self, mock_release, mock_boost, mock_processor_cls
  ):
    """Test successful incremental staging."""
    staging_result = _make_staging_result(total_rows=500)
    mock_processor = MagicMock()
    mock_processor.stage_incremental_to_duckdb = AsyncMock(return_value=staging_result)
    mock_processor_cls.return_value = mock_processor

    mock_boost.return_value = {"message": "boosted"}
    mock_release.return_value = {"message": "released"}

    config = SECIncrementalStageConfig(year=2025, quarter=1)
    context = build_asset_context()

    result = sec_duckdb_incremental_staged(context, config)

    assert isinstance(result, MaterializeResult)
    assert result.metadata["status"] == "success"
    assert result.metadata["year"] == 2025
    assert result.metadata["quarter"] == 1

  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  @patch("robosystems.graph_api.client.factory.boost_graph_memory")
  def test_incremental_staging_error(self, mock_boost, mock_processor_cls):
    """Test incremental staging error handling."""
    staging_result = _make_staging_result(status="error", error="Tables do not exist")
    mock_processor = MagicMock()
    mock_processor.stage_incremental_to_duckdb = AsyncMock(return_value=staging_result)
    mock_processor_cls.return_value = mock_processor

    mock_boost.return_value = {"message": "boosted"}

    config = SECIncrementalStageConfig(year=2025, quarter=1)
    context = build_asset_context()

    result = sec_duckdb_incremental_staged(context, config)

    assert result.metadata["status"] == "error"

  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  @patch("robosystems.graph_api.client.factory.boost_graph_memory")
  def test_incremental_staging_partial_failure(self, mock_boost, mock_processor_cls):
    """Test incremental staging with partial failure reports as error."""
    staging_result = _make_staging_result(status="partial")
    mock_processor = MagicMock()
    mock_processor.stage_incremental_to_duckdb = AsyncMock(return_value=staging_result)
    mock_processor_cls.return_value = mock_processor

    mock_boost.return_value = {"message": "boosted"}

    config = SECIncrementalStageConfig(year=2025, quarter=1)
    context = build_asset_context()

    result = sec_duckdb_incremental_staged(context, config)

    # Partial staging should report as error to prevent downstream materialization
    assert result.metadata["status"] == "error"

  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  @patch("robosystems.graph_api.client.factory.boost_graph_memory")
  @patch("robosystems.graph_api.client.factory.release_graph_memory")
  def test_incremental_staging_passes_config(
    self, mock_release, mock_boost, mock_processor_cls
  ):
    """Test incremental staging passes correct configuration."""
    staging_result = _make_staging_result()
    mock_processor = MagicMock()
    mock_processor.stage_incremental_to_duckdb = AsyncMock(return_value=staging_result)
    mock_processor_cls.return_value = mock_processor

    mock_boost.return_value = {"message": "boosted"}
    mock_release.return_value = {"message": "released"}

    config = SECIncrementalStageConfig(
      year=2025, quarter=2, skip_taxonomy_relationships=True
    )
    context = build_asset_context()

    sec_duckdb_incremental_staged(context, config)

    call_kwargs = mock_processor.stage_incremental_to_duckdb.call_args
    assert call_kwargs.kwargs["year"] == 2025
    assert call_kwargs.kwargs["quarter"] == 2
    assert call_kwargs.kwargs["skip_taxonomy_relationships"] is True
