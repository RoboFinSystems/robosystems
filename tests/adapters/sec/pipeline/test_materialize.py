"""Tests for SEC graph materialization assets."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from dagster import Failure, MaterializeResult, build_asset_context

from robosystems.adapters.sec.pipeline.configs import SECMaterializeConfig
from robosystems.adapters.sec.pipeline.materialize import (
  sec_graph_materialized,
  sec_historical_materialized,
)


def _make_materialize_result(
  status="success",
  error=None,
  total_rows_ingested=100000,
  duration_ms=60000,
  tables=None,
):
  """Create a mock materialization result."""
  result = MagicMock()
  result.status = status
  result.error = error
  result.total_rows_ingested = total_rows_ingested
  result.duration_ms = duration_ms
  result.tables = tables or {"Entity": 5000, "Report": 50000, "Fact": 45000}
  return result


@pytest.mark.unit
class TestSecGraphMaterialized:
  """Tests for sec_graph_materialized asset."""

  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  @patch("robosystems.graph_api.client.factory.boost_graph_memory")
  @patch("robosystems.graph_api.client.factory.release_graph_memory")
  def test_successful_materialization(
    self, mock_release, mock_boost, mock_processor_cls
  ):
    """Test successful materialization."""
    mat_result = _make_materialize_result()
    mock_processor = MagicMock()
    mock_processor.materialize_from_duckdb = AsyncMock(return_value=mat_result)
    mock_processor_cls.return_value = mock_processor

    mock_boost.return_value = {"message": "boosted"}
    mock_release.return_value = {"message": "released"}

    config = SECMaterializeConfig()
    context = build_asset_context()

    result = sec_graph_materialized(context, config)

    assert isinstance(result, MaterializeResult)
    assert result.metadata["status"] == "success"
    assert result.metadata["total_rows_ingested"] == 100000
    assert result.metadata["graph_id"] == "sec"

  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  @patch("robosystems.graph_api.client.factory.boost_graph_memory")
  def test_materialization_error(self, mock_boost, mock_processor_cls):
    """Test materialization error raises Failure to block downstream sensors."""
    mat_result = _make_materialize_result(status="error", error="Connection refused")
    mock_processor = MagicMock()
    mock_processor.materialize_from_duckdb = AsyncMock(return_value=mat_result)
    mock_processor_cls.return_value = mock_processor

    mock_boost.return_value = {"message": "boosted"}

    config = SECMaterializeConfig()
    context = build_asset_context()

    with pytest.raises(Failure, match="Connection refused"):
      sec_graph_materialized(context, config)

  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  @patch("robosystems.graph_api.client.factory.boost_graph_memory")
  def test_materialization_partial_raises_failure(self, mock_boost, mock_processor_cls):
    """Test partial materialization raises Failure to block S3 publish."""
    failed_tables = [
      {"table_name": "Fact", "status": "error", "error": "OOM"},
      {"table_name": "Entity", "rows_ingested": 5000, "status": "success"},
    ]
    mat_result = _make_materialize_result(
      status="partial", total_rows_ingested=5000, tables=failed_tables
    )
    mock_processor = MagicMock()
    mock_processor.materialize_from_duckdb = AsyncMock(return_value=mat_result)
    mock_processor_cls.return_value = mock_processor

    mock_boost.return_value = {"message": "boosted"}

    config = SECMaterializeConfig()
    context = build_asset_context()

    with pytest.raises(Failure, match=r"1 table.*failed.*Fact"):
      sec_graph_materialized(context, config)

  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  @patch("robosystems.graph_api.client.factory.boost_graph_memory")
  @patch("robosystems.graph_api.client.factory.release_graph_memory")
  def test_materialization_with_rebuild(
    self, mock_release, mock_boost, mock_processor_cls
  ):
    """Test materialization with rebuild flag."""
    mat_result = _make_materialize_result()
    mock_processor = MagicMock()
    mock_processor.materialize_from_duckdb = AsyncMock(return_value=mat_result)
    mock_processor_cls.return_value = mock_processor

    mock_boost.return_value = {"message": "boosted"}
    mock_release.return_value = {"message": "released"}

    config = SECMaterializeConfig(rebuild_graph=True)
    context = build_asset_context()

    result = sec_graph_materialized(context, config)

    # Verify rebuild parameter was passed
    call_kwargs = mock_processor.materialize_from_duckdb.call_args
    assert call_kwargs.kwargs["rebuild"] is True
    assert result.metadata["rebuild_graph"] is True

  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  @patch("robosystems.graph_api.client.factory.boost_graph_memory")
  @patch("robosystems.graph_api.client.factory.release_graph_memory")
  def test_materialization_without_rebuild(
    self, mock_release, mock_boost, mock_processor_cls
  ):
    """Test materialization without rebuild (resume scenario)."""
    mat_result = _make_materialize_result()
    mock_processor = MagicMock()
    mock_processor.materialize_from_duckdb = AsyncMock(return_value=mat_result)
    mock_processor_cls.return_value = mock_processor

    mock_boost.return_value = {"message": "boosted"}
    mock_release.return_value = {"message": "released"}

    config = SECMaterializeConfig(rebuild_graph=False)
    context = build_asset_context()

    result = sec_graph_materialized(context, config)

    call_kwargs = mock_processor.materialize_from_duckdb.call_args
    assert call_kwargs.kwargs["rebuild"] is False
    assert result.metadata["rebuild_graph"] is False

  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  @patch("robosystems.graph_api.client.factory.boost_graph_memory")
  @patch("robosystems.graph_api.client.factory.release_graph_memory")
  def test_memory_release_failure_non_fatal(
    self, mock_release, mock_boost, mock_processor_cls
  ):
    """Test that memory release failure does not fail materialization."""
    mat_result = _make_materialize_result()
    mock_processor = MagicMock()
    mock_processor.materialize_from_duckdb = AsyncMock(return_value=mat_result)
    mock_processor_cls.return_value = mock_processor

    mock_boost.return_value = {"message": "boosted"}
    mock_release.side_effect = Exception("Release failed")

    config = SECMaterializeConfig()
    context = build_asset_context()

    # Should not raise despite release failure
    result = sec_graph_materialized(context, config)
    assert result.metadata["status"] == "success"

  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  @patch("robosystems.graph_api.client.factory.boost_graph_memory")
  @patch("robosystems.graph_api.client.factory.release_graph_memory")
  def test_materialization_batch_settings(
    self, mock_release, mock_boost, mock_processor_cls
  ):
    """Test materialization respects batch settings."""
    mat_result = _make_materialize_result()
    mock_processor = MagicMock()
    mock_processor.materialize_from_duckdb = AsyncMock(return_value=mat_result)
    mock_processor_cls.return_value = mock_processor

    mock_boost.return_value = {"message": "boosted"}
    mock_release.return_value = {"message": "released"}

    config = SECMaterializeConfig(
      batch_materialization=True,
      materialization_batch_size=10_000_000,
    )
    context = build_asset_context()

    sec_graph_materialized(context, config)

    call_kwargs = mock_processor.materialize_from_duckdb.call_args
    assert call_kwargs.kwargs["batch_materialization"] is True
    assert call_kwargs.kwargs["batch_size"] == 10_000_000


@pytest.mark.unit
class TestSecHistoricalMaterialized:
  """Tests for sec_historical_materialized asset."""

  @patch("robosystems.config.env")
  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  @patch(
    "robosystems.operations.graph.shared_repository_service.ensure_shared_subgraph_exists"
  )
  @patch("robosystems.graph_api.client.factory.boost_graph_memory")
  @patch("robosystems.graph_api.client.factory.release_graph_memory")
  def test_historical_materialization_success(
    self, mock_release, mock_boost, mock_ensure_subgraph, mock_processor_cls, mock_env
  ):
    """Test successful historical materialization."""
    mock_env.ENVIRONMENT = "dev"

    mat_result = _make_materialize_result()
    mock_processor = MagicMock()
    mock_processor.materialize_from_duckdb = AsyncMock(return_value=mat_result)
    mock_processor_cls.return_value = mock_processor

    mock_ensure_subgraph.return_value = {"status": "exists"}
    mock_boost.return_value = {"message": "boosted"}
    mock_release.return_value = {"message": "released"}

    config = SECMaterializeConfig(graph_id="sec_historical")
    context = build_asset_context()

    result = sec_historical_materialized(context, config)

    assert isinstance(result, MaterializeResult)
    assert result.metadata["graph_id"] == "sec_historical"
    assert result.metadata["status"] == "success"

  @patch("robosystems.config.env")
  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  @patch(
    "robosystems.operations.graph.shared_repository_service.ensure_shared_subgraph_exists"
  )
  @patch("robosystems.graph_api.client.factory.boost_graph_memory")
  @patch("robosystems.graph_api.client.factory.release_graph_memory")
  def test_historical_materialization_defaults_graph_id(
    self, mock_release, mock_boost, mock_ensure_subgraph, mock_processor_cls, mock_env
  ):
    """Test that graph_id defaults to sec_historical when sec is provided."""
    mock_env.ENVIRONMENT = "dev"

    mat_result = _make_materialize_result()
    mock_processor = MagicMock()
    mock_processor.materialize_from_duckdb = AsyncMock(return_value=mat_result)
    mock_processor_cls.return_value = mock_processor

    mock_ensure_subgraph.return_value = {"status": "exists"}
    mock_boost.return_value = {"message": "boosted"}
    mock_release.return_value = {"message": "released"}

    # Use default "sec" graph_id - should be overridden to "sec_historical"
    config = SECMaterializeConfig(graph_id="sec")
    context = build_asset_context()

    result = sec_historical_materialized(context, config)

    # The asset applies the override internally
    assert result.metadata["graph_id"] == "sec_historical"

  @patch("robosystems.config.env")
  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  @patch(
    "robosystems.operations.graph.shared_repository_service.ensure_shared_subgraph_exists"
  )
  @patch("robosystems.graph_api.client.factory.boost_graph_memory")
  def test_historical_materialization_error(
    self, mock_boost, mock_ensure_subgraph, mock_processor_cls, mock_env
  ):
    """Test historical materialization error raises Failure to block downstream sensors."""
    mock_env.ENVIRONMENT = "dev"

    mat_result = _make_materialize_result(status="error", error="Timeout")
    mock_processor = MagicMock()
    mock_processor.materialize_from_duckdb = AsyncMock(return_value=mat_result)
    mock_processor_cls.return_value = mock_processor

    mock_ensure_subgraph.return_value = {"status": "exists"}
    mock_boost.return_value = {"message": "boosted"}

    config = SECMaterializeConfig(graph_id="sec_historical")
    context = build_asset_context()

    with pytest.raises(Failure, match="Timeout"):
      sec_historical_materialized(context, config)
