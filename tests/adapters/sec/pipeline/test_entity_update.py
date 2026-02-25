"""Tests for SEC entity update asset."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from dagster import MaterializeResult, build_asset_context

from robosystems.adapters.sec.pipeline.configs import SECEntityUpdateConfig
from robosystems.adapters.sec.pipeline.entity_update import (
  sec_entity_incremental_update,
)


def _make_entity_update_result(
  status="success",
  error=None,
  entities_checked=1000,
  entities_updated=50,
  entities_unchanged=940,
  entities_failed=10,
  duration_ms=5000,
):
  """Create a mock entity update result."""
  result = MagicMock()
  result.status = status
  result.error = error
  result.entities_checked = entities_checked
  result.entities_updated = entities_updated
  result.entities_unchanged = entities_unchanged
  result.entities_failed = entities_failed
  result.duration_ms = duration_ms
  return result


@pytest.mark.unit
class TestSecEntityIncrementalUpdate:
  """Tests for sec_entity_incremental_update asset."""

  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  def test_successful_entity_update(self, mock_processor_cls):
    """Test successful entity update."""
    update_result = _make_entity_update_result()
    mock_processor = MagicMock()
    mock_processor.update_entities_from_s3 = AsyncMock(return_value=update_result)
    mock_processor_cls.return_value = mock_processor

    config = SECEntityUpdateConfig()
    context = build_asset_context()

    result = sec_entity_incremental_update(context, config)

    assert isinstance(result, MaterializeResult)
    assert result.metadata["status"] == "success"
    assert result.metadata["entities_checked"] == 1000
    assert result.metadata["entities_updated"] == 50
    assert result.metadata["entities_unchanged"] == 940
    assert result.metadata["entities_failed"] == 10

  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  def test_entity_update_error(self, mock_processor_cls):
    """Test entity update error handling."""
    update_result = _make_entity_update_result(
      status="error", error="LadybugDB connection failed"
    )
    mock_processor = MagicMock()
    mock_processor.update_entities_from_s3 = AsyncMock(return_value=update_result)
    mock_processor_cls.return_value = mock_processor

    config = SECEntityUpdateConfig()
    context = build_asset_context()

    result = sec_entity_incremental_update(context, config)

    assert result.metadata["status"] == "error"
    assert "LadybugDB connection failed" in str(result.metadata["error"])

  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  def test_entity_update_with_specific_quarter(self, mock_processor_cls):
    """Test entity update with specific year/quarter."""
    update_result = _make_entity_update_result()
    mock_processor = MagicMock()
    mock_processor.update_entities_from_s3 = AsyncMock(return_value=update_result)
    mock_processor_cls.return_value = mock_processor

    config = SECEntityUpdateConfig(year=2025, quarter=1)
    context = build_asset_context()

    result = sec_entity_incremental_update(context, config)

    # Verify the year/quarter parameters were passed
    call_kwargs = mock_processor.update_entities_from_s3.call_args
    assert call_kwargs.kwargs["year"] == 2025
    assert call_kwargs.kwargs["quarter"] == 1

    assert result.metadata["year"] == 2025
    assert result.metadata["quarter"] == 1

  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  def test_entity_update_default_year_quarter(self, mock_processor_cls):
    """Test entity update with default (None) year/quarter."""
    update_result = _make_entity_update_result()
    mock_processor = MagicMock()
    mock_processor.update_entities_from_s3 = AsyncMock(return_value=update_result)
    mock_processor_cls.return_value = mock_processor

    config = SECEntityUpdateConfig()
    context = build_asset_context()

    sec_entity_incremental_update(context, config)

    call_kwargs = mock_processor.update_entities_from_s3.call_args
    assert call_kwargs.kwargs["year"] is None
    assert call_kwargs.kwargs["quarter"] is None

  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  def test_entity_update_no_changes(self, mock_processor_cls):
    """Test entity update with no entities changed."""
    update_result = _make_entity_update_result(
      entities_checked=500,
      entities_updated=0,
      entities_unchanged=500,
      entities_failed=0,
    )
    mock_processor = MagicMock()
    mock_processor.update_entities_from_s3 = AsyncMock(return_value=update_result)
    mock_processor_cls.return_value = mock_processor

    config = SECEntityUpdateConfig()
    context = build_asset_context()

    result = sec_entity_incremental_update(context, config)

    assert result.metadata["status"] == "success"
    assert result.metadata["entities_updated"] == 0
    assert result.metadata["entities_unchanged"] == 500

  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  def test_entity_update_uses_correct_graph_id(self, mock_processor_cls):
    """Test entity update creates processor with correct graph_id."""
    update_result = _make_entity_update_result()
    mock_processor = MagicMock()
    mock_processor.update_entities_from_s3 = AsyncMock(return_value=update_result)
    mock_processor_cls.return_value = mock_processor

    config = SECEntityUpdateConfig(graph_id="sec")
    context = build_asset_context()

    sec_entity_incremental_update(context, config)

    mock_processor_cls.assert_called_once_with(graph_id="sec")

  @patch("robosystems.adapters.sec.XBRLDuckDBGraphProcessor")
  def test_entity_update_duration_metadata(self, mock_processor_cls):
    """Test that duration is included in metadata."""
    update_result = _make_entity_update_result(duration_ms=12345)
    mock_processor = MagicMock()
    mock_processor.update_entities_from_s3 = AsyncMock(return_value=update_result)
    mock_processor_cls.return_value = mock_processor

    config = SECEntityUpdateConfig()
    context = build_asset_context()

    result = sec_entity_incremental_update(context, config)

    assert result.metadata["duration_ms"] == 12345
