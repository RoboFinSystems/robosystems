"""Tests for SEC S3 publish assets (LadybugDB and DuckDB)."""

from unittest.mock import patch

import pytest
from dagster import MaterializeResult, build_asset_context

from robosystems.adapters.sec.pipeline.duckdb_s3_publish import (
  sec_duckdb_s3_published,
  sec_historical_duckdb_s3_published,
)
from robosystems.adapters.sec.pipeline.s3_publish import (
  sec_historical_lbug_s3_published,
  sec_lbug_s3_published,
)


@pytest.mark.unit
class TestSecLbugS3Published:
  """Tests for sec_lbug_s3_published asset."""

  @patch("robosystems.adapters.sec.pipeline.s3_publish.publish_to_s3")
  def test_delegates_to_publish_to_s3(self, mock_publish):
    """Test asset delegates to shared publish_to_s3 helper."""
    mock_publish.return_value = MaterializeResult(
      metadata={"status": "success", "graph_id": "sec"}
    )

    context = build_asset_context()
    result = sec_lbug_s3_published(context)

    mock_publish.assert_called_once_with(context, graph_id="sec")
    assert isinstance(result, MaterializeResult)

  @patch("robosystems.adapters.sec.pipeline.s3_publish.publish_to_s3")
  def test_passes_sec_graph_id(self, mock_publish):
    """Test asset passes the correct graph_id."""
    mock_publish.return_value = MaterializeResult(metadata={})

    context = build_asset_context()
    sec_lbug_s3_published(context)

    _, kwargs = mock_publish.call_args
    assert kwargs["graph_id"] == "sec"


@pytest.mark.unit
class TestSecHistoricalLbugS3Published:
  """Tests for sec_historical_lbug_s3_published asset."""

  @patch("robosystems.adapters.sec.pipeline.s3_publish.publish_to_s3")
  def test_delegates_to_publish_to_s3(self, mock_publish):
    """Test asset delegates to shared publish_to_s3 helper."""
    mock_publish.return_value = MaterializeResult(
      metadata={"status": "success", "graph_id": "sec_historical"}
    )

    context = build_asset_context()
    result = sec_historical_lbug_s3_published(context)

    mock_publish.assert_called_once_with(context, graph_id="sec_historical")
    assert isinstance(result, MaterializeResult)

  @patch("robosystems.adapters.sec.pipeline.s3_publish.publish_to_s3")
  def test_passes_sec_historical_graph_id(self, mock_publish):
    """Test asset passes the correct graph_id for historical."""
    mock_publish.return_value = MaterializeResult(metadata={})

    context = build_asset_context()
    sec_historical_lbug_s3_published(context)

    _, kwargs = mock_publish.call_args
    assert kwargs["graph_id"] == "sec_historical"


@pytest.mark.unit
class TestSecDuckdbS3Published:
  """Tests for sec_duckdb_s3_published asset."""

  @patch("robosystems.adapters.sec.pipeline.duckdb_s3_publish.publish_duckdb_to_s3")
  def test_delegates_to_publish_duckdb_to_s3(self, mock_publish):
    """Test asset delegates to shared publish_duckdb_to_s3 helper."""
    mock_publish.return_value = MaterializeResult(
      metadata={"status": "success", "graph_id": "sec"}
    )

    context = build_asset_context()
    result = sec_duckdb_s3_published(context)

    mock_publish.assert_called_once_with(context, graph_id="sec")
    assert isinstance(result, MaterializeResult)


@pytest.mark.unit
class TestSecHistoricalDuckdbS3Published:
  """Tests for sec_historical_duckdb_s3_published asset."""

  @patch("robosystems.adapters.sec.pipeline.duckdb_s3_publish.publish_duckdb_to_s3")
  def test_delegates_to_publish_duckdb_to_s3(self, mock_publish):
    """Test asset delegates to shared publish_duckdb_to_s3 helper."""
    mock_publish.return_value = MaterializeResult(
      metadata={"status": "success", "graph_id": "sec_historical"}
    )

    context = build_asset_context()
    result = sec_historical_duckdb_s3_published(context)

    mock_publish.assert_called_once_with(context, graph_id="sec_historical")
    assert isinstance(result, MaterializeResult)

  @patch("robosystems.adapters.sec.pipeline.duckdb_s3_publish.publish_duckdb_to_s3")
  def test_passes_correct_graph_id(self, mock_publish):
    """Test asset passes the correct graph_id."""
    mock_publish.return_value = MaterializeResult(metadata={})

    context = build_asset_context()
    sec_historical_duckdb_s3_published(context)

    _, kwargs = mock_publish.call_args
    assert kwargs["graph_id"] == "sec_historical"
