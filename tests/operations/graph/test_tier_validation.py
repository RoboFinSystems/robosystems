# pyright: reportGeneralTypeIssues=false, reportArgumentType=false
"""Tests for graph tier upgrade/downgrade validation functions."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.operations.graph.tier_validation import (
  validate_storage_capacity,
  validate_subgraph_count,
)


class TestValidateSubgraphCount:
  """Tests for subgraph count validation on downgrade."""

  @patch("robosystems.operations.graph.tier_validation.get_tier_max_subgraphs")
  @patch("robosystems.operations.graph.tier_validation.Graph")
  def test_passes_when_under_limit(self, mock_graph, mock_max_subgraphs):
    mock_max_subgraphs.return_value = 3
    mock_graph.get_subgraphs.return_value = [MagicMock(), MagicMock()]
    db = MagicMock()

    validate_subgraph_count("kg123", "ladybug-standard", db)

  @patch("robosystems.operations.graph.tier_validation.get_tier_max_subgraphs")
  @patch("robosystems.operations.graph.tier_validation.Graph")
  def test_passes_when_at_limit(self, mock_graph, mock_max_subgraphs):
    mock_max_subgraphs.return_value = 3
    mock_graph.get_subgraphs.return_value = [MagicMock()] * 3
    db = MagicMock()

    validate_subgraph_count("kg123", "ladybug-standard", db)

  @patch("robosystems.operations.graph.tier_validation.get_tier_max_subgraphs")
  @patch("robosystems.operations.graph.tier_validation.Graph")
  def test_fails_when_over_limit(self, mock_graph, mock_max_subgraphs):
    mock_max_subgraphs.return_value = 3
    mock_graph.get_subgraphs.return_value = [MagicMock()] * 7
    db = MagicMock()

    with pytest.raises(HTTPException) as exc_info:
      validate_subgraph_count("kg123", "ladybug-standard", db)

    assert exc_info.value.status_code == 400
    assert "7 active subgraphs" in exc_info.value.detail
    assert "limit of 3" in exc_info.value.detail
    assert "Delete 4 subgraph(s)" in exc_info.value.detail

  @patch("robosystems.operations.graph.tier_validation.get_tier_max_subgraphs")
  @patch("robosystems.operations.graph.tier_validation.Graph")
  def test_passes_when_no_limit(self, mock_graph, mock_max_subgraphs):
    mock_max_subgraphs.return_value = None
    db = MagicMock()

    validate_subgraph_count("kg123", "ladybug-xlarge", db)
    mock_graph.get_subgraphs.assert_not_called()

  @patch("robosystems.operations.graph.tier_validation.get_tier_max_subgraphs")
  @patch("robosystems.operations.graph.tier_validation.Graph")
  def test_passes_with_zero_subgraphs(self, mock_graph, mock_max_subgraphs):
    mock_max_subgraphs.return_value = 3
    mock_graph.get_subgraphs.return_value = []
    db = MagicMock()

    validate_subgraph_count("kg123", "ladybug-standard", db)


class TestValidateStorageCapacity:
  """Tests for storage capacity validation on downgrade."""

  @pytest.mark.asyncio
  @patch("robosystems.operations.graph.tier_validation.GraphTierConfig")
  async def test_skips_for_upgrade(self, mock_tier_config):
    mock_tier_config.get_instance_storage_limit_gb.side_effect = [50, 20]
    db = MagicMock()

    await validate_storage_capacity("kg123", "ladybug-standard", "ladybug-large", db)

  @pytest.mark.asyncio
  @patch(
    "robosystems.middleware.graph.ingestion_limits.IngestionLimitChecker",
    new_callable=MagicMock,
  )
  @patch("robosystems.operations.graph.tier_validation.GraphTierConfig")
  async def test_passes_when_under_limit(self, mock_tier_config, mock_checker):
    mock_tier_config.get_instance_storage_limit_gb.side_effect = [20, 50]
    mock_checker.check_instance_storage = AsyncMock(
      return_value={"total_storage_gb": 15}
    )
    db = MagicMock()

    await validate_storage_capacity("kg123", "ladybug-large", "ladybug-standard", db)

  @pytest.mark.asyncio
  @patch(
    "robosystems.middleware.graph.ingestion_limits.IngestionLimitChecker",
    new_callable=MagicMock,
  )
  @patch("robosystems.operations.graph.tier_validation.GraphTierConfig")
  async def test_fails_when_over_limit(self, mock_tier_config, mock_checker):
    mock_tier_config.get_instance_storage_limit_gb.side_effect = [20, 50]
    mock_checker.check_instance_storage = AsyncMock(
      return_value={"total_storage_gb": 45}
    )
    db = MagicMock()

    with pytest.raises(HTTPException) as exc_info:
      await validate_storage_capacity("kg123", "ladybug-large", "ladybug-standard", db)

    assert exc_info.value.status_code == 400
    assert "45.0 GB storage" in exc_info.value.detail
    assert "limit of 20 GB" in exc_info.value.detail
