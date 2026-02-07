"""Tests for IngestionLimitChecker."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.middleware.graph.ingestion_limits import IngestionLimitChecker


class TestIsRelationshipTable:
  """Test relationship table detection by naming convention."""

  def test_uppercase_with_underscore_is_relationship(self):
    assert IngestionLimitChecker._is_relationship_table("ENTITY_HAS_FACT") is True

  def test_contains_has_pattern(self):
    assert IngestionLimitChecker._is_relationship_table("PERSON_HAS_ADDRESS") is True

  def test_contains_in_pattern(self):
    assert IngestionLimitChecker._is_relationship_table("PERSON_IN_ORG") is True

  def test_contains_of_pattern(self):
    assert IngestionLimitChecker._is_relationship_table("PART_OF_GROUP") is True

  def test_pascal_case_is_node(self):
    assert IngestionLimitChecker._is_relationship_table("Entity") is False

  def test_single_word_pascal_is_node(self):
    assert IngestionLimitChecker._is_relationship_table("Fact") is False

  def test_empty_string_is_not_relationship(self):
    assert IngestionLimitChecker._is_relationship_table("") is False

  def test_none_is_not_relationship(self):
    assert IngestionLimitChecker._is_relationship_table(None) is False

  def test_lowercase_single_word_is_node(self):
    assert IngestionLimitChecker._is_relationship_table("person") is False

  def test_uppercase_single_word_is_node(self):
    # Single word without underscore is a node
    assert IngestionLimitChecker._is_relationship_table("ENTITY") is False


class TestCheckMaterializationLimits:
  """Test materialization limit checking."""

  @pytest.mark.asyncio
  async def test_within_limits_allowed(self):
    """Test that materialization is allowed when within limits."""
    mock_db = MagicMock()
    # Mock _get_pending_row_counts to return small counts
    with (
      patch.object(
        IngestionLimitChecker,
        "_get_pending_row_counts",
        return_value={"Entity": 1000, "Fact": 2000, "ENTITY_HAS_FACT": 500},
      ),
      patch.object(
        IngestionLimitChecker,
        "_get_current_graph_counts",
        new_callable=AsyncMock,
        return_value=(100_000, 200_000),
      ),
    ):
      result = await IngestionLimitChecker.check_materialization_limits(
        db=mock_db,
        graph_id="kg_test",
        tier="ladybug-standard",
      )

    assert result["allowed"] is True
    assert result["errors"] == []
    assert result["tier"] == "ladybug-standard"
    assert result["current_usage"]["total_pending_rows"] == 3500

  @pytest.mark.asyncio
  async def test_exceeds_max_rows_per_copy(self):
    """Test that exceeding max_rows_per_copy blocks materialization."""
    mock_db = MagicMock()
    with (
      patch.object(
        IngestionLimitChecker,
        "_get_pending_row_counts",
        return_value={"Entity": 3_000_000},
      ),
      patch.object(
        IngestionLimitChecker,
        "_get_current_graph_counts",
        new_callable=AsyncMock,
        return_value=(0, 0),
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.GraphTierConfig.get_graph_limits",
        return_value={
          "max_nodes": 5_000_000,
          "max_relationships": 10_000_000,
          "max_rows_per_copy": 2_000_000,
          "max_single_table_rows": 5_000_000,
          "chunk_size_rows": 1_000_000,
          "warn_at_percentage": 80,
        },
      ),
    ):
      result = await IngestionLimitChecker.check_materialization_limits(
        db=mock_db,
        graph_id="kg_test",
        tier="ladybug-standard",
      )

    assert result["allowed"] is False
    assert any("max_rows_per_copy" in e for e in result["errors"])

  @pytest.mark.asyncio
  async def test_exceeds_node_limit(self):
    """Test that exceeding node limit blocks materialization."""
    mock_db = MagicMock()
    with (
      patch.object(
        IngestionLimitChecker,
        "_get_pending_row_counts",
        return_value={"Entity": 1_000_000},
      ),
      patch.object(
        IngestionLimitChecker,
        "_get_current_graph_counts",
        new_callable=AsyncMock,
        return_value=(4_500_000, 0),
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.GraphTierConfig.get_graph_limits",
        return_value={
          "max_nodes": 5_000_000,
          "max_relationships": 10_000_000,
          "max_rows_per_copy": 2_000_000,
          "max_single_table_rows": 5_000_000,
          "chunk_size_rows": 1_000_000,
          "warn_at_percentage": 80,
        },
      ),
    ):
      result = await IngestionLimitChecker.check_materialization_limits(
        db=mock_db,
        graph_id="kg_test",
        tier="ladybug-standard",
      )

    assert result["allowed"] is False
    assert any("Projected nodes" in e for e in result["errors"])

  @pytest.mark.asyncio
  async def test_approaching_limit_generates_warning(self):
    """Test that approaching 80% generates a warning."""
    mock_db = MagicMock()
    with (
      patch.object(
        IngestionLimitChecker,
        "_get_pending_row_counts",
        return_value={"Entity": 100_000},
      ),
      patch.object(
        IngestionLimitChecker,
        "_get_current_graph_counts",
        new_callable=AsyncMock,
        return_value=(4_100_000, 0),
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.GraphTierConfig.get_graph_limits",
        return_value={
          "max_nodes": 5_000_000,
          "max_relationships": 10_000_000,
          "max_rows_per_copy": 2_000_000,
          "max_single_table_rows": 5_000_000,
          "chunk_size_rows": 1_000_000,
          "warn_at_percentage": 80,
        },
      ),
    ):
      result = await IngestionLimitChecker.check_materialization_limits(
        db=mock_db,
        graph_id="kg_test",
        tier="ladybug-standard",
      )

    assert result["allowed"] is True
    assert len(result["warnings"]) > 0
    assert any("node limit" in w.lower() for w in result["warnings"])

  @pytest.mark.asyncio
  async def test_graph_counts_unavailable_still_checks_rows(self):
    """Test that limit check works even when graph counts are unavailable."""
    mock_db = MagicMock()
    with (
      patch.object(
        IngestionLimitChecker,
        "_get_pending_row_counts",
        return_value={"Entity": 1000},
      ),
      patch.object(
        IngestionLimitChecker,
        "_get_current_graph_counts",
        new_callable=AsyncMock,
        side_effect=Exception("Connection failed"),
      ),
    ):
      result = await IngestionLimitChecker.check_materialization_limits(
        db=mock_db,
        graph_id="kg_test",
        tier="ladybug-standard",
      )

    assert result["allowed"] is True
    assert result["current_usage"]["current_nodes"] is None
    assert result["current_usage"]["current_relationships"] is None


class TestCheckGraphUsage:
  """Test graph usage checking for /limits endpoint."""

  @pytest.mark.asyncio
  async def test_within_limits(self):
    with patch.object(
      IngestionLimitChecker,
      "_get_current_graph_counts",
      new_callable=AsyncMock,
      return_value=(1_000_000, 2_000_000),
    ):
      result = await IngestionLimitChecker.check_graph_usage(
        graph_id="kg_test",
        tier="ladybug-standard",
      )

    assert result["within_limits"] is True
    assert result["warnings"] == []

  @pytest.mark.asyncio
  async def test_over_limits(self):
    with (
      patch.object(
        IngestionLimitChecker,
        "_get_current_graph_counts",
        new_callable=AsyncMock,
        return_value=(6_000_000, 0),
      ),
      patch(
        "robosystems.middleware.graph.ingestion_limits.GraphTierConfig.get_graph_limits",
        return_value={
          "max_nodes": 5_000_000,
          "max_relationships": 10_000_000,
          "max_rows_per_copy": 2_000_000,
          "max_single_table_rows": 5_000_000,
          "chunk_size_rows": 1_000_000,
          "warn_at_percentage": 80,
        },
      ),
    ):
      result = await IngestionLimitChecker.check_graph_usage(
        graph_id="kg_test",
        tier="ladybug-standard",
      )

    assert result["within_limits"] is False
