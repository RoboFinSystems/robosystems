"""
Tests for the views router.

This test suite covers:
- get_graph_tier helper (sync, maps graph_id to GraphTier enum)
- create_view endpoint (async, dispatches to trial balance or fact query)
- save_view endpoint (async, delegates to save_view_as_report)
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import HTTPException

from robosystems.config.graph_tier import GraphTier
from robosystems.models.api.views import ViewSourceType
from robosystems.routers.graphs.views import create_view, get_graph_tier, save_view

MODULE = "robosystems.routers.graphs.views"


class TestGetGraphTier:
  """Tests for get_graph_tier helper."""

  @pytest.mark.unit
  def test_graph_with_known_tier(self):
    """Known tier string on the graph record maps to the correct GraphTier enum."""
    mock_graph = MagicMock()
    mock_graph.graph_tier = "ladybug-large"

    mock_session = MagicMock()
    mock_session.query.return_value.filter.return_value.first.return_value = mock_graph

    result = get_graph_tier("kg_test123", mock_session)

    assert result == GraphTier.LADYBUG_LARGE

  @pytest.mark.unit
  def test_graph_not_found_returns_default(self):
    """When no graph record is found, returns LADYBUG_STANDARD as default."""
    mock_session = MagicMock()
    mock_session.query.return_value.filter.return_value.first.return_value = None

    result = get_graph_tier("kg_nonexistent", mock_session)

    assert result == GraphTier.LADYBUG_STANDARD

  @pytest.mark.unit
  def test_graph_with_no_tier_returns_default(self):
    """When graph exists but graph_tier is None, returns LADYBUG_STANDARD."""
    mock_graph = MagicMock()
    mock_graph.graph_tier = None

    mock_session = MagicMock()
    mock_session.query.return_value.filter.return_value.first.return_value = mock_graph

    result = get_graph_tier("kg_test456", mock_session)

    assert result == GraphTier.LADYBUG_STANDARD


def _make_create_view_request(
  source_type=ViewSourceType.TRANSACTIONS,
  fact_set_id=None,
  mapping_structure_id=None,
):
  """Helper to build a mock CreateViewRequest."""
  mock_request = MagicMock()
  mock_request.source.type = source_type
  mock_request.source.period_start = "2024-01-01"
  mock_request.source.period_end = "2024-12-31"
  mock_request.source.entity_id = None
  mock_request.source.fact_set_id = fact_set_id
  mock_request.view_config = None
  mock_request.mapping_structure_id = mapping_structure_id
  return mock_request


def _make_mock_fact_grid():
  """Helper to build a mock FactGrid result from FactGridBuilder."""
  mock_grid = MagicMock()
  mock_grid.metadata.fact_count = 5
  return mock_grid


@pytest.mark.asyncio
class TestCreateView:
  """Tests for create_view endpoint."""

  @pytest.mark.unit
  async def test_transactions_source_calls_aggregate(self):
    """TRANSACTIONS source type dispatches to aggregate_trial_balance."""
    mock_fact_data = MagicMock()
    mock_grid = _make_mock_fact_grid()
    mock_pivot = MagicMock()

    mock_builder_instance = MagicMock()
    mock_builder_instance.build.return_value = mock_grid
    mock_builder_instance.generate_pivot_table.return_value = mock_pivot

    request = _make_create_view_request(source_type=ViewSourceType.TRANSACTIONS)
    mock_req = MagicMock()
    mock_user = MagicMock()
    mock_session = MagicMock()

    with (
      patch(
        f"{MODULE}.aggregate_trial_balance",
        new_callable=AsyncMock,
        return_value=mock_fact_data,
      ) as mock_agg,
      patch(f"{MODULE}.FactGridBuilder", return_value=mock_builder_instance),
    ):
      result = await create_view(
        graph_id="kg_test",
        request=request,
        req=mock_req,
        current_user=mock_user,
        session=mock_session,
      )

    mock_agg.assert_called_once()
    call_kwargs = mock_agg.call_args
    assert call_kwargs.kwargs["graph_id"] == "kg_test"
    mock_builder_instance.build.assert_called_once()

  @pytest.mark.unit
  async def test_fact_set_source_calls_query_facts(self):
    """FACT_SET source type dispatches to query_facts_with_aspects with fact_set_id."""
    mock_fact_data = MagicMock()
    mock_grid = _make_mock_fact_grid()
    mock_pivot = MagicMock()

    mock_builder_instance = MagicMock()
    mock_builder_instance.build.return_value = mock_grid
    mock_builder_instance.generate_pivot_table.return_value = mock_pivot

    request = _make_create_view_request(
      source_type=ViewSourceType.FACT_SET, fact_set_id="fs_123"
    )
    mock_req = MagicMock()
    mock_user = MagicMock()
    mock_session = MagicMock()

    with (
      patch(
        f"{MODULE}.query_facts_with_aspects",
        new_callable=AsyncMock,
        return_value=mock_fact_data,
      ) as mock_query,
      patch(f"{MODULE}.FactGridBuilder", return_value=mock_builder_instance),
    ):
      result = await create_view(
        graph_id="kg_test",
        request=request,
        req=mock_req,
        current_user=mock_user,
        session=mock_session,
      )

    mock_query.assert_called_once()
    call_kwargs = mock_query.call_args
    assert call_kwargs.kwargs["fact_set_id"] == "fs_123"

  @pytest.mark.unit
  async def test_unsupported_source_raises_error(self):
    """An unsupported source type raises an HTTPException.

    Note: The catch-all exception handler in create_view wraps the original
    HTTPException(400) into an HTTPException(500). This tests the actual
    behavior of the endpoint.
    """
    request = _make_create_view_request()
    # Override source type to something unexpected (bypass enum validation)
    request.source.type = "unsupported_type"

    mock_req = MagicMock()
    mock_user = MagicMock()
    mock_session = MagicMock()

    with pytest.raises(HTTPException) as exc_info:
      await create_view(
        graph_id="kg_test",
        request=request,
        req=mock_req,
        current_user=mock_user,
        session=mock_session,
      )

    # The catch-all wraps the original 400 into a 500
    assert exc_info.value.status_code == 500
    assert "Unsupported source type" in exc_info.value.detail

  @pytest.mark.unit
  async def test_with_mapping_structure_applies_mapping(self):
    """When mapping_structure_id is set, get_mapping_structure and apply_element_mapping are called."""
    mock_fact_data = MagicMock()
    mock_mapped_data = MagicMock()
    mock_grid = _make_mock_fact_grid()
    mock_pivot = MagicMock()

    mock_builder_instance = MagicMock()
    mock_builder_instance.build.return_value = mock_grid
    mock_builder_instance.generate_pivot_table.return_value = mock_pivot

    mock_mapping = MagicMock()
    mock_mapping.structure = {"some": "mapping"}

    request = _make_create_view_request(
      source_type=ViewSourceType.TRANSACTIONS,
      mapping_structure_id="ms_abc",
    )
    mock_req = MagicMock()
    mock_user = MagicMock()
    mock_session = MagicMock()

    # Mock get_graph_tier to return a known tier
    mock_graph = MagicMock()
    mock_graph.graph_tier = "ladybug-standard"
    mock_session.query.return_value.filter.return_value.first.return_value = mock_graph

    with (
      patch(
        f"{MODULE}.aggregate_trial_balance",
        new_callable=AsyncMock,
        return_value=mock_fact_data,
      ),
      patch(
        f"{MODULE}.get_mapping_structure",
        new_callable=AsyncMock,
        return_value=mock_mapping,
      ) as mock_get_mapping,
      patch(
        f"{MODULE}.apply_element_mapping",
        return_value=mock_mapped_data,
      ) as mock_apply,
      patch(f"{MODULE}.FactGridBuilder", return_value=mock_builder_instance),
    ):
      result = await create_view(
        graph_id="kg_test",
        request=request,
        req=mock_req,
        current_user=mock_user,
        session=mock_session,
      )

    mock_get_mapping.assert_called_once_with("kg_test", "ms_abc", GraphTier.LADYBUG_STANDARD)
    mock_apply.assert_called_once_with(mock_fact_data, {"some": "mapping"})


@pytest.mark.asyncio
class TestSaveView:
  """Tests for save_view endpoint."""

  @pytest.mark.unit
  async def test_happy_path_calls_save_view_as_report(self):
    """Successful save delegates to save_view_as_report and returns its response."""
    mock_response = MagicMock()
    mock_response.report_id = "rpt_001"

    mock_request = MagicMock()
    mock_req = MagicMock()
    mock_user = MagicMock()
    mock_session = MagicMock()

    with patch(
      f"{MODULE}.save_view_as_report",
      new_callable=AsyncMock,
      return_value=mock_response,
    ) as mock_save:
      result = await save_view(
        graph_id="kg_test",
        request=mock_request,
        req=mock_req,
        current_user=mock_user,
        session=mock_session,
      )

    mock_save.assert_called_once_with("kg_test", mock_request)
    assert result == mock_response

  @pytest.mark.unit
  async def test_error_wraps_in_500(self):
    """RuntimeError from save_view_as_report is wrapped in HTTPException(500)."""
    mock_request = MagicMock()
    mock_req = MagicMock()
    mock_user = MagicMock()
    mock_session = MagicMock()

    with patch(
      f"{MODULE}.save_view_as_report",
      new_callable=AsyncMock,
      side_effect=RuntimeError("disk full"),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await save_view(
          graph_id="kg_test",
          request=mock_request,
          req=mock_req,
          current_user=mock_user,
          session=mock_session,
        )

    assert exc_info.value.status_code == 500
    assert "disk full" in exc_info.value.detail
