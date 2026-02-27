"""
Tests for the views router.

This test suite covers:
- create_view endpoint (async, dispatches to trial balance or fact query)
- save_view endpoint (async, delegates to save_view_as_report)
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.api.views import ViewSourceType
from robosystems.routers.graphs.views import create_view, save_view

MODULE = "robosystems.routers.graphs.views"


def _make_create_view_request(
  source_type=ViewSourceType.TRANSACTIONS,
  fact_set_id=None,
):
  """Helper to build a mock CreateViewRequest."""
  mock_request = MagicMock()
  mock_request.source.type = source_type
  mock_request.source.period_start = "2024-01-01"
  mock_request.source.period_end = "2024-12-31"
  mock_request.source.entity_id = None
  mock_request.source.fact_set_id = fact_set_id
  mock_request.view_config = None
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
      await create_view(
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
      await create_view(
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
