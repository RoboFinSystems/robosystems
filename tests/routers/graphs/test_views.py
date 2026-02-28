"""
Tests for the views router.

This test suite covers:
- create_view endpoint (fact grid query)
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.routers.graphs.views import create_view

MODULE = "robosystems.routers.graphs.views"


def _make_create_view_request(
  elements=None,
  canonical_concepts=None,
  periods=None,
  period_type="annual",
  entity=None,
  entities=None,
):
  """Helper to build a mock CreateViewRequest."""
  mock_request = MagicMock()
  mock_request.elements = elements if elements is not None else ["us-gaap:Assets"]
  mock_request.canonical_concepts = (
    canonical_concepts if canonical_concepts is not None else []
  )
  mock_request.periods = periods or []
  mock_request.period_type = period_type
  mock_request.entity = entity
  mock_request.entities = entities or []
  mock_request.form = None
  mock_request.fiscal_year = None
  mock_request.fiscal_period = None
  mock_request.include_summary = False
  mock_request.view_config = MagicMock()
  return mock_request


def _make_mock_fact_grid():
  """Helper to build a mock FactGrid result from FactGridBuilder."""
  mock_grid = MagicMock()
  mock_grid.metadata.fact_count = 5
  mock_grid.facts_df = None
  return mock_grid


@pytest.mark.asyncio
class TestCreateView:
  """Tests for create_view endpoint."""

  @pytest.mark.unit
  async def test_fact_grid_calls_query(self):
    """create_view dispatches to query_fact_grid."""
    mock_fact_data = MagicMock()
    mock_grid = _make_mock_fact_grid()
    mock_pivot = MagicMock()

    mock_builder_instance = MagicMock()
    mock_builder_instance.build.return_value = mock_grid
    mock_builder_instance.generate_pivot_table.return_value = mock_pivot

    request = _make_create_view_request(
      elements=["us-gaap:Assets"], period_type="instant"
    )

    with (
      patch(
        f"{MODULE}.query_fact_grid",
        new_callable=AsyncMock,
        return_value=mock_fact_data,
      ) as mock_query,
      patch(f"{MODULE}.FactGridBuilder", return_value=mock_builder_instance),
    ):
      await create_view(
        graph_id="kg_test",
        request=request,
        req=MagicMock(),
        current_user=MagicMock(),
      )

    mock_query.assert_called_once()
    call_kwargs = mock_query.call_args.kwargs
    assert call_kwargs["graph_id"] == "kg_test"
    assert call_kwargs["elements"] == ["us-gaap:Assets"]
    assert call_kwargs["period_type"] == "instant"
    mock_builder_instance.build.assert_called_once()

  @pytest.mark.unit
  async def test_entity_filter_passed_through(self):
    """Entity filter is passed to query_fact_grid."""
    mock_grid = _make_mock_fact_grid()
    mock_builder_instance = MagicMock()
    mock_builder_instance.build.return_value = mock_grid
    mock_builder_instance.generate_pivot_table.return_value = MagicMock()

    request = _make_create_view_request(entity="NVDA")

    with (
      patch(
        f"{MODULE}.query_fact_grid",
        new_callable=AsyncMock,
        return_value=MagicMock(),
      ) as mock_query,
      patch(f"{MODULE}.FactGridBuilder", return_value=mock_builder_instance),
    ):
      await create_view(
        graph_id="kg_test",
        request=request,
        req=MagicMock(),
        current_user=MagicMock(),
      )

    assert mock_query.call_args.kwargs["entity"] == "NVDA"

  @pytest.mark.unit
  async def test_missing_elements_raises_400(self):
    """Missing elements and canonical_concepts raises 400."""
    request = _make_create_view_request(elements=[], canonical_concepts=[])

    with pytest.raises(HTTPException) as exc_info:
      await create_view(
        graph_id="kg_test",
        request=request,
        req=MagicMock(),
        current_user=MagicMock(),
      )

    assert exc_info.value.status_code == 400
    assert "elements" in exc_info.value.detail

  @pytest.mark.unit
  async def test_missing_period_scope_raises_400(self):
    """Missing periods, period_type, and fiscal_year raises 400."""
    request = _make_create_view_request(period_type=None)
    request.fiscal_year = None

    with pytest.raises(HTTPException) as exc_info:
      await create_view(
        graph_id="kg_test",
        request=request,
        req=MagicMock(),
        current_user=MagicMock(),
      )

    assert exc_info.value.status_code == 400
    assert "period" in exc_info.value.detail.lower()

  @pytest.mark.unit
  async def test_query_error_wraps_in_500(self):
    """Internal errors are wrapped in HTTPException(500)."""
    request = _make_create_view_request()

    with patch(
      f"{MODULE}.query_fact_grid",
      new_callable=AsyncMock,
      side_effect=RuntimeError("connection lost"),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await create_view(
          graph_id="kg_test",
          request=request,
          req=MagicMock(),
          current_user=MagicMock(),
        )

    assert exc_info.value.status_code == 500
    assert "connection lost" in exc_info.value.detail
