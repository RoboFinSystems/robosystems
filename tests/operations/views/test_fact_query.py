from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from robosystems.operations.views.fact_query import query_facts_with_aspects

MOCK_GRAPH_ID = "kg_test123"


@pytest.fixture
def mock_repository():
  repo = AsyncMock()
  repo.execute_query = AsyncMock(return_value=[])
  return repo


class TestQueryFactsWithAspects:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_no_dimensions_excludes_dimensional_facts(self, mock_repository):
    mock_repository.execute_query.return_value = []

    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_facts_with_aspects(MOCK_GRAPH_ID)

    query = mock_repository.execute_query.call_args[0][0]
    assert "d IS NULL" in query

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_empty_dimensions_excludes_dimensional_facts(self, mock_repository):
    mock_repository.execute_query.return_value = []

    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_facts_with_aspects(MOCK_GRAPH_ID, requested_dimensions=[])

    query = mock_repository.execute_query.call_args[0][0]
    assert "d IS NULL" in query

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_with_requested_dimensions(self, mock_repository):
    mock_repository.execute_query.return_value = []

    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_facts_with_aspects(
        MOCK_GRAPH_ID, requested_dimensions=["Segment", "Product"]
      )

    query = mock_repository.execute_query.call_args[0][0]
    params = mock_repository.execute_query.call_args[0][1]
    assert "axis.name IN $requested_dimensions" in query
    assert params["requested_dimensions"] == ["Segment", "Product"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_with_fact_set_id_filter(self, mock_repository):
    mock_repository.execute_query.return_value = []

    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_facts_with_aspects(MOCK_GRAPH_ID, fact_set_id="fs-2024")

    query = mock_repository.execute_query.call_args[0][0]
    params = mock_repository.execute_query.call_args[0][1]
    assert "fs.identifier = $fact_set_id" in query
    assert "FACT_SET_CONTAINS_FACT" in query
    assert params["fact_set_id"] == "fs-2024"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_with_period_filters(self, mock_repository):
    mock_repository.execute_query.return_value = []

    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_facts_with_aspects(
        MOCK_GRAPH_ID,
        period_start="2024-01-01",
        period_end="2024-12-31",
      )

    params = mock_repository.execute_query.call_args[0][1]
    assert params["period_start"] == "2024-01-01"
    assert params["period_end"] == "2024-12-31"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_with_entity_filter(self, mock_repository):
    mock_repository.execute_query.return_value = []

    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_facts_with_aspects(MOCK_GRAPH_ID, entity_id="NVDA")

    query = mock_repository.execute_query.call_args[0][0]
    params = mock_repository.execute_query.call_args[0][1]
    assert "ent.identifier = $entity_id" in query
    assert params["entity_id"] == "NVDA"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_empty_results_returns_empty_dataframe(self, mock_repository):
    mock_repository.execute_query.return_value = []

    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      result = await query_facts_with_aspects(MOCK_GRAPH_ID)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0
    assert "fact_id" in result.columns
    assert "numeric_value" in result.columns
    assert "element_id" in result.columns
    assert "dimension_axis" in result.columns
    assert "dimension_member" in result.columns

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_none_results_returns_empty_dataframe(self, mock_repository):
    mock_repository.execute_query.return_value = None

    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      result = await query_facts_with_aspects(MOCK_GRAPH_ID)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_results_converted_to_dataframe(self, mock_repository):
    mock_repository.execute_query.return_value = [
      {
        "fact_id": "f1",
        "numeric_value": 1000.0,
        "element_id": "us-gaap:Assets",
        "element_name": "Assets",
        "element_classification": "asset",
        "element_period_type": "instant",
        "period_id": "p1",
        "period_start": "2024-01-01",
        "period_end": "2024-12-31",
        "calendar_year": 2024,
        "unit_value": "USD",
        "entity_id": "NVDA",
        "dimension_axis": None,
        "dimension_member": None,
      },
      {
        "fact_id": "f2",
        "numeric_value": 500.0,
        "element_id": "us-gaap:Cash",
        "element_name": "Cash",
        "element_classification": "asset",
        "element_period_type": "instant",
        "period_id": "p1",
        "period_start": "2024-01-01",
        "period_end": "2024-12-31",
        "calendar_year": 2024,
        "unit_value": "USD",
        "entity_id": "NVDA",
        "dimension_axis": None,
        "dimension_member": None,
      },
    ]

    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      result = await query_facts_with_aspects(MOCK_GRAPH_ID)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert result.iloc[0]["fact_id"] == "f1"
    assert result.iloc[1]["numeric_value"] == 500.0

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_all_filters_combined(self, mock_repository):
    mock_repository.execute_query.return_value = []

    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_facts_with_aspects(
        MOCK_GRAPH_ID,
        fact_set_id="fs-2024",
        period_start="2024-01-01",
        period_end="2024-12-31",
        entity_id="NVDA",
        requested_dimensions=["Segment"],
      )

    query = mock_repository.execute_query.call_args[0][0]
    params = mock_repository.execute_query.call_args[0][1]
    assert "WHERE" in query
    assert params["fact_set_id"] == "fs-2024"
    assert params["period_start"] == "2024-01-01"
    assert params["period_end"] == "2024-12-31"
    assert params["entity_id"] == "NVDA"
    assert params["requested_dimensions"] == ["Segment"]
