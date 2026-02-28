from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from robosystems.operations.views.fact_query import (
  query_fact_grid,
  query_facts_with_aspects,
)

MOCK_GRAPH_ID = "kg_test123"


@pytest.fixture
def mock_repository():
  repo = AsyncMock()
  repo.execute_query = AsyncMock(return_value=[])
  return repo


# ---------------------------------------------------------------------------
# query_fact_grid (new, aligned with MCP tool)
# ---------------------------------------------------------------------------


class TestQueryFactGrid:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_element_qname_filter(self, mock_repository):
    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_fact_grid(MOCK_GRAPH_ID, elements=["us-gaap:Assets"])

    query = mock_repository.execute_query.call_args[0][0]
    params = mock_repository.execute_query.call_args[0][1]
    assert "el.qname IN $elements" in query
    assert params["elements"] == ["us-gaap:Assets"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_canonical_concept_filter(self, mock_repository):
    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_fact_grid(MOCK_GRAPH_ID, canonical_concepts=["revenue"])

    query = mock_repository.execute_query.call_args[0][0]
    params = mock_repository.execute_query.call_args[0][1]
    assert "el.canonical_concept IN $canonical_concepts" in query
    assert params["canonical_concepts"] == ["revenue"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_combined_elements_and_concepts(self, mock_repository):
    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_fact_grid(
        MOCK_GRAPH_ID,
        elements=["us-gaap:Assets"],
        canonical_concepts=["revenue"],
      )

    query = mock_repository.execute_query.call_args[0][0]
    assert (
      "el.qname IN $elements OR el.canonical_concept IN $canonical_concepts" in query
    )

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_period_type_annual(self, mock_repository):
    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_fact_grid(
        MOCK_GRAPH_ID, elements=["us-gaap:NetIncomeLoss"], period_type="annual"
      )

    query = mock_repository.execute_query.call_args[0][0]
    assert "p.duration_type = 'annual'" in query

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_period_type_instant(self, mock_repository):
    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_fact_grid(
        MOCK_GRAPH_ID, elements=["us-gaap:Assets"], period_type="instant"
      )

    query = mock_repository.execute_query.call_args[0][0]
    assert "p.period_type = 'instant'" in query

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_entity_ticker_filter(self, mock_repository):
    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_fact_grid(MOCK_GRAPH_ID, elements=["us-gaap:Assets"], entity="NVDA")

    query = mock_repository.execute_query.call_args[0][0]
    params = mock_repository.execute_query.call_args[0][1]
    assert "ent.ticker IN $entities" in query
    assert params["entities"] == ["NVDA"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_multi_entity_filter(self, mock_repository):
    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_fact_grid(
        MOCK_GRAPH_ID, elements=["us-gaap:Assets"], entities=["NVDA", "AAPL"]
      )

    params = mock_repository.execute_query.call_args[0][1]
    assert params["entities"] == ["NVDA", "AAPL"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_report_filters(self, mock_repository):
    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_fact_grid(
        MOCK_GRAPH_ID,
        elements=["us-gaap:Assets"],
        form="10-K",
        fiscal_year=2024,
        fiscal_period="FY",
      )

    query = mock_repository.execute_query.call_args[0][0]
    params = mock_repository.execute_query.call_args[0][1]
    assert "REPORT_HAS_FACT" in query
    assert params["form"] == "10-K"
    assert params["fiscal_year"] == 2024
    assert params["fiscal_period"] == "FY"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_has_dimensions_always_false(self, mock_repository):
    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_fact_grid(MOCK_GRAPH_ID, elements=["us-gaap:Assets"])

    query = mock_repository.execute_query.call_args[0][0]
    assert "f.has_dimensions = false" in query

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_return_distinct(self, mock_repository):
    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_fact_grid(MOCK_GRAPH_ID, elements=["us-gaap:Assets"])

    query = mock_repository.execute_query.call_args[0][0]
    assert "RETURN DISTINCT" in query

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_entity_columns_included_when_filtered(self, mock_repository):
    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_fact_grid(MOCK_GRAPH_ID, elements=["us-gaap:Assets"], entity="NVDA")

    query = mock_repository.execute_query.call_args[0][0]
    assert "ent.ticker as entity_ticker" in query
    assert "ent.name as entity_name" in query

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_entity_columns_excluded_when_no_filter(self, mock_repository):
    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_fact_grid(MOCK_GRAPH_ID, elements=["us-gaap:Assets"])

    query = mock_repository.execute_query.call_args[0][0]
    assert "entity_ticker" not in query

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_empty_results_returns_empty_dataframe(self, mock_repository):
    mock_repository.execute_query.return_value = []
    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      result = await query_fact_grid(MOCK_GRAPH_ID, elements=["us-gaap:Assets"])

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0
    assert "element_id" in result.columns
    assert "value" in result.columns

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_periods_filter(self, mock_repository):
    with patch(
      "robosystems.operations.views.fact_query.get_graph_repository",
      return_value=mock_repository,
    ):
      await query_fact_grid(
        MOCK_GRAPH_ID,
        elements=["us-gaap:Assets"],
        periods=["2024-12-31", "2025-01-26"],
      )

    query = mock_repository.execute_query.call_args[0][0]
    params = mock_repository.execute_query.call_args[0][1]
    assert "p.end_date IN $periods" in query
    assert params["periods"] == ["2024-12-31", "2025-01-26"]


# ---------------------------------------------------------------------------
# query_facts_with_aspects (legacy, retained for save_view)
# ---------------------------------------------------------------------------


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
