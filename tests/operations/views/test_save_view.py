from unittest.mock import AsyncMock, patch

import pytest

from robosystems.models.api.views.save_view import SaveViewRequest
from robosystems.operations.views.save_view import (
  check_report_exists,
  create_calculation_structure,
  create_fact_nodes,
  create_presentation_structure,
  create_report_node,
  delete_report_data,
  generate_report_id,
  get_entity_info,
  query_view_facts,
  save_view_as_report,
)

MOCK_GRAPH_ID = "kg_test123"


@pytest.fixture
def mock_repository():
  repo = AsyncMock()
  repo.execute_query = AsyncMock(return_value=[])
  return repo


class TestGenerateReportId:
  @pytest.mark.unit
  def test_basic_format(self):
    result = generate_report_id("nvidia-corp", "2024-12-31", "10-K")
    assert result == "nvidia-c-10-k-20241231"

  @pytest.mark.unit
  def test_short_entity_id(self):
    result = generate_report_id("NVDA", "2024-01-31", "Annual Report")
    assert result == "NVDA-annual-report-20240131"

  @pytest.mark.unit
  def test_entity_id_truncated_to_8_chars(self):
    result = generate_report_id("very-long-entity-id", "2024-06-30", "10-Q")
    assert result.startswith("very-lon-")

  @pytest.mark.unit
  def test_type_slug_lowered(self):
    result = generate_report_id("AAPL", "2024-12-31", "Quarterly Report")
    assert "quarterly-report" in result


class TestGetEntityInfo:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_found_entity(self, mock_repository):
    mock_repository.execute_query.return_value = [
      {"entity_id": "NVDA", "entity_name": "NVIDIA Corporation"}
    ]

    with patch(
      "robosystems.operations.views.save_view.get_universal_repository",
      return_value=mock_repository,
    ):
      result = await get_entity_info(MOCK_GRAPH_ID)

    assert result["entity_id"] == "NVDA"
    assert result["entity_name"] == "NVIDIA Corporation"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_no_entity_returns_default(self, mock_repository):
    mock_repository.execute_query.return_value = []

    with patch(
      "robosystems.operations.views.save_view.get_universal_repository",
      return_value=mock_repository,
    ):
      result = await get_entity_info(MOCK_GRAPH_ID)

    assert result["entity_id"] == "unknown-entity"
    assert result["entity_name"] == "Unknown Entity"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_none_results_returns_default(self, mock_repository):
    mock_repository.execute_query.return_value = None

    with patch(
      "robosystems.operations.views.save_view.get_universal_repository",
      return_value=mock_repository,
    ):
      result = await get_entity_info(MOCK_GRAPH_ID)

    assert result["entity_id"] == "unknown-entity"


class TestQueryViewFacts:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_income_and_balance_results(self, mock_repository):
    income_data = [
      {
        "element_uri": "us-gaap:Revenue",
        "element_name": "RevenueFromContractWithCustomer",
        "amount": 50000.0,
        "statement_type": "income_statement",
      }
    ]
    balance_data = [
      {
        "element_uri": "us-gaap:Cash",
        "element_name": "CashAndCashEquivalents",
        "amount": 10000.0,
        "statement_type": "balance_sheet",
      }
    ]

    mock_repository.execute_query.side_effect = [income_data, balance_data]

    with patch(
      "robosystems.operations.views.save_view.get_universal_repository",
      return_value=mock_repository,
    ):
      result = await query_view_facts(MOCK_GRAPH_ID)

    assert len(result) == 2
    assert result[0]["statement_type"] == "income_statement"
    assert result[0]["numeric_value"] == 50000.0
    assert result[1]["statement_type"] == "balance_sheet"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_no_results(self, mock_repository):
    mock_repository.execute_query.side_effect = [[], []]

    with patch(
      "robosystems.operations.views.save_view.get_universal_repository",
      return_value=mock_repository,
    ):
      result = await query_view_facts(MOCK_GRAPH_ID)

    assert result == []

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_none_results(self, mock_repository):
    mock_repository.execute_query.side_effect = [None, None]

    with patch(
      "robosystems.operations.views.save_view.get_universal_repository",
      return_value=mock_repository,
    ):
      result = await query_view_facts(MOCK_GRAPH_ID)

    assert result == []


class TestCheckReportExists:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_report_exists(self, mock_repository):
    mock_repository.execute_query.return_value = [{"report_id": "rpt-123"}]

    with patch(
      "robosystems.operations.views.save_view.get_universal_repository",
      return_value=mock_repository,
    ):
      result = await check_report_exists(MOCK_GRAPH_ID, "rpt-123")

    assert result is True

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_report_not_exists(self, mock_repository):
    mock_repository.execute_query.return_value = []

    with patch(
      "robosystems.operations.views.save_view.get_universal_repository",
      return_value=mock_repository,
    ):
      result = await check_report_exists(MOCK_GRAPH_ID, "rpt-999")

    assert result is False

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_none_results_means_not_exists(self, mock_repository):
    mock_repository.execute_query.return_value = None

    with patch(
      "robosystems.operations.views.save_view.get_universal_repository",
      return_value=mock_repository,
    ):
      result = await check_report_exists(MOCK_GRAPH_ID, "rpt-999")

    assert result is False


class TestDeleteReportData:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_deletes_facts(self, mock_repository):
    mock_repository.execute_query.return_value = [{"fact_count": 5}]

    with patch(
      "robosystems.operations.views.save_view.get_universal_repository",
      return_value=mock_repository,
    ):
      result = await delete_report_data(MOCK_GRAPH_ID, "rpt-123")

    assert result["facts_deleted"] == 5

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_no_facts_to_delete(self, mock_repository):
    mock_repository.execute_query.return_value = []

    with patch(
      "robosystems.operations.views.save_view.get_universal_repository",
      return_value=mock_repository,
    ):
      result = await delete_report_data(MOCK_GRAPH_ID, "rpt-123")

    assert result["facts_deleted"] == 0


class TestCreateReportNode:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_creates_report(self, mock_repository):
    mock_repository.execute_query.return_value = [{"report_id": "rpt-123"}]

    with patch(
      "robosystems.operations.views.save_view.get_universal_repository",
      return_value=mock_repository,
    ):
      result = await create_report_node(
        MOCK_GRAPH_ID, "rpt-123", "NVDA", "NVIDIA", "2024-01-01", "2024-12-31", "10-K"
      )

    assert result is True
    query = mock_repository.execute_query.call_args[0][0]
    assert "CREATE (r:Report" in query

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_create_fails(self, mock_repository):
    mock_repository.execute_query.return_value = []

    with patch(
      "robosystems.operations.views.save_view.get_universal_repository",
      return_value=mock_repository,
    ):
      result = await create_report_node(
        MOCK_GRAPH_ID, "rpt-123", "NVDA", "NVIDIA", "2024-01-01", "2024-12-31", "10-K"
      )

    assert result is False


class TestCreateFactNodes:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_creates_facts(self, mock_repository):
    mock_repository.execute_query.return_value = [{"fact_id": "f-uuid"}]

    facts = [
      {
        "element_uri": "us-gaap:Revenue",
        "element_name": "Revenue",
        "numeric_value": 50000.0,
        "fact_type": "monetary",
      }
    ]

    with patch(
      "robosystems.operations.views.save_view.get_universal_repository",
      return_value=mock_repository,
    ):
      result = await create_fact_nodes(
        MOCK_GRAPH_ID, "rpt-123", facts, "NVDA", "2024-01-01", "2024-12-31"
      )

    assert len(result) == 1
    assert result[0].element_name == "Revenue"
    assert result[0].numeric_value == 50000.0

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_no_facts_returns_empty(self, mock_repository):
    with patch(
      "robosystems.operations.views.save_view.get_universal_repository",
      return_value=mock_repository,
    ):
      result = await create_fact_nodes(
        MOCK_GRAPH_ID, "rpt-123", [], "NVDA", "2024-01-01", "2024-12-31"
      )

    assert result == []


class TestCreatePresentationStructure:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_creates_structure(self, mock_repository):
    mock_repository.execute_query.return_value = [{"structure_id": "s-uuid"}]

    with patch(
      "robosystems.operations.views.save_view.get_universal_repository",
      return_value=mock_repository,
    ):
      result = await create_presentation_structure(
        MOCK_GRAPH_ID,
        "rpt-123",
        "Income Statement",
        "http://example.com/role/IncomeStatement",
        [{"element": "Revenue"}],
      )

    assert result is not None
    assert result.structure_type == "presentation"
    assert result.name == "Income Statement"
    assert result.element_count == 1

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_create_fails_returns_none(self, mock_repository):
    mock_repository.execute_query.return_value = []

    with patch(
      "robosystems.operations.views.save_view.get_universal_repository",
      return_value=mock_repository,
    ):
      result = await create_presentation_structure(
        MOCK_GRAPH_ID, "rpt-123", "Income Statement", "uri", []
      )

    assert result is None


class TestCreateCalculationStructure:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_creates_structure(self, mock_repository):
    mock_repository.execute_query.return_value = [{"structure_id": "s-uuid"}]

    children = [
      {"element_uri": "us-gaap:Revenue", "element_name": "Revenue", "weight": 1.0},
      {"element_uri": "us-gaap:Expenses", "element_name": "Expenses", "weight": -1.0},
    ]

    with patch(
      "robosystems.operations.views.save_view.get_universal_repository",
      return_value=mock_repository,
    ):
      result = await create_calculation_structure(
        MOCK_GRAPH_ID, "rpt-123", "Net Income", "us-gaap:NetIncome", children
      )

    assert result is not None
    assert result.structure_type == "calculation"
    assert result.element_count == 2


class TestSaveViewAsReport:
  @pytest.fixture
  def save_request(self):
    return SaveViewRequest(
      report_type="10-K",
      period_start="2024-01-01",
      period_end="2024-12-31",
      include_presentation=False,
      include_calculation=False,
    )

  @pytest.fixture
  def save_request_with_structures(self):
    return SaveViewRequest(
      report_type="10-K",
      period_start="2024-01-01",
      period_end="2024-12-31",
      include_presentation=True,
      include_calculation=True,
    )

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_create_new_report(self, save_request):
    with (
      patch(
        "robosystems.operations.views.save_view.get_entity_info",
        return_value={"entity_id": "NVDA", "entity_name": "NVIDIA"},
      ),
      patch(
        "robosystems.operations.views.save_view.query_view_facts",
        return_value=[],
      ),
      patch(
        "robosystems.operations.views.save_view.create_report_node",
        return_value=True,
      ) as mock_create,
      patch(
        "robosystems.operations.views.save_view.create_fact_nodes",
        return_value=[],
      ),
    ):
      result = await save_view_as_report(MOCK_GRAPH_ID, save_request)

    assert result.report_id == "NVDA-10-k-20241231"
    assert result.entity_id == "NVDA"
    assert result.fact_count == 0
    mock_create.assert_called_once()

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_update_existing_report(self):
    request = SaveViewRequest(
      report_id="existing-report",
      report_type="10-K",
      period_start="2024-01-01",
      period_end="2024-12-31",
      include_presentation=False,
      include_calculation=False,
    )

    with (
      patch(
        "robosystems.operations.views.save_view.get_entity_info",
        return_value={"entity_id": "NVDA", "entity_name": "NVIDIA"},
      ),
      patch(
        "robosystems.operations.views.save_view.check_report_exists",
        return_value=True,
      ),
      patch(
        "robosystems.operations.views.save_view.delete_report_data",
        return_value={"facts_deleted": 3},
      ) as mock_delete,
      patch(
        "robosystems.operations.views.save_view.update_report_metadata",
        return_value=True,
      ) as mock_update,
      patch(
        "robosystems.operations.views.save_view.query_view_facts",
        return_value=[],
      ),
      patch(
        "robosystems.operations.views.save_view.create_fact_nodes",
        return_value=[],
      ),
    ):
      result = await save_view_as_report(MOCK_GRAPH_ID, request)

    assert result.report_id == "existing-report"
    mock_delete.assert_called_once()
    mock_update.assert_called_once()

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_with_entity_id_override(self, save_request):
    save_request.entity_id = "AAPL"

    with (
      patch(
        "robosystems.operations.views.save_view.get_entity_info",
        return_value={"entity_id": "NVDA", "entity_name": "NVIDIA"},
      ),
      patch(
        "robosystems.operations.views.save_view.query_view_facts",
        return_value=[],
      ),
      patch(
        "robosystems.operations.views.save_view.create_report_node",
        return_value=True,
      ),
      patch(
        "robosystems.operations.views.save_view.create_fact_nodes",
        return_value=[],
      ),
    ):
      result = await save_view_as_report(MOCK_GRAPH_ID, save_request)

    assert result.entity_id == "AAPL"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_with_presentation_structures(self, save_request_with_structures):
    income_fact = {
      "element_uri": "us-gaap:Revenue",
      "element_name": "RevenueFromContractWithCustomer",
      "numeric_value": 50000.0,
      "fact_type": "monetary",
      "statement_type": "income_statement",
    }
    expense_fact = {
      "element_uri": "us-gaap:Expenses",
      "element_name": "OperatingExpenses",
      "numeric_value": 30000.0,
      "fact_type": "monetary",
      "statement_type": "income_statement",
    }
    balance_fact = {
      "element_uri": "us-gaap:Cash",
      "element_name": "CashAndCashEquivalents",
      "numeric_value": 10000.0,
      "fact_type": "monetary",
      "statement_type": "balance_sheet",
    }

    from robosystems.models.api.views.save_view import StructureDetail

    with (
      patch(
        "robosystems.operations.views.save_view.get_entity_info",
        return_value={"entity_id": "NVDA", "entity_name": "NVIDIA"},
      ),
      patch(
        "robosystems.operations.views.save_view.query_view_facts",
        return_value=[income_fact, expense_fact, balance_fact],
      ),
      patch(
        "robosystems.operations.views.save_view.create_report_node",
        return_value=True,
      ),
      patch(
        "robosystems.operations.views.save_view.create_fact_nodes",
        return_value=[],
      ),
      patch(
        "robosystems.operations.views.save_view.create_presentation_structure",
        return_value=StructureDetail(
          structure_id="s1",
          structure_type="presentation",
          name="test",
          element_count=1,
        ),
      ) as mock_pres,
      patch(
        "robosystems.operations.views.save_view.create_calculation_structure",
        return_value=StructureDetail(
          structure_id="s2",
          structure_type="calculation",
          name="calc",
          element_count=2,
        ),
      ) as mock_calc,
    ):
      result = await save_view_as_report(MOCK_GRAPH_ID, save_request_with_structures)

    # 2 presentation calls (income + balance) + 1 calculation call
    assert mock_pres.call_count == 2
    assert mock_calc.call_count == 1
    assert result.presentation_count == 2
    assert result.calculation_count == 1
