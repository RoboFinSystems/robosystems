import pandas as pd
import pytest
from unittest.mock import AsyncMock, patch

from robosystems.operations.views.trial_balance import aggregate_trial_balance


MOCK_GRAPH_ID = "kg_test123"


@pytest.fixture
def mock_repository():
    repo = AsyncMock()
    repo.execute_query = AsyncMock(return_value=[])
    return repo


class TestAggregateTrialBalance:
    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_basic_aggregation(self, mock_repository):
        mock_repository.execute_query.return_value = [
            {
                "element_id": "us-gaap:Cash",
                "element_uri": "https://xbrl.us/us-gaap/Cash",
                "element_name": "Cash",
                "element_classification": "asset",
                "element_balance": "debit",
                "element_period_type": "instant",
                "total_debits": 5000.0,
                "total_credits": 1000.0,
                "net_balance": 4000.0,
            },
            {
                "element_id": "us-gaap:Revenue",
                "element_uri": "https://xbrl.us/us-gaap/Revenue",
                "element_name": "Revenue",
                "element_classification": "revenue",
                "element_balance": "credit",
                "element_period_type": "duration",
                "total_debits": 0.0,
                "total_credits": 10000.0,
                "net_balance": -10000.0,
            },
        ]

        with patch(
            "robosystems.operations.views.trial_balance.get_graph_repository",
            return_value=mock_repository,
        ):
            result = await aggregate_trial_balance(
                MOCK_GRAPH_ID, "2024-01-01", "2024-12-31"
            )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert result.iloc[0]["element_id"] == "us-gaap:Cash"
        assert result.iloc[0]["net_balance"] == 4000.0
        assert result.iloc[1]["element_classification"] == "revenue"

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_with_entity_filter(self, mock_repository):
        mock_repository.execute_query.return_value = []

        with patch(
            "robosystems.operations.views.trial_balance.get_graph_repository",
            return_value=mock_repository,
        ):
            await aggregate_trial_balance(
                MOCK_GRAPH_ID, "2024-01-01", "2024-12-31", entity_id="NVDA"
            )

        query = mock_repository.execute_query.call_args[0][0]
        params = mock_repository.execute_query.call_args[0][1]
        assert "e.identifier = $entity_id" in query
        assert params["entity_id"] == "NVDA"

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_without_entity_filter(self, mock_repository):
        mock_repository.execute_query.return_value = []

        with patch(
            "robosystems.operations.views.trial_balance.get_graph_repository",
            return_value=mock_repository,
        ):
            await aggregate_trial_balance(MOCK_GRAPH_ID, "2024-01-01", "2024-12-31")

        query = mock_repository.execute_query.call_args[0][0]
        params = mock_repository.execute_query.call_args[0][1]
        assert "e.identifier = $entity_id" not in query
        assert "entity_id" not in params

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_period_params_passed(self, mock_repository):
        mock_repository.execute_query.return_value = []

        with patch(
            "robosystems.operations.views.trial_balance.get_graph_repository",
            return_value=mock_repository,
        ):
            await aggregate_trial_balance(MOCK_GRAPH_ID, "2024-06-01", "2024-06-30")

        params = mock_repository.execute_query.call_args[0][1]
        assert params["period_start"] == "2024-06-01"
        assert params["period_end"] == "2024-06-30"

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_empty_results_returns_empty_dataframe(self, mock_repository):
        mock_repository.execute_query.return_value = []

        with patch(
            "robosystems.operations.views.trial_balance.get_graph_repository",
            return_value=mock_repository,
        ):
            result = await aggregate_trial_balance(
                MOCK_GRAPH_ID, "2024-01-01", "2024-12-31"
            )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        expected_columns = [
            "element_id",
            "element_uri",
            "element_name",
            "element_classification",
            "element_balance",
            "element_period_type",
            "total_debits",
            "total_credits",
            "net_balance",
        ]
        for col in expected_columns:
            assert col in result.columns

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_none_results_returns_empty_dataframe(self, mock_repository):
        mock_repository.execute_query.return_value = None

        with patch(
            "robosystems.operations.views.trial_balance.get_graph_repository",
            return_value=mock_repository,
        ):
            result = await aggregate_trial_balance(
                MOCK_GRAPH_ID, "2024-01-01", "2024-12-31"
            )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
