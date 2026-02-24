import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from robosystems.operations.graph.metrics_service import GraphMetricsService

MODULE = "robosystems.operations.graph.metrics_service"
MOCK_GRAPH_ID = "kg_test123"
MOCK_USER_ID = "usr_test456"


@pytest.fixture
def mock_metrics_instance():
    m = MagicMock()
    m.record_graph_metrics = MagicMock()
    m.record_business_event = MagicMock()
    return m


@pytest.fixture
def mock_repository():
    repo = AsyncMock()
    repo.execute_query = AsyncMock(return_value=[])
    repo.health_check = AsyncMock(return_value={"status": "healthy"})
    return repo


@pytest.fixture
def metrics_service(mock_metrics_instance):
    with patch(f"{MODULE}.get_endpoint_metrics", return_value=mock_metrics_instance):
        service = GraphMetricsService()
    return service


class TestCollectMetricsForGraph:
    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_happy_path(self, metrics_service, mock_repository, mock_metrics_instance):
        with (
            patch(f"{MODULE}.MultiTenantUtils") as mock_mt,
            patch(
                f"{MODULE}.get_universal_repository",
                new_callable=AsyncMock,
                return_value=mock_repository,
            ),
            patch.object(
                metrics_service,
                "_get_node_counts_by_label",
                new_callable=AsyncMock,
                return_value={"Person": 10, "Company": 5},
            ),
            patch.object(
                metrics_service,
                "_get_relationship_counts_by_type",
                new_callable=AsyncMock,
                return_value={"WORKS_AT": 8},
            ),
            patch.object(
                metrics_service,
                "_estimate_database_size",
                new_callable=AsyncMock,
                return_value={"estimated_bytes": 2250},
            ),
            patch.object(
                metrics_service,
                "_check_graph_health",
                new_callable=AsyncMock,
                return_value={"status": "healthy"},
            ),
        ):
            mock_mt.get_database_name.return_value = "test_db"
            result = await metrics_service.collect_metrics_for_graph(MOCK_GRAPH_ID)

        assert result["graph_id"] == MOCK_GRAPH_ID
        assert result["total_nodes"] == 15
        assert result["total_relationships"] == 8
        assert "timestamp" in result
        mock_metrics_instance.record_graph_metrics.assert_called_once()
        mock_metrics_instance.record_business_event.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_error_returns_error_dict(self, metrics_service):
        with (
            patch(f"{MODULE}.MultiTenantUtils") as mock_mt,
            patch(
                f"{MODULE}.get_universal_repository",
                new_callable=AsyncMock,
                side_effect=RuntimeError("connection failed"),
            ),
        ):
            mock_mt.get_database_name.return_value = "test_db"
            result = await metrics_service.collect_metrics_for_graph(MOCK_GRAPH_ID)

        assert result["graph_id"] == MOCK_GRAPH_ID
        assert "error" in result
        assert "connection failed" in result["error"]
        assert "timestamp" in result


class TestCollectMetricsForUserGraphs:
    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_happy_path(self, metrics_service):
        mock_graph1 = MagicMock()
        mock_graph1.graph_id = "kg_1"
        mock_graph1.graph = MagicMock()
        mock_graph1.graph.graph_name = "Graph 1"
        mock_graph1.role = "owner"

        mock_graph2 = MagicMock()
        mock_graph2.graph_id = "kg_2"
        mock_graph2.graph = MagicMock()
        mock_graph2.graph.graph_name = "Graph 2"
        mock_graph2.role = "viewer"

        with (
            patch(f"{MODULE}.GraphUser") as mock_gu,
            patch.object(
                metrics_service,
                "collect_metrics_for_graph",
                new_callable=AsyncMock,
                side_effect=[
                    {"total_nodes": 10, "total_relationships": 5},
                    {"total_nodes": 20, "total_relationships": 15},
                ],
            ),
        ):
            mock_gu.get_by_user_id.return_value = [mock_graph1, mock_graph2]
            result = await metrics_service.collect_metrics_for_user_graphs(MOCK_USER_ID)

        assert "kg_1" in result
        assert "kg_2" in result
        assert "_summary" in result
        assert result["_summary"]["total_graphs"] == 2
        assert result["_summary"]["total_nodes_across_graphs"] == 30
        assert result["_summary"]["total_relationships_across_graphs"] == 20

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_graph_with_error_excluded_from_totals(self, metrics_service):
        mock_graph = MagicMock()
        mock_graph.graph_id = "kg_1"

        with (
            patch(f"{MODULE}.GraphUser") as mock_gu,
            patch.object(
                metrics_service,
                "collect_metrics_for_graph",
                new_callable=AsyncMock,
                return_value={"error": "connection failed"},
            ),
        ):
            mock_gu.get_by_user_id.return_value = [mock_graph]
            result = await metrics_service.collect_metrics_for_user_graphs(MOCK_USER_ID)

        assert result["_summary"]["total_nodes_across_graphs"] == 0
        assert result["_summary"]["total_relationships_across_graphs"] == 0

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_error_returns_error_dict(self, metrics_service):
        with patch(f"{MODULE}.GraphUser") as mock_gu:
            mock_gu.get_by_user_id.side_effect = RuntimeError("db error")
            result = await metrics_service.collect_metrics_for_user_graphs(MOCK_USER_ID)

        assert "_error" in result
        assert result["_error"]["user_id"] == MOCK_USER_ID
        assert "db error" in result["_error"]["error"]


class TestGetUsageSummary:
    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_user_specific(self, metrics_service):
        mock_graph = MagicMock()
        mock_graph.graph_id = "kg_1"

        with (
            patch(f"{MODULE}.GraphUser") as mock_gu,
            patch.object(
                metrics_service,
                "collect_metrics_for_graph",
                new_callable=AsyncMock,
                return_value={"total_nodes": 100, "total_relationships": 50},
            ),
        ):
            mock_gu.get_by_user_id.return_value = [mock_graph]
            result = await metrics_service.get_usage_summary(user_id=MOCK_USER_ID)

        assert result["user_id"] == MOCK_USER_ID
        assert result["graph_count"] == 1
        assert result["total_nodes"] == 100
        assert result["total_relationships"] == 50

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_system_wide(self, metrics_service):
        with patch(f"{MODULE}.session") as mock_session:
            mock_session.query.return_value.count.return_value = 42
            result = await metrics_service.get_usage_summary(user_id=None)

        assert result["system_wide"] is True
        assert result["total_graphs"] == 42

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_error_returns_error_dict(self, metrics_service):
        with patch(f"{MODULE}.GraphUser") as mock_gu:
            mock_gu.get_by_user_id.side_effect = RuntimeError("db error")
            result = await metrics_service.get_usage_summary(user_id=MOCK_USER_ID)

        assert "error" in result
        assert "db error" in result["error"]

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_user_graph_metric_failure_skipped(self, metrics_service):
        mock_graph1 = MagicMock()
        mock_graph1.graph_id = "kg_1"
        mock_graph2 = MagicMock()
        mock_graph2.graph_id = "kg_2"

        with (
            patch(f"{MODULE}.GraphUser") as mock_gu,
            patch.object(
                metrics_service,
                "collect_metrics_for_graph",
                new_callable=AsyncMock,
                side_effect=[
                    {"total_nodes": 10, "total_relationships": 5},
                    Exception("timeout"),
                ],
            ),
        ):
            mock_gu.get_by_user_id.return_value = [mock_graph1, mock_graph2]
            result = await metrics_service.get_usage_summary(user_id=MOCK_USER_ID)

        assert result["graph_count"] == 2
        assert result["total_nodes"] == 10
        assert result["total_relationships"] == 5


class TestGetNodeCountsByLabel:
    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_happy_path(self, metrics_service, mock_repository):
        mock_repository.execute_query = AsyncMock(
            side_effect=[
                [{"label": "Person"}, {"label": "Company"}],
                [{"count": 10}],
                [{"count": 5}],
            ]
        )

        result = await metrics_service._get_node_counts_by_label(mock_repository)
        assert result == {"Person": 10, "Company": 5}

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_fallback_to_total(self, metrics_service, mock_repository):
        mock_repository.execute_query = AsyncMock(
            side_effect=[
                RuntimeError("labels query failed"),
                [{"total": 42}],
            ]
        )

        result = await metrics_service._get_node_counts_by_label(mock_repository)
        assert result == {"_total": 42}

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_complete_failure_returns_zero(self, metrics_service, mock_repository):
        mock_repository.execute_query = AsyncMock(
            side_effect=RuntimeError("all queries fail")
        )

        result = await metrics_service._get_node_counts_by_label(mock_repository)
        assert result == {"_total": 0}


class TestGetRelationshipCountsByType:
    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_happy_path(self, metrics_service, mock_repository):
        mock_repository.execute_query = AsyncMock(
            side_effect=[
                [{"relationshipType": "WORKS_AT"}],
                [{"count": 8}],
            ]
        )

        result = await metrics_service._get_relationship_counts_by_type(mock_repository)
        assert result == {"WORKS_AT": 8}

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_fallback_to_total(self, metrics_service, mock_repository):
        mock_repository.execute_query = AsyncMock(
            side_effect=[
                RuntimeError("types query failed"),
                [{"total": 20}],
            ]
        )

        result = await metrics_service._get_relationship_counts_by_type(mock_repository)
        assert result == {"_total": 20}


class TestEstimateDatabaseSize:
    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_query_success(self, metrics_service, mock_repository):
        mock_repository.execute_query = AsyncMock(
            return_value=[{"nodeCount": 100, "estimatedBytes": 10000}]
        )

        result = await metrics_service._estimate_database_size(mock_repository)
        assert result["method"] == "database_query"
        assert result["size_info"]["nodeCount"] == 100

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_fallback_estimation(self, metrics_service, mock_repository):
        mock_repository.execute_query = AsyncMock(
            side_effect=[
                Exception("query not supported"),
                [{"total": 100}],
            ]
        )

        result = await metrics_service._estimate_database_size(mock_repository)
        assert result["method"] == "estimation"
        assert result["estimated_bytes"] == 100 * 150


class TestCheckGraphHealth:
    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_healthy(self, metrics_service, mock_repository):
        mock_repository.health_check = AsyncMock(return_value={"status": "healthy"})

        result = await metrics_service._check_graph_health(mock_repository)
        assert result["status"] == "healthy"

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_error(self, metrics_service, mock_repository):
        mock_repository.health_check = AsyncMock(
            side_effect=RuntimeError("health check failed")
        )

        result = await metrics_service._check_graph_health(mock_repository)
        assert result["status"] == "unhealthy"
        assert "health check failed" in result["error"]


class TestRecordOtelMetrics:
    @pytest.mark.unit
    def test_skips_when_error_in_metrics(self, metrics_service, mock_metrics_instance):
        metrics_service._record_otel_metrics(
            MOCK_GRAPH_ID, {"error": "something went wrong"}
        )
        mock_metrics_instance.record_graph_metrics.assert_not_called()

    @pytest.mark.unit
    def test_records_metrics(self, metrics_service, mock_metrics_instance):
        metrics = {
            "total_nodes": 100,
            "total_relationships": 50,
            "node_counts": {"Person": 100},
            "relationship_counts": {"WORKS_AT": 50},
            "estimated_size": {"estimated_bytes": 15000},
            "health_status": {"status": "healthy"},
        }

        metrics_service._record_otel_metrics(MOCK_GRAPH_ID, metrics)
        mock_metrics_instance.record_graph_metrics.assert_called_once()
        mock_metrics_instance.record_business_event.assert_called_once()

    @pytest.mark.unit
    def test_exception_does_not_propagate(self, metrics_service, mock_metrics_instance):
        mock_metrics_instance.record_graph_metrics.side_effect = RuntimeError(
            "otel failure"
        )
        metrics = {
            "total_nodes": 100,
            "total_relationships": 50,
            "node_counts": {},
            "relationship_counts": {},
            "estimated_size": {},
            "health_status": {},
        }

        # Should not raise
        metrics_service._record_otel_metrics(MOCK_GRAPH_ID, metrics)
