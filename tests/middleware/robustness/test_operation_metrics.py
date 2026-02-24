"""Operation metrics collection tests."""

import time

import pytest

from robosystems.middleware.robustness.operation_metrics import (
    OperationMetric,
    OperationMetricsCollector,
    OperationMetricsSummary,
    OperationStatus,
    OperationType,
    get_operation_metrics_collector,
    record_operation_metric,
)


@pytest.fixture
def collector():
    """Create a fresh metrics collector for each test."""
    return OperationMetricsCollector(
        max_metrics=100,
        retention_hours=1,
        aggregation_window_minutes=5,
    )


class TestOperationMetricsSummary:
    """Tests for OperationMetricsSummary."""

    def test_default_values(self):
        summary = OperationMetricsSummary()
        assert summary.total_operations == 0
        assert summary.error_rate == 0.0

    def test_to_dict(self):
        summary = OperationMetricsSummary(
            total_operations=10,
            success_count=8,
            failure_count=2,
            avg_duration_ms=150.123456,
            error_rate=0.2,
        )
        d = summary.to_dict()
        assert d["total_operations"] == 10
        assert d["success_count"] == 8
        assert d["failure_count"] == 2
        assert d["avg_duration_ms"] == 150.12
        assert d["error_rate"] == 20.0  # percentage


class TestRecordMetric:
    """Tests for OperationMetricsCollector.record_metric."""

    def test_records_metric(self, collector):
        collector.record_metric(
            operation_type=OperationType.API_REQUEST,
            status=OperationStatus.SUCCESS,
            duration_ms=100.0,
            endpoint="/test",
        )
        assert len(collector._metrics) == 1

    def test_records_multiple_metrics(self, collector):
        for i in range(5):
            collector.record_metric(
                operation_type=OperationType.API_REQUEST,
                status=OperationStatus.SUCCESS,
                duration_ms=float(i * 10),
                endpoint="/test",
            )
        assert len(collector._metrics) == 5

    def test_max_metrics_enforced(self):
        collector = OperationMetricsCollector(max_metrics=3)
        for i in range(10):
            collector.record_metric(
                operation_type=OperationType.API_REQUEST,
                status=OperationStatus.SUCCESS,
                duration_ms=10.0,
                endpoint="/test",
            )
        assert len(collector._metrics) == 3

    def test_records_failure_status(self, collector):
        collector.record_metric(
            operation_type=OperationType.DATABASE_QUERY,
            status=OperationStatus.FAILURE,
            duration_ms=50.0,
            endpoint="/test",
            error_details="Connection refused",
        )
        assert collector._metrics[0].status == OperationStatus.FAILURE


class TestUpdateAggregatedSummaries:
    """Tests for internal aggregation logic."""

    def test_first_metric_sets_avg(self, collector):
        collector.record_metric(
            operation_type=OperationType.API_REQUEST,
            status=OperationStatus.SUCCESS,
            duration_ms=200.0,
            endpoint="/test",
        )
        # Check that a summary exists
        assert len(collector._summaries) > 0

    def test_aggregation_counts_statuses(self, collector):
        status_count = {
            OperationStatus.SUCCESS: 3,
            OperationStatus.FAILURE: 2,
            OperationStatus.TIMEOUT: 1,
            OperationStatus.CIRCUIT_OPEN: 1,
            OperationStatus.QUEUE_FULL: 1,
            OperationStatus.RATE_LIMITED: 1,
        }
        for status, count in status_count.items():
            for _ in range(count):
                collector.record_metric(
                    operation_type=OperationType.API_REQUEST,
                    status=status,
                    duration_ms=10.0,
                    endpoint="/test",
                    graph_id="kg123",
                )

        summary = collector.get_metrics_summary(
            endpoint="/test", graph_id="kg123"
        )
        s = summary["summary"]
        assert s["success_count"] == 3
        assert s["failure_count"] == 2
        assert s["timeout_count"] == 1
        assert s["circuit_open_count"] == 1
        assert s["queue_full_count"] == 1
        assert s["rate_limited_count"] == 1


class TestGetMetricsSummary:
    """Tests for OperationMetricsCollector.get_metrics_summary."""

    def test_empty_collector(self, collector):
        summary = collector.get_metrics_summary()
        assert summary["summary"]["total_operations"] == 0
        assert summary["time_range_minutes"] == 60

    def test_filter_by_endpoint(self, collector):
        collector.record_metric(
            operation_type=OperationType.API_REQUEST,
            status=OperationStatus.SUCCESS,
            duration_ms=10.0,
            endpoint="/a",
        )
        collector.record_metric(
            operation_type=OperationType.API_REQUEST,
            status=OperationStatus.SUCCESS,
            duration_ms=10.0,
            endpoint="/b",
        )
        summary = collector.get_metrics_summary(endpoint="/a")
        assert summary["summary"]["total_operations"] == 1

    def test_filter_by_graph_id(self, collector):
        collector.record_metric(
            operation_type=OperationType.API_REQUEST,
            status=OperationStatus.SUCCESS,
            duration_ms=10.0,
            endpoint="/test",
            graph_id="kg123",
        )
        collector.record_metric(
            operation_type=OperationType.API_REQUEST,
            status=OperationStatus.SUCCESS,
            duration_ms=10.0,
            endpoint="/test",
            graph_id="kg456",
        )
        summary = collector.get_metrics_summary(graph_id="kg123")
        assert summary["summary"]["total_operations"] == 1

    def test_duration_percentiles(self, collector):
        durations = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 5000]
        for d in durations:
            collector.record_metric(
                operation_type=OperationType.API_REQUEST,
                status=OperationStatus.SUCCESS,
                duration_ms=float(d),
                endpoint="/test",
            )
        summary = collector.get_metrics_summary()
        s = summary["summary"]
        assert s["p95_duration_ms"] > 0
        assert s["p99_duration_ms"] >= s["p95_duration_ms"]
        assert s["avg_duration_ms"] > 0

    def test_error_rate_calculation(self, collector):
        for _ in range(8):
            collector.record_metric(
                operation_type=OperationType.API_REQUEST,
                status=OperationStatus.SUCCESS,
                duration_ms=10.0,
                endpoint="/test",
            )
        for _ in range(2):
            collector.record_metric(
                operation_type=OperationType.API_REQUEST,
                status=OperationStatus.FAILURE,
                duration_ms=10.0,
                endpoint="/test",
            )
        summary = collector.get_metrics_summary()
        assert summary["summary"]["error_rate"] == 20.0  # 2/10 = 20%

    def test_throughput_calculation(self, collector):
        for _ in range(60):
            collector.record_metric(
                operation_type=OperationType.API_REQUEST,
                status=OperationStatus.SUCCESS,
                duration_ms=10.0,
                endpoint="/test",
            )
        summary = collector.get_metrics_summary(time_range_minutes=60)
        assert summary["summary"]["throughput_per_minute"] == 1.0  # 60/60


class TestCircuitBreakerStatus:
    """Tests for circuit breaker status tracking."""

    def test_update_circuit_breaker_status(self, collector):
        # Need a metric for the graph so summary doesn't return early
        collector.record_metric(
            operation_type=OperationType.API_REQUEST,
            status=OperationStatus.FAILURE,
            duration_ms=10.0,
            endpoint="/test",
            graph_id="kg123",
        )
        collector.update_circuit_breaker_status(
            graph_id="kg123",
            operation="query",
            state="open",
            failure_count=5,
        )
        summary = collector.get_metrics_summary(graph_id="kg123")
        assert "query" in summary["circuit_breakers"]
        assert summary["circuit_breakers"]["query"]["state"] == "open"

    def test_circuit_status_aggregation(self, collector):
        collector.update_circuit_breaker_status("kg1", "query", "open", 3)
        collector.update_circuit_breaker_status("kg2", "query", "closed", 0)
        # Record metrics so summary doesn't return early
        for gid in ["kg1", "kg2"]:
            collector.record_metric(
                operation_type=OperationType.API_REQUEST,
                status=OperationStatus.SUCCESS,
                duration_ms=10.0,
                endpoint="/test",
                graph_id=gid,
            )
        summary = collector.get_metrics_summary()
        assert "kg1" in summary["circuit_breakers"]
        assert "kg2" in summary["circuit_breakers"]


class TestResourceMetrics:
    """Tests for resource utilization tracking."""

    def test_update_resource_metrics(self, collector):
        collector.update_resource_metrics(
            "handler_pool",
            {"active": 5, "idle": 10, "total": 15},
        )
        summary = collector.get_metrics_summary()
        assert "handler_pool" in summary["resources"]
        assert summary["resources"]["handler_pool"]["active"] == 5


class TestOperationPerformanceBreakdown:
    """Tests for get_operation_performance_breakdown."""

    def test_breakdown_by_operation(self, collector):
        collector.record_metric(
            operation_type=OperationType.API_REQUEST,
            status=OperationStatus.SUCCESS,
            duration_ms=100.0,
            endpoint="/test",
            operation_name="get_entities",
        )
        collector.record_metric(
            operation_type=OperationType.DATABASE_QUERY,
            status=OperationStatus.SUCCESS,
            duration_ms=50.0,
            endpoint="/test",
            operation_name="cypher_query",
        )

        breakdown = collector.get_operation_performance_breakdown()
        assert "api_request:get_entities" in breakdown
        assert "database_query:cypher_query" in breakdown

    def test_breakdown_statistics(self, collector):
        for d in [10.0, 20.0, 30.0]:
            collector.record_metric(
                operation_type=OperationType.API_REQUEST,
                status=OperationStatus.SUCCESS,
                duration_ms=d,
                endpoint="/test",
                operation_name="op1",
            )
        breakdown = collector.get_operation_performance_breakdown()
        stats = breakdown["api_request:op1"]
        assert stats["total_executions"] == 3
        assert stats["avg_duration_ms"] == 20.0
        assert stats["min_duration_ms"] == 10.0
        assert stats["max_duration_ms"] == 30.0

    def test_empty_breakdown(self, collector):
        breakdown = collector.get_operation_performance_breakdown()
        assert breakdown == {}


class TestCleanupOldMetrics:
    """Tests for cleanup_old_metrics."""

    def test_removes_old_metrics(self, collector):
        # Add a metric with old timestamp
        old_metric = OperationMetric(
            timestamp=time.time() - 7200,  # 2 hours ago
            operation_type=OperationType.API_REQUEST,
            status=OperationStatus.SUCCESS,
            duration_ms=10.0,
            endpoint="/old",
        )
        collector._metrics.append(old_metric)

        # Add a recent metric
        collector.record_metric(
            operation_type=OperationType.API_REQUEST,
            status=OperationStatus.SUCCESS,
            duration_ms=10.0,
            endpoint="/new",
        )

        collector.cleanup_old_metrics()
        # Old metric should be removed (retention is 1 hour)
        assert all(m.endpoint != "/old" for m in collector._metrics)

    def test_cleanup_removes_old_summaries(self, collector):
        # Add old summary entry
        collector._summaries["test:/global"]["0"] = OperationMetricsSummary()
        collector.cleanup_old_metrics()
        # Old summary window should be cleaned up
        assert "0" not in collector._summaries.get("test:/global", {})


class TestGlobalInstances:
    """Tests for module-level convenience functions."""

    def test_get_operation_metrics_collector_singleton(self):
        c1 = get_operation_metrics_collector()
        c2 = get_operation_metrics_collector()
        assert c1 is c2

    def test_record_operation_metric_convenience(self):
        # Should not raise
        record_operation_metric(
            operation_type=OperationType.API_REQUEST,
            status=OperationStatus.SUCCESS,
            duration_ms=10.0,
            endpoint="/test",
        )
