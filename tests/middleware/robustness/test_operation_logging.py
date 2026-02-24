"""Operation logging tests."""

import time

import pytest

from robosystems.middleware.robustness.operation_logging import (
    LogLevel,
    OperationLogEntry,
    OperationLogEventType,
    OperationLogger,
)


@pytest.fixture
def op_logger():
    """Create a fresh OperationLogger for each test."""
    return OperationLogger(
        enable_performance_logging=True,
        enable_debug_logging=True,
        slow_operation_threshold_ms=100.0,
        max_log_entries=100,
    )


class TestOperationLogEntry:
    """Tests for OperationLogEntry dataclass."""

    def test_to_dict(self):
        entry = OperationLogEntry(
            timestamp=1000.0,
            event_type=OperationLogEventType.OPERATION_START,
            level=LogLevel.INFO,
            endpoint="/test",
            graph_id="kg123",
            user_id="user1",
        )
        d = entry.to_dict()
        assert d["event_type"] == "operation_start"
        assert d["level"] == "info"
        assert d["endpoint"] == "/test"
        assert d["graph_id"] == "kg123"

    def test_to_dict_excludes_none_fields(self):
        entry = OperationLogEntry(
            timestamp=1000.0,
            event_type=OperationLogEventType.OPERATION_START,
            level=LogLevel.INFO,
            endpoint="/test",
        )
        d = entry.to_dict()
        assert "graph_id" not in d
        assert "user_id" not in d
        assert "error_message" not in d


class TestOperationLogger:
    """Tests for OperationLogger."""

    def test_log_operation_start_returns_id(self, op_logger):
        op_id = op_logger.log_operation_start(
            operation="test_op",
            endpoint="/test",
            graph_id="kg123",
            user_id="user1",
        )
        assert isinstance(op_id, str)
        assert "test_op" in op_id

    def test_log_operation_start_with_trace_id(self, op_logger):
        op_id = op_logger.log_operation_start(
            operation="test_op",
            endpoint="/test",
            trace_id="custom_trace_123",
        )
        assert op_id == "custom_trace_123"

    def test_log_operation_success(self, op_logger):
        op_id = op_logger.log_operation_start(
            operation="test_op", endpoint="/test"
        )
        op_logger.log_operation_success(op_id)
        # Should have 2 entries: start + success
        logs = op_logger.get_recent_logs()
        assert len(logs) >= 2

    def test_log_operation_success_cleans_up_context(self, op_logger):
        op_id = op_logger.log_operation_start(
            operation="test_op", endpoint="/test"
        )
        op_logger.log_operation_success(op_id)
        assert op_id not in op_logger._operation_contexts

    def test_log_operation_success_no_context(self, op_logger):
        # Should not raise, just log warning
        op_logger.log_operation_success("nonexistent_id")

    def test_log_operation_failure(self, op_logger):
        op_id = op_logger.log_operation_start(
            operation="test_op", endpoint="/test"
        )
        op_logger.log_operation_failure(op_id, ValueError("test error"))
        logs = op_logger.get_recent_logs(
            event_type=OperationLogEventType.OPERATION_FAILURE
        )
        assert len(logs) == 1
        assert logs[0]["error_message"] == "test error"

    def test_log_operation_failure_cleans_up_context(self, op_logger):
        op_id = op_logger.log_operation_start(
            operation="test_op", endpoint="/test"
        )
        op_logger.log_operation_failure(op_id, RuntimeError("boom"))
        assert op_id not in op_logger._operation_contexts

    def test_log_operation_failure_no_context(self, op_logger):
        # Should not raise
        op_logger.log_operation_failure("nonexistent_id", RuntimeError("boom"))

    def test_slow_operation_detection(self, op_logger):
        op_id = op_logger.log_operation_start(
            operation="slow_op", endpoint="/slow"
        )
        # Manually set start time in the past
        op_logger._operation_contexts[op_id]["start_time"] = time.time() - 1.0
        op_logger.log_operation_success(op_id)
        logs = op_logger.get_recent_logs(
            event_type=OperationLogEventType.PERFORMANCE_ALERT
        )
        assert len(logs) == 1

    def test_log_circuit_breaker_event(self, op_logger):
        op_logger.log_circuit_breaker_event(
            event_type=OperationLogEventType.CIRCUIT_BREAKER_OPEN,
            endpoint="/test",
            graph_id="kg123",
            operation="query",
            failure_count=5,
        )
        logs = op_logger.get_recent_logs(
            event_type=OperationLogEventType.CIRCUIT_BREAKER_OPEN
        )
        assert len(logs) == 1

    def test_log_resource_event(self, op_logger):
        op_logger.log_resource_event(
            event_type=OperationLogEventType.RESOURCE_CREATED,
            endpoint="/test",
            graph_id="kg123",
            resource_type="connection",
        )
        logs = op_logger.get_recent_logs(
            event_type=OperationLogEventType.RESOURCE_CREATED
        )
        assert len(logs) == 1

    def test_log_external_service_call(self, op_logger):
        op_logger.log_external_service_call(
            endpoint="/test",
            service_name="QuickBooks",
            operation="get_accounts",
            duration_ms=250.0,
            status="success",
            graph_id="kg123",
        )
        logs = op_logger.get_recent_logs(
            event_type=OperationLogEventType.EXTERNAL_SERVICE_CALL
        )
        assert len(logs) == 1
        assert logs[0]["metadata"]["service_name"] == "QuickBooks"

    def test_log_credit_operation(self, op_logger):
        op_logger.log_credit_operation(
            endpoint="/test",
            graph_id="kg123",
            user_id="user1",
            operation_type="ai_query",
            cost=5.0,
            status="success",
        )
        logs = op_logger.get_recent_logs(
            event_type=OperationLogEventType.CREDIT_OPERATION
        )
        assert len(logs) == 1
        assert logs[0]["metadata"]["credit_cost"] == 5.0


class TestGetRecentLogs:
    """Tests for OperationLogger.get_recent_logs."""

    def test_filter_by_endpoint(self, op_logger):
        op_logger.log_resource_event(
            event_type=OperationLogEventType.RESOURCE_CREATED,
            endpoint="/endpoint_a",
        )
        op_logger.log_resource_event(
            event_type=OperationLogEventType.RESOURCE_CREATED,
            endpoint="/endpoint_b",
        )
        logs = op_logger.get_recent_logs(endpoint="/endpoint_a")
        assert all(log["endpoint"] == "/endpoint_a" for log in logs)

    def test_filter_by_graph_id(self, op_logger):
        op_logger.log_resource_event(
            event_type=OperationLogEventType.RESOURCE_CREATED,
            endpoint="/test",
            graph_id="kg123",
        )
        op_logger.log_resource_event(
            event_type=OperationLogEventType.RESOURCE_CREATED,
            endpoint="/test",
            graph_id="kg456",
        )
        logs = op_logger.get_recent_logs(graph_id="kg123")
        assert all(log["graph_id"] == "kg123" for log in logs)

    def test_filter_by_event_type(self, op_logger):
        op_logger.log_resource_event(
            event_type=OperationLogEventType.RESOURCE_CREATED,
            endpoint="/test",
        )
        op_logger.log_circuit_breaker_event(
            event_type=OperationLogEventType.CIRCUIT_BREAKER_OPEN,
            endpoint="/test",
            graph_id="kg123",
            operation="query",
        )
        logs = op_logger.get_recent_logs(
            event_type=OperationLogEventType.CIRCUIT_BREAKER_OPEN
        )
        assert len(logs) == 1

    def test_limit(self, op_logger):
        for i in range(10):
            op_logger.log_resource_event(
                event_type=OperationLogEventType.RESOURCE_CREATED,
                endpoint="/test",
            )
        logs = op_logger.get_recent_logs(limit=3)
        assert len(logs) == 3

    def test_returns_most_recent_first(self, op_logger):
        for i in range(3):
            op_logger.log_resource_event(
                event_type=OperationLogEventType.RESOURCE_CREATED,
                endpoint=f"/test_{i}",
            )
        logs = op_logger.get_recent_logs()
        assert logs[0]["endpoint"] == "/test_2"


class TestMaxLogEntries:
    """Tests for log entry trimming."""

    def test_trims_old_entries(self):
        logger = OperationLogger(max_log_entries=5)
        for i in range(10):
            logger.log_resource_event(
                event_type=OperationLogEventType.RESOURCE_CREATED,
                endpoint=f"/test_{i}",
            )
        assert len(logger._log_entries) == 5


class TestOperationContext:
    """Tests for operation_context context manager."""

    def test_success_path(self, op_logger):
        with op_logger.operation_context(
            operation="test_op", endpoint="/test"
        ) as op_id:
            assert isinstance(op_id, str)
        # Success should be logged
        logs = op_logger.get_recent_logs(
            event_type=OperationLogEventType.OPERATION_SUCCESS
        )
        assert len(logs) == 1

    def test_failure_path(self, op_logger):
        with pytest.raises(ValueError, match="boom"):
            with op_logger.operation_context(
                operation="test_op", endpoint="/test"
            ):
                raise ValueError("boom")
        logs = op_logger.get_recent_logs(
            event_type=OperationLogEventType.OPERATION_FAILURE
        )
        assert len(logs) == 1
