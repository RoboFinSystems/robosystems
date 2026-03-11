"""Circuit breaker metrics and operation metric shim tests."""

from robosystems.middleware.robustness.operation_metrics import (
  CircuitBreakerMetrics,
  OperationStatus,
  OperationType,
  get_operation_metrics_collector,
  record_operation_metric,
)


class TestCircuitBreakerMetrics:
  """Tests for CircuitBreakerMetrics."""

  def test_update_and_get_status(self):
    metrics = CircuitBreakerMetrics()
    metrics.update_circuit_breaker_status(
      graph_id="kg001",
      operation="query",
      state="open",
      failure_count=5,
      last_failure_time=1000.0,
      recovery_time=1060.0,
    )

    status = metrics.get_circuit_status("kg001")
    assert status["query"]["state"] == "open"
    assert status["query"]["failure_count"] == 5

  def test_get_status_unknown_graph(self):
    metrics = CircuitBreakerMetrics()
    assert metrics.get_circuit_status("unknown") == {}

  def test_get_all_circuit_status(self):
    metrics = CircuitBreakerMetrics()
    metrics.update_circuit_breaker_status("kg001", "query", "closed")
    metrics.update_circuit_breaker_status("kg002", "ingest", "open", failure_count=3)

    all_status = metrics.get_circuit_status()
    assert "kg001" in all_status
    assert "kg002" in all_status
    assert all_status["kg002"]["ingest"]["state"] == "open"

  def test_update_overwrites_previous(self):
    metrics = CircuitBreakerMetrics()
    metrics.update_circuit_breaker_status("kg001", "query", "open", failure_count=5)
    metrics.update_circuit_breaker_status("kg001", "query", "closed", failure_count=0)

    status = metrics.get_circuit_status("kg001")
    assert status["query"]["state"] == "closed"
    assert status["query"]["failure_count"] == 0


class TestGlobalCollector:
  """Tests for the global collector singleton."""

  def test_get_operation_metrics_collector_returns_instance(self):
    collector = get_operation_metrics_collector()
    assert isinstance(collector, CircuitBreakerMetrics)

  def test_singleton_returns_same_instance(self):
    a = get_operation_metrics_collector()
    b = get_operation_metrics_collector()
    assert a is b


class TestOperationEnums:
  """Tests for operation type and status enums."""

  def test_operation_types(self):
    assert OperationType.CONNECTION_OPERATION.value == "connection_operation"
    assert OperationType.AI_OPERATION.value == "ai_operation"

  def test_operation_statuses(self):
    assert OperationStatus.SUCCESS.value == "success"
    assert OperationStatus.FAILURE.value == "failure"
    assert OperationStatus.RATE_LIMITED.value == "rate_limited"


class TestRecordOperationMetric:
  """Tests for the record_operation_metric shim."""

  def test_record_success_does_not_raise(self):
    record_operation_metric(
      operation_type=OperationType.CONNECTION_OPERATION,
      status=OperationStatus.SUCCESS,
      duration_ms=50.0,
      endpoint="/v1/graphs/kg001/connections",
      graph_id="kg001",
    )

  def test_record_failure_does_not_raise(self):
    record_operation_metric(
      operation_type=OperationType.CONNECTION_OPERATION,
      status=OperationStatus.FAILURE,
      duration_ms=100.0,
      endpoint="/v1/graphs/kg001/connections",
      graph_id="kg001",
      error_details="Connection refused",
    )

  def test_record_slow_operation_does_not_raise(self):
    record_operation_metric(
      operation_type=OperationType.DATABASE_QUERY,
      status=OperationStatus.SUCCESS,
      duration_ms=15000.0,
      endpoint="/v1/graphs/kg001/query",
    )
