"""Full circuit breaker manager tests."""

import pytest
from fastapi import HTTPException

from robosystems.graph_api.client.exceptions import GraphClientError
from robosystems.middleware.robustness.circuit_breaker import (
  CircuitBreakerManager,
  CircuitState,
)


@pytest.fixture
def breaker():
  return CircuitBreakerManager(failure_threshold=3, recovery_timeout=10)


class TestCircuitState:
  def test_defaults(self):
    state = CircuitState()
    assert state.failure_count == 0
    assert state.is_open is False
    assert state.last_failure_time is None
    assert state.last_success_time is None


class TestCheckCircuit:
  def test_closed_circuit_allows(self, breaker):
    assert breaker.check_circuit("kg123", "query") is True

  def test_open_circuit_raises_503(self, breaker):
    # Trip the breaker
    for _ in range(3):
      breaker.record_failure("kg123", "query")

    with pytest.raises(HTTPException) as exc:
      breaker.check_circuit("kg123", "query")
    assert exc.value.status_code == 503
    assert "Retry-After" in exc.value.headers

  def test_recovery_after_timeout(self, breaker):
    breaker.recovery_timeout = 0  # Instant recovery for testing
    for _ in range(3):
      breaker.record_failure("kg123", "query")

    # Should recover immediately
    assert breaker.check_circuit("kg123", "query") is True


class TestRecordSuccess:
  def test_resets_failure_count(self, breaker):
    breaker.record_failure("kg123", "query")
    breaker.record_failure("kg123", "query")
    breaker.record_success("kg123", "query")

    status = breaker.get_circuit_status("kg123", "query")
    assert status["failure_count"] == 0

  def test_closes_open_circuit(self, breaker):
    breaker.recovery_timeout = 0
    for _ in range(3):
      breaker.record_failure("kg123", "query")

    # Allow recovery
    breaker.check_circuit("kg123", "query")
    breaker.record_success("kg123", "query")

    status = breaker.get_circuit_status("kg123", "query")
    assert status["is_open"] is False

  def test_sets_last_success_time(self, breaker):
    breaker.record_success("kg123", "query")
    status = breaker.get_circuit_status("kg123", "query")
    assert status["last_success_time"] is not None


class TestRecordFailure:
  def test_increments_failure_count(self, breaker):
    breaker.record_failure("kg123", "query")
    status = breaker.get_circuit_status("kg123", "query")
    assert status["failure_count"] == 1

  def test_opens_circuit_at_threshold(self, breaker):
    for _ in range(3):
      breaker.record_failure("kg123", "query")

    status = breaker.get_circuit_status("kg123", "query")
    assert status["is_open"] is True

  def test_ignores_client_errors(self, breaker):
    error = GraphClientError("Bad query syntax")
    for _ in range(5):
      breaker.record_failure("kg123", "query", error=error)

    status = breaker.get_circuit_status("kg123", "query")
    assert status["failure_count"] == 0
    assert status["is_open"] is False

  def test_counts_server_errors(self, breaker):
    error = RuntimeError("Connection timeout")
    breaker.record_failure("kg123", "query", error=error)
    status = breaker.get_circuit_status("kg123", "query")
    assert status["failure_count"] == 1

  def test_counts_none_errors(self, breaker):
    breaker.record_failure("kg123", "query", error=None)
    status = breaker.get_circuit_status("kg123", "query")
    assert status["failure_count"] == 1

  def test_separate_circuits_per_graph(self, breaker):
    for _ in range(3):
      breaker.record_failure("kg1", "query")
    breaker.record_failure("kg2", "query")

    assert breaker.get_circuit_status("kg1", "query")["is_open"] is True
    assert breaker.get_circuit_status("kg2", "query")["is_open"] is False

  def test_separate_circuits_per_operation(self, breaker):
    for _ in range(3):
      breaker.record_failure("kg1", "query")

    assert breaker.get_circuit_status("kg1", "query")["is_open"] is True
    assert breaker.get_circuit_status("kg1", "schema")["is_open"] is False


class TestGetCircuitStatus:
  def test_returns_status_dict(self, breaker):
    status = breaker.get_circuit_status("kg123", "query")
    assert "circuit_key" in status
    assert "is_open" in status
    assert "failure_count" in status
    assert "last_failure_time" in status
    assert "last_success_time" in status


class TestGetAllCircuitStatus:
  def test_empty(self, breaker):
    assert breaker.get_all_circuit_status() == {}

  def test_returns_grouped_by_graph(self, breaker):
    breaker.record_failure("kg1", "query")
    breaker.record_failure("kg2", "schema")

    status = breaker.get_all_circuit_status()
    assert "kg1" in status
    assert "kg2" in status
    assert "query" in status["kg1"]
    assert "schema" in status["kg2"]
    assert "state" in status["kg1"]["query"]
