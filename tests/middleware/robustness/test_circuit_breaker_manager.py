"""Tests for CircuitBreakerManager error classification."""

from robosystems.graph_api.client.exceptions import (
  GraphClientError,
  GraphServerError,
  GraphSyntaxError,
  GraphTimeoutError,
  GraphTransientError,
)
from robosystems.middleware.robustness.circuit_breaker import CircuitBreakerManager


class TestRecordFailureErrorClassification:
  """Test that record_failure correctly classifies errors."""

  def _make_breaker(self) -> CircuitBreakerManager:
    return CircuitBreakerManager(failure_threshold=5, recovery_timeout=60)

  def test_no_error_increments_failure_count(self):
    """error=None (default) always increments — backward compatibility."""
    cb = self._make_breaker()
    cb.record_failure("sec", "cypher_query")
    assert cb.circuits["sec:cypher_query"].failure_count == 1

  def test_client_error_does_not_increment(self):
    """GraphClientError (400-level) should not trip the breaker."""
    cb = self._make_breaker()
    error = GraphClientError("Bad request", status_code=400)
    cb.record_failure("sec", "cypher_query", error=error)
    assert cb.circuits["sec:cypher_query"].failure_count == 0

  def test_syntax_error_does_not_increment(self):
    """GraphSyntaxError (subclass of GraphClientError) should not trip the breaker."""
    cb = self._make_breaker()
    error = GraphSyntaxError("Parser exception: invalid syntax", status_code=400)
    cb.record_failure("sec", "cypher_query", error=error)
    assert cb.circuits["sec:cypher_query"].failure_count == 0

  def test_server_error_increments(self):
    """GraphServerError (500) should increment failure count."""
    cb = self._make_breaker()
    error = GraphServerError("Internal server error", status_code=500)
    cb.record_failure("sec", "cypher_query", error=error)
    assert cb.circuits["sec:cypher_query"].failure_count == 1

  def test_transient_error_increments(self):
    """GraphTransientError (503, 502) should increment failure count."""
    cb = self._make_breaker()
    error = GraphTransientError("Service unavailable", status_code=503)
    cb.record_failure("sec", "cypher_query", error=error)
    assert cb.circuits["sec:cypher_query"].failure_count == 1

  def test_timeout_error_increments(self):
    """GraphTimeoutError should increment failure count."""
    cb = self._make_breaker()
    error = GraphTimeoutError("Request timeout", status_code=408)
    cb.record_failure("sec", "cypher_query", error=error)
    assert cb.circuits["sec:cypher_query"].failure_count == 1

  def test_generic_exception_increments(self):
    """Non-graph exceptions (ConnectionError, etc.) should increment."""
    cb = self._make_breaker()
    error = ConnectionError("Connection refused")
    cb.record_failure("sec", "cypher_query", error=error)
    assert cb.circuits["sec:cypher_query"].failure_count == 1

  def test_client_errors_do_not_open_circuit(self):
    """Even many client errors should never open the circuit."""
    cb = self._make_breaker()
    error = GraphSyntaxError("bad query")
    for _ in range(20):
      cb.record_failure("sec", "cypher_query", error=error)
    assert cb.circuits["sec:cypher_query"].is_open is False
    assert cb.circuits["sec:cypher_query"].failure_count == 0

  def test_server_errors_open_circuit_at_threshold(self):
    """Infrastructure errors should open the circuit at threshold."""
    cb = self._make_breaker()
    error = GraphServerError("OOM", status_code=500)
    for _ in range(5):
      cb.record_failure("sec", "cypher_query", error=error)
    assert cb.circuits["sec:cypher_query"].is_open is True

  def test_mixed_errors_only_count_infrastructure(self):
    """Client errors interleaved with server errors — only server errors count."""
    cb = self._make_breaker()
    client_error = GraphClientError("bad request", status_code=400)
    server_error = GraphServerError("OOM", status_code=500)

    cb.record_failure("sec", "cypher_query", error=client_error)
    cb.record_failure("sec", "cypher_query", error=client_error)
    cb.record_failure("sec", "cypher_query", error=server_error)
    cb.record_failure("sec", "cypher_query", error=client_error)
    cb.record_failure("sec", "cypher_query", error=server_error)

    assert cb.circuits["sec:cypher_query"].failure_count == 2
    assert cb.circuits["sec:cypher_query"].is_open is False

  def test_circuit_isolation_between_graphs(self):
    """Failures on one graph don't affect another."""
    cb = self._make_breaker()
    error = GraphServerError("OOM", status_code=500)
    for _ in range(5):
      cb.record_failure("sec", "cypher_query", error=error)

    assert cb.circuits["sec:cypher_query"].is_open is True
    assert cb.circuits["kg123:cypher_query"].is_open is False
