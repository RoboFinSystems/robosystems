"""
Circuit breaker implementation for preventing cascade failures.

Provides circuit breaker functionality to protect against repeated failures
and enable graceful degradation across all endpoints.
"""

import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException

from robosystems.logger import logger


@dataclass
class CircuitState:
  """Circuit breaker state tracking."""

  failure_count: int = 0
  last_failure_time: float | None = None
  is_open: bool = False
  last_success_time: float | None = None


class CircuitBreakerManager:
  """Circuit breaker for operations to prevent cascade failures."""

  def __init__(
    self,
    failure_threshold: int = 5,
    recovery_timeout: int = 60,
    half_open_max_calls: int = 3,
  ):
    """
    Initialize circuit breaker manager.

    Args:
        failure_threshold: Number of failures before opening circuit
        recovery_timeout: Seconds before attempting recovery
        half_open_max_calls: Max calls allowed in half-open state
    """
    self.failure_threshold = failure_threshold
    self.recovery_timeout = recovery_timeout
    self.half_open_max_calls = half_open_max_calls

    # Track circuit state per graph_id + operation key
    self.circuits: dict[str, CircuitState] = defaultdict(CircuitState)

    logger.debug(
      f"Initialized CircuitBreakerManager with threshold={failure_threshold}, "
      f"recovery_timeout={recovery_timeout}s"
    )

  def _get_circuit_key(self, graph_id: str, operation: str) -> str:
    """Generate circuit key for tracking."""
    return f"{graph_id}:{operation}"

  def _should_allow_request(self, circuit_key: str) -> bool:
    """Check if request should be allowed through circuit."""
    circuit = self.circuits[circuit_key]
    current_time = time.time()

    # If circuit is closed, allow request
    if not circuit.is_open:
      return True

    # If circuit is open, check if we should attempt recovery
    if circuit.last_failure_time and (
      current_time - circuit.last_failure_time >= self.recovery_timeout
    ):
      # Move to half-open state
      circuit.is_open = False
      circuit.failure_count = 0
      logger.info(f"Circuit {circuit_key} moving to half-open state")
      return True

    # Circuit is open and not ready for recovery
    return False

  def check_circuit(self, graph_id: str, operation: str) -> bool:
    """
    Check circuit breaker before operation.

    Args:
        graph_id: Target graph identifier
        operation: Operation name (e.g., 'cypher_query', 'get_schema')

    Returns:
        True if operation should proceed

    Raises:
        HTTPException: If circuit is open
    """
    circuit_key = self._get_circuit_key(graph_id, operation)

    if not self._should_allow_request(circuit_key):
      circuit = self.circuits[circuit_key]
      time_since_failure = time.time() - (circuit.last_failure_time or 0)

      raise HTTPException(
        status_code=503,
        detail=f"Circuit breaker open for {operation} on {graph_id}",
        headers={
          "Retry-After": str(max(30, self.recovery_timeout - time_since_failure))
        },
      )

    return True

  def record_success(self, graph_id: str, operation: str) -> None:
    """Record successful operation."""
    circuit_key = self._get_circuit_key(graph_id, operation)
    circuit = self.circuits[circuit_key]

    # Reset failure count on success
    circuit.failure_count = 0
    circuit.last_success_time = time.time()

    # Close circuit if it was open
    if circuit.is_open:
      circuit.is_open = False
      logger.info(f"Circuit {circuit_key} closed after successful operation")

    # Update metrics
    self._update_metrics(graph_id, operation, circuit)

  def record_failure(self, graph_id: str, operation: str) -> None:
    """Record failed operation and potentially open circuit."""
    circuit_key = self._get_circuit_key(graph_id, operation)
    circuit = self.circuits[circuit_key]

    circuit.failure_count += 1
    circuit.last_failure_time = time.time()

    # Open circuit if threshold exceeded
    if circuit.failure_count >= self.failure_threshold and not circuit.is_open:
      circuit.is_open = True
      logger.warning(
        f"Circuit {circuit_key} opened after {circuit.failure_count} failures"
      )

    # Update metrics
    self._update_metrics(graph_id, operation, circuit)

  def get_circuit_status(self, graph_id: str, operation: str) -> dict[str, Any]:
    """Get current circuit status for monitoring."""
    circuit_key = self._get_circuit_key(graph_id, operation)
    circuit = self.circuits[circuit_key]

    return {
      "circuit_key": circuit_key,
      "is_open": circuit.is_open,
      "failure_count": circuit.failure_count,
      "last_failure_time": circuit.last_failure_time,
      "last_success_time": circuit.last_success_time,
    }

  def get_all_circuit_status(self) -> dict[str, dict[str, Any]]:
    """Get status of all circuits for monitoring."""
    status = {}
    for circuit_key, circuit in self.circuits.items():
      graph_id, operation = circuit_key.split(":", 1)
      if graph_id not in status:
        status[graph_id] = {}

      status[graph_id][operation] = {
        "is_open": circuit.is_open,
        "failure_count": circuit.failure_count,
        "last_failure_time": circuit.last_failure_time,
        "last_success_time": circuit.last_success_time,
        "state": "open" if circuit.is_open else "closed",
      }

    return status

  def _update_metrics(
    self, graph_id: str, operation: str, circuit: CircuitState
  ) -> None:
    """Update metrics collector with circuit breaker status."""
    try:
      # Import here to avoid circular imports
      from .operation_metrics import get_operation_metrics_collector

      collector = get_operation_metrics_collector()
      collector.update_circuit_breaker_status(
        graph_id=graph_id,
        operation=operation,
        state="open" if circuit.is_open else "closed",
        failure_count=circuit.failure_count,
        last_failure_time=circuit.last_failure_time,
        recovery_time=None
        if not circuit.is_open
        else (
          circuit.last_failure_time + self.recovery_timeout
          if circuit.last_failure_time
          else None
        ),
      )
    except Exception as e:
      # Don't let metrics failures break circuit breaker functionality
      logger.warning(f"Failed to update circuit breaker metrics: {e}")
