"""Circuit breaker for failing dependencies.

After a threshold of failures the breaker opens and calls fail immediately
instead of piling up against a dependency that is already down; after a
recovery timeout it half-opens to test whether the dependency is back.
"""

import time
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException

from robosystems.config.tuning import TuningConfig
from robosystems.graph_api.client.exceptions import GraphClientError
from robosystems.logger import logger


@dataclass
class CircuitState:
  failure_count: int = 0
  last_failure_time: float | None = None
  is_open: bool = False
  last_success_time: float | None = None


class CircuitBreakerManager:
  """Per-(graph, operation) circuit breakers."""

  def __init__(
    self,
    failure_threshold: int | None = None,
    recovery_timeout: int | None = None,
  ):
    self.failure_threshold = (
      failure_threshold
      if failure_threshold is not None
      else TuningConfig.get_circuit_breaker_threshold()
    )
    self.recovery_timeout = (
      recovery_timeout
      if recovery_timeout is not None
      else TuningConfig.get_circuit_breaker_timeout()
    )

    # Only circuits with a recorded failure are stored, so caller-supplied
    # operation names can't grow this map.
    self.circuits: dict[str, CircuitState] = {}

    logger.debug(
      f"Initialized CircuitBreakerManager with threshold={self.failure_threshold}, "
      f"recovery_timeout={self.recovery_timeout}s"
    )

  def _get_circuit_key(self, graph_id: str, operation: str) -> str:
    return f"{graph_id}:{operation}"

  def _should_allow_request(self, circuit_key: str) -> bool:
    circuit = self.circuits.get(circuit_key)
    current_time = time.time()

    if circuit is None or not circuit.is_open:
      return True

    if circuit.last_failure_time and (
      current_time - circuit.last_failure_time >= self.recovery_timeout
    ):
      circuit.is_open = False
      circuit.failure_count = 0
      logger.info(f"Circuit {circuit_key} moving to half-open state")
      return True

    return False

  def check_circuit(self, graph_id: str, operation: str) -> bool:
    """Raise 503 with Retry-After while the circuit is open."""
    circuit_key = self._get_circuit_key(graph_id, operation)

    if not self._should_allow_request(circuit_key):
      circuit = self.circuits.get(circuit_key, CircuitState())
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
    circuit_key = self._get_circuit_key(graph_id, operation)
    circuit = self.circuits.pop(circuit_key, None)
    if circuit is None:
      return

    circuit.failure_count = 0
    circuit.last_success_time = time.time()

    if circuit.is_open:
      circuit.is_open = False
      logger.info(f"Circuit {circuit_key} closed after successful operation")

    self._update_metrics(graph_id, operation, circuit)

  def record_failure(
    self, graph_id: str, operation: str, error: Exception | None = None
  ) -> None:
    """Record a failure, opening the circuit at the threshold. Client errors
    (bad Cypher) don't count: only infrastructure failures trip the breaker."""
    if error is not None and isinstance(error, GraphClientError):
      logger.debug(
        f"Circuit {graph_id}:{operation} ignoring client error: {type(error).__name__}"
      )
      return

    circuit_key = self._get_circuit_key(graph_id, operation)
    circuit = self.circuits.setdefault(circuit_key, CircuitState())

    circuit.failure_count += 1
    circuit.last_failure_time = time.time()

    if circuit.failure_count >= self.failure_threshold and not circuit.is_open:
      circuit.is_open = True
      logger.warning(
        f"Circuit {circuit_key} opened after {circuit.failure_count} failures"
      )

    self._update_metrics(graph_id, operation, circuit)

  def get_circuit_status(self, graph_id: str, operation: str) -> dict[str, Any]:
    circuit_key = self._get_circuit_key(graph_id, operation)
    circuit = self.circuits.get(circuit_key, CircuitState())

    return {
      "circuit_key": circuit_key,
      "is_open": circuit.is_open,
      "failure_count": circuit.failure_count,
      "last_failure_time": circuit.last_failure_time,
      "last_success_time": circuit.last_success_time,
    }

  def get_all_circuit_status(self) -> dict[str, dict[str, Any]]:
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
    try:
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
      logger.warning(f"Failed to update circuit breaker metrics: {e}")
