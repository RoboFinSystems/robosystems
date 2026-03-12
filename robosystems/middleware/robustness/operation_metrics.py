"""
Operation metrics for robustness middleware.

Circuit breaker status tracking is handled here. All other operation metrics
(request duration, errors, business events, query queue, SSE) are handled by
the OpenTelemetry system in middleware/otel/metrics.py.

The OperationType, OperationStatus enums and record_operation_metric function
are retained for backward compatibility with connection operation logging.
"""

import threading
import time
from collections import defaultdict
from enum import Enum
from typing import Any

from robosystems.logger import logger


class OperationType(Enum):
  """Types of operations for metrics categorization."""

  API_REQUEST = "api_request"
  DATABASE_QUERY = "database_query"
  EXTERNAL_SERVICE = "external_service"
  AI_OPERATION = "ai_operation"
  ANALYTICS_QUERY = "analytics_query"
  SCHEMA_OPERATION = "schema_operation"
  BACKUP_OPERATION = "backup_operation"
  ENTITY_OPERATION = "entity_operation"
  CONNECTION_OPERATION = "connection_operation"
  TOOL_EXECUTION = "tool_execution"
  HANDLER_LIFECYCLE = "handler_lifecycle"
  QUEUE_OPERATION = "queue_operation"
  CIRCUIT_BREAKER = "circuit_breaker"


class OperationStatus(Enum):
  """Status of operations."""

  SUCCESS = "success"
  FAILURE = "failure"
  TIMEOUT = "timeout"
  CIRCUIT_OPEN = "circuit_open"
  QUEUE_FULL = "queue_full"
  INSUFFICIENT_CREDITS = "insufficient_credits"
  RATE_LIMITED = "rate_limited"
  VALIDATION_ERROR = "validation_error"
  EXTERNAL_SERVICE_ERROR = "external_service_error"


class CircuitBreakerMetrics:
  """Thread-safe circuit breaker status tracker."""

  def __init__(self):
    self._lock = threading.RLock()
    self._circuit_status: dict[str, dict[str, Any]] = defaultdict(dict)

  def update_circuit_breaker_status(
    self,
    graph_id: str,
    operation: str,
    state: str,
    failure_count: int = 0,
    last_failure_time: float | None = None,
    recovery_time: float | None = None,
  ):
    """Update circuit breaker status for monitoring."""
    with self._lock:
      self._circuit_status[graph_id][operation] = {
        "state": state,
        "failure_count": failure_count,
        "last_failure_time": last_failure_time,
        "recovery_time": recovery_time,
        "updated_at": time.time(),
      }

  def get_circuit_status(self, graph_id: str | None = None) -> dict[str, Any]:
    """Get circuit breaker status for a graph or all graphs."""
    with self._lock:
      if graph_id:
        return dict(self._circuit_status.get(graph_id, {}))
      return {gid: dict(circuits) for gid, circuits in self._circuit_status.items()}


# Global instance
_metrics_collector: CircuitBreakerMetrics | None = None


def get_operation_metrics_collector() -> CircuitBreakerMetrics:
  """Get the global circuit breaker metrics instance."""
  global _metrics_collector
  if _metrics_collector is None:
    _metrics_collector = CircuitBreakerMetrics()
  return _metrics_collector


def record_operation_metric(
  operation_type: OperationType,
  status: OperationStatus,
  duration_ms: float,
  endpoint: str,
  graph_id: str | None = None,
  user_id: str | None = None,
  operation_name: str | None = None,
  error_details: str | None = None,
  metadata: dict[str, Any] | None = None,
):
  """Record an operation metric.

  This is a lightweight shim that logs notable events. The primary metrics
  pipeline is OTel (middleware/otel/metrics.py).
  """
  if status != OperationStatus.SUCCESS:
    logger.warning(
      f"Operation {operation_type.value} {status.value}: "
      f"endpoint={endpoint} graph={graph_id} op={operation_name} "
      f"duration={duration_ms:.1f}ms error={error_details}"
    )
  elif duration_ms > 10000:
    logger.warning(
      f"Slow operation {operation_type.value}: "
      f"endpoint={endpoint} graph={graph_id} op={operation_name} "
      f"duration={duration_ms:.1f}ms"
    )
