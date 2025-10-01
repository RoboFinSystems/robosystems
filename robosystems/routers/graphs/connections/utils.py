"""
Shared utilities for connection operations.
"""

import time

# Import robustness middleware
from robosystems.middleware.robustness import (
  CircuitBreakerManager,
  TimeoutCoordinator,
  OperationType,
  OperationStatus,
  record_operation_metric,
  get_operation_logger,
)

# Import provider registry
from robosystems.operations.providers.registry import ProviderRegistry

# Initialize provider registry singleton
provider_registry = ProviderRegistry()


def create_robustness_components():
  """Create robustness components for connection operations."""
  return {
    "circuit_breaker": CircuitBreakerManager(),
    "timeout_coordinator": TimeoutCoordinator(),
    "operation_logger": get_operation_logger(),
    "operation_start_time": time.time(),
  }


def record_operation_start(
  operation_name: str, endpoint: str, graph_id: str, user_id: str, metadata: dict = None
):
  """Record the start of an operation for metrics."""
  return record_operation_metric(
    operation_type=OperationType.CONNECTION_OPERATION,
    status=OperationStatus.SUCCESS,  # Will be updated on completion
    duration_ms=0.0,  # Will be updated on completion
    endpoint=endpoint,
    graph_id=graph_id,
    user_id=user_id,
    operation_name=operation_name,
    metadata=metadata or {},
  )


def record_operation_success(
  components: dict,
  operation_name: str,
  endpoint: str,
  graph_id: str,
  user_id: str,
  metadata: dict = None,
):
  """Record successful operation completion."""
  operation_duration_ms = (time.time() - components["operation_start_time"]) * 1000
  components["circuit_breaker"].record_success(graph_id, operation_name)

  record_operation_metric(
    operation_type=OperationType.CONNECTION_OPERATION,
    status=OperationStatus.SUCCESS,
    duration_ms=operation_duration_ms,
    endpoint=endpoint,
    graph_id=graph_id,
    user_id=user_id,
    operation_name=operation_name,
    metadata=metadata or {},
  )


def record_operation_failure(
  components: dict,
  operation_name: str,
  endpoint: str,
  graph_id: str,
  user_id: str,
  error_type: str = None,
  error_message: str = None,
  timeout_seconds: float = None,
):
  """Record operation failure."""
  operation_duration_ms = (time.time() - components["operation_start_time"]) * 1000
  components["circuit_breaker"].record_failure(graph_id, operation_name)

  metadata = {"error_type": error_type} if error_type else {}
  if error_message:
    metadata["error_message"] = error_message
  if timeout_seconds:
    metadata["timeout_seconds"] = timeout_seconds

  record_operation_metric(
    operation_type=OperationType.CONNECTION_OPERATION,
    status=OperationStatus.FAILURE,
    duration_ms=operation_duration_ms,
    endpoint=endpoint,
    graph_id=graph_id,
    user_id=user_id,
    operation_name=operation_name,
    metadata=metadata,
  )
