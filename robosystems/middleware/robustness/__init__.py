"""
Robustness middleware components.

This module provides reusable components for error handling, circuit breaking,
credit management, metrics collection, and observability across all endpoints.
"""

from .circuit_breaker import CircuitBreakerManager
from .timeout_coordinator import TimeoutCoordinator


# Operation components
from .operation_metrics import (
  OperationMetricsCollector,
  OperationType,
  OperationStatus,
  get_operation_metrics_collector,
  record_operation_metric,
)
from .operation_logging import (
  OperationLogger,
  OperationLogEventType,
  LogLevel,
  get_operation_logger,
)

__all__ = [
  # Core robustness components
  "CircuitBreakerManager",
  "TimeoutCoordinator",
  # Operation components
  "OperationMetricsCollector",
  "OperationType",
  "OperationStatus",
  "get_operation_metrics_collector",
  "record_operation_metric",
  "OperationLogger",
  "OperationLogEventType",
  "LogLevel",
  "get_operation_logger",
]
