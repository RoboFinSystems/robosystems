"""
Generic Operation Metrics Collection

Provides comprehensive metrics collection for all API operations including:
- Operation execution times and performance tracking
- Success/failure rates and error categorization
- Circuit breaker status monitoring
- Resource utilization metrics
- Queue performance statistics
"""

import time
from typing import Dict, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque
import threading

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


@dataclass
class OperationMetric:
  """Individual operation metric."""

  timestamp: float
  operation_type: OperationType
  status: OperationStatus
  duration_ms: float
  endpoint: str
  graph_id: Optional[str] = None
  user_id: Optional[str] = None
  operation_name: Optional[str] = None
  error_details: Optional[str] = None
  metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OperationMetricsSummary:
  """Aggregated metrics summary for a time period."""

  total_operations: int = 0
  success_count: int = 0
  failure_count: int = 0
  timeout_count: int = 0
  circuit_open_count: int = 0
  queue_full_count: int = 0
  rate_limited_count: int = 0
  avg_duration_ms: float = 0.0
  p95_duration_ms: float = 0.0
  p99_duration_ms: float = 0.0
  error_rate: float = 0.0
  throughput_per_minute: float = 0.0

  def to_dict(self) -> Dict[str, Any]:
    """Convert summary to dictionary for JSON serialization."""
    return {
      "total_operations": self.total_operations,
      "success_count": self.success_count,
      "failure_count": self.failure_count,
      "timeout_count": self.timeout_count,
      "circuit_open_count": self.circuit_open_count,
      "queue_full_count": self.queue_full_count,
      "rate_limited_count": self.rate_limited_count,
      "avg_duration_ms": round(self.avg_duration_ms, 2),
      "p95_duration_ms": round(self.p95_duration_ms, 2),
      "p99_duration_ms": round(self.p99_duration_ms, 2),
      "error_rate": round(self.error_rate * 100, 2),  # Convert to percentage
      "throughput_per_minute": round(self.throughput_per_minute, 2),
    }


class OperationMetricsCollector:
  """
  Centralized metrics collector for all API operations.

  Provides thread-safe collection of metrics with automatic aggregation
  and time-based sliding windows for performance analysis.
  """

  def __init__(
    self,
    max_metrics: int = 10000,
    retention_hours: int = 24,
    aggregation_window_minutes: int = 5,
  ):
    """
    Initialize metrics collector.

    Args:
        max_metrics: Maximum number of individual metrics to retain
        retention_hours: How long to keep metrics (hours)
        aggregation_window_minutes: Window size for aggregated summaries
    """
    self.max_metrics = max_metrics
    self.retention_hours = retention_hours
    self.aggregation_window_minutes = aggregation_window_minutes

    # Thread-safe storage
    self._lock = threading.RLock()
    self._metrics: deque[OperationMetric] = deque(maxlen=max_metrics)

    # Aggregated summaries by endpoint and time window
    self._summaries: Dict[str, Dict[str, OperationMetricsSummary]] = defaultdict(dict)

    # Circuit breaker status tracking
    self._circuit_status: Dict[str, Dict[str, Any]] = defaultdict(dict)

    # Resource metrics (handler pools, queues, etc.)
    self._resource_metrics: Dict[str, Any] = {}

    logger.info(
      f"Initialized OperationMetricsCollector with max_metrics={max_metrics}, "
      f"retention_hours={retention_hours}, aggregation_window_minutes={aggregation_window_minutes}"
    )

  def record_metric(
    self,
    operation_type: OperationType,
    status: OperationStatus,
    duration_ms: float,
    endpoint: str,
    graph_id: Optional[str] = None,
    user_id: Optional[str] = None,
    operation_name: Optional[str] = None,
    error_details: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
  ):
    """
    Record a new operation metric.

    Args:
        operation_type: Type of operation performed
        status: Success/failure status
        duration_ms: Operation duration in milliseconds
        endpoint: API endpoint path
        graph_id: Graph identifier (if applicable)
        user_id: User identifier (if applicable)
        operation_name: Specific operation name (e.g., tool name, query type)
        error_details: Error message for failed operations
        metadata: Additional operation metadata
    """
    metric = OperationMetric(
      timestamp=time.time(),
      operation_type=operation_type,
      status=status,
      duration_ms=duration_ms,
      endpoint=endpoint,
      graph_id=graph_id,
      user_id=user_id,
      operation_name=operation_name,
      error_details=error_details,
      metadata=metadata or {},
    )

    with self._lock:
      self._metrics.append(metric)
      self._update_aggregated_summaries(metric)

    # Log important events
    if status != OperationStatus.SUCCESS:
      logger.warning(
        f"Operation failed - Type: {operation_type.value}, "
        f"Status: {status.value}, Endpoint: {endpoint}, Graph: {graph_id}, "
        f"Operation: {operation_name}, Duration: {duration_ms:.1f}ms, Error: {error_details}"
      )
    elif duration_ms > 10000:  # Log slow operations (>10s)
      logger.warning(
        f"Slow operation detected - Type: {operation_type.value}, "
        f"Endpoint: {endpoint}, Graph: {graph_id}, Operation: {operation_name}, "
        f"Duration: {duration_ms:.1f}ms"
      )

  def _update_aggregated_summaries(self, metric: OperationMetric):
    """Update aggregated summaries with new metric (called with lock held)."""
    # Create time window key (rounded to aggregation window)
    window_start = int(metric.timestamp // (self.aggregation_window_minutes * 60))
    window_key = f"{window_start}"

    # Update summary for this endpoint and time window
    endpoint_key = f"{metric.endpoint}:{metric.graph_id or 'global'}"
    summary = self._summaries[endpoint_key].get(window_key, OperationMetricsSummary())

    summary.total_operations += 1

    if metric.status == OperationStatus.SUCCESS:
      summary.success_count += 1
    elif metric.status == OperationStatus.FAILURE:
      summary.failure_count += 1
    elif metric.status == OperationStatus.TIMEOUT:
      summary.timeout_count += 1
    elif metric.status == OperationStatus.CIRCUIT_OPEN:
      summary.circuit_open_count += 1
    elif metric.status == OperationStatus.QUEUE_FULL:
      summary.queue_full_count += 1
    elif metric.status == OperationStatus.RATE_LIMITED:
      summary.rate_limited_count += 1

    # Update duration statistics (simple running average for now)
    if summary.total_operations == 1:
      summary.avg_duration_ms = metric.duration_ms
    else:
      # Running average
      summary.avg_duration_ms = (
        summary.avg_duration_ms * (summary.total_operations - 1) + metric.duration_ms
      ) / summary.total_operations

    # Calculate error rate
    total_errors = (
      summary.failure_count
      + summary.timeout_count
      + summary.circuit_open_count
      + summary.rate_limited_count
    )
    summary.error_rate = total_errors / summary.total_operations

    self._summaries[endpoint_key][window_key] = summary

  def update_circuit_breaker_status(
    self,
    graph_id: str,
    operation: str,
    state: str,
    failure_count: int = 0,
    last_failure_time: Optional[float] = None,
    recovery_time: Optional[float] = None,
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

  def update_resource_metrics(self, resource_type: str, metrics: Dict[str, Any]):
    """Update resource utilization metrics (handler pools, queues, etc.)."""
    with self._lock:
      self._resource_metrics[resource_type] = {
        **metrics,
        "updated_at": time.time(),
      }

  def get_metrics_summary(
    self,
    endpoint: Optional[str] = None,
    graph_id: Optional[str] = None,
    time_range_minutes: int = 60,
  ) -> Dict[str, Any]:
    """
    Get aggregated metrics summary.

    Args:
        endpoint: Specific endpoint to get metrics for (None for all)
        graph_id: Specific graph to get metrics for (None for all)
        time_range_minutes: Time range to aggregate over

    Returns:
        Dictionary containing aggregated metrics
    """
    with self._lock:
      current_time = time.time()
      cutoff_time = current_time - (time_range_minutes * 60)

      # Filter metrics by time range and criteria
      recent_metrics = [
        m
        for m in self._metrics
        if (
          m.timestamp >= cutoff_time
          and (endpoint is None or m.endpoint == endpoint)
          and (graph_id is None or m.graph_id == graph_id)
        )
      ]

      if not recent_metrics:
        return {
          "summary": OperationMetricsSummary().to_dict(),
          "circuit_breakers": {},
          "resources": self._resource_metrics,
          "time_range_minutes": time_range_minutes,
          "endpoint": endpoint,
          "graph_id": graph_id,
        }

      # Aggregate metrics
      summary = OperationMetricsSummary()
      durations = []

      for metric in recent_metrics:
        summary.total_operations += 1
        durations.append(metric.duration_ms)

        if metric.status == OperationStatus.SUCCESS:
          summary.success_count += 1
        elif metric.status == OperationStatus.FAILURE:
          summary.failure_count += 1
        elif metric.status == OperationStatus.TIMEOUT:
          summary.timeout_count += 1
        elif metric.status == OperationStatus.CIRCUIT_OPEN:
          summary.circuit_open_count += 1
        elif metric.status == OperationStatus.QUEUE_FULL:
          summary.queue_full_count += 1
        elif metric.status == OperationStatus.RATE_LIMITED:
          summary.rate_limited_count += 1

      # Calculate duration statistics
      if durations:
        durations.sort()
        summary.avg_duration_ms = sum(durations) / len(durations)
        summary.p95_duration_ms = durations[int(len(durations) * 0.95)]
        summary.p99_duration_ms = durations[int(len(durations) * 0.99)]

      # Calculate error rate and throughput
      total_errors = (
        summary.failure_count
        + summary.timeout_count
        + summary.circuit_open_count
        + summary.rate_limited_count
      )
      summary.error_rate = (
        total_errors / summary.total_operations if summary.total_operations > 0 else 0.0
      )
      summary.throughput_per_minute = summary.total_operations / time_range_minutes

      # Get circuit breaker status
      circuit_status = {}
      if graph_id:
        circuit_status = self._circuit_status.get(graph_id, {})
      else:
        # Aggregate all circuit breaker statuses
        for gid, circuits in self._circuit_status.items():
          circuit_status[gid] = circuits

      return {
        "summary": summary.to_dict(),
        "circuit_breakers": circuit_status,
        "resources": self._resource_metrics,
        "time_range_minutes": time_range_minutes,
        "endpoint": endpoint,
        "graph_id": graph_id,
        "total_metrics_collected": len(recent_metrics),
      }

  def get_operation_performance_breakdown(
    self,
    endpoint: Optional[str] = None,
    graph_id: Optional[str] = None,
    time_range_minutes: int = 60,
  ) -> Dict[str, Dict[str, Any]]:
    """Get performance breakdown by operation type and name."""
    with self._lock:
      current_time = time.time()
      cutoff_time = current_time - (time_range_minutes * 60)

      # Group metrics by operation
      operation_metrics = defaultdict(list)

      for metric in self._metrics:
        if (
          metric.timestamp >= cutoff_time
          and (endpoint is None or metric.endpoint == endpoint)
          and (graph_id is None or metric.graph_id == graph_id)
        ):
          key = f"{metric.operation_type.value}:{metric.operation_name or 'unknown'}"
          operation_metrics[key].append(metric)

      # Aggregate by operation
      operation_summary = {}
      for operation_key, metrics in operation_metrics.items():
        durations = [m.duration_ms for m in metrics]
        success_count = sum(1 for m in metrics if m.status == OperationStatus.SUCCESS)

        operation_summary[operation_key] = {
          "total_executions": len(metrics),
          "success_count": success_count,
          "failure_count": len(metrics) - success_count,
          "success_rate": success_count / len(metrics) if metrics else 0.0,
          "avg_duration_ms": sum(durations) / len(durations) if durations else 0.0,
          "min_duration_ms": min(durations) if durations else 0.0,
          "max_duration_ms": max(durations) if durations else 0.0,
        }

      return operation_summary

  def cleanup_old_metrics(self):
    """Remove metrics older than retention period."""
    with self._lock:
      current_time = time.time()
      cutoff_time = current_time - (self.retention_hours * 3600)

      # Remove old individual metrics (deque automatically handles max size)
      old_count = len(self._metrics)
      self._metrics = deque(
        (m for m in self._metrics if m.timestamp >= cutoff_time),
        maxlen=self.max_metrics,
      )

      # Remove old aggregated summaries
      for endpoint_key in list(self._summaries.keys()):
        for window_key in list(self._summaries[endpoint_key].keys()):
          window_start_time = int(window_key) * self.aggregation_window_minutes * 60
          if window_start_time < cutoff_time:
            del self._summaries[endpoint_key][window_key]

        # Remove empty endpoint entries
        if not self._summaries[endpoint_key]:
          del self._summaries[endpoint_key]

      new_count = len(self._metrics)
      if old_count != new_count:
        logger.info(f"Cleaned up {old_count - new_count} old operation metrics")


# Global metrics collector instance
_metrics_collector: Optional[OperationMetricsCollector] = None


def get_operation_metrics_collector() -> OperationMetricsCollector:
  """Get the global operation metrics collector instance."""
  global _metrics_collector

  if _metrics_collector is None:
    _metrics_collector = OperationMetricsCollector()

  return _metrics_collector


def record_operation_metric(
  operation_type: OperationType,
  status: OperationStatus,
  duration_ms: float,
  endpoint: str,
  graph_id: Optional[str] = None,
  user_id: Optional[str] = None,
  operation_name: Optional[str] = None,
  error_details: Optional[str] = None,
  metadata: Optional[Dict[str, Any]] = None,
):
  """Convenience function to record operation metrics."""
  collector = get_operation_metrics_collector()
  collector.record_metric(
    operation_type=operation_type,
    status=status,
    duration_ms=duration_ms,
    endpoint=endpoint,
    graph_id=graph_id,
    user_id=user_id,
    operation_name=operation_name,
    error_details=error_details,
    metadata=metadata,
  )
