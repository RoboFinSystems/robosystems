"""
Generic Operation Logging for API Endpoints

Provides structured, detailed logging for all API operations to support:
- Operation debugging and troubleshooting
- Performance analysis and optimization
- Security auditing and compliance
- Operational visibility and monitoring
"""

import threading
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any

from robosystems.logger import logger


class LogLevel(Enum):
  """Log levels for operations."""

  DEBUG = "debug"
  INFO = "info"
  WARNING = "warning"
  ERROR = "error"


class OperationLogEventType(Enum):
  """Types of operation log events."""

  OPERATION_START = "operation_start"
  OPERATION_SUCCESS = "operation_success"
  OPERATION_FAILURE = "operation_failure"
  CIRCUIT_BREAKER_OPEN = "circuit_breaker_open"
  CIRCUIT_BREAKER_CLOSE = "circuit_breaker_close"
  RESOURCE_CREATED = "resource_created"
  RESOURCE_CACHED = "resource_cached"
  RESOURCE_CLOSED = "resource_closed"
  QUEUE_OPERATION = "queue_operation"
  CREDIT_OPERATION = "credit_operation"
  TIMEOUT_OCCURRED = "timeout_occurred"
  PERFORMANCE_ALERT = "performance_alert"
  VALIDATION_ERROR = "validation_error"
  EXTERNAL_SERVICE_CALL = "external_service_call"
  DATABASE_OPERATION = "database_operation"
  ANALYTICS_QUERY = "analytics_query"
  SCHEMA_OPERATION = "schema_operation"
  AI_OPERATION = "ai_operation"
  BACKUP_OPERATION = "backup_operation"


@dataclass
class OperationLogEntry:
  """Structured log entry for API operations."""

  timestamp: float
  event_type: OperationLogEventType
  level: LogLevel
  endpoint: str
  graph_id: str | None = None
  user_id: str | None = None
  operation: str | None = None
  operation_name: str | None = None
  duration_ms: float | None = None
  status: str | None = None
  error_message: str | None = None
  metadata: dict[str, Any] | None = None
  trace_id: str | None = None

  def to_dict(self) -> dict[str, Any]:
    """Convert log entry to dictionary for JSON serialization."""
    data = asdict(self)
    # Convert enums to strings
    data["event_type"] = self.event_type.value
    data["level"] = self.level.value
    return {k: v for k, v in data.items() if v is not None}


class OperationLogger:
  """
  Enhanced logger for API operations with structured logging and context tracking.

  Provides operation-scoped logging with automatic correlation IDs,
  performance tracking, and security audit trails.
  """

  def __init__(
    self,
    enable_performance_logging: bool = True,
    enable_debug_logging: bool = False,
    slow_operation_threshold_ms: float = 5000.0,
    max_log_entries: int = 10000,
  ):
    """
    Initialize operation logger.

    Args:
        enable_performance_logging: Enable performance-related logging
        enable_debug_logging: Enable verbose debug logging
        slow_operation_threshold_ms: Threshold for logging slow operations
        max_log_entries: Maximum log entries to retain in memory
    """
    self.enable_performance_logging = enable_performance_logging
    self.enable_debug_logging = enable_debug_logging
    self.slow_operation_threshold_ms = slow_operation_threshold_ms
    self.max_log_entries = max_log_entries

    # Thread-safe storage for structured logs
    self._lock = threading.RLock()
    self._log_entries: list[OperationLogEntry] = []

    # Operation context tracking
    self._operation_contexts: dict[str, dict[str, Any]] = {}

    logger.info(
      f"Initialized OperationLogger with performance_logging={enable_performance_logging}, "
      f"debug_logging={enable_debug_logging}, slow_threshold={slow_operation_threshold_ms}ms"
    )

  def log_operation_start(
    self,
    operation: str,
    endpoint: str,
    graph_id: str | None = None,
    user_id: str | None = None,
    operation_name: str | None = None,
    trace_id: str | None = None,
    metadata: dict[str, Any] | None = None,
  ) -> str:
    """
    Log the start of an API operation.

    Args:
        operation: Operation type (e.g., 'cypher_query', 'entity_create')
        endpoint: API endpoint path
        graph_id: Graph identifier
        user_id: User identifier
        operation_name: Specific operation name
        trace_id: Optional trace ID for correlation
        metadata: Additional operation metadata

    Returns:
        Operation ID for correlation with completion logging
    """
    operation_id = trace_id or f"{operation}_{int(time.time() * 1000000)}"
    start_time = time.time()

    # Store operation context
    with self._lock:
      self._operation_contexts[operation_id] = {
        "operation": operation,
        "endpoint": endpoint,
        "graph_id": graph_id,
        "user_id": user_id,
        "operation_name": operation_name,
        "start_time": start_time,
        "metadata": metadata or {},
      }

    # Log operation start
    log_entry = OperationLogEntry(
      timestamp=start_time,
      event_type=OperationLogEventType.OPERATION_START,
      level=LogLevel.DEBUG if self.enable_debug_logging else LogLevel.INFO,
      endpoint=endpoint,
      graph_id=graph_id,
      user_id=user_id,
      operation=operation,
      operation_name=operation_name,
      trace_id=operation_id,
      metadata=metadata,
    )

    self._add_log_entry(log_entry)

    if self.enable_debug_logging:
      logger.debug(
        f"Operation started - ID: {operation_id}, Operation: {operation}, "
        f"Endpoint: {endpoint}, Graph: {graph_id}, User: {user_id}"
      )

    return operation_id

  def log_operation_success(
    self,
    operation_id: str,
    result_metadata: dict[str, Any] | None = None,
  ) -> None:
    """
    Log successful completion of an API operation.

    Args:
        operation_id: Operation ID from log_operation_start
        result_metadata: Additional result metadata
    """
    end_time = time.time()

    with self._lock:
      context = self._operation_contexts.get(operation_id)
      if not context:
        logger.warning(f"No context found for operation {operation_id}")
        return

      duration_ms = (end_time - context["start_time"]) * 1000

      # Create log entry
      log_entry = OperationLogEntry(
        timestamp=end_time,
        event_type=OperationLogEventType.OPERATION_SUCCESS,
        level=LogLevel.INFO,
        endpoint=context["endpoint"],
        graph_id=context["graph_id"],
        user_id=context["user_id"],
        operation=context["operation"],
        operation_name=context["operation_name"],
        duration_ms=duration_ms,
        status="success",
        trace_id=operation_id,
        metadata={
          **context["metadata"],
          **(result_metadata or {}),
        },
      )

      self._add_log_entry(log_entry)

      # Log performance if enabled
      if self.enable_performance_logging:
        if duration_ms > self.slow_operation_threshold_ms:
          self._log_slow_operation(context, duration_ms, operation_id)
        else:
          logger.info(
            f"Operation completed - ID: {operation_id}, "
            f"Operation: {context['operation']}, Endpoint: {context['endpoint']}, "
            f"Duration: {duration_ms:.1f}ms"
          )

      # Clean up context
      del self._operation_contexts[operation_id]

  def log_operation_failure(
    self,
    operation_id: str,
    error: Exception,
    error_metadata: dict[str, Any] | None = None,
  ) -> None:
    """
    Log failed completion of an API operation.

    Args:
        operation_id: Operation ID from log_operation_start
        error: Exception that caused the failure
        error_metadata: Additional error metadata
    """
    end_time = time.time()

    with self._lock:
      context = self._operation_contexts.get(operation_id)
      if not context:
        logger.warning(f"No context found for failed operation {operation_id}")
        return

      duration_ms = (end_time - context["start_time"]) * 1000
      error_message = str(error)

      # Create log entry
      log_entry = OperationLogEntry(
        timestamp=end_time,
        event_type=OperationLogEventType.OPERATION_FAILURE,
        level=LogLevel.ERROR,
        endpoint=context["endpoint"],
        graph_id=context["graph_id"],
        user_id=context["user_id"],
        operation=context["operation"],
        operation_name=context["operation_name"],
        duration_ms=duration_ms,
        status="failure",
        error_message=error_message,
        trace_id=operation_id,
        metadata={
          **context["metadata"],
          **(error_metadata or {}),
          "error_type": type(error).__name__,
        },
      )

      self._add_log_entry(log_entry)

      # Log error
      logger.error(
        f"Operation failed - ID: {operation_id}, "
        f"Operation: {context['operation']}, Endpoint: {context['endpoint']}, "
        f"Duration: {duration_ms:.1f}ms, Error: {error_message}"
      )

      # Clean up context
      del self._operation_contexts[operation_id]

  def log_circuit_breaker_event(
    self,
    event_type: OperationLogEventType,
    endpoint: str,
    graph_id: str,
    operation: str,
    failure_count: int = 0,
    metadata: dict[str, Any] | None = None,
  ) -> None:
    """Log circuit breaker state changes."""
    log_entry = OperationLogEntry(
      timestamp=time.time(),
      event_type=event_type,
      level=LogLevel.WARNING,
      endpoint=endpoint,
      graph_id=graph_id,
      operation=operation,
      metadata={
        "failure_count": failure_count,
        **(metadata or {}),
      },
    )

    self._add_log_entry(log_entry)

    logger.warning(
      f"Circuit breaker {event_type.value} - Endpoint: {endpoint}, "
      f"Graph: {graph_id}, Operation: {operation}, Failures: {failure_count}"
    )

  def log_resource_event(
    self,
    event_type: OperationLogEventType,
    endpoint: str,
    graph_id: str | None = None,
    user_id: str | None = None,
    resource_type: str | None = None,
    metadata: dict[str, Any] | None = None,
  ) -> None:
    """Log resource lifecycle events (handlers, connections, etc.)."""
    log_entry = OperationLogEntry(
      timestamp=time.time(),
      event_type=event_type,
      level=LogLevel.DEBUG if self.enable_debug_logging else LogLevel.INFO,
      endpoint=endpoint,
      graph_id=graph_id,
      user_id=user_id,
      metadata={
        "resource_type": resource_type,
        **(metadata or {}),
      },
    )

    self._add_log_entry(log_entry)

    if self.enable_debug_logging:
      logger.debug(
        f"Resource {event_type.value} - Endpoint: {endpoint}, Graph: {graph_id}, "
        f"User: {user_id}, Type: {resource_type}"
      )

  def log_external_service_call(
    self,
    endpoint: str,
    service_name: str,
    operation: str,
    duration_ms: float,
    status: str,
    graph_id: str | None = None,
    user_id: str | None = None,
    metadata: dict[str, Any] | None = None,
  ) -> None:
    """Log external service calls (QuickBooks, AI services, etc.)."""
    log_entry = OperationLogEntry(
      timestamp=time.time(),
      event_type=OperationLogEventType.EXTERNAL_SERVICE_CALL,
      level=LogLevel.INFO,
      endpoint=endpoint,
      graph_id=graph_id,
      user_id=user_id,
      operation=operation,
      duration_ms=duration_ms,
      status=status,
      metadata={
        "service_name": service_name,
        **(metadata or {}),
      },
    )

    self._add_log_entry(log_entry)

    logger.info(
      f"External service call - Service: {service_name}, Operation: {operation}, "
      f"Endpoint: {endpoint}, Duration: {duration_ms:.1f}ms, Status: {status}"
    )

  def log_credit_operation(
    self,
    endpoint: str,
    graph_id: str,
    user_id: str,
    operation_type: str,
    cost: float,
    status: str,
    metadata: dict[str, Any] | None = None,
  ) -> None:
    """Log credit-related operations for audit trail."""
    log_entry = OperationLogEntry(
      timestamp=time.time(),
      event_type=OperationLogEventType.CREDIT_OPERATION,
      level=LogLevel.INFO,
      endpoint=endpoint,
      graph_id=graph_id,
      user_id=user_id,
      operation=operation_type,
      status=status,
      metadata={
        "credit_cost": cost,
        **(metadata or {}),
      },
    )

    self._add_log_entry(log_entry)

    logger.info(
      f"Credit operation - User: {user_id}, Graph: {graph_id}, "
      f"Endpoint: {endpoint}, Operation: {operation_type}, Cost: {cost}, Status: {status}"
    )

  def _log_slow_operation(
    self,
    context: dict[str, Any],
    duration_ms: float,
    operation_id: str,
  ) -> None:
    """Log slow operation for performance monitoring."""
    log_entry = OperationLogEntry(
      timestamp=time.time(),
      event_type=OperationLogEventType.PERFORMANCE_ALERT,
      level=LogLevel.WARNING,
      endpoint=context["endpoint"],
      graph_id=context["graph_id"],
      user_id=context["user_id"],
      operation=context["operation"],
      operation_name=context["operation_name"],
      duration_ms=duration_ms,
      trace_id=operation_id,
      metadata={
        **context["metadata"],
        "threshold_ms": self.slow_operation_threshold_ms,
      },
    )

    self._add_log_entry(log_entry)

    logger.warning(
      f"Slow operation detected - ID: {operation_id}, "
      f"Operation: {context['operation']}, Endpoint: {context['endpoint']}, "
      f"Duration: {duration_ms:.1f}ms (threshold: {self.slow_operation_threshold_ms:.1f}ms)"
    )

  def _add_log_entry(self, entry: OperationLogEntry) -> None:
    """Add log entry to structured storage (called with lock held)."""
    with self._lock:
      self._log_entries.append(entry)

      # Trim old entries if over limit
      if len(self._log_entries) > self.max_log_entries:
        self._log_entries = self._log_entries[-self.max_log_entries :]

  def get_recent_logs(
    self,
    endpoint: str | None = None,
    graph_id: str | None = None,
    event_type: OperationLogEventType | None = None,
    time_range_minutes: int = 60,
    limit: int = 100,
  ) -> list[dict[str, Any]]:
    """
    Get recent log entries matching criteria.

    Args:
        endpoint: Filter by endpoint
        graph_id: Filter by graph ID
        event_type: Filter by event type
        time_range_minutes: Time range to search
        limit: Maximum entries to return

    Returns:
        List of log entry dictionaries
    """
    cutoff_time = time.time() - (time_range_minutes * 60)

    with self._lock:
      filtered_entries = []

      for entry in reversed(self._log_entries):  # Most recent first
        if entry.timestamp < cutoff_time:
          break

        if endpoint and entry.endpoint != endpoint:
          continue

        if graph_id and entry.graph_id != graph_id:
          continue

        if event_type and entry.event_type != event_type:
          continue

        filtered_entries.append(entry.to_dict())

        if len(filtered_entries) >= limit:
          break

      return filtered_entries

  @contextmanager
  def operation_context(
    self,
    operation: str,
    endpoint: str,
    graph_id: str | None = None,
    user_id: str | None = None,
    operation_name: str | None = None,
    metadata: dict[str, Any] | None = None,
  ):
    """
    Context manager for automatic operation logging.

    Usage:
        async with operation_logger.operation_context("entity_create", endpoint, graph_id, user_id) as op_id:
            # Perform operation
            result = await some_operation()
            # Success logged automatically
    """
    operation_id = self.log_operation_start(
      operation=operation,
      endpoint=endpoint,
      graph_id=graph_id,
      user_id=user_id,
      operation_name=operation_name,
      metadata=metadata,
    )

    try:
      yield operation_id
      self.log_operation_success(operation_id)
    except Exception as e:
      self.log_operation_failure(operation_id, e)
      raise


# Global operation logger instance
_operation_logger: OperationLogger | None = None


def get_operation_logger() -> OperationLogger:
  """Get the global operation logger instance."""
  global _operation_logger

  if _operation_logger is None:
    _operation_logger = OperationLogger()

  return _operation_logger
