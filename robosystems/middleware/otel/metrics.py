"""
Standardized OpenTelemetry metrics collection for RoboSystems API endpoints.

This module provides consistent metrics patterns for observability across all API endpoints.
"""

import functools
import inspect
import time
from collections.abc import Callable
from contextlib import contextmanager
from enum import Enum
from typing import Any

from opentelemetry import metrics
from opentelemetry.metrics import CallbackOptions, Observation
from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View

# Custom histogram buckets tuned for API latency distribution:
# Dense coverage in 10ms-500ms range where most graph queries land,
# sparse coverage at sub-ms (health checks excluded) and multi-second (agent/AI ops)
API_DURATION_BUCKETS = (
  0.005,
  0.01,
  0.025,
  0.05,
  0.075,
  0.1,
  0.15,
  0.2,
  0.3,
  0.5,
  0.75,
  1.0,
  2.5,
  5.0,
  10.0,
)

QUERY_DURATION_BUCKETS = (
  0.01,
  0.025,
  0.05,
  0.1,
  0.25,
  0.5,
  1.0,
  2.5,
  5.0,
  10.0,
  30.0,
  60.0,
)

# Graph API proxy calls — covers fast reads through slow ingestion/backup
GRAPH_API_DURATION_BUCKETS = (
  0.005,
  0.01,
  0.025,
  0.05,
  0.1,
  0.25,
  0.5,
  1.0,
  2.5,
  5.0,
  10.0,
  30.0,
  60.0,
  120.0,
  300.0,
)


def get_metric_views() -> list[View]:
  """Return View configurations for custom histogram buckets."""
  return [
    View(
      instrument_name="robosystems_api_request_duration_seconds",
      aggregation=ExplicitBucketHistogramAggregation(boundaries=API_DURATION_BUCKETS),
    ),
    View(
      instrument_name="robosystems_query_wait_time_seconds",
      aggregation=ExplicitBucketHistogramAggregation(boundaries=QUERY_DURATION_BUCKETS),
    ),
    View(
      instrument_name="robosystems_query_execution_time_seconds",
      aggregation=ExplicitBucketHistogramAggregation(boundaries=QUERY_DURATION_BUCKETS),
    ),
    View(
      instrument_name="robosystems_graph_api_duration_seconds",
      aggregation=ExplicitBucketHistogramAggregation(
        boundaries=GRAPH_API_DURATION_BUCKETS
      ),
    ),
  ]


class MetricType(Enum):
  """Standard metric types for API endpoints."""

  REQUEST = "request"
  AUTH = "auth"
  ERROR = "error"
  BUSINESS = "business"


class EndpointMetrics:
  """Container for endpoint-specific metrics instruments."""

  def __init__(self, meter_name: str):
    self.meter = metrics.get_meter(meter_name)
    self._request_counter = None
    self._request_duration = None
    self._error_counter = None
    self._auth_attempts = None
    self._auth_failures = None
    self._business_counter = None
    self._graph_node_count = None
    self._graph_relationship_count = None
    self._graph_size_estimate = None
    self._rate_limit_counter = None
    self._credit_consumption = None
    self._graph_api_duration = None
    self._graph_api_requests = None
    self._graph_api_errors = None

  def _ensure_instruments(self):
    """Lazy initialization of metric instruments."""
    if self._request_counter is None:
      self._request_counter = self.meter.create_counter(
        "robosystems_api_requests_total",
        description="Total number of API requests by endpoint, method, and status",
      )

      self._request_duration = self.meter.create_histogram(
        "robosystems_api_request_duration_seconds",
        description="Request duration in seconds",
        unit="s",
      )

      self._error_counter = self.meter.create_counter(
        "robosystems_api_errors_total",
        description="Total number of API errors by endpoint, method, and error type",
      )

      self._auth_attempts = self.meter.create_counter(
        "robosystems_auth_attempts_total",
        description="Total authentication attempts by endpoint and method",
      )

      self._auth_failures = self.meter.create_counter(
        "robosystems_auth_failures_total",
        description="Total authentication failures by endpoint, method, and failure reason",
      )

      self._business_counter = self.meter.create_counter(
        "robosystems_business_events_total",
        description="Business logic events by endpoint, method, and event type",
      )

      self._graph_node_count = self.meter.create_up_down_counter(
        "robosystems_graph_nodes_total",
        description="Total number of nodes in graph databases by graph_id",
      )

      self._graph_relationship_count = self.meter.create_up_down_counter(
        "robosystems_graph_relationships_total",
        description="Total number of relationships in graph databases by graph_id",
      )

      self._graph_size_estimate = self.meter.create_up_down_counter(
        "robosystems_graph_size_bytes",
        description="Estimated size of graph databases in bytes by graph_id",
        unit="By",
      )

      # Query queue metrics
      self._query_queue_size = self.meter.create_observable_gauge(
        "robosystems_query_queue_size",
        callbacks=[self._observe_queue_size],
        description="Current number of queries in the queue by priority",
        unit="queries",
      )

      # Database connection pool metrics
      self._db_pool_gauge = self.meter.create_observable_gauge(
        "robosystems_db_pool_connections",
        callbacks=[self._observe_db_pool],
        description="SQLAlchemy connection pool state",
        unit="connections",
      )

      # SSE monitoring metrics
      self._sse_connections_active = self.meter.create_up_down_counter(
        "robosystems_sse_connections_active",
        description="Current number of active SSE connections",
        unit="connections",
      )

      self._sse_connections_opened = self.meter.create_counter(
        "robosystems_sse_connections_opened_total",
        description="Total number of SSE connections opened",
      )

      self._sse_connections_closed = self.meter.create_counter(
        "robosystems_sse_connections_closed_total",
        description="Total number of SSE connections closed",
      )

      self._sse_connections_rejected = self.meter.create_counter(
        "robosystems_sse_connections_rejected_total",
        description="Total number of SSE connections rejected (rate limits, etc)",
      )

      self._sse_events_emitted = self.meter.create_counter(
        "robosystems_sse_events_emitted_total",
        description="Total number of SSE events successfully emitted",
      )

      self._sse_events_failed = self.meter.create_counter(
        "robosystems_sse_events_failed_total",
        description="Total number of SSE events that failed to emit",
      )

      self._sse_redis_circuit_breaker_opens = self.meter.create_counter(
        "robosystems_sse_redis_circuit_breaker_opens_total",
        description="Number of times SSE Redis circuit breaker opened",
      )

      self._sse_connection_queue_overflows = self.meter.create_counter(
        "robosystems_sse_connection_queue_overflows_total",
        description="Number of SSE connection queue overflow events",
      )

      self._query_submissions = self.meter.create_counter(
        "robosystems_query_submissions_total",
        description="Total query submissions to the queue",
      )

      self._query_queue_rejections = self.meter.create_counter(
        "robosystems_query_queue_rejections_total",
        description="Queries rejected due to queue limits",
      )

      self._query_wait_time = self.meter.create_histogram(
        "robosystems_query_wait_time_seconds",
        description="Time queries spend waiting in queue before execution",
        unit="s",
      )

      self._query_execution_time = self.meter.create_histogram(
        "robosystems_query_execution_time_seconds",
        description="Time to execute queries after dequeuing",
        unit="s",
      )

      self._query_concurrent_executions = self.meter.create_up_down_counter(
        "robosystems_query_concurrent_executions",
        description="Number of queries currently executing",
      )

      self._query_completions = self.meter.create_counter(
        "robosystems_query_completions_total",
        description="Total query completions by status",
      )

      self._query_user_limits = self.meter.create_counter(
        "robosystems_query_user_limit_rejections_total",
        description="Queries rejected due to per-user limits",
      )

      # Rate limit metrics
      self._rate_limit_counter = self.meter.create_counter(
        "robosystems_rate_limit_rejections_total",
        description="Total rate limit rejections by endpoint and limit type",
      )

      # Credit consumption metrics
      self._credit_consumption = self.meter.create_counter(
        "robosystems_credits_consumed_total",
        description="Total credits consumed by operation type",
      )

      # Graph API proxy metrics — indirect observability of LadybugDB from the API side
      self._graph_api_duration = self.meter.create_histogram(
        "robosystems_graph_api_duration_seconds",
        description="Duration of HTTP calls from API to Graph API (LadybugDB)",
        unit="s",
      )

      self._graph_api_requests = self.meter.create_counter(
        "robosystems_graph_api_requests_total",
        description="Total HTTP requests from API to Graph API",
      )

      self._graph_api_errors = self.meter.create_counter(
        "robosystems_graph_api_errors_total",
        description="Total failed HTTP requests from API to Graph API",
      )

  def record_request(
    self,
    endpoint: str,
    method: str,
    status_code: int,
    duration: float,
    user_id: str | None = None,
    additional_attributes: dict[str, Any] | None = None,
  ):
    """Record standard request metrics."""
    self._ensure_instruments()

    base_attributes = {
      "endpoint": endpoint,
      "method": method,
      "status_code": str(status_code),
      "status_class": f"{status_code // 100}xx",
    }

    if user_id:
      base_attributes["user_authenticated"] = "true"
    else:
      base_attributes["user_authenticated"] = "false"

    if additional_attributes:
      base_attributes.update(additional_attributes)

    if self._request_counter is not None:
      self._request_counter.add(1, base_attributes)
    if self._request_duration is not None:
      self._request_duration.record(duration, base_attributes)

  def record_request_duration(
    self,
    endpoint: str,
    method: str,
    status_code: int,
    duration: float,
    user_id: str | None = None,
  ):
    """Record request duration metrics only.

    Note: `status_code` is serialized to `str` to match the attribute
    shape used by `record_request`. OTel histogram attributes must be
    type-consistent across recordings of the same instrument, otherwise
    Prometheus will silently split the series by attribute type.
    """
    self._ensure_instruments()

    attributes = {
      "endpoint": endpoint,
      "method": method,
      "status_code": str(status_code),
    }

    if user_id:
      attributes["user_id"] = user_id

    if self._request_duration is not None:
      self._request_duration.record(duration, attributes)

  def record_auth_attempt(
    self,
    endpoint: str,
    method: str,
    auth_type: str,
    success: bool,
    failure_reason: str | None = None,
    user_id: str | None = None,
  ):
    """Record authentication attempt metrics."""
    self._ensure_instruments()

    base_attributes = {
      "endpoint": endpoint,
      "method": method,
      "auth_type": auth_type,
      "success": success,
    }

    # Include user_id if provided
    if user_id:
      base_attributes["user_id"] = user_id

    # Record attempt
    if self._auth_attempts is not None:
      self._auth_attempts.add(1, base_attributes)

    # Record failure if applicable
    if not success:
      failure_attributes = base_attributes.copy()
      failure_attributes["failure_reason"] = failure_reason or "unknown"
      if self._auth_failures is not None:
        self._auth_failures.add(1, failure_attributes)

  def record_error(
    self,
    endpoint: str,
    method: str,
    error_type: str,
    error_code: str | None = None,
    user_id: str | None = None,
  ):
    """Record error metrics."""
    self._ensure_instruments()

    attributes = {
      "endpoint": endpoint,
      "method": method,
      "error_type": error_type,
    }

    if error_code:
      attributes["error_code"] = error_code

    if user_id:
      attributes["user_authenticated"] = "true"
    else:
      attributes["user_authenticated"] = "false"

    if self._error_counter is not None:
      self._error_counter.add(1, attributes)

  def record_business_event(
    self,
    endpoint: str,
    method: str,
    event_type: str,
    event_data: dict[str, Any] | None = None,
    user_id: str | None = None,
  ):
    """Record business logic events."""
    # Skip business events for high-frequency health check endpoints to reduce costs
    if endpoint in ["/v1/status", "/status"] and event_type in [
      "health_check",
      "metrics_access",
    ]:
      return

    self._ensure_instruments()

    attributes = {
      "endpoint": endpoint,
      "method": method,
      "event_type": event_type,
    }

    if user_id:
      attributes["user_id"] = user_id

    if event_data:
      # Flatten event data into attributes (convert values to strings)
      for key, value in event_data.items():
        attributes[f"event_{key}"] = str(value)

    if self._business_counter is not None:
      self._business_counter.add(1, attributes)

  def record_graph_metrics(
    self,
    graph_id: str,
    node_count: int,
    relationship_count: int,
    estimated_size_bytes: int,
    user_id: str | None = None,
    additional_attributes: dict[str, Any] | None = None,
  ):
    """Record graph database metrics."""
    self._ensure_instruments()

    base_attributes = {
      "graph_id": graph_id,
    }

    if user_id:
      base_attributes["user_id"] = user_id

    if additional_attributes:
      base_attributes.update(additional_attributes)

    # Record the current values (up-down counters will track changes)
    if self._graph_node_count is not None:
      self._graph_node_count.add(node_count, base_attributes)
    if self._graph_relationship_count is not None:
      self._graph_relationship_count.add(relationship_count, base_attributes)
    if self._graph_size_estimate is not None:
      self._graph_size_estimate.add(estimated_size_bytes, base_attributes)

  def record_query_submission(
    self,
    graph_id: str,
    user_id: str,
    priority: int,
    success: bool,
    rejection_reason: str | None = None,
  ):
    """Record query submission to queue.

    Note: graph_id/user_id are NOT used as metric attributes to avoid
    unbounded cardinality. Only priority and success are dimensions.
    """
    self._ensure_instruments()

    attributes = {
      "priority": str(priority),
      "success": str(success),
    }

    if self._query_submissions is not None:
      self._query_submissions.add(1, attributes)

    if not success:
      rejection_attrs = {
        "priority": str(priority),
        "reason": rejection_reason or "unknown",
      }

      if rejection_reason == "queue_full" and self._query_queue_rejections is not None:
        self._query_queue_rejections.add(1, rejection_attrs)
      elif rejection_reason == "user_limit" and self._query_user_limits is not None:
        self._query_user_limits.add(1, rejection_attrs)

  def record_query_wait_time(
    self,
    graph_id: str,
    user_id: str,
    priority: int,
    wait_time_seconds: float,
  ):
    """Record time query spent waiting in queue."""
    self._ensure_instruments()

    attributes = {
      "priority": str(priority),
    }

    if self._query_wait_time is not None:
      self._query_wait_time.record(wait_time_seconds, attributes)

  def record_query_execution(
    self,
    graph_id: str,
    user_id: str,
    execution_time_seconds: float,
    status: str,  # completed, failed, cancelled, timeout
    error_type: str | None = None,
  ):
    """Record query execution metrics."""
    self._ensure_instruments()

    attributes = {
      "status": status,
    }

    if error_type:
      attributes["error_type"] = error_type

    if self._query_execution_time is not None:
      self._query_execution_time.record(execution_time_seconds, attributes)
    if self._query_completions is not None:
      self._query_completions.add(1, attributes)

  def update_concurrent_executions(self, delta: int):
    """Update the count of concurrent query executions."""
    self._ensure_instruments()
    if self._query_concurrent_executions is not None:
      self._query_concurrent_executions.add(delta, {})

  # SSE Monitoring Methods

  def record_sse_connection_opened(self, user_id: str, operation_id: str):
    """Record an SSE connection being opened.

    Note: user_id/operation_id are NOT used as metric attributes to avoid
    unbounded cardinality. They are kept in the method signature for logging context.
    """
    self._ensure_instruments()
    if self._sse_connections_opened is not None:
      self._sse_connections_opened.add(1, {})
    if self._sse_connections_active is not None:
      self._sse_connections_active.add(1, {})

  def record_sse_connection_closed(self, user_id: str, operation_id: str):
    """Record an SSE connection being closed."""
    self._ensure_instruments()
    if self._sse_connections_closed is not None:
      self._sse_connections_closed.add(1, {})
    if self._sse_connections_active is not None:
      self._sse_connections_active.add(-1, {})

  def record_sse_connection_rejected(self, user_id: str, reason: str):
    """Record an SSE connection being rejected."""
    self._ensure_instruments()
    attributes = {
      "reason": reason,
    }
    if self._sse_connections_rejected is not None:
      self._sse_connections_rejected.add(1, attributes)

  def record_sse_event_emitted(self, operation_id: str, event_type: str):
    """Record a successful SSE event emission."""
    self._ensure_instruments()
    attributes = {
      "event_type": event_type,
    }
    if self._sse_events_emitted is not None:
      self._sse_events_emitted.add(1, attributes)

  def record_sse_event_failed(self, operation_id: str, failure_reason: str):
    """Record a failed SSE event emission."""
    self._ensure_instruments()
    attributes = {
      "failure_reason": failure_reason,
    }
    if self._sse_events_failed is not None:
      self._sse_events_failed.add(1, attributes)

    # Check if this is a Redis failure that opened the circuit breaker
    if (
      failure_reason == "redis_error"
      and self._sse_redis_circuit_breaker_opens is not None
    ):
      self._sse_redis_circuit_breaker_opens.add(1, {})

  def record_sse_queue_overflow(self, operation_id: str, connection_id: str):
    """Record an SSE connection queue overflow event."""
    self._ensure_instruments()
    if self._sse_connection_queue_overflows is not None:
      self._sse_connection_queue_overflows.add(1, {})

  def record_rate_limit_rejection(
    self,
    endpoint: str,
    limit_type: str,
    identifier_type: str,
  ):
    """Record a rate limit rejection."""
    self._ensure_instruments()
    attributes = {
      "endpoint": endpoint,
      "limit_type": limit_type,
      "identifier_type": identifier_type,
    }
    if self._rate_limit_counter is not None:
      self._rate_limit_counter.add(1, attributes)

  def record_credit_consumption(
    self,
    operation_type: str,
    credits_consumed: float,
    graph_tier: str,
  ):
    """Record credit consumption for an AI operation."""
    self._ensure_instruments()
    attributes = {
      "operation_type": operation_type,
      "graph_tier": graph_tier,
    }
    if self._credit_consumption is not None:
      self._credit_consumption.add(credits_consumed, attributes)

  def record_graph_api_call(
    self,
    method: str,
    operation: str,
    duration: float,
    status_code: int,
    route_target: str,
    error: bool = False,
    error_type: str | None = None,
  ):
    """Record an HTTP call from the API to the Graph API (LadybugDB).

    This provides indirect observability of the Graph API from the API side,
    since the Graph API runs on EC2 without an OTel sidecar.

    Args:
        method: HTTP method (GET, POST, DELETE)
        operation: Normalized operation name (query, create_database, backup, etc.)
        duration: Request duration in seconds
        status_code: HTTP response status code
        route_target: Routing target (user_graph, shared_master, shared_replica)
        error: Whether the request failed
        error_type: Error category (timeout, connection, client_4xx, server_5xx)
    """
    self._ensure_instruments()

    attributes = {
      "method": method,
      "operation": operation,
      "status_code": str(status_code),
      "route_target": route_target,
    }

    if self._graph_api_duration is not None:
      self._graph_api_duration.record(duration, attributes)
    if self._graph_api_requests is not None:
      self._graph_api_requests.add(1, attributes)

    if error and self._graph_api_errors is not None:
      error_attributes = {
        "method": method,
        "operation": operation,
        "route_target": route_target,
        "error_type": error_type or "unknown",
      }
      self._graph_api_errors.add(1, error_attributes)

  def _observe_queue_size(self, options: CallbackOptions) -> list[Observation]:
    """Observable callback for queue size metrics."""
    try:
      # Import here to avoid circular dependency
      from robosystems.middleware.graph.query_queue import get_query_queue

      queue_manager = get_query_queue()
      priority_counts = queue_manager.get_queue_metrics_by_priority()

      observations = []
      for priority, count in priority_counts.items():
        observations.append(Observation(count, {"priority": str(priority)}))

      return observations
    except Exception:
      # Return empty list if queue not initialized
      return []

  def _observe_db_pool(self, options: CallbackOptions) -> list[Observation]:
    """Observable callback for SQLAlchemy connection pool metrics.

    Note: pool.overflow() returns negative values when overflow slots are
    available (e.g., -5 means 5 unused overflow slots). We clamp to 0 for
    the "overflow" observation since negative connections don't make sense
    in dashboards.
    """
    try:
      from robosystems.database import engine

      pool = engine.pool
      return [
        Observation(pool.checkedout(), {"state": "checked_out"}),
        Observation(pool.checkedin(), {"state": "idle"}),
        Observation(max(0, pool.overflow()), {"state": "overflow"}),
        Observation(pool.size(), {"state": "pool_size"}),
      ]
    except Exception:
      return []


# Global metrics instance
_global_metrics: EndpointMetrics | None = None


def get_endpoint_metrics() -> EndpointMetrics:
  """Get or create the global endpoint metrics instance."""
  global _global_metrics
  if _global_metrics is None:
    _global_metrics = EndpointMetrics("robosystems.api")
  return _global_metrics


def record_request_metrics(
  endpoint: str,
  method: str,
  status_code: int,
  duration: float,
  user_id: str | None = None,
  **kwargs,
):
  """Convenience function to record request metrics."""
  # Skip recording for high-frequency health check endpoints to reduce costs
  if endpoint in ["/v1/status", "/status"] and method == "GET":
    return

  metrics_instance = get_endpoint_metrics()
  metrics_instance.record_request(
    endpoint, method, status_code, duration, user_id, kwargs
  )


def record_auth_metrics(
  endpoint: str,
  method: str,
  auth_type: str,
  success: bool,
  failure_reason: str | None = None,
  user_id: str | None = None,
):
  """Convenience function to record auth metrics."""
  metrics_instance = get_endpoint_metrics()
  metrics_instance.record_auth_attempt(
    endpoint, method, auth_type, success, failure_reason, user_id
  )


def record_error_metrics(
  endpoint: str,
  method: str,
  error_type: str,
  error_code: str | None = None,
  user_id: str | None = None,
):
  """Convenience function to record error metrics."""
  metrics_instance = get_endpoint_metrics()
  metrics_instance.record_error(endpoint, method, error_type, error_code, user_id)


def record_query_queue_metrics(
  metric_type: str,
  graph_id: str,
  user_id: str,
  **kwargs,
):
  """Convenience function to record query queue metrics."""
  metrics_instance = get_endpoint_metrics()

  if metric_type == "submission":
    metrics_instance.record_query_submission(
      graph_id=graph_id,
      user_id=user_id,
      priority=kwargs.get("priority", 5),
      success=kwargs.get("success", True),
      rejection_reason=kwargs.get("rejection_reason"),
    )
  elif metric_type == "wait_time":
    metrics_instance.record_query_wait_time(
      graph_id=graph_id,
      user_id=user_id,
      priority=kwargs.get("priority", 5),
      wait_time_seconds=kwargs.get("wait_time_seconds", 0),
    )
  elif metric_type == "execution":
    metrics_instance.record_query_execution(
      graph_id=graph_id,
      user_id=user_id,
      execution_time_seconds=kwargs.get("execution_time_seconds", 0),
      status=kwargs.get("status", "completed"),
      error_type=kwargs.get("error_type"),
    )
  elif metric_type == "concurrent_update":
    metrics_instance.update_concurrent_executions(kwargs.get("delta", 0))


def endpoint_metrics_decorator(
  endpoint_name: str | None = None,
  extract_user_id: bool = True,
  business_event_type: str | None = None,
  method: str | None = None,
):
  """
  Enhanced decorator to automatically collect standard metrics for FastAPI endpoints.

  Args:
      endpoint_name: Override endpoint name (defaults to function name)
      extract_user_id: Whether to attempt user_id extraction from request context
      business_event_type: Optional business event to record on success
      method: HTTP method label (e.g. "POST"). When supplied, skips the
          runtime `fastapi.Request` scan and uses the explicit value.
          Strongly preferred for routes that don't declare `request: Request`
          in their signature — without it the decorator falls back to
          `"UNKNOWN"` which silently degrades Prometheus dashboards that
          filter by method.

  Idempotent replays:
      If the decorated function returns an object with a truthy
      `idempotent_replay` attribute (duck-typed — no import of
      `OperationEnvelope`), the `business_event_type` counter is NOT
      incremented for that call. The request count and latency histogram
      still fire because the HTTP call genuinely happened; only the
      business-event counter is suppressed so "how many times did this
      operation actually execute" stays meaningful on dashboards.

  Usage:
      @router.post("/login")
      @endpoint_metrics_decorator(
          "/v1/auth/login",
          method="POST",
          business_event_type="user_login",
      )
      async def login_endpoint(request: LoginRequest):
          # endpoint logic - metrics recorded automatically
          return response
  """

  def decorator(func: Callable) -> Callable:
    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
      start_time = time.time()
      endpoint = endpoint_name or f"/{func.__name__}"
      # If the caller supplied an explicit method we trust it; otherwise
      # fall back to the legacy runtime scan (which only works for routes
      # that declare `request: Request` in their signature).
      resolved_method = method or "UNKNOWN"
      status_code = 200
      user_id = None
      error_occurred = False
      graph_id = None

      try:
        # Enhanced context extraction from FastAPI
        from fastapi import Request

        request_obj = None

        # Find Request object in args
        for arg in args:
          if isinstance(arg, Request):
            request_obj = arg
            if method is None:
              resolved_method = arg.method
            break

        # Try to extract from kwargs if not found in args
        if not request_obj:
          for key, value in kwargs.items():
            if isinstance(value, Request):
              request_obj = value
              if method is None:
                resolved_method = value.method
              break

        # Extract user_id and graph_id from path parameters or headers
        if extract_user_id and request_obj:
          # Try to get user_id from various sources
          user_id = (
            request_obj.path_params.get("user_id")
            or request_obj.headers.get("X-User-Id")
            or getattr(request_obj.state, "user_id", None)
          )

          # Extract graph_id from path
          graph_id = request_obj.path_params.get("graph_id")

        # Fallback: FastAPI injects path parameters as typed kwargs on the
        # route handler, so `graph_id` is available even when no `Request`
        # object is declared. Only use this if we didn't already pull it
        # from `request_obj.path_params` above.
        if graph_id is None:
          kw_graph = kwargs.get("graph_id")
          if isinstance(kw_graph, str):
            graph_id = kw_graph

        # Execute the endpoint function
        result = await func(*args, **kwargs)

        # Record business event on success if specified.
        # Suppress for idempotent replays so retry traffic doesn't inflate
        # the counter — the underlying operation did not execute again.
        if business_event_type and not getattr(result, "idempotent_replay", False):
          metrics_instance = get_endpoint_metrics()
          event_data = {"graph_id": graph_id} if graph_id else {}
          metrics_instance.record_business_event(
            endpoint=endpoint,
            method=resolved_method,
            event_type=business_event_type,
            event_data=event_data,
            user_id=user_id,
          )

        return result

      except Exception as e:
        error_occurred = True
        status_code = getattr(e, "status_code", 500)

        # Record error metrics
        record_error_metrics(
          endpoint=endpoint,
          method=resolved_method,
          error_type=type(e).__name__,
          error_code=str(getattr(e, "detail", "Unknown error")),
          user_id=user_id,
        )

        raise  # Re-raise the exception

      finally:
        # Record request metrics
        duration = time.time() - start_time
        record_request_metrics(
          endpoint=endpoint,
          method=resolved_method,
          status_code=status_code,
          duration=duration,
          user_id=user_id,
          error_occurred=error_occurred,
        )

    @functools.wraps(func)
    def sync_wrapper(*args, **kwargs):
      start_time = time.time()
      endpoint = endpoint_name or func.__name__
      resolved_method = method or "UNKNOWN"
      status_code = 200
      user_id = None
      error_occurred = False

      try:
        # Try to extract request info from FastAPI context (only if the
        # caller didn't supply an explicit method label).
        if method is None and len(args) > 0 and hasattr(args[0], "method"):
          resolved_method = args[0].method

        # Execute the endpoint function
        result = func(*args, **kwargs)
        return result

      except Exception as e:
        error_occurred = True
        status_code = getattr(e, "status_code", 500)

        # Record error metrics
        record_error_metrics(
          endpoint=endpoint,
          method=resolved_method,
          error_type=type(e).__name__,
          error_code=str(getattr(e, "detail", "Unknown error")),
          user_id=user_id,
        )

        raise  # Re-raise the exception

      finally:
        # Record request metrics
        duration = time.time() - start_time
        record_request_metrics(
          endpoint=endpoint,
          method=resolved_method,
          status_code=status_code,
          duration=duration,
          user_id=user_id,
          error_occurred=error_occurred,
        )

    # Return appropriate wrapper based on function type
    if inspect.iscoroutinefunction(func):
      return async_wrapper
    else:
      return sync_wrapper

  return decorator


@contextmanager
def endpoint_metrics_context(
  endpoint: str,
  method: str,
  user_id: str | None = None,
  business_event_type: str | None = None,
  event_data: dict[str, Any] | None = None,
):
  """
  Context manager for manual metrics collection with automatic timing and error handling.

  Usage:
      async def my_endpoint():
          with endpoint_metrics_context("/v1/auth/login", "POST", user_id="123") as ctx:
              # endpoint logic here
              result = await some_operation()

              # Optionally record business events
              ctx.record_business_event("user_authenticated", {"success": True})

              return result
  """
  start_time = time.time()
  status_code = 200
  error_occurred = False

  class MetricsContext:
    def __init__(self):
      self.business_events = []

    def record_business_event(
      self, event_type: str, data: dict[str, Any] | None = None
    ):
      """Record a business event within the context."""
      self.business_events.append((event_type, data or {}))

  ctx = MetricsContext()

  try:
    yield ctx

    # Record any business events that were added during execution
    if business_event_type or ctx.business_events:
      metrics_instance = get_endpoint_metrics()

      # Record the main business event if specified
      if business_event_type:
        metrics_instance.record_business_event(
          endpoint=endpoint,
          method=method,
          event_type=business_event_type,
          event_data=event_data or {},
          user_id=user_id,
        )

      # Record additional business events
      for event_type, data in ctx.business_events:
        metrics_instance.record_business_event(
          endpoint=endpoint,
          method=method,
          event_type=event_type,
          event_data=data,
          user_id=user_id,
        )

  except Exception as e:
    error_occurred = True
    status_code = getattr(e, "status_code", 500)

    # Record error metrics
    record_error_metrics(
      endpoint=endpoint,
      method=method,
      error_type=type(e).__name__,
      error_code=str(getattr(e, "detail", "Unknown error")),
      user_id=user_id,
    )

    raise  # Re-raise the exception

  finally:
    # Record request metrics
    duration = time.time() - start_time
    record_request_metrics(
      endpoint=endpoint,
      method=method,
      status_code=status_code,
      duration=duration,
      user_id=user_id,
      error_occurred=error_occurred,
    )
