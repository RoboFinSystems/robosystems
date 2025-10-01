"""
OpenTelemetry metrics for Kuzu clustering system.

This module provides comprehensive metrics collection for the Kuzu database
clustering infrastructure including allocation, routing, and health monitoring.
"""

import time
from typing import Optional
from contextlib import contextmanager
from enum import Enum

from opentelemetry import metrics
from opentelemetry.metrics import CallbackOptions, Observation


class KuzuMetricType(Enum):
  """Kuzu-specific metric types."""

  ALLOCATION = "allocation"
  ROUTING = "routing"
  HEALTH = "health"
  CAPACITY = "capacity"
  REPLICATION = "replication"


class KuzuClusterMetrics:
  """Comprehensive metrics for Kuzu clustering system."""

  def __init__(self):
    self.meter = metrics.get_meter("robosystems.kuzu.cluster")
    self._setup_instruments()

  def _setup_instruments(self):
    """Initialize all metric instruments."""

    # Allocation metrics
    self.allocation_counter = self.meter.create_counter(
      "kuzu_database_allocations_total",
      description="Total database allocation attempts",
      unit="1",
    )

    self.allocation_duration = self.meter.create_histogram(
      "kuzu_database_allocation_duration_seconds",
      description="Time to allocate a new database",
      unit="s",
    )

    self.allocation_failures = self.meter.create_counter(
      "kuzu_database_allocation_failures_total",
      description="Failed database allocation attempts",
      unit="1",
    )

    # Routing metrics
    self.routing_requests = self.meter.create_counter(
      "kuzu_routing_requests_total",
      description="Total routing requests by graph_id",
      unit="1",
    )

    self.routing_latency = self.meter.create_histogram(
      "kuzu_routing_latency_milliseconds",
      description="Latency of routing decisions",
      unit="ms",
    )

    self.routing_errors = self.meter.create_counter(
      "kuzu_routing_errors_total", description="Routing errors by type", unit="1"
    )

    # Instance health metrics
    self.instance_health_checks = self.meter.create_counter(
      "kuzu_instance_health_checks_total",
      description="Health check attempts per instance",
      unit="1",
    )

    self.instance_health_status = self.meter.create_up_down_counter(
      "kuzu_instance_health_status",
      description="Current health status of instances (1=healthy, 0=unhealthy)",
      unit="1",
    )

    # Capacity metrics
    self.cluster_capacity = self.meter.create_observable_gauge(
      "kuzu_cluster_total_capacity",
      callbacks=[self._observe_cluster_capacity],
      description="Total database capacity across all instances",
      unit="databases",
    )

    self.cluster_utilization = self.meter.create_observable_gauge(
      "kuzu_cluster_utilization_percent",
      callbacks=[self._observe_cluster_utilization],
      description="Cluster utilization percentage",
      unit="%",
    )

    self.instance_database_count = self.meter.create_observable_gauge(
      "kuzu_instance_database_count",
      callbacks=[self._observe_instance_databases],
      description="Number of databases per instance",
      unit="databases",
    )

    # DynamoDB metrics
    self.dynamodb_operations = self.meter.create_counter(
      "kuzu_dynamodb_operations_total",
      description="DynamoDB operations by table and operation",
      unit="1",
    )

    self.dynamodb_throttles = self.meter.create_counter(
      "kuzu_dynamodb_throttles_total",
      description="DynamoDB throttled requests",
      unit="1",
    )

    # Lambda routing metrics
    self.lambda_invocations = self.meter.create_counter(
      "kuzu_lambda_router_invocations_total",
      description="Lambda router invocations",
      unit="1",
    )

    self.lambda_cold_starts = self.meter.create_counter(
      "kuzu_lambda_router_cold_starts_total",
      description="Lambda router cold starts",
      unit="1",
    )

    self.lambda_errors = self.meter.create_counter(
      "kuzu_lambda_router_errors_total",
      description="Lambda router errors by type",
      unit="1",
    )

    # Auto-scaling metrics
    self.scaling_events = self.meter.create_counter(
      "kuzu_autoscaling_events_total",
      description="Auto-scaling events by type",
      unit="1",
    )

    self.desired_capacity = self.meter.create_observable_gauge(
      "kuzu_autoscaling_desired_capacity",
      callbacks=[self._observe_desired_capacity],
      description="Desired capacity of auto-scaling groups",
      unit="instances",
    )

    # EBS snapshot metrics
    self.snapshot_count = self.meter.create_observable_gauge(
      "kuzu_ebs_snapshot_count",
      callbacks=[self._observe_snapshot_count],
      description="Number of EBS snapshots by type",
      unit="snapshots",
    )

    self.snapshot_size = self.meter.create_observable_gauge(
      "kuzu_ebs_snapshot_size_bytes",
      callbacks=[self._observe_snapshot_size],
      description="Total size of EBS snapshots",
      unit="By",
    )

  def record_allocation(
    self,
    graph_id: str,
    entity_id: str,
    success: bool,
    duration: float,
    instance_id: Optional[str] = None,
    error_type: Optional[str] = None,
  ):
    """Record database allocation metrics."""
    base_attrs = {
      "graph_id": graph_id,
      "entity_id": entity_id,
      "success": str(success),
    }

    if instance_id:
      base_attrs["instance_id"] = instance_id

    self.allocation_counter.add(1, base_attrs)
    self.allocation_duration.record(duration, base_attrs)

    if not success:
      failure_attrs = base_attrs.copy()
      failure_attrs["error_type"] = error_type or "unknown"
      self.allocation_failures.add(1, failure_attrs)

  def record_routing_request(
    self,
    graph_id: str,
    method: str,
    status_code: int,
    latency_ms: float,
    instance_id: Optional[str] = None,
    cache_hit: bool = False,
  ):
    """Record routing request metrics."""
    attrs = {
      "graph_id": graph_id,
      "method": method,
      "status_code": str(status_code),
      "cache_hit": str(cache_hit),
    }

    if instance_id:
      attrs["instance_id"] = instance_id

    self.routing_requests.add(1, attrs)
    self.routing_latency.record(latency_ms, attrs)

  def record_routing_error(
    self, graph_id: str, error_type: str, error_code: Optional[str] = None
  ):
    """Record routing error metrics."""
    attrs = {"graph_id": graph_id, "error_type": error_type}

    if error_code:
      attrs["error_code"] = error_code

    self.routing_errors.add(1, attrs)

  def record_health_check(
    self, instance_id: str, healthy: bool, response_time_ms: Optional[float] = None
  ):
    """Record instance health check metrics."""
    attrs = {"instance_id": instance_id, "healthy": str(healthy)}

    self.instance_health_checks.add(1, attrs)

    # Update health status (1 for healthy, -1 to set to 0 for unhealthy)
    if healthy:
      self.instance_health_status.add(1, {"instance_id": instance_id})
    else:
      self.instance_health_status.add(-1, {"instance_id": instance_id})

  def record_dynamodb_operation(
    self, table_name: str, operation: str, success: bool, throttled: bool = False
  ):
    """Record DynamoDB operation metrics."""
    attrs = {"table": table_name, "operation": operation, "success": str(success)}

    self.dynamodb_operations.add(1, attrs)

    if throttled:
      self.dynamodb_throttles.add(1, {"table": table_name})

  def record_lambda_invocation(
    self,
    cold_start: bool,
    success: bool,
    error_type: Optional[str] = None,
    duration_ms: Optional[float] = None,
  ):
    """Record Lambda router invocation metrics."""
    attrs = {"success": str(success)}

    self.lambda_invocations.add(1, attrs)

    if cold_start:
      self.lambda_cold_starts.add(1, {})

    if not success and error_type:
      self.lambda_errors.add(1, {"error_type": error_type})

  def record_scaling_event(
    self,
    asg_name: str,
    scaling_type: str,  # scale_up, scale_down
    old_capacity: int,
    new_capacity: int,
  ):
    """Record auto-scaling event metrics."""
    attrs = {
      "asg_name": asg_name,
      "scaling_type": scaling_type,
      "capacity_change": str(new_capacity - old_capacity),
    }

    self.scaling_events.add(1, attrs)

  # Observable callbacks
  def _observe_cluster_capacity(self, options: CallbackOptions) -> list[Observation]:
    """Observe total cluster capacity."""
    # This would query DynamoDB or CloudWatch in production
    # For now, return placeholder
    return [Observation(200, {"cluster": "entity-writers"})]

  def _observe_cluster_utilization(self, options: CallbackOptions) -> list[Observation]:
    """Observe cluster utilization percentage."""
    # This would calculate from DynamoDB data in production
    return [Observation(65.5, {"cluster": "entity-writers"})]

  def _observe_instance_databases(self, options: CallbackOptions) -> list[Observation]:
    """Observe database count per instance."""
    # This would query instance registry in production
    observations = []
    # Placeholder data
    observations.append(Observation(25, {"instance_id": "i-1234567890abcdef0"}))
    observations.append(Observation(30, {"instance_id": "i-0987654321fedcba0"}))
    return observations

  def _observe_desired_capacity(self, options: CallbackOptions) -> list[Observation]:
    """Observe auto-scaling group desired capacity."""
    # This would query ASG API in production
    return [Observation(3, {"asg_name": "prod-KuzuWriterASG"})]

  def _observe_snapshot_count(self, options: CallbackOptions) -> list[Observation]:
    """Observe EBS snapshot count."""
    # This would query EBS API in production
    observations = []
    observations.append(Observation(7, {"snapshot_type": "daily"}))
    observations.append(Observation(4, {"snapshot_type": "weekly"}))
    observations.append(Observation(3, {"snapshot_type": "monthly"}))
    return observations

  def _observe_snapshot_size(self, options: CallbackOptions) -> list[Observation]:
    """Observe total EBS snapshot size."""
    # This would calculate from EBS API in production
    return [Observation(1073741824000, {"environment": "prod"})]  # 1TB example


# Global metrics instance
_kuzu_metrics: Optional[KuzuClusterMetrics] = None


def get_kuzu_metrics() -> KuzuClusterMetrics:
  """Get or create the global Kuzu metrics instance."""
  global _kuzu_metrics
  if _kuzu_metrics is None:
    _kuzu_metrics = KuzuClusterMetrics()
  return _kuzu_metrics


@contextmanager
def kuzu_allocation_metrics(graph_id: str, entity_id: str):
  """Context manager for tracking database allocation metrics."""
  start_time = time.time()
  success = False
  instance_id = None
  error_type = None

  try:
    yield locals()  # Allows setting success, instance_id, error_type
    success = True
  except Exception as e:
    error_type = type(e).__name__
    raise
  finally:
    duration = time.time() - start_time
    metrics = get_kuzu_metrics()
    metrics.record_allocation(
      graph_id=graph_id,
      entity_id=entity_id,
      success=success,
      duration=duration,
      instance_id=instance_id,
      error_type=error_type,
    )


@contextmanager
def kuzu_routing_metrics(graph_id: str, method: str):
  """Context manager for tracking routing metrics."""
  start_time = time.time()
  status_code = 200
  instance_id = None
  cache_hit = False

  try:
    yield locals()  # Allows setting status_code, instance_id, cache_hit
  except Exception as e:
    status_code = 500
    metrics = get_kuzu_metrics()
    metrics.record_routing_error(
      graph_id=graph_id, error_type=type(e).__name__, error_code=str(e)
    )
    raise
  finally:
    latency_ms = (time.time() - start_time) * 1000
    metrics = get_kuzu_metrics()
    metrics.record_routing_request(
      graph_id=graph_id,
      method=method,
      status_code=status_code,
      latency_ms=latency_ms,
      instance_id=instance_id,
      cache_hit=cache_hit,
    )
