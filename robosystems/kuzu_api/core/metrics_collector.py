"""
Kuzu Database Metrics Collector

This module provides metrics collection for Kuzu databases, including:
- Database disk usage
- Database count per instance
- Query statistics
- Health metrics

The metrics are exposed via OpenTelemetry for Prometheus consumption.
"""

import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
import logging

from robosystems.config import env

# OpenTelemetry imports - conditional based on OTEL_ENABLED


# Create no-op metrics if OTEL is disabled
class NoOpMeter:
  """No-op meter for when metrics are disabled."""

  def create_observable_gauge(self, name, callbacks=None, **kwargs):
    return None

  def create_histogram(self, name, **kwargs):
    return None

  def create_counter(self, name, **kwargs):
    return None


class NoOpMetrics:
  """No-op metrics module for when metrics are disabled."""

  def get_meter(self, name, version):
    return NoOpMeter()


class Observation:
  """Dummy Observation class for when metrics are disabled."""

  def __init__(self, value, attributes=None):
    self.value = value
    self.attributes = attributes or {}


class NoOpCounter:
  """No-op counter for when metrics are disabled."""

  def add(self, *args, **kwargs):
    pass


class NoOpHistogram:
  """No-op histogram for when metrics are disabled."""

  def record(self, *args, **kwargs):
    pass


# Only import real metrics if OTEL is enabled
if env.OTEL_ENABLED:
  try:
    from opentelemetry import metrics
    from opentelemetry.metrics import Observation as OTelObservation
  except ImportError:
    metrics = NoOpMetrics()
    OTelObservation = Observation
else:
  metrics = NoOpMetrics()
  OTelObservation = Observation

logger = logging.getLogger(__name__)


class KuzuMetricsCollector:
  """Collects and exposes metrics for Kuzu databases."""

  def __init__(self, base_path: str, node_type: str = "entity_writer"):
    """
    Initialize the metrics collector.

    Args:
        base_path: Base directory where Kuzu databases are stored
        node_type: Type of node (writer, shared_master, shared_replica)
    """
    self.base_path = Path(base_path)
    self.node_type = node_type

    # Track query counts for the current hour
    self._query_counts: Dict[str, int] = {}
    self._query_count_reset_time = time.time()

    # Get meter from OpenTelemetry
    meter = metrics.get_meter(__name__, "1.0.0")

    # Create metrics instruments with callbacks (these will be no-ops if OTEL is disabled)
    # Only register callbacks if we have real OpenTelemetry available
    callbacks = None
    if env.OTEL_ENABLED and not isinstance(meter, NoOpMeter):
      # Use type casting to ensure the callback has the correct type for OpenTelemetry
      from typing import cast, List
      from opentelemetry.metrics import CallbackT

      callbacks = cast(List[CallbackT], [self._observe_database_metrics])

    self.database_size_bytes = meter.create_observable_gauge(
      name="kuzu_database_size_bytes",
      callbacks=callbacks,
      description="Size of individual Kuzu databases in bytes",
      unit="bytes",
    )

    self.total_disk_usage_bytes = meter.create_observable_gauge(
      name="kuzu_total_disk_usage_bytes",
      callbacks=callbacks,
      description="Total disk usage for all Kuzu databases",
      unit="bytes",
    )

    self.database_count = meter.create_observable_gauge(
      name="kuzu_database_count",
      callbacks=callbacks,
      description="Number of Kuzu databases on this instance",
      unit="1",
    )

    # Create no-op counters if meter is no-op
    self.query_count = (
      meter.create_counter(
        name="kuzu_query_count",
        description="Number of queries executed",
        unit="1",
      )
      or NoOpCounter()
    )

    self.query_duration = (
      meter.create_histogram(
        name="kuzu_query_duration_ms",
        description="Query execution duration in milliseconds",
        unit="ms",
      )
      or NoOpHistogram()
    )

    self.database_operation_count = (
      meter.create_counter(
        name="kuzu_database_operation_count",
        description="Number of database operations (create, delete, etc)",
        unit="1",
      )
      or NoOpCounter()
    )

    # Cache for database sizes to avoid excessive disk I/O
    self._size_cache: Dict[str, int] = {}
    self._cache_timestamp: Optional[float] = None
    self._cache_ttl = 300  # 5 minutes

  def _get_directory_size(self, path: Path) -> int:
    """Calculate the total size of a directory recursively."""
    total_size = 0
    try:
      if path.is_file():
        return path.stat().st_size
      elif path.is_dir():
        for item in path.rglob("*"):
          if item.is_file():
            total_size += item.stat().st_size
    except Exception as e:
      logger.warning(f"Error calculating size for {path}: {e}")
    return total_size

  def _get_database_sizes(self) -> Dict[str, int]:
    """Get sizes of all databases, using cache if available."""
    current_time = time.time()

    # Check if cache is still valid
    if self._cache_timestamp and current_time - self._cache_timestamp < self._cache_ttl:
      return self._size_cache

    # Refresh cache
    logger.debug("Refreshing database size cache")
    new_cache = {}

    try:
      # List all items in base path
      for item in self.base_path.iterdir():
        if item.is_dir():
          # It's a directory-based database
          db_name = item.name
          db_size = self._get_directory_size(item)
          new_cache[db_name] = db_size
        elif item.is_file() and item.suffix == ".kuzu":
          # It's a file-based database (Kuzu 0.11.x+ format)
          db_name = item.stem
          db_size = item.stat().st_size
          new_cache[db_name] = db_size

      self._size_cache = new_cache
      self._cache_timestamp = current_time

    except Exception as e:
      logger.error(f"Error scanning database directory: {e}")
      # Return previous cache if available
      if self._size_cache:
        return self._size_cache
      return {}

    return new_cache

  def _observe_database_metrics(self, options):
    """Callback for observable metrics."""
    try:
      database_sizes = self._get_database_sizes()

      # Emit individual database sizes
      for db_name, size_bytes in database_sizes.items():
        yield OTelObservation(
          size_bytes, {"database": db_name, "node_type": self.node_type}
        )

      # Emit total disk usage
      total_size = sum(database_sizes.values())
      yield OTelObservation(total_size, {"node_type": self.node_type})

      # Emit database count
      yield OTelObservation(len(database_sizes), {"node_type": self.node_type})

    except Exception as e:
      logger.error(f"Error collecting database metrics: {e}")
      # Yield empty observations when there's an error
      yield OTelObservation(0, {"database": "error", "node_type": self.node_type})

  def record_query(self, database: str, duration_ms: float, success: bool = True):
    """Record a query execution."""
    # Only record if we have a real metrics counter (not no-op)
    if hasattr(self.query_count, "add"):
      self.query_count.add(
        1, {"database": database, "node_type": self.node_type, "success": str(success)}
      )

    # Track query counts for hourly reporting
    if success:
      current_time = time.time()
      # Reset counts every hour
      if current_time - self._query_count_reset_time > 3600:
        self._query_counts = {}
        self._query_count_reset_time = current_time

      # Increment counter for this database
      self._query_counts[database] = self._query_counts.get(database, 0) + 1

    if success and hasattr(self.query_duration, "record"):
      self.query_duration.record(
        duration_ms, {"database": database, "node_type": self.node_type}
      )

  def record_database_operation(
    self, operation: str, database: str, success: bool = True
  ):
    """Record a database operation (create, delete, etc)."""
    # Only record if we have a real metrics counter (not no-op)
    if hasattr(self.database_operation_count, "add"):
      self.database_operation_count.add(
        1,
        {
          "operation": operation,
          "database": database,
          "node_type": self.node_type,
          "success": str(success),
        },
      )

  async def get_database_metrics_for_usage(self) -> List[Dict]:
    """
    Get database metrics formatted for usage tracking.

    Returns:
        List of dictionaries with database metrics
    """
    database_sizes = self._get_database_sizes()

    metrics_list = []
    timestamp = datetime.now(timezone.utc)

    for db_name, size_bytes in database_sizes.items():
      metrics_list.append(
        {
          "database_name": db_name,
          "size_bytes": size_bytes,
          "size_gb": size_bytes / (1024**3),  # Convert to GB
          "query_count": self._query_counts.get(db_name, 0),  # Include query count
          "timestamp": timestamp.isoformat(),
          "node_type": self.node_type,
        }
      )

    return metrics_list

  def collect_system_metrics(self) -> Dict[str, Any]:
    """
    Collect system-level metrics including CPU, memory, and disk usage.

    Returns:
        Dictionary with system metrics
    """
    import psutil
    import os

    # Get disk usage for the data volume
    data_path = "/mnt/kuzu-data"
    if os.path.exists(data_path):
      disk_usage = psutil.disk_usage(data_path)
      disk_metrics = {
        "total_gb": disk_usage.total / (1024**3),
        "used_gb": disk_usage.used / (1024**3),
        "free_gb": disk_usage.free / (1024**3),
        "usage_percent": disk_usage.percent,
        "mount_point": data_path,
      }
    else:
      # Fallback to base path if /mnt/kuzu-data doesn't exist
      disk_usage = psutil.disk_usage(str(self.base_path))
      disk_metrics = {
        "total_gb": disk_usage.total / (1024**3),
        "used_gb": disk_usage.used / (1024**3),
        "free_gb": disk_usage.free / (1024**3),
        "usage_percent": disk_usage.percent,
        "mount_point": str(self.base_path),
      }

    # Get memory usage
    memory = psutil.virtual_memory()

    # Get CPU usage
    cpu_percent = psutil.cpu_percent(interval=0.1)

    return {
      "timestamp": datetime.now(timezone.utc).isoformat(),
      "cpu": {
        "usage_percent": cpu_percent,
        "count": psutil.cpu_count(),
      },
      "memory": {
        "total_gb": memory.total / (1024**3),
        "used_gb": memory.used / (1024**3),
        "available_gb": memory.available / (1024**3),
        "usage_percent": memory.percent,
      },
      "disk": disk_metrics,
      "volumes": {
        "data_volume": {
          "volume_size_gb": disk_metrics["total_gb"],
          "used_gb": disk_metrics["used_gb"],
          "free_gb": disk_metrics["free_gb"],
          "usage_percent": disk_metrics["usage_percent"]
          / 100,  # Convert to decimal for Lambda
          "mount_point": disk_metrics["mount_point"],
        }
      },
    }

  def collect_database_metrics(self) -> Dict[str, Any]:
    """
    Collect metrics for all databases.

    Returns:
        Dictionary with database metrics
    """
    database_sizes = self._get_database_sizes()

    total_size = sum(database_sizes.values())

    return {
      "count": len(database_sizes),
      "total_size_gb": total_size / (1024**3),
      "databases": {
        db_name: {
          "size_bytes": size_bytes,
          "size_gb": size_bytes / (1024**3),
          "query_count": self._query_counts.get(db_name, 0),
        }
        for db_name, size_bytes in database_sizes.items()
      },
    }

  def get_query_metrics(self) -> Dict[str, Any]:
    """
    Get query execution metrics.

    Returns:
        Dictionary with query metrics
    """
    total_queries = sum(self._query_counts.values())

    return {
      "total_queries": total_queries,
      "queries_by_database": self._query_counts.copy(),
      "reset_time": datetime.fromtimestamp(
        self._query_count_reset_time, tz=timezone.utc
      ).isoformat(),
    }

  async def collect_ingestion_metrics(self) -> Dict[str, Any]:
    """
    Collect ingestion queue metrics.

    Returns:
        Dictionary with ingestion metrics
    """
    # For now, return empty metrics as ingestion is handled elsewhere
    return {
      "queue_depth": 0,
      "active_tasks": 0,
      "processing_rate": 0,
    }
