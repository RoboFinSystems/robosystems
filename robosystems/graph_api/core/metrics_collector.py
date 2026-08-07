"""Disk, query and system metrics for the LadybugDB databases on this instance.

Exposed through OpenTelemetry for Prometheus. When OTEL_ENABLED is off, the
no-op meter below stands in so callers need no conditionals.
"""

import logging
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from robosystems.config import env

logger = logging.getLogger(__name__)


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


class LadybugMetricsCollector:
  """Collects and exposes metrics for LadybugDB databases."""

  def __init__(self, base_path: str, node_type: str = "entity_writer"):
    """Collect over the databases under ``base_path``, tagging every metric
    with ``node_type`` (writer, shared_master, shared_replica)."""
    self.base_path = Path(base_path)
    self.node_type = node_type

    # Query counts for the current hour; reset by record_query.
    self._query_counts: dict[str, int] = {}
    self._query_count_reset_time = time.time()

    meter = metrics.get_meter(__name__, "1.0.0")

    # Observable-gauge callbacks only work against a real meter.
    callbacks = None
    if env.OTEL_ENABLED and not isinstance(meter, NoOpMeter):
      from typing import cast

      from opentelemetry.metrics import CallbackT

      callbacks = cast(list[CallbackT], [self._observe_database_metrics])

    self.database_size_bytes = meter.create_observable_gauge(
      name="lbug_database_size_bytes",
      callbacks=callbacks,
      description="Size of individual LadybugDB databases in bytes",
      unit="bytes",
    )

    self.total_disk_usage_bytes = meter.create_observable_gauge(
      name="lbug_total_disk_usage_bytes",
      callbacks=callbacks,
      description="Total disk usage for all LadybugDB databases",
      unit="bytes",
    )

    self.database_count = meter.create_observable_gauge(
      name="lbug_database_count",
      callbacks=callbacks,
      description="Number of LadybugDB databases on this instance",
      unit="1",
    )

    self.query_count = (
      meter.create_counter(
        name="lbug_query_count",
        description="Number of queries executed",
        unit="1",
      )
      or NoOpCounter()
    )

    self.query_duration = (
      meter.create_histogram(
        name="lbug_query_duration_ms",
        description="Query execution duration in milliseconds",
        unit="ms",
      )
      or NoOpHistogram()
    )

    self.database_operation_count = (
      meter.create_counter(
        name="lbug_database_operation_count",
        description="Number of database operations (create, delete, etc)",
        unit="1",
      )
      or NoOpCounter()
    )

    # Sizing walks every file on the data volume, so it is cached.
    self._size_cache: dict[str, int] = {}
    self._cache_timestamp: float | None = None
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

  def _get_database_sizes(self) -> dict[str, int]:
    """Size every database on this instance, refreshing at most every
    ``_cache_ttl`` seconds. A scan failure falls back to the stale cache."""
    current_time = time.time()

    if self._cache_timestamp and current_time - self._cache_timestamp < self._cache_ttl:
      return self._size_cache

    logger.debug("Refreshing database size cache")
    new_cache = {}

    try:
      for item in self.base_path.iterdir():
        if item.is_dir():
          db_name = item.name
          db_size = self._get_directory_size(item)
          new_cache[db_name] = db_size
        elif item.is_file() and item.suffix == ".lbug":
          db_name = item.stem
          db_size = item.stat().st_size
          new_cache[db_name] = db_size

      self._size_cache = new_cache
      self._cache_timestamp = current_time

    except Exception as e:
      logger.error(f"Error scanning database directory: {e}")
      if self._size_cache:
        return self._size_cache
      return {}

    return new_cache

  def _observe_database_metrics(self, options):
    """Observable-gauge callback: per-database size, total disk usage, count."""
    try:
      database_sizes = self._get_database_sizes()

      for db_name, size_bytes in database_sizes.items():
        yield OTelObservation(
          size_bytes, {"database": db_name, "node_type": self.node_type}
        )

      total_size = sum(database_sizes.values())
      yield OTelObservation(total_size, {"node_type": self.node_type})

      yield OTelObservation(len(database_sizes), {"node_type": self.node_type})

    except Exception as e:
      logger.error(f"Error collecting database metrics: {e}")
      yield OTelObservation(0, {"database": "error", "node_type": self.node_type})

  def record_query(self, database: str, duration_ms: float, success: bool = True):
    """Record a query execution, both to OTEL and to the hourly local tally."""
    if hasattr(self.query_count, "add"):
      self.query_count.add(
        1, {"database": database, "node_type": self.node_type, "success": str(success)}
      )

    if success:
      current_time = time.time()
      if current_time - self._query_count_reset_time > 3600:
        self._query_counts = {}
        self._query_count_reset_time = current_time

      self._query_counts[database] = self._query_counts.get(database, 0) + 1

    if success and hasattr(self.query_duration, "record"):
      self.query_duration.record(
        duration_ms, {"database": database, "node_type": self.node_type}
      )

  def record_database_operation(
    self, operation: str, database: str, success: bool = True
  ):
    """Record a database operation (create, delete, etc)."""
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

  async def get_database_metrics_for_usage(self) -> list[dict]:
    """One row per database, shaped for the usage/billing collector."""
    database_sizes = self._get_database_sizes()

    metrics_list = []
    timestamp = datetime.now(UTC)

    for db_name, size_bytes in database_sizes.items():
      metrics_list.append(
        {
          "database_name": db_name,
          "size_bytes": size_bytes,
          "size_gb": size_bytes / (1024**3),
          "query_count": self._query_counts.get(db_name, 0),
          "timestamp": timestamp.isoformat(),
          "node_type": self.node_type,
        }
      )

    return metrics_list

  def collect_system_metrics(self) -> dict[str, Any]:
    """CPU, memory, and disk usage for this instance's data volume."""

    import psutil

    data_path = str(self.base_path)
    disk_usage = psutil.disk_usage(data_path)
    disk_metrics = {
      "total_gb": disk_usage.total / (1024**3),
      "used_gb": disk_usage.used / (1024**3),
      "free_gb": disk_usage.free / (1024**3),
      "usage_percent": disk_usage.percent,
      "mount_point": data_path,
    }

    memory = psutil.virtual_memory()
    cpu_percent = psutil.cpu_percent(interval=0.1)

    return {
      "timestamp": datetime.now(UTC).isoformat(),
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

  def collect_database_metrics(self) -> dict[str, Any]:
    """Count, total size, and per-database size/query counts."""
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

  def collect_database_metrics_cached(self) -> dict[str, Any]:
    """Same shape as ``collect_database_metrics`` but never rescans disk.

    The fallback when a full collection times out under heavy I/O.
    """
    database_sizes = self._size_cache

    total_size = sum(database_sizes.values())

    return {
      "count": len(database_sizes),
      "total_size_gb": total_size / (1024**3),
      "cached": True,
      "databases": {
        db_name: {
          "size_bytes": size_bytes,
          "size_gb": size_bytes / (1024**3),
          "query_count": self._query_counts.get(db_name, 0),
        }
        for db_name, size_bytes in database_sizes.items()
      },
    }

  def get_query_metrics(self) -> dict[str, Any]:
    """Query counts for the current hour, with the hour's start time."""
    total_queries = sum(self._query_counts.values())

    return {
      "total_queries": total_queries,
      "queries_by_database": self._query_counts.copy(),
      "reset_time": datetime.fromtimestamp(
        self._query_count_reset_time, tz=UTC
      ).isoformat(),
    }

  async def collect_ingestion_metrics(self) -> dict[str, Any]:
    """Ingestion queue metrics — a zeroed placeholder, since ingestion is
    orchestrated by Dagster rather than queued on the instance."""
    return {
      "queue_depth": 0,
      "active_tasks": 0,
      "processing_rate": 0,
    }
