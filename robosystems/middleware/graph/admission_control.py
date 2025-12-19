"""
Admission control and load shedding for query queue.

Provides intelligent request filtering based on system resources
and load to prevent overload and maintain system stability.
"""

import random
import time
from dataclasses import dataclass
from enum import Enum

import psutil

from robosystems.logger import logger


class AdmissionDecision(str, Enum):
  """Admission control decisions."""

  ACCEPT = "accept"
  ADMIT = "accept"  # Alias for tool queue compatibility
  REJECT_MEMORY = "reject_memory"
  REJECT_CPU = "reject_cpu"
  REJECT_QUEUE = "reject_queue"
  REJECT_LOAD_SHED = "reject_load_shed"


@dataclass
class SystemResources:
  """Current system resource utilization."""

  memory_percent: float
  cpu_percent: float
  queue_depth: int
  active_queries: int
  load_average: float

  @property
  def is_healthy(self) -> bool:
    """Check if system resources are within healthy limits."""
    return (
      self.memory_percent < 80 and self.cpu_percent < 85 and self.load_average < 2.0
    )


class AdmissionController:
  """
  Controls admission of new queries based on system resources.

  Features:
  - CPU and memory threshold checks
  - Probabilistic load shedding
  - Gradual backpressure
  - Resource monitoring
  """

  def __init__(
    self,
    memory_threshold: float = 85.0,
    cpu_threshold: float = 90.0,
    queue_threshold: float = 0.8,
    check_interval: float = 1.0,
    load_shedding_enabled: bool = True,
  ):
    """
    Initialize admission controller.

    Args:
        memory_threshold: Max memory usage percent
        cpu_threshold: Max CPU usage percent
        queue_threshold: Queue depth threshold (0-1)
        check_interval: Seconds between resource checks
        load_shedding_enabled: Enable admission control and load shedding
    """
    self.memory_threshold = memory_threshold
    self.cpu_threshold = cpu_threshold
    self.queue_threshold = queue_threshold
    self.check_interval = check_interval
    self.load_shedding_enabled = load_shedding_enabled

    # Cached resource data
    self._last_check = 0.0
    self._cached_resources: SystemResources | None = None

    # Load shedding state
    self._rejection_rate = 0.0
    self._shed_start_time: float | None = None

  def check_admission(
    self,
    queue_depth: int,
    max_queue_size: int,
    active_queries: int,
    priority: int = 5,
  ) -> tuple[AdmissionDecision, str | None]:
    """
    Check if a new query should be admitted.

    Args:
        queue_depth: Current queue size
        max_queue_size: Maximum queue size
        active_queries: Currently executing queries
        priority: Query priority (1-10)

    Returns:
        (decision, rejection_reason)
    """
    # If load shedding is disabled, always accept
    if not self.load_shedding_enabled:
      return AdmissionDecision.ACCEPT, None

    # Get current resources
    resources = self._get_system_resources(queue_depth, active_queries)

    # Check hard limits first
    if resources.memory_percent > self.memory_threshold:
      logger.warning(
        f"Rejecting query: memory usage {resources.memory_percent:.1f}% "
        f"exceeds threshold {self.memory_threshold}%"
      )
      return (
        AdmissionDecision.REJECT_MEMORY,
        f"System memory usage too high: {resources.memory_percent:.1f}%",
      )

    if resources.cpu_percent > self.cpu_threshold:
      logger.warning(
        f"Rejecting query: CPU usage {resources.cpu_percent:.1f}% "
        f"exceeds threshold {self.cpu_threshold}%"
      )
      return (
        AdmissionDecision.REJECT_CPU,
        f"System CPU usage too high: {resources.cpu_percent:.1f}%",
      )

    # Check queue depth
    queue_ratio = queue_depth / max_queue_size if max_queue_size > 0 else 0
    if queue_ratio > self.queue_threshold:
      # Apply probabilistic rejection based on how full the queue is
      if queue_ratio > 0.95:
        rejection_prob = 0.9  # Reject 90% when almost full
      elif queue_ratio > 0.9:
        rejection_prob = 0.7  # Reject 70% when very full
      else:
        rejection_prob = 0.5  # Reject 50% when above threshold

      # Higher priority queries have lower rejection probability
      rejection_prob *= (11 - priority) / 10

      if random.random() < rejection_prob:
        logger.info(
          f"Load shedding query: queue {queue_ratio:.1%} full, "
          f"rejection probability {rejection_prob:.1%}"
        )
        return (
          AdmissionDecision.REJECT_LOAD_SHED,
          f"System under heavy load. Queue {queue_ratio:.0%} full.",
        )

    # Check gradual degradation based on resource pressure
    pressure_score = self._calculate_pressure_score(resources, queue_ratio)
    if pressure_score > 0.7:
      # Probabilistic rejection based on pressure
      rejection_prob = (pressure_score - 0.7) * 2  # 0-60% rejection
      rejection_prob *= (11 - priority) / 10  # Priority adjustment

      if random.random() < rejection_prob:
        logger.info(
          f"Pressure-based load shedding: score {pressure_score:.2f}, "
          f"rejection probability {rejection_prob:.1%}"
        )
        return (
          AdmissionDecision.REJECT_LOAD_SHED,
          "System under resource pressure. Please retry.",
        )

    # Update load shedding state
    if pressure_score > 0.8 and self._shed_start_time is None:
      self._shed_start_time = time.time()
      logger.warning(f"Entering load shedding mode: pressure {pressure_score:.2f}")
    elif pressure_score < 0.6 and self._shed_start_time is not None:
      duration = time.time() - self._shed_start_time
      logger.info(f"Exiting load shedding mode after {duration:.1f} seconds")
      self._shed_start_time = None

    return (AdmissionDecision.ACCEPT, None)

  async def should_admit_request(self) -> AdmissionDecision:
    """Simple admission check for tool queue (async version)."""
    # Use cached resources for quick check
    resources = self._get_system_resources(0, 0)  # No queue info available

    # Check resource thresholds only
    if resources.memory_percent > self.memory_threshold:
      return AdmissionDecision.REJECT_MEMORY

    if resources.cpu_percent > self.cpu_threshold:
      return AdmissionDecision.REJECT_CPU

    # Check general system pressure
    pressure_score = self._calculate_pressure_score(resources, 0.0)
    if pressure_score > 0.8:
      return AdmissionDecision.REJECT_LOAD_SHED

    return AdmissionDecision.ADMIT

  def _get_system_resources(
    self, queue_depth: int, active_queries: int
  ) -> SystemResources:
    """Get current system resources with caching."""
    now = time.time()

    # Use cached data if recent
    if self._cached_resources and now - self._last_check < self.check_interval:
      # Update queue metrics (always current)
      self._cached_resources.queue_depth = queue_depth
      self._cached_resources.active_queries = active_queries
      return self._cached_resources

    # Collect fresh resource data
    try:
      # Memory usage
      memory = psutil.virtual_memory()
      memory_percent = memory.percent

      # CPU usage (interval=0.1 for quick check)
      cpu_percent = psutil.cpu_percent(interval=0.1)

      # Load average (1 minute)
      load_avg = psutil.getloadavg()[0]

    except Exception as e:
      logger.error(f"Failed to get system resources: {e}")
      # Use conservative defaults on error
      memory_percent = 50.0
      cpu_percent = 50.0
      load_avg = 1.0

    self._cached_resources = SystemResources(
      memory_percent=memory_percent,
      cpu_percent=cpu_percent,
      queue_depth=queue_depth,
      active_queries=active_queries,
      load_average=load_avg,
    )
    self._last_check = now

    return self._cached_resources

  def _calculate_pressure_score(
    self, resources: SystemResources, queue_ratio: float
  ) -> float:
    """
    Calculate overall system pressure score (0-1).

    Combines multiple factors into a single pressure metric.
    """
    # Normalize each metric to 0-1 range
    memory_pressure = min(resources.memory_percent / 100, 1.0)
    cpu_pressure = min(resources.cpu_percent / 100, 1.0)
    queue_pressure = queue_ratio
    load_pressure = min(resources.load_average / 4.0, 1.0)  # 4 cores assumed

    # Weighted average (memory most important)
    pressure = (
      memory_pressure * 0.4
      + cpu_pressure * 0.3
      + queue_pressure * 0.2
      + load_pressure * 0.1
    )

    return pressure

  def get_health_status(
    self,
    queue_depth: int,
    max_queue_size: int,
    active_queries: int,
  ) -> dict:
    """Get detailed health status for monitoring."""
    resources = self._get_system_resources(queue_depth, active_queries)
    queue_ratio = queue_depth / max_queue_size if max_queue_size > 0 else 0
    pressure_score = self._calculate_pressure_score(resources, queue_ratio)

    # Determine health state
    if resources.is_healthy and pressure_score < 0.5:
      health_state = "healthy"
    elif pressure_score < 0.7:
      health_state = "degraded"
    else:
      health_state = "unhealthy"

    return {
      "state": health_state,
      "pressure_score": round(pressure_score, 3),
      "resources": {
        "memory_percent": round(resources.memory_percent, 1),
        "cpu_percent": round(resources.cpu_percent, 1),
        "load_average": round(resources.load_average, 2),
      },
      "queue": {
        "depth": queue_depth,
        "ratio": round(queue_ratio, 3),
        "active_queries": active_queries,
      },
      "load_shedding": {
        "active": self._shed_start_time is not None,
        "duration_seconds": (
          time.time() - self._shed_start_time if self._shed_start_time else 0
        ),
      },
      "thresholds": {
        "memory": self.memory_threshold,
        "cpu": self.cpu_threshold,
        "queue": self.queue_threshold,
      },
    }


# Global admission controller instance
_admission_controller: AdmissionController | None = None


def get_admission_controller() -> AdmissionController:
  """Get the global admission controller instance."""
  global _admission_controller
  if _admission_controller is None:
    # Load configuration
    from robosystems.config.query_queue import QueryQueueConfig

    _admission_controller = AdmissionController(
      **QueryQueueConfig.get_admission_config()
    )
  return _admission_controller
