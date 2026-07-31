"""
Admission control for Graph API server.

Monitors system resources and rejects requests when the server is overloaded,
preventing OOM kills and maintaining service stability.
"""

import time
from enum import Enum
from pathlib import Path

import psutil

from robosystems.logger import logger

# Cgroup v2 unified hierarchy as seen from inside the container. Absent on
# dev machines and bare-metal hosts; tests point this at a fixture directory.
_CGROUP_DIR = Path("/sys/fs/cgroup")


def _read_cgroup_limit_bytes() -> int | None:
  """Read the container's cgroup v2 memory limit, or None if unlimited/absent."""
  try:
    raw = (_CGROUP_DIR / "memory.max").read_text().strip()
  except (FileNotFoundError, NotADirectoryError):
    return None
  if raw == "max":
    return None
  return int(raw)


class AdmissionDecision(str, Enum):
  """Admission control decisions for Graph API."""

  ACCEPT = "accept"
  REJECT_MEMORY = "reject_memory"
  REJECT_CPU = "reject_cpu"
  REJECT_CONNECTIONS = "reject_connections"


class LadybugAdmissionController:
  """
  Admission controller for Graph API server.

  Critical - prevents server crashes that would affect
  all databases on the instance.
  """

  def __init__(
    self,
    memory_threshold: float = 85.0,  # Reporting only - see min_available_mb for the gate
    min_available_mb: float = 1024.0,
    cpu_threshold: float = 95.0,
    max_connections_per_db: int = 10,
    check_interval: float = 1.0,
  ):
    """
    Initialize LadybugDB admission controller.

    Args:
        memory_threshold: Memory usage percent at which the node reports
            pressure. Not a rejection gate - a buffer pool is meant to fill,
            so percent of total conflates that fixed allocation with the
            query working set. See min_available_mb.
        min_available_mb: Reject when free memory falls below this many MB.
            Absolute rather than proportional, so it stays correct when the
            buffer pool or the instance size changes. Free memory is the
            tighter of host-available and headroom under the container's
            cgroup memory limit, since whichever binds first is the one
            that kills the process.
        cpu_threshold: Max CPU usage percent before rejecting
        max_connections_per_db: Max concurrent connections per database
        check_interval: Seconds between resource checks (cached)
    """
    self.memory_threshold = memory_threshold
    self.min_available_mb = min_available_mb
    self.cpu_threshold = cpu_threshold
    self.max_connections_per_db = max_connections_per_db
    self.check_interval = check_interval

    # Cached resource data to avoid frequent psutil calls
    self._last_check = 0.0
    self._cached_memory = 0.0
    self._cached_available_mb = float("inf")
    self._cached_cgroup_available_mb: float | None = None
    self._cached_cpu = 0.0

    # Connection tracking
    self._connections_per_db: dict[str, int] = {}

    try:
      limit_bytes = _read_cgroup_limit_bytes()
    except Exception as e:
      logger.warning(f"Failed to read cgroup memory limit: {e}")
      limit_bytes = None
    self.cgroup_limit_mb = (
      limit_bytes / (1024 * 1024) if limit_bytes is not None else None
    )

    cgroup_desc = (
      f"{self.cgroup_limit_mb:.0f}MB" if self.cgroup_limit_mb is not None else "none"
    )
    logger.info(
      f"LadybugAdmissionController initialized - Min available: {min_available_mb}MB, "
      f"Cgroup memory limit: {cgroup_desc}, "
      f"Memory report threshold: {memory_threshold}%, "
      f"CPU: {cpu_threshold}%, Max connections/DB: {max_connections_per_db}"
    )

  def _cgroup_available_mb(self) -> float | None:
    """Headroom before the container's cgroup memory limit binds.

    psutil reads host /proc, but a Docker container is killed by its own
    memory cgroup long before the host runs dry, so the gate must measure
    distance to whichever limit is tighter. Returns None when no cgroup v2
    memory limit applies (dev machines, unlimited containers).

    File cache counts toward memory.current but the kernel reclaims it
    before OOM-killing, so it is headroom, not pressure - without adding it
    back a warm node would latch into rejection just like the old percent
    gate did.
    """
    limit = _read_cgroup_limit_bytes()
    if limit is None:
      return None
    current = int((_CGROUP_DIR / "memory.current").read_text().strip())
    reclaimable = 0
    for line in (_CGROUP_DIR / "memory.stat").read_text().splitlines():
      key, _, value = line.partition(" ")
      if key in ("inactive_file", "active_file"):
        reclaimable += int(value)
    return max(0.0, (limit - current + reclaimable) / (1024 * 1024))

  def _update_resource_cache(self) -> None:
    """Update cached resource metrics if stale."""
    now = time.time()
    if now - self._last_check >= self.check_interval:
      try:
        memory = psutil.virtual_memory()
        available_mb = memory.available / (1024 * 1024)
        cgroup_available_mb = self._cgroup_available_mb()
        if cgroup_available_mb is not None:
          available_mb = min(available_mb, cgroup_available_mb)
        self._cached_memory = memory.percent
        self._cached_available_mb = available_mb
        self._cached_cgroup_available_mb = cgroup_available_mb
        self._cached_cpu = psutil.cpu_percent(interval=0.1)
        self._last_check = now
      except Exception as e:
        logger.error(f"Failed to get system resources: {e}")
        # Fail closed on error: assume no headroom rather than admit blindly
        self._cached_memory = 80.0
        self._cached_available_mb = 0.0
        self._cached_cgroup_available_mb = 0.0
        self._cached_cpu = 80.0

  def check_admission(
    self, database_name: str, operation_type: str = "query"
  ) -> tuple[AdmissionDecision, str | None]:
    """
    Check if a new request should be admitted.

    Args:
        database_name: Database the request targets
        operation_type: Type of operation (query, ingestion, backup)

    Returns:
        Tuple of (decision, rejection_reason)
    """
    # Update cached metrics
    self._update_resource_cache()

    # Check free memory headroom. Gated on absolute available memory, not
    # percent used: the buffer pool is a fixed allocation that fills by design,
    # so a warm node legitimately sits high on percent-used while still having
    # room to serve. Available is the tighter of host memory and the
    # container's cgroup limit - the memcg cap binds first under Docker.
    if self._cached_available_mb < self.min_available_mb:
      reason = (
        f"Insufficient memory headroom: {self._cached_available_mb:.0f}MB available "
        f"(minimum: {self.min_available_mb:.0f}MB)"
      )
      logger.warning(f"Rejecting request for {database_name}: {reason}")
      return AdmissionDecision.REJECT_MEMORY, reason

    # Check CPU threshold (more lenient for queries)
    cpu_limit = self.cpu_threshold
    if operation_type == "ingestion":
      cpu_limit = self.cpu_threshold - 10  # More strict for heavy operations

    if self._cached_cpu > cpu_limit:
      reason = f"CPU usage too high: {self._cached_cpu:.1f}% (threshold: {cpu_limit}%)"
      logger.warning(f"Rejecting request for {database_name}: {reason}")
      return AdmissionDecision.REJECT_CPU, reason

    # Check connection limits per database
    current_connections = self._connections_per_db.get(database_name, 0)
    if current_connections >= self.max_connections_per_db:
      reason = (
        f"Too many connections to {database_name}: {current_connections} "
        f"(max: {self.max_connections_per_db})"
      )
      logger.warning(f"Rejecting request: {reason}")
      return AdmissionDecision.REJECT_CONNECTIONS, reason

    # Accept the request
    return AdmissionDecision.ACCEPT, None

  def register_connection(self, database_name: str) -> None:
    """Register a new active connection to a database."""
    self._connections_per_db[database_name] = (
      self._connections_per_db.get(database_name, 0) + 1
    )
    logger.debug(
      f"Connection registered for {database_name}. "
      f"Total: {self._connections_per_db[database_name]}"
    )

  def release_connection(self, database_name: str) -> None:
    """Release a connection from a database."""
    if database_name in self._connections_per_db:
      self._connections_per_db[database_name] = max(
        0, self._connections_per_db[database_name] - 1
      )
      logger.debug(
        f"Connection released for {database_name}. "
        f"Remaining: {self._connections_per_db[database_name]}"
      )

  def get_metrics(self) -> dict:
    """Get current admission control metrics."""
    self._update_resource_cache()
    return {
      "memory_percent": self._cached_memory,
      "memory_available_mb": self._cached_available_mb,
      "cgroup_limit_mb": self.cgroup_limit_mb,
      "cgroup_available_mb": self._cached_cgroup_available_mb,
      "cpu_percent": self._cached_cpu,
      "memory_threshold": self.memory_threshold,
      "min_available_mb": self.min_available_mb,
      "cpu_threshold": self.cpu_threshold,
      "connections_per_db": dict(self._connections_per_db),
      "total_connections": sum(self._connections_per_db.values()),
    }


# Global admission controller instance
_admission_controller: LadybugAdmissionController | None = None


def get_admission_controller() -> LadybugAdmissionController:
  """Get or create the global admission controller."""
  global _admission_controller
  if _admission_controller is None:
    from robosystems.config import env
    from robosystems.config.constants import LBUG_MAX_CONNECTIONS_PER_DB

    # Use centralized config (which handles env vars and defaults properly)
    _admission_controller = LadybugAdmissionController(
      memory_threshold=env.LBUG_ADMISSION_MEMORY_THRESHOLD,
      min_available_mb=env.LBUG_ADMISSION_MIN_AVAILABLE_MB,
      cpu_threshold=env.LBUG_ADMISSION_CPU_THRESHOLD,
      max_connections_per_db=LBUG_MAX_CONNECTIONS_PER_DB,
    )

  return _admission_controller
