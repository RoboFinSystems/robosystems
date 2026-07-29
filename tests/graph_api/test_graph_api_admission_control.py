"""Tests for Graph API admission control module."""

from types import SimpleNamespace

import pytest

from robosystems.graph_api.core.admission_control import (
  AdmissionDecision,
  LadybugAdmissionController,
  get_admission_controller,
)


@pytest.fixture
def set_resource_usage(monkeypatch):
  """Helper to patch psutil resource metrics."""

  def _setter(memory_percent: float, cpu_percent: float, available_mb: float = 8192.0):
    monkeypatch.setattr(
      "robosystems.graph_api.core.admission_control.psutil.virtual_memory",
      lambda: SimpleNamespace(
        percent=memory_percent, available=int(available_mb * 1024 * 1024)
      ),
    )
    monkeypatch.setattr(
      "robosystems.graph_api.core.admission_control.psutil.cpu_percent",
      lambda interval=0.1: cpu_percent,
    )

  return _setter


def test_check_admission_accepts_when_under_thresholds(set_resource_usage):
  """Requests should be accepted when metrics are healthy."""
  controller = LadybugAdmissionController(
    memory_threshold=80.0,
    cpu_threshold=90.0,
    max_connections_per_db=2,
    check_interval=0.0,
  )
  set_resource_usage(memory_percent=45.0, cpu_percent=25.0)

  decision, reason = controller.check_admission("graph-123", operation_type="query")

  assert decision == AdmissionDecision.ACCEPT
  assert reason is None


def test_check_admission_rejects_when_headroom_exhausted(set_resource_usage):
  """Ensure genuinely low free memory triggers load shedding."""
  controller = LadybugAdmissionController(
    min_available_mb=1024.0,
    cpu_threshold=95.0,
    max_connections_per_db=2,
    check_interval=0.0,
  )
  set_resource_usage(memory_percent=99.0, cpu_percent=30.0, available_mb=512.0)

  decision, reason = controller.check_admission("graph-123")

  assert decision == AdmissionDecision.REJECT_MEMORY
  assert reason is not None
  assert "Insufficient memory headroom" in reason


def test_warm_buffer_pool_does_not_reject(set_resource_usage):
  """A full buffer pool reads as high percent-used but must still serve.

  Regression: replicas ran an 8GB pool on a 15.7GB instance, so warm nodes sat
  at ~87% used and the old percent gate rejected every query even though ~2GB
  was free.
  """
  controller = LadybugAdmissionController(
    memory_threshold=85.0,
    min_available_mb=1024.0,
    cpu_threshold=95.0,
    max_connections_per_db=2,
    check_interval=0.0,
  )
  set_resource_usage(memory_percent=87.4, cpu_percent=30.0, available_mb=1974.0)

  decision, reason = controller.check_admission("graph-123")

  assert decision == AdmissionDecision.ACCEPT
  assert reason is None


def test_headroom_gate_is_independent_of_instance_size(set_resource_usage):
  """The gate must not move when total memory changes.

  Same absolute headroom on a small and a large instance yields the same
  decision, which is what stops the guard needing a re-tune every time the
  pool or instance type changes.
  """
  controller = LadybugAdmissionController(
    min_available_mb=1024.0,
    cpu_threshold=95.0,
    max_connections_per_db=5,
    check_interval=0.0,
  )

  # Small instance, mostly consumed by the pool
  set_resource_usage(memory_percent=88.0, cpu_percent=20.0, available_mb=1800.0)
  assert controller.check_admission("graph-123")[0] == AdmissionDecision.ACCEPT

  # Large instance at a far lower percentage, but genuinely starved
  set_resource_usage(memory_percent=40.0, cpu_percent=20.0, available_mb=256.0)
  assert controller.check_admission("graph-123")[0] == AdmissionDecision.REJECT_MEMORY


def test_resource_probe_failure_fails_closed(monkeypatch):
  """If psutil raises, assume no headroom rather than admitting blindly."""

  def _boom():
    raise RuntimeError("psutil unavailable")

  monkeypatch.setattr(
    "robosystems.graph_api.core.admission_control.psutil.virtual_memory", _boom
  )
  controller = LadybugAdmissionController(min_available_mb=1024.0, check_interval=0.0)

  decision, reason = controller.check_admission("graph-123")

  assert decision == AdmissionDecision.REJECT_MEMORY
  assert reason is not None


def test_check_admission_rejects_on_cpu_for_ingestion(set_resource_usage):
  """CPU limits are stricter for ingestion workloads."""
  controller = LadybugAdmissionController(
    memory_threshold=90.0,
    cpu_threshold=80.0,
    max_connections_per_db=2,
    check_interval=0.0,
  )
  # Ingestion drops CPU limit by 10, so 75 should pass, 72 should not in ingestion
  set_resource_usage(memory_percent=55.0, cpu_percent=75.0)

  decision, reason = controller.check_admission("graph-123", operation_type="ingestion")

  assert decision == AdmissionDecision.REJECT_CPU
  assert reason is not None
  assert "CPU usage too high" in reason


def test_check_admission_rejects_when_connections_exceeded(set_resource_usage):
  """Connection pool guard should reject once max connections reached."""
  controller = LadybugAdmissionController(
    memory_threshold=90.0,
    cpu_threshold=90.0,
    max_connections_per_db=1,
    check_interval=0.0,
  )
  set_resource_usage(memory_percent=50.0, cpu_percent=20.0)

  controller.register_connection("graph-123")
  decision, reason = controller.check_admission("graph-123")

  assert decision == AdmissionDecision.REJECT_CONNECTIONS
  assert reason is not None
  assert "Too many connections" in reason


def test_get_metrics_includes_live_counters(set_resource_usage):
  """Metrics payload should include latest resource values and totals."""
  controller = LadybugAdmissionController(
    memory_threshold=85.0,
    cpu_threshold=90.0,
    max_connections_per_db=3,
    check_interval=0.0,
  )
  set_resource_usage(memory_percent=60.0, cpu_percent=40.0)
  controller.register_connection("graph-123")
  controller.register_connection("graph-xyz")
  controller.release_connection("graph-xyz")

  metrics = controller.get_metrics()

  assert metrics["memory_percent"] == pytest.approx(60.0)
  assert metrics["memory_available_mb"] == pytest.approx(8192.0)
  assert metrics["cpu_percent"] == pytest.approx(40.0)
  assert metrics["connections_per_db"]["graph-123"] == 1
  assert metrics["total_connections"] == 1


def test_get_admission_controller_uses_env_configuration(monkeypatch):
  """Singleton helper should honor environment configuration and cache instances."""
  # Reset cached singleton
  monkeypatch.setattr(
    "robosystems.graph_api.core.admission_control._admission_controller",
    None,
    raising=False,
  )

  monkeypatch.setattr(
    "robosystems.config.env.LBUG_ADMISSION_MEMORY_THRESHOLD", 77.0, raising=False
  )
  monkeypatch.setattr(
    "robosystems.config.env.LBUG_ADMISSION_CPU_THRESHOLD", 88.0, raising=False
  )
  monkeypatch.setattr(
    "robosystems.config.env.LBUG_ADMISSION_MIN_AVAILABLE_MB", 2048.0, raising=False
  )
  # Patch the constant at its source module before the function imports it
  monkeypatch.setattr(
    "robosystems.config.constants.LBUG_MAX_CONNECTIONS_PER_DB",
    5,
    raising=False,
  )

  controller_one = get_admission_controller()
  controller_two = get_admission_controller()

  assert controller_one is controller_two  # Singleton
  assert controller_one.memory_threshold == 77.0
  assert controller_one.min_available_mb == 2048.0
  assert controller_one.cpu_threshold == 88.0
  assert controller_one.max_connections_per_db == 5
