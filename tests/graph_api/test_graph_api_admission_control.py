"""Tests for Graph API admission control module."""

from types import SimpleNamespace

import pytest

from robosystems.graph_api.core.admission_control import (
  AdmissionDecision,
  KuzuAdmissionController,
  get_admission_controller,
)


@pytest.fixture
def set_resource_usage(monkeypatch):
  """Helper to patch psutil resource metrics."""

  def _setter(memory_percent: float, cpu_percent: float):
    monkeypatch.setattr(
      "robosystems.graph_api.core.admission_control.psutil.virtual_memory",
      lambda: SimpleNamespace(percent=memory_percent),
    )
    monkeypatch.setattr(
      "robosystems.graph_api.core.admission_control.psutil.cpu_percent",
      lambda interval=0.1: cpu_percent,
    )

  return _setter


def test_check_admission_accepts_when_under_thresholds(set_resource_usage):
  """Requests should be accepted when metrics are healthy."""
  controller = KuzuAdmissionController(
    memory_threshold=80.0,
    cpu_threshold=90.0,
    max_connections_per_db=2,
    check_interval=0.0,
  )
  set_resource_usage(memory_percent=45.0, cpu_percent=25.0)

  decision, reason = controller.check_admission("graph-123", operation_type="query")

  assert decision == AdmissionDecision.ACCEPT
  assert reason is None


def test_check_admission_rejects_on_memory_pressure(set_resource_usage):
  """Ensure high memory usage triggers load shedding."""
  controller = KuzuAdmissionController(
    memory_threshold=70.0,
    cpu_threshold=95.0,
    max_connections_per_db=2,
    check_interval=0.0,
  )
  set_resource_usage(memory_percent=85.0, cpu_percent=30.0)

  decision, reason = controller.check_admission("graph-123")

  assert decision == AdmissionDecision.REJECT_MEMORY
  assert reason is not None
  assert "Memory usage too high" in reason


def test_check_admission_rejects_on_cpu_for_ingestion(set_resource_usage):
  """CPU limits are stricter for ingestion workloads."""
  controller = KuzuAdmissionController(
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
  controller = KuzuAdmissionController(
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
  controller = KuzuAdmissionController(
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
    "robosystems.config.env.KUZU_ADMISSION_MEMORY_THRESHOLD", 77.0, raising=False
  )
  monkeypatch.setattr(
    "robosystems.config.env.KUZU_ADMISSION_CPU_THRESHOLD", 88.0, raising=False
  )
  monkeypatch.setattr(
    "robosystems.config.env.KUZU_MAX_CONNECTIONS_PER_DB", 5, raising=False
  )

  controller_one = get_admission_controller()
  controller_two = get_admission_controller()

  assert controller_one is controller_two  # Singleton
  assert controller_one.memory_threshold == 77.0
  assert controller_one.cpu_threshold == 88.0
  assert controller_one.max_connections_per_db == 5
