"""Tests for admission control and load shedding module."""

import time
from unittest.mock import Mock, patch

import pytest

from robosystems.middleware.graph.admission_control import (
  AdmissionController,
  AdmissionDecision,
  SystemResources,
  get_admission_controller,
)


class TestSystemResources:
  """Tests for SystemResources dataclass."""

  def test_system_resources_healthy(self):
    """Test healthy system resources."""
    resources = SystemResources(
      memory_percent=70.0,
      cpu_percent=80.0,
      queue_depth=10,
      active_queries=5,
      load_average=1.5,
    )

    assert resources.is_healthy is True
    assert resources.memory_percent == 70.0
    assert resources.cpu_percent == 80.0
    assert resources.queue_depth == 10
    assert resources.active_queries == 5
    assert resources.load_average == 1.5

  def test_system_resources_unhealthy_memory(self):
    """Test unhealthy system with high memory."""
    resources = SystemResources(
      memory_percent=85.0,  # Above 80% threshold
      cpu_percent=70.0,
      queue_depth=10,
      active_queries=5,
      load_average=1.0,
    )

    assert resources.is_healthy is False

  def test_system_resources_unhealthy_cpu(self):
    """Test unhealthy system with high CPU."""
    resources = SystemResources(
      memory_percent=70.0,
      cpu_percent=90.0,  # Above 85% threshold
      queue_depth=10,
      active_queries=5,
      load_average=1.0,
    )

    assert resources.is_healthy is False

  def test_system_resources_unhealthy_load(self):
    """Test unhealthy system with high load."""
    resources = SystemResources(
      memory_percent=70.0,
      cpu_percent=70.0,
      queue_depth=10,
      active_queries=5,
      load_average=2.5,  # Above 2.0 threshold
    )

    assert resources.is_healthy is False


class TestAdmissionDecision:
  """Tests for AdmissionDecision enum."""

  def test_decision_values(self):
    """Test admission decision values."""
    assert AdmissionDecision.ACCEPT.value == "accept"
    assert AdmissionDecision.ADMIT.value == "accept"  # Alias
    assert AdmissionDecision.REJECT_MEMORY.value == "reject_memory"
    assert AdmissionDecision.REJECT_CPU.value == "reject_cpu"
    assert AdmissionDecision.REJECT_QUEUE.value == "reject_queue"
    assert AdmissionDecision.REJECT_LOAD_SHED.value == "reject_load_shed"

  def test_admit_alias(self):
    """Test that ADMIT is an alias for ACCEPT."""
    assert AdmissionDecision.ADMIT == AdmissionDecision.ACCEPT


class TestAdmissionController:
  """Tests for AdmissionController."""

  @pytest.fixture
  def controller(self):
    """Create an AdmissionController instance."""
    return AdmissionController(
      memory_threshold=85.0,
      cpu_threshold=90.0,
      queue_threshold=0.8,
      check_interval=1.0,
    )

  @pytest.fixture
  def mock_psutil(self):
    """Mock psutil functions."""
    with patch("robosystems.middleware.graph.admission_control.psutil") as mock:
      # Set up virtual memory mock
      mock_memory = Mock()
      mock_memory.percent = 50.0
      mock.virtual_memory.return_value = mock_memory

      # Set up CPU mock
      mock.cpu_percent.return_value = 40.0

      # Set up load average mock
      mock.getloadavg.return_value = (1.0, 1.5, 2.0)

      yield mock

  def test_initialization(self, controller):
    """Test controller initialization."""
    assert controller.memory_threshold == 85.0
    assert controller.cpu_threshold == 90.0
    assert controller.queue_threshold == 0.8
    assert controller.check_interval == 1.0
    assert controller._last_check == 0.0
    assert controller._cached_resources is None
    assert controller._rejection_rate == 0.0
    assert controller._shed_start_time is None

  def test_check_admission_accept(self, controller, mock_psutil):
    """Test accepting a request under normal conditions."""
    decision, reason = controller.check_admission(
      queue_depth=5,
      max_queue_size=100,
      active_queries=2,
      priority=5,
    )

    assert decision == AdmissionDecision.ACCEPT
    assert reason is None

  def test_check_admission_reject_memory(self, controller, mock_psutil):
    """Test rejecting due to high memory usage."""
    # Set high memory usage
    mock_psutil.virtual_memory.return_value.percent = 90.0

    decision, reason = controller.check_admission(
      queue_depth=5,
      max_queue_size=100,
      active_queries=2,
      priority=5,
    )

    assert decision == AdmissionDecision.REJECT_MEMORY
    assert "memory usage too high" in reason

  def test_check_admission_reject_cpu(self, controller, mock_psutil):
    """Test rejecting due to high CPU usage."""
    # Set high CPU usage
    mock_psutil.cpu_percent.return_value = 95.0

    decision, reason = controller.check_admission(
      queue_depth=5,
      max_queue_size=100,
      active_queries=2,
      priority=5,
    )

    assert decision == AdmissionDecision.REJECT_CPU
    assert "CPU usage too high" in reason

  @patch("random.random")
  def test_check_admission_reject_queue_depth(
    self, mock_random, controller, mock_psutil
  ):
    """Test rejecting due to queue depth."""
    # Make random always trigger rejection
    mock_random.return_value = 0.1

    decision, reason = controller.check_admission(
      queue_depth=85,  # 85% of max queue
      max_queue_size=100,
      active_queries=2,
      priority=5,
    )

    assert decision == AdmissionDecision.REJECT_LOAD_SHED
    assert "Queue" in reason and "full" in reason

  @patch("random.random")
  def test_check_admission_queue_depth_probabilistic(
    self, mock_random, controller, mock_psutil
  ):
    """Test probabilistic rejection based on queue fullness."""
    # Test different queue fullness levels
    # Note: Priority 5 gives adjustment factor of (11-5)/10 = 0.6
    test_cases = [
      (96, 0.5, True),  # >95% full, 90% * 0.6 = 54% rejection prob, should reject
      (92, 0.4, True),  # >90% full, 70% * 0.6 = 42% rejection prob, should reject
      (85, 0.25, True),  # >80% full, 50% * 0.6 = 30% rejection prob, should reject
      (85, 0.35, False),  # >80% full, 50% * 0.6 = 30% rejection prob, should accept
    ]

    for queue_depth, random_val, should_reject in test_cases:
      mock_random.return_value = random_val
      decision, _ = controller.check_admission(
        queue_depth=queue_depth,
        max_queue_size=100,
        active_queries=2,
        priority=5,
      )

      if should_reject:
        assert decision == AdmissionDecision.REJECT_LOAD_SHED
      else:
        assert decision == AdmissionDecision.ACCEPT

  @patch("random.random")
  def test_check_admission_priority_adjustment(
    self, mock_random, controller, mock_psutil
  ):
    """Test that higher priority queries have lower rejection probability."""
    mock_random.return_value = 0.3

    # High priority (10) should be accepted
    decision, _ = controller.check_admission(
      queue_depth=85,
      max_queue_size=100,
      active_queries=2,
      priority=10,  # High priority
    )
    assert decision == AdmissionDecision.ACCEPT

    # Low priority (1) should be rejected
    decision, _ = controller.check_admission(
      queue_depth=85,
      max_queue_size=100,
      active_queries=2,
      priority=1,  # Low priority
    )
    assert decision == AdmissionDecision.REJECT_LOAD_SHED

  @patch("random.random")
  def test_check_admission_pressure_based_rejection(
    self, mock_random, controller, mock_psutil
  ):
    """Test pressure-based load shedding."""
    # Set high resource usage to create pressure > 0.7
    # Pressure = memory*0.4 + cpu*0.3 + queue*0.2 + load*0.1
    # Need pressure > 0.7 for rejection consideration
    mock_psutil.virtual_memory.return_value.percent = 80.0  # 0.8 * 0.4 = 0.32
    mock_psutil.cpu_percent.return_value = 85.0  # 0.85 * 0.3 = 0.255
    mock_psutil.getloadavg.return_value = (3.0, 2.5, 2.0)  # 0.75 * 0.1 = 0.075
    mock_random.return_value = 0.01  # Low value to ensure rejection

    decision, reason = controller.check_admission(
      queue_depth=70,  # 0.7 * 0.2 = 0.14
      max_queue_size=100,
      active_queries=2,
      priority=5,
    )
    # Total pressure = 0.32 + 0.255 + 0.14 + 0.075 = 0.79

    assert decision == AdmissionDecision.REJECT_LOAD_SHED
    assert "resource pressure" in reason

  @patch("random.random")
  def test_load_shedding_state_transitions(self, mock_random, controller, mock_psutil):
    """Test entering and exiting load shedding mode."""
    # Mock random to ensure we don't get rejected by probabilistic rejection
    mock_random.return_value = 0.99  # Will not trigger rejection

    # Set high pressure to enter load shedding (need pressure > 0.8)
    # BUT avoid hitting hard limits and queue threshold
    # Pressure = memory*0.4 + cpu*0.3 + queue*0.2 + load*0.1
    mock_psutil.virtual_memory.return_value.percent = 84.0  # 0.84 * 0.4 = 0.336
    mock_psutil.cpu_percent.return_value = 89.0  # 0.89 * 0.3 = 0.267
    mock_psutil.getloadavg.return_value = (5.0, 4.0, 3.0)  # 1.25 * 0.1 = 0.125

    controller.check_admission(
      queue_depth=75,  # 0.75 * 0.2 = 0.15 (below 0.8 threshold)
      max_queue_size=100,
      active_queries=2,
      priority=5,
    )
    # Total pressure = 0.336 + 0.267 + 0.15 + 0.125 = 0.878 > 0.8
    # Since pressure > 0.8, it should enter load shedding mode

    assert controller._shed_start_time is not None

    # Set low pressure to exit load shedding (need pressure < 0.6)
    mock_psutil.virtual_memory.return_value.percent = 40.0  # 0.40 * 0.4 = 0.16
    mock_psutil.cpu_percent.return_value = 30.0  # 0.30 * 0.3 = 0.09
    mock_psutil.getloadavg.return_value = (1.0, 1.0, 1.0)  # 0.25 * 0.1 = 0.025

    # Clear cache to force resource refresh
    controller._last_check = 0

    controller.check_admission(
      queue_depth=10,  # 0.1 * 0.2 = 0.02
      max_queue_size=100,
      active_queries=2,
      priority=5,
    )
    # Total pressure = 0.16 + 0.09 + 0.02 + 0.025 = 0.295 < 0.6

    assert controller._shed_start_time is None

  @pytest.mark.asyncio
  async def test_should_admit_request_async(self, controller, mock_psutil):
    """Test async admission check."""
    decision = await controller.should_admit_request()
    assert decision == AdmissionDecision.ADMIT

  @pytest.mark.asyncio
  async def test_should_admit_request_reject_memory(self, controller, mock_psutil):
    """Test async admission check rejecting due to memory."""
    mock_psutil.virtual_memory.return_value.percent = 90.0

    decision = await controller.should_admit_request()
    assert decision == AdmissionDecision.REJECT_MEMORY

  @pytest.mark.asyncio
  async def test_should_admit_request_reject_cpu(self, controller, mock_psutil):
    """Test async admission check rejecting due to CPU."""
    mock_psutil.cpu_percent.return_value = 95.0

    decision = await controller.should_admit_request()
    assert decision == AdmissionDecision.REJECT_CPU

  @pytest.mark.asyncio
  async def test_should_admit_request_high_resources(self, controller, mock_psutil):
    """Test async admission check with very high resources."""
    # With 100% memory, it should reject based on memory threshold (85%)
    mock_psutil.virtual_memory.return_value.percent = 100.0
    mock_psutil.cpu_percent.return_value = 100.0
    mock_psutil.getloadavg.return_value = (8.0, 7.0, 6.0)

    decision = await controller.should_admit_request()
    # Should reject due to memory exceeding threshold
    assert decision == AdmissionDecision.REJECT_MEMORY

  def test_get_system_resources_caching(self, controller, mock_psutil):
    """Test that system resources are cached."""
    # First call should fetch fresh data
    controller._get_system_resources(10, 5)
    assert mock_psutil.virtual_memory.call_count == 1
    assert mock_psutil.cpu_percent.call_count == 1

    # Second call within interval should use cache
    resources2 = controller._get_system_resources(15, 7)
    assert mock_psutil.virtual_memory.call_count == 1  # Still 1
    assert mock_psutil.cpu_percent.call_count == 1  # Still 1

    # But queue metrics should be updated
    assert resources2.queue_depth == 15
    assert resources2.active_queries == 7

    # After interval, should fetch fresh data
    controller._last_check = time.time() - 2.0
    controller._get_system_resources(20, 10)
    assert mock_psutil.virtual_memory.call_count == 2
    assert mock_psutil.cpu_percent.call_count == 2

  def test_get_system_resources_error_handling(self, controller):
    """Test handling errors when getting system resources."""
    with patch("robosystems.middleware.graph.admission_control.psutil") as mock_psutil:
      mock_psutil.virtual_memory.side_effect = Exception("Test error")

      resources = controller._get_system_resources(10, 5)

      # Should use conservative defaults
      assert resources.memory_percent == 50.0
      assert resources.cpu_percent == 50.0
      assert resources.load_average == 1.0
      assert resources.queue_depth == 10
      assert resources.active_queries == 5

  def test_calculate_pressure_score(self, controller):
    """Test pressure score calculation."""
    resources = SystemResources(
      memory_percent=80.0,
      cpu_percent=60.0,
      queue_depth=0,
      active_queries=0,
      load_average=2.0,
    )

    score = controller._calculate_pressure_score(resources, 0.5)

    # Expected: 0.8*0.4 + 0.6*0.3 + 0.5*0.2 + 0.5*0.1 = 0.32 + 0.18 + 0.1 + 0.05 = 0.65
    assert 0.64 < score < 0.66

  def test_calculate_pressure_score_clamping(self, controller):
    """Test that pressure score components are clamped to 1.0."""
    resources = SystemResources(
      memory_percent=150.0,  # Over 100%
      cpu_percent=120.0,  # Over 100%
      queue_depth=0,
      active_queries=0,
      load_average=10.0,  # Very high load
    )

    # Queue ratio is not clamped in the implementation, so it can be > 1
    score = controller._calculate_pressure_score(resources, 1.5)  # Queue ratio > 1

    # Memory, CPU, and load are clamped, but queue is not
    # Expected: 1.0*0.4 + 1.0*0.3 + 1.5*0.2 + 1.0*0.1 = 0.4 + 0.3 + 0.3 + 0.1 = 1.1
    assert score == 1.1

  def test_get_health_status_healthy(self, controller, mock_psutil):
    """Test getting health status for healthy system."""
    status = controller.get_health_status(
      queue_depth=10,
      max_queue_size=100,
      active_queries=2,
    )

    assert status["state"] == "healthy"
    assert status["pressure_score"] < 0.5
    assert status["resources"]["memory_percent"] == 50.0
    assert status["resources"]["cpu_percent"] == 40.0
    assert status["resources"]["load_average"] == 1.0
    assert status["queue"]["depth"] == 10
    assert status["queue"]["ratio"] == 0.1
    assert status["queue"]["active_queries"] == 2
    assert status["load_shedding"]["active"] is False
    assert status["thresholds"]["memory"] == 85.0
    assert status["thresholds"]["cpu"] == 90.0
    assert status["thresholds"]["queue"] == 0.8

  def test_get_health_status_degraded(self, controller, mock_psutil):
    """Test getting health status for degraded system."""
    mock_psutil.virtual_memory.return_value.percent = 70.0
    mock_psutil.cpu_percent.return_value = 65.0

    status = controller.get_health_status(
      queue_depth=50,
      max_queue_size=100,
      active_queries=10,
    )

    assert status["state"] == "degraded"
    assert 0.5 <= status["pressure_score"] < 0.7

  def test_get_health_status_unhealthy(self, controller, mock_psutil):
    """Test getting health status for unhealthy system."""
    mock_psutil.virtual_memory.return_value.percent = 82.0
    mock_psutil.cpu_percent.return_value = 85.0

    status = controller.get_health_status(
      queue_depth=85,
      max_queue_size=100,
      active_queries=20,
    )

    assert status["state"] == "unhealthy"
    assert status["pressure_score"] >= 0.7

  def test_get_health_status_with_load_shedding(self, controller, mock_psutil):
    """Test health status when load shedding is active."""
    # Activate load shedding
    controller._shed_start_time = time.time() - 10.0

    status = controller.get_health_status(
      queue_depth=10,
      max_queue_size=100,
      active_queries=2,
    )

    assert status["load_shedding"]["active"] is True
    assert status["load_shedding"]["duration_seconds"] >= 10.0

  def test_get_health_status_zero_max_queue(self, controller, mock_psutil):
    """Test health status with zero max queue size."""
    status = controller.get_health_status(
      queue_depth=0,
      max_queue_size=0,
      active_queries=0,
    )

    assert status["queue"]["ratio"] == 0.0


class TestGetAdmissionController:
  """Tests for get_admission_controller function."""

  @patch("robosystems.config.query_queue.QueryQueueConfig")
  def test_get_admission_controller_singleton(self, mock_config):
    """Test that get_admission_controller returns singleton."""
    mock_config.get_admission_config.return_value = {
      "memory_threshold": 85.0,
      "cpu_threshold": 90.0,
      "queue_threshold": 0.8,
      "check_interval": 1.0,
    }

    # Reset global state
    import robosystems.middleware.graph.admission_control as ac

    ac._admission_controller = None

    controller1 = get_admission_controller()
    controller2 = get_admission_controller()

    assert controller1 is controller2
    assert mock_config.get_admission_config.call_count == 1

  @patch("robosystems.config.query_queue.QueryQueueConfig")
  def test_get_admission_controller_configuration(self, mock_config):
    """Test that admission controller is configured correctly."""
    mock_config.get_admission_config.return_value = {
      "memory_threshold": 75.0,
      "cpu_threshold": 80.0,
      "queue_threshold": 0.7,
      "check_interval": 2.0,
    }

    # Reset global state
    import robosystems.middleware.graph.admission_control as ac

    ac._admission_controller = None

    controller = get_admission_controller()

    assert controller.memory_threshold == 75.0
    assert controller.cpu_threshold == 80.0
    assert controller.queue_threshold == 0.7
    assert controller.check_interval == 2.0
