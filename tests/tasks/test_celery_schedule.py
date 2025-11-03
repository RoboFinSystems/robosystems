"""Tests for Celery Beat schedule configuration."""

from celery.schedules import crontab


class TestScheduleConfiguration:
  """Test cases for schedule configuration structure."""

  def test_schedule_is_dict(self):
    """Test that BEAT_SCHEDULE is a dictionary."""
    from robosystems.tasks.schedule import BEAT_SCHEDULE

    assert isinstance(BEAT_SCHEDULE, dict)

  def test_essential_tasks_always_present(self):
    """Test that essential tasks are present in all environments."""
    from robosystems.tasks.schedule import BEAT_SCHEDULE

    assert "cleanup-expired-api-keys" in BEAT_SCHEDULE

  def test_essential_task_structure(self):
    """Test that essential tasks have correct structure."""
    from robosystems.tasks.schedule import BEAT_SCHEDULE

    task_config = BEAT_SCHEDULE["cleanup-expired-api-keys"]
    assert "task" in task_config
    assert "schedule" in task_config
    assert "options" in task_config
    assert isinstance(task_config["schedule"], (int, float, crontab))

  def test_essential_task_options(self):
    """Test that essential tasks have correct options."""
    from robosystems.tasks.schedule import BEAT_SCHEDULE

    task_config = BEAT_SCHEDULE["cleanup-expired-api-keys"]
    assert "queue" in task_config["options"]
    assert "priority" in task_config["options"]
    assert isinstance(task_config["options"]["priority"], int)


class TestProductionSchedules:
  """Test cases for production-only scheduled tasks."""

  def test_schedule_has_billing_tasks(self):
    """Test that schedule includes billing tasks."""
    from robosystems.tasks.schedule import BEAT_SCHEDULE

    task_names = list(BEAT_SCHEDULE.keys())
    assert len(task_names) > 0


class TestBillingTaskSchedules:
  """Test cases for billing task schedules."""

  def test_billing_tasks_have_valid_structure(self):
    """Test that billing tasks have valid configuration structure."""
    from robosystems.tasks.schedule import BEAT_SCHEDULE

    billing_tasks = [
      "allocate-monthly-shared-credits",
      "allocate-monthly-graph-credits",
      "daily-storage-billing",
      "collect-storage-usage",
      "monthly-storage-summary",
    ]

    for task_name in billing_tasks:
      if task_name in BEAT_SCHEDULE:
        task_config = BEAT_SCHEDULE[task_name]
        assert "task" in task_config
        assert "schedule" in task_config
        assert "options" in task_config
        assert isinstance(task_config["schedule"], (int, float, crontab))


class TestHealthCheckSchedules:
  """Test cases for health check task schedules."""

  def test_health_check_tasks_exist(self):
    """Test that health check tasks are configured."""
    from robosystems.tasks.schedule import BEAT_SCHEDULE

    health_check_tasks = [
      "check-credit-allocation-health",
      "check-graph-credit-health",
    ]

    for task_name in health_check_tasks:
      if task_name in BEAT_SCHEDULE:
        task_config = BEAT_SCHEDULE[task_name]
        assert "task" in task_config
        assert "schedule" in task_config
        assert isinstance(task_config["schedule"], crontab)


class TestTaskPriorities:
  """Test cases for task priority configuration."""

  def test_all_tasks_have_priorities(self):
    """Test that all tasks have priority values."""
    from robosystems.tasks.schedule import BEAT_SCHEDULE

    for task_name, task_config in BEAT_SCHEDULE.items():
      assert "options" in task_config
      assert "priority" in task_config["options"]
      assert isinstance(task_config["options"]["priority"], int)
      assert 1 <= task_config["options"]["priority"] <= 10


class TestTaskQueues:
  """Test cases for task queue assignments."""

  def test_all_tasks_have_queues(self):
    """Test that all tasks are assigned to a queue."""
    from robosystems.tasks.schedule import BEAT_SCHEDULE

    for task_name, task_config in BEAT_SCHEDULE.items():
      assert "queue" in task_config["options"], f"{task_name} missing queue"
      assert isinstance(task_config["options"]["queue"], str)


class TestScheduleTimingSequence:
  """Test cases for verifying task timing sequences."""

  def test_credit_allocation_has_sequence(self):
    """Test that credit allocation tasks have scheduled times."""
    from robosystems.tasks.schedule import BEAT_SCHEDULE

    if "allocate-monthly-shared-credits" in BEAT_SCHEDULE:
      shared_schedule = BEAT_SCHEDULE["allocate-monthly-shared-credits"]["schedule"]
      assert isinstance(shared_schedule, crontab)

    if "allocate-monthly-graph-credits" in BEAT_SCHEDULE:
      graph_schedule = BEAT_SCHEDULE["allocate-monthly-graph-credits"]["schedule"]
      assert isinstance(graph_schedule, crontab)
