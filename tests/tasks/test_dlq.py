"""Tests for Dead Letter Queue (DLQ) task handlers."""

import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock, PropertyMock
from kombu import Exchange, Queue

from robosystems.tasks.dlq import (
  DLQTask,
  store_failed_task,
  reprocess_dlq_task,
  get_dlq_stats,
  create_dlq_task,
  DLQ_NAME,
  DLQ_EXCHANGE,
  DLQ_QUEUE,
)
from robosystems.celery import celery_app


@pytest.fixture
def mock_task():
  """Create a mock task instance."""
  task = Mock(spec=DLQTask)
  task.name = "test.task"
  task.max_retries = 3
  task.request = Mock()
  task.request.retries = 3
  task.request.queue = "test-queue"
  task.request.routing_key = "test.routing"
  task.request.priority = 5
  return task


@pytest.fixture
def dlq_message():
  """Sample DLQ message."""
  return {
    "task_id": "test-task-123",
    "task_name": "test.task",
    "args": [1, 2, 3],
    "kwargs": {"user_id": "user123", "entity_id": "entity456"},
    "failed_at": datetime.now(timezone.utc).isoformat(),
    "retries": 3,
    "exception": {
      "type": "ValueError",
      "message": "Test error",
      "traceback": "Traceback...",
    },
    "metadata": {
      "queue": "test-queue",
      "routing_key": "test.routing",
      "priority": 5,
      "user_id": "user123",
      "entity_id": "entity456",
      "graph_id": None,
    },
  }


class TestDLQConfiguration:
  """Test DLQ configuration and setup."""

  def test_dlq_exchange_configuration(self):
    """Test DLQ exchange is configured correctly."""
    assert isinstance(DLQ_EXCHANGE, Exchange)
    assert DLQ_EXCHANGE.name == DLQ_NAME
    assert DLQ_EXCHANGE.type == "direct"
    assert DLQ_EXCHANGE.durable is True

  def test_dlq_queue_configuration(self):
    """Test DLQ queue is configured correctly."""
    assert isinstance(DLQ_QUEUE, Queue)
    assert DLQ_QUEUE.name == DLQ_NAME
    assert DLQ_QUEUE.exchange == DLQ_EXCHANGE
    assert DLQ_QUEUE.routing_key == DLQ_NAME
    assert DLQ_QUEUE.durable is True
    # Check TTL is set to 7 days
    assert DLQ_QUEUE.queue_arguments["x-message-ttl"] == 7 * 24 * 60 * 60 * 1000

  def test_dlq_added_to_celery_config(self):
    """Test DLQ is added to Celery task queues."""
    # Check that DLQ_QUEUE is in the celery configuration
    queue_names = [q.name for q in celery_app.conf.task_queues if hasattr(q, "name")]
    assert DLQ_NAME in queue_names


class TestDLQTask:
  """Test DLQTask base class."""

  def test_dlq_task_configuration(self):
    """Test DLQTask default configuration."""
    task = DLQTask()
    assert task.autoretry_for == (Exception,)
    assert task.retry_backoff is True
    assert task.retry_backoff_max == 600
    assert task.retry_jitter is True

  @patch("robosystems.tasks.dlq.DLQTask._send_to_dlq")
  def test_on_failure_sends_to_dlq_after_max_retries(self, mock_send, mock_task):
    """Test task is sent to DLQ after max retries."""
    exc = ValueError("Test error")
    task_id = "test-task-123"
    args = [1, 2, 3]
    kwargs = {"user_id": "user123"}
    einfo = Mock()

    # Create real DLQTask instance
    task = DLQTask()
    task.name = "test.task"
    task.max_retries = 3

    # Mock the request property
    mock_request = Mock()
    mock_request.retries = 3  # Max retries reached
    mock_request.queue = "test-queue"
    mock_request.routing_key = "test.routing"
    mock_request.priority = 5

    with patch.object(
      type(task), "request", new_callable=PropertyMock
    ) as mock_request_prop:
      mock_request_prop.return_value = mock_request
      # Mock parent's on_failure to avoid calling super()
      with patch("celery.Task.on_failure"):
        task.on_failure(exc, task_id, args, kwargs, einfo)
        mock_send.assert_called_once_with(exc, task_id, args, kwargs, einfo)

  @patch("robosystems.tasks.dlq.DLQTask._send_to_dlq")
  def test_on_failure_does_not_send_before_max_retries(self, mock_send, mock_task):
    """Test task is not sent to DLQ before max retries."""
    exc = ValueError("Test error")
    task_id = "test-task-123"
    args = [1, 2, 3]
    kwargs = {"user_id": "user123"}
    einfo = Mock()

    task = DLQTask()
    task.name = "test.task"
    task.max_retries = 3

    # Mock the request property
    mock_request = Mock()
    mock_request.retries = 1  # Not yet at max
    mock_request.queue = "test-queue"
    mock_request.routing_key = "test.routing"
    mock_request.priority = 5

    with patch.object(
      type(task), "request", new_callable=PropertyMock
    ) as mock_request_prop:
      mock_request_prop.return_value = mock_request
      # Mock parent's on_failure to avoid calling super()
      with patch("celery.Task.on_failure"):
        task.on_failure(exc, task_id, args, kwargs, einfo)
        mock_send.assert_not_called()

  @patch("robosystems.tasks.dlq.celery_app.send_task")
  @patch("robosystems.tasks.dlq.logger")
  def test_send_to_dlq_success(self, mock_logger, mock_send_task, mock_task):
    """Test successful sending to DLQ."""
    exc = ValueError("Test error")
    task_id = "test-task-123"
    args = [1, 2, 3]
    kwargs = {"user_id": "user123", "entity_id": "entity456"}
    einfo = Mock()
    einfo.__str__ = Mock(return_value="Traceback...")

    task = DLQTask()
    task.name = "test.task"

    # Mock the request property
    mock_request = Mock()
    mock_request.retries = 3
    mock_request.queue = "test-queue"
    mock_request.routing_key = "test.routing"
    mock_request.priority = 5

    with patch.object(
      type(task), "request", new_callable=PropertyMock
    ) as mock_request_prop:
      mock_request_prop.return_value = mock_request
      task._send_to_dlq(exc, task_id, args, kwargs, einfo)

      # Verify send_task was called
      mock_send_task.assert_called_once()
      call_args = mock_send_task.call_args

      # Check the task name (first positional arg)
      assert call_args.args[0] == "robosystems.tasks.dlq.store_failed_task"

      # Check kwargs
      assert call_args.kwargs["queue"] == DLQ_NAME
      assert call_args.kwargs["routing_key"] == DLQ_NAME
      assert call_args.kwargs["priority"] == 1

      # The message is passed via args kwarg, not positional
      assert "args" in call_args.kwargs
      dlq_message = call_args.kwargs["args"][0]
      assert dlq_message["task_id"] == task_id
      assert dlq_message["task_name"] == "test.task"
      assert dlq_message["args"] == args
      assert dlq_message["kwargs"] == kwargs
      assert dlq_message["retries"] == 3
      assert dlq_message["exception"]["type"] == "ValueError"
      assert dlq_message["exception"]["message"] == "Test error"

      # Verify logging
      mock_logger.error.assert_called_once()

  @patch("robosystems.tasks.dlq.celery_app.send_task")
  @patch("robosystems.tasks.dlq.logger")
  def test_send_to_dlq_failure_logs_critical(
    self, mock_logger, mock_send_task, mock_task
  ):
    """Test DLQ send failure is logged but doesn't raise."""
    mock_send_task.side_effect = Exception("DLQ send failed")

    exc = ValueError("Test error")
    task_id = "test-task-123"
    args = [1, 2, 3]
    kwargs = {"user_id": "user123"}
    einfo = Mock()

    task = DLQTask()
    task.name = "test.task"

    # Mock the request property
    mock_request = Mock()
    mock_request.retries = 3
    mock_request.queue = "test-queue"
    mock_request.routing_key = "test.routing"
    mock_request.priority = 5

    with patch.object(
      type(task), "request", new_callable=PropertyMock
    ) as mock_request_prop:
      mock_request_prop.return_value = mock_request
      # Should not raise exception
      task._send_to_dlq(exc, task_id, args, kwargs, einfo)

      # Verify critical log
      mock_logger.critical.assert_called_once()
      assert (
        "Failed to send task test-task-123 to DLQ"
        in mock_logger.critical.call_args[0][0]
      )


class TestStoreFailedTask:
  """Test store_failed_task function."""

  @patch("robosystems.tasks.dlq.datetime")
  @patch("robosystems.tasks.dlq.logger")
  def test_store_failed_task(self, mock_logger, mock_datetime, dlq_message):
    """Test storing failed task."""
    mock_now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    mock_datetime.now.return_value = mock_now

    # Call the task with apply() to simulate task execution
    result = store_failed_task.apply(args=[dlq_message]).get()  # type: ignore[attr-defined]

    assert result["status"] == "stored"
    assert result["task_id"] == "test-task-123"
    assert result["stored_at"] == mock_now.isoformat()

    # Verify logging
    mock_logger.warning.assert_called_once()
    log_message = mock_logger.warning.call_args[0][0]
    assert "DLQ: Storing failed task test-task-123" in log_message

  @patch("robosystems.tasks.dlq.logger")
  def test_store_failed_task_with_unknown_ids(self, mock_logger):
    """Test storing task with missing IDs."""
    dlq_message = {"other": "data"}

    # Call the task with apply() to simulate task execution
    result = store_failed_task.apply(args=[dlq_message]).get()  # type: ignore[attr-defined]

    assert result["status"] == "stored"
    assert result["task_id"] == "unknown"

    # Verify logging with unknown values
    mock_logger.warning.assert_called_once()
    log_message = mock_logger.warning.call_args[0][0]
    assert "unknown" in log_message


class TestReprocessDLQTask:
  """Test reprocess_dlq_task function."""

  @patch("robosystems.tasks.dlq.celery_app.send_task")
  @patch("robosystems.tasks.dlq.logger")
  def test_reprocess_dlq_task_success(self, mock_logger, mock_send_task, dlq_message):
    """Test successful reprocessing of DLQ task."""
    mock_result = Mock()
    mock_result.id = "new-task-456"
    mock_send_task.return_value = mock_result

    with patch("robosystems.tasks.dlq.datetime") as mock_datetime:
      mock_now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
      mock_datetime.now.return_value = mock_now

      # Call the task with apply() to simulate task execution
      result = reprocess_dlq_task.apply(args=[dlq_message]).get()  # type: ignore[attr-defined]

      assert result["status"] == "reprocessed"
      assert result["new_task_id"] == "new-task-456"
      assert result["original_task_id"] == "test-task-123"
      assert result["reprocessed_at"] == mock_now.isoformat()

      # Verify send_task called correctly
      mock_send_task.assert_called_once_with(
        "test.task",
        args=[1, 2, 3],
        kwargs={"user_id": "user123", "entity_id": "entity456"},
        queue="test-queue",
      )

      # Verify logging
      mock_logger.info.assert_called_once()

  def test_reprocess_dlq_task_missing_name(self, dlq_message):
    """Test reprocessing fails without task name."""
    dlq_message["task_name"] = None

    with pytest.raises(ValueError, match="Cannot reprocess task without task_name"):
      reprocess_dlq_task.apply(args=[dlq_message]).get()  # type: ignore[attr-defined]

  @patch("robosystems.tasks.dlq.celery_app.send_task")
  def test_reprocess_dlq_task_with_default_queue(self, mock_send_task):
    """Test reprocessing uses default queue when not specified."""
    mock_result = Mock()
    mock_result.id = "new-task-789"
    mock_send_task.return_value = mock_result

    dlq_message = {
      "task_name": "test.task",
      "args": [],
      "kwargs": {},
      "metadata": {},  # No queue specified
    }

    reprocess_dlq_task.apply(args=[dlq_message]).get()  # type: ignore[attr-defined]

    # Should use QUEUE_DEFAULT
    from robosystems.celery import QUEUE_DEFAULT

    mock_send_task.assert_called_once()
    assert mock_send_task.call_args[1]["queue"] == QUEUE_DEFAULT


class TestGetDLQStats:
  """Test get_dlq_stats function."""

  @patch("robosystems.tasks.dlq.celery_app.connection_or_acquire")
  def test_get_dlq_stats_healthy(self, mock_connection):
    """Test getting DLQ stats when queue is healthy."""
    mock_channel = Mock()
    mock_queue_info = Mock()
    mock_queue_info.message_count = 10
    mock_channel.queue_declare.return_value = mock_queue_info

    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    mock_conn.default_channel = mock_channel
    mock_connection.return_value = mock_conn

    with patch("robosystems.tasks.dlq.datetime") as mock_datetime:
      mock_now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
      mock_datetime.now.return_value = mock_now

      result = get_dlq_stats.apply().get()  # type: ignore[attr-defined]

      assert result["queue_name"] == DLQ_NAME
      assert result["message_count"] == 10
      assert result["status"] == "healthy"
      assert result["checked_at"] == mock_now.isoformat()

      # Verify queue_declare called correctly
      mock_channel.queue_declare.assert_called_once_with(queue=DLQ_NAME, passive=True)

  @patch("robosystems.tasks.dlq.celery_app.connection_or_acquire")
  def test_get_dlq_stats_warning(self, mock_connection):
    """Test DLQ stats show warning with high message count."""
    mock_channel = Mock()
    mock_queue_info = Mock()
    mock_queue_info.message_count = 150  # Over 100 threshold
    mock_channel.queue_declare.return_value = mock_queue_info

    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    mock_conn.default_channel = mock_channel
    mock_connection.return_value = mock_conn

    result = get_dlq_stats.apply().get()  # type: ignore[attr-defined]

    assert result["message_count"] == 150
    assert result["status"] == "warning"

  @patch("robosystems.tasks.dlq.celery_app.connection_or_acquire")
  @patch("robosystems.tasks.dlq.logger")
  def test_get_dlq_stats_error(self, mock_logger, mock_connection):
    """Test DLQ stats on error."""
    mock_connection.side_effect = Exception("Connection failed")

    with patch("robosystems.tasks.dlq.datetime") as mock_datetime:
      mock_now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
      mock_datetime.now.return_value = mock_now

      result = get_dlq_stats.apply().get()  # type: ignore[attr-defined]

      assert result["queue_name"] == DLQ_NAME
      assert result["message_count"] == -1
      assert result["status"] == "error"
      assert result["error"] == "Connection failed"
      assert result["checked_at"] == mock_now.isoformat()

      # Verify error logged
      mock_logger.error.assert_called_once()


class TestCreateDLQTask:
  """Test create_dlq_task decorator factory."""

  @patch("robosystems.tasks.dlq.celery_app.task")
  def test_create_dlq_task_decorator(self, mock_celery_task):
    """Test create_dlq_task creates task with DLQ support."""
    mock_task_decorator = Mock()
    mock_celery_task.return_value = mock_task_decorator

    def sample_function(x, y):
      return x + y

    decorator = create_dlq_task(name="test.task")
    decorator(sample_function)

    # Verify celery task called with DLQTask base
    mock_celery_task.assert_called_once_with(base=DLQTask, name="test.task")
    mock_task_decorator.assert_called_once_with(sample_function)

  def test_create_dlq_task_with_actual_task(self):
    """Test create_dlq_task with actual task creation."""

    @create_dlq_task(name="test.dlq.task")
    def test_task(x, y):
      return x + y

    # Verify the task is registered
    assert "test.dlq.task" in celery_app.tasks

    # Verify it has DLQ properties
    task_instance = celery_app.tasks["test.dlq.task"]
    assert isinstance(task_instance, DLQTask)
