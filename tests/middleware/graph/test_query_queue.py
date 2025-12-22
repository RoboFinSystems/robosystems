"""Tests for the query queue manager."""

import asyncio
import time
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock, patch

import pytest

from robosystems.middleware.graph.admission_control import AdmissionDecision
from robosystems.middleware.graph.query_queue import (
  QueryQueueManager,
  QueryStatus,
  QueuedQuery,
  get_query_queue,
)


class TestQueryStatus:
  """Tests for QueryStatus enum."""

  def test_status_values(self):
    """Test that all query status values are defined."""
    assert QueryStatus.PENDING.value == "pending"
    assert QueryStatus.RUNNING.value == "running"
    assert QueryStatus.COMPLETED.value == "completed"
    assert QueryStatus.FAILED.value == "failed"
    assert QueryStatus.CANCELLED.value == "cancelled"


class TestQueuedQuery:
  """Tests for QueuedQuery dataclass."""

  @pytest.fixture
  def sample_query(self):
    """Create a sample queued query."""
    return QueuedQuery(
      id="test_query_123",
      cypher="MATCH (n) RETURN n LIMIT 10",
      parameters={"limit": 10},
      graph_id="test_graph",
      user_id="user_123",
      credits_reserved=10.0,
      priority=5,
    )

  def test_query_initialization(self, sample_query):
    """Test query initialization with default values."""
    assert sample_query.id == "test_query_123"
    assert sample_query.cypher == "MATCH (n) RETURN n LIMIT 10"
    assert sample_query.parameters == {"limit": 10}
    assert sample_query.graph_id == "test_graph"
    assert sample_query.user_id == "user_123"
    assert sample_query.credits_reserved == 10.0
    assert sample_query.priority == 5
    assert sample_query.status == QueryStatus.PENDING
    assert sample_query.started_at is None
    assert sample_query.completed_at is None
    assert sample_query.result is None
    assert sample_query.error is None

  def test_wait_time_seconds_not_started(self, sample_query):
    """Test wait time calculation when query hasn't started."""
    # Mock the current time
    sample_query.created_at = datetime.now(UTC) - timedelta(seconds=5)
    wait_time = sample_query.wait_time_seconds
    assert 4.9 < wait_time < 5.1  # Allow for small timing variations

  def test_wait_time_seconds_started(self, sample_query):
    """Test wait time calculation when query has started."""
    sample_query.created_at = datetime.now(UTC) - timedelta(seconds=10)
    sample_query.started_at = datetime.now(UTC) - timedelta(seconds=3)
    wait_time = sample_query.wait_time_seconds
    assert 6.9 < wait_time < 7.1  # 10 - 3 seconds

  def test_execution_time_seconds_not_completed(self, sample_query):
    """Test execution time when query hasn't completed."""
    sample_query.started_at = datetime.now(UTC)
    assert sample_query.execution_time_seconds is None

  def test_execution_time_seconds_completed(self, sample_query):
    """Test execution time calculation when query has completed."""
    sample_query.started_at = datetime.now(UTC) - timedelta(seconds=5)
    sample_query.completed_at = datetime.now(UTC)
    exec_time = sample_query.execution_time_seconds
    assert 4.9 < exec_time < 5.1  # Allow for small timing variations


class TestQueryQueueManager:
  """Tests for QueryQueueManager."""

  @pytest.fixture
  def queue_manager(self):
    """Create a QueryQueueManager instance."""
    return QueryQueueManager(
      max_queue_size=100,
      max_concurrent_queries=10,
      max_queries_per_user=5,
      query_timeout=60,
    )

  @pytest.fixture
  def mock_admission_controller(self):
    """Mock admission controller."""
    with patch(
      "robosystems.middleware.graph.query_queue.get_admission_controller"
    ) as mock:
      controller = Mock()
      controller.check_admission.return_value = (AdmissionDecision.ACCEPT, None)
      controller.get_health_status.return_value = {
        "state": "healthy",
        "pressure_score": 0.3,
      }
      mock.return_value = controller
      yield controller

  @pytest.fixture
  def mock_metrics(self):
    """Mock metrics recording."""
    with patch(
      "robosystems.middleware.graph.query_queue.record_query_queue_metrics"
    ) as mock:
      yield mock

  def test_initialization(self, queue_manager):
    """Test queue manager initialization."""
    assert queue_manager.max_queue_size == 100
    assert queue_manager.max_concurrent_queries == 10
    assert queue_manager.max_queries_per_user == 5
    assert queue_manager.query_timeout == 60
    assert queue_manager._started is False
    assert queue_manager._query_executor is None
    assert len(queue_manager._queries) == 0
    assert len(queue_manager._user_query_counts) == 0

  @pytest.mark.asyncio
  async def test_ensure_started(self, queue_manager):
    """Test that worker task is started on first use."""
    assert queue_manager._started is False
    assert queue_manager._worker_task is None

    await queue_manager._ensure_started()

    assert queue_manager._started is True
    assert queue_manager._worker_task is not None
    assert not queue_manager._worker_task.done()

    # Clean up
    queue_manager._worker_task.cancel()
    try:
      await queue_manager._worker_task
    except asyncio.CancelledError:
      pass

  @pytest.mark.asyncio
  async def test_submit_query_success(
    self, queue_manager, mock_admission_controller, mock_metrics
  ):
    """Test successful query submission."""
    query_id = await queue_manager.submit_query(
      cypher="MATCH (n) RETURN n",
      parameters=None,
      graph_id="test_graph",
      user_id="user_123",
      credits_required=5.0,
      priority=7,
    )

    assert query_id.startswith("q_")
    assert query_id in queue_manager._queries
    assert queue_manager._user_query_counts["user_123"] == 1
    assert queue_manager._queue.qsize() == 1

    # Check metrics were recorded
    mock_metrics.assert_called_with(
      metric_type="submission",
      graph_id="test_graph",
      user_id="user_123",
      priority=7,
      success=True,
    )

    # Clean up
    if queue_manager._worker_task:
      queue_manager._worker_task.cancel()
      try:
        await queue_manager._worker_task
      except asyncio.CancelledError:
        pass

  @pytest.mark.asyncio
  async def test_submit_query_admission_rejection(
    self, queue_manager, mock_admission_controller, mock_metrics
  ):
    """Test query submission rejected by admission control."""
    mock_admission_controller.check_admission.return_value = (
      AdmissionDecision.REJECT_MEMORY,
      "Memory usage too high",
    )

    with pytest.raises(Exception, match="Query rejected: Memory usage too high"):
      await queue_manager.submit_query(
        cypher="MATCH (n) RETURN n",
        parameters=None,
        graph_id="test_graph",
        user_id="user_123",
        credits_required=5.0,
        priority=5,
      )

    # Check rejection metric was recorded
    mock_metrics.assert_called_with(
      metric_type="submission",
      graph_id="test_graph",
      user_id="user_123",
      priority=5,
      success=False,
      rejection_reason="memory",
    )

  @pytest.mark.asyncio
  async def test_submit_query_queue_full(self, mock_admission_controller, mock_metrics):
    """Test query submission when queue is full."""
    queue_manager = QueryQueueManager(
      max_queue_size=1,  # Very small queue
      max_concurrent_queries=10,
      max_queries_per_user=5,
      query_timeout=60,
    )

    # Submit first query successfully
    await queue_manager.submit_query(
      cypher="MATCH (n) RETURN n",
      parameters=None,
      graph_id="test_graph",
      user_id="user_123",
      credits_required=5.0,
      priority=5,
    )

    # Second query should fail
    with pytest.raises(Exception, match="Query queue is full"):
      await queue_manager.submit_query(
        cypher="MATCH (n) RETURN n",
        parameters=None,
        graph_id="test_graph",
        user_id="user_456",
        credits_required=5.0,
        priority=5,
      )

    # Clean up
    if queue_manager._worker_task:
      queue_manager._worker_task.cancel()
      try:
        await queue_manager._worker_task
      except asyncio.CancelledError:
        pass

  @pytest.mark.asyncio
  async def test_submit_query_user_limit(self, mock_admission_controller, mock_metrics):
    """Test query submission when user limit is exceeded."""
    queue_manager = QueryQueueManager(
      max_queue_size=100,
      max_concurrent_queries=10,
      max_queries_per_user=2,  # Low per-user limit
      query_timeout=60,
    )

    # Submit two queries successfully
    for i in range(2):
      await queue_manager.submit_query(
        cypher=f"MATCH (n) RETURN n LIMIT {i}",
        parameters=None,
        graph_id="test_graph",
        user_id="user_123",
        credits_required=5.0,
        priority=5,
      )

    # Third query should fail
    with pytest.raises(Exception, match="User query limit exceeded"):
      await queue_manager.submit_query(
        cypher="MATCH (n) RETURN n",
        parameters=None,
        graph_id="test_graph",
        user_id="user_123",
        credits_required=5.0,
        priority=5,
      )

    # Clean up
    if queue_manager._worker_task:
      queue_manager._worker_task.cancel()
      try:
        await queue_manager._worker_task
      except asyncio.CancelledError:
        pass

  @pytest.mark.asyncio
  async def test_get_query_status_pending(
    self, queue_manager, mock_admission_controller
  ):
    """Test getting status of pending query."""
    query_id = await queue_manager.submit_query(
      cypher="MATCH (n) RETURN n",
      parameters=None,
      graph_id="test_graph",
      user_id="user_123",
      credits_required=5.0,
      priority=5,
    )

    status = await queue_manager.get_query_status(query_id)

    assert status["id"] == query_id
    assert status["status"] == QueryStatus.PENDING
    assert "queue_position" in status
    assert "wait_time" in status
    assert "estimated_wait" in status

    # Clean up
    if queue_manager._worker_task:
      queue_manager._worker_task.cancel()
      try:
        await queue_manager._worker_task
      except asyncio.CancelledError:
        pass

  @pytest.mark.asyncio
  async def test_get_query_status_running(self, queue_manager):
    """Test getting status of running query."""
    # Create a query and mark it as running
    query = QueuedQuery(
      id="test_query",
      cypher="MATCH (n) RETURN n",
      parameters=None,
      graph_id="test_graph",
      user_id="user_123",
      credits_reserved=5.0,
      priority=5,
    )
    query.status = QueryStatus.RUNNING
    query.started_at = datetime.now(UTC)

    queue_manager._queries["test_query"] = query
    queue_manager._running_queries["test_query"] = Mock()  # Mock task

    status = await queue_manager.get_query_status("test_query")

    assert status["id"] == "test_query"
    assert status["status"] == QueryStatus.RUNNING
    assert "wait_time" in status
    assert "started_at" in status

  @pytest.mark.asyncio
  async def test_get_query_status_completed(self, queue_manager):
    """Test getting status of completed query."""
    query = QueuedQuery(
      id="test_query",
      cypher="MATCH (n) RETURN n",
      parameters=None,
      graph_id="test_graph",
      user_id="user_123",
      credits_reserved=5.0,
      priority=5,
    )
    query.status = QueryStatus.COMPLETED
    query.started_at = datetime.now(UTC) - timedelta(seconds=5)
    query.completed_at = datetime.now(UTC)
    query.result = {"nodes": []}

    queue_manager._completed_queries["test_query"] = query

    status = await queue_manager.get_query_status("test_query")

    assert status["id"] == "test_query"
    assert status["status"] == QueryStatus.COMPLETED
    assert "execution_time" in status
    assert "completed_at" in status
    assert status["error"] is None

  @pytest.mark.asyncio
  async def test_get_query_status_not_found(self, queue_manager):
    """Test getting status of non-existent query."""
    status = await queue_manager.get_query_status("nonexistent")
    assert status is None

  @pytest.mark.asyncio
  async def test_get_query_result_completed(self, queue_manager):
    """Test getting result of completed query."""
    query = QueuedQuery(
      id="test_query",
      cypher="MATCH (n) RETURN n",
      parameters=None,
      graph_id="test_graph",
      user_id="user_123",
      credits_reserved=5.0,
      priority=5,
    )
    query.status = QueryStatus.COMPLETED
    query.started_at = datetime.now(UTC) - timedelta(seconds=5)
    query.completed_at = datetime.now(UTC)
    query.result = {"nodes": [1, 2, 3]}

    queue_manager._completed_queries["test_query"] = query

    result = await queue_manager.get_query_result("test_query")

    assert result["status"] == "completed"
    assert result["data"] == {"nodes": [1, 2, 3]}
    assert "execution_time" in result

  @pytest.mark.asyncio
  async def test_get_query_result_failed(self, queue_manager):
    """Test getting result of failed query."""
    query = QueuedQuery(
      id="test_query",
      cypher="MATCH (n) RETURN n",
      parameters=None,
      graph_id="test_graph",
      user_id="user_123",
      credits_reserved=5.0,
      priority=5,
    )
    query.status = QueryStatus.FAILED
    query.error = "Query syntax error"

    queue_manager._completed_queries["test_query"] = query

    result = await queue_manager.get_query_result("test_query")

    assert result["status"] == QueryStatus.FAILED
    assert result["error"] == "Query syntax error"

  @pytest.mark.asyncio
  async def test_get_query_result_wait_timeout(self, queue_manager):
    """Test waiting for query result with timeout."""
    # Submit a query that won't complete
    query = QueuedQuery(
      id="test_query",
      cypher="MATCH (n) RETURN n",
      parameters=None,
      graph_id="test_graph",
      user_id="user_123",
      credits_reserved=5.0,
      priority=5,
    )
    queue_manager._queries["test_query"] = query

    # Wait for 0.2 seconds
    start = time.time()
    result = await queue_manager.get_query_result("test_query", wait_seconds=0.2)
    elapsed = time.time() - start

    assert 0.19 < elapsed < 0.3  # Allow for timing variations
    assert result["status"] == QueryStatus.PENDING

  @pytest.mark.asyncio
  async def test_cancel_query_success(self, queue_manager, mock_metrics):
    """Test cancelling a pending query."""
    query = QueuedQuery(
      id="test_query",
      cypher="MATCH (n) RETURN n",
      parameters=None,
      graph_id="test_graph",
      user_id="user_123",
      credits_reserved=5.0,
      priority=5,
    )
    queue_manager._queries["test_query"] = query
    queue_manager._user_query_counts["user_123"] = 1

    result = await queue_manager.cancel_query("test_query", "user_123")

    assert result is True
    assert (
      queue_manager._completed_queries["test_query"].status == QueryStatus.CANCELLED
    )
    assert queue_manager._user_query_counts.get("user_123", 0) == 0

    # Check metrics were recorded
    mock_metrics.assert_called_with(
      metric_type="execution",
      graph_id="test_graph",
      user_id="user_123",
      execution_time_seconds=0,
      status="cancelled",
    )

  @pytest.mark.asyncio
  async def test_cancel_query_wrong_user(self, queue_manager):
    """Test cancelling query with wrong user."""
    query = QueuedQuery(
      id="test_query",
      cypher="MATCH (n) RETURN n",
      parameters=None,
      graph_id="test_graph",
      user_id="user_123",
      credits_reserved=5.0,
      priority=5,
    )
    queue_manager._queries["test_query"] = query

    result = await queue_manager.cancel_query("test_query", "user_456")
    assert result is False

  @pytest.mark.asyncio
  async def test_cancel_query_already_running(self, queue_manager):
    """Test cancelling a running query."""
    query = QueuedQuery(
      id="test_query",
      cypher="MATCH (n) RETURN n",
      parameters=None,
      graph_id="test_graph",
      user_id="user_123",
      credits_reserved=5.0,
      priority=5,
    )
    query.status = QueryStatus.RUNNING
    queue_manager._queries["test_query"] = query

    result = await queue_manager.cancel_query("test_query", "user_123")
    assert result is False

  def test_set_query_executor(self, queue_manager):
    """Test setting query executor function."""
    executor = Mock()
    queue_manager.set_query_executor(executor)
    assert queue_manager._query_executor == executor

  @pytest.mark.asyncio
  async def test_execute_query_success(self, queue_manager, mock_metrics):
    """Test successful query execution."""

    # Mock executor
    async def mock_executor(cypher, params, graph_id):
      return {"result": "success"}

    queue_manager._query_executor = mock_executor

    query = QueuedQuery(
      id="test_query",
      cypher="MATCH (n) RETURN n",
      parameters=None,
      graph_id="test_graph",
      user_id="user_123",
      credits_reserved=5.0,
      priority=5,
    )
    query.started_at = datetime.now(UTC)

    await queue_manager._execute_query(query)

    assert query.status == QueryStatus.COMPLETED
    assert query.result == {"result": "success"}
    assert query.completed_at is not None

  @pytest.mark.asyncio
  async def test_execute_query_timeout(self, queue_manager, mock_metrics):
    """Test query execution timeout."""

    # Mock executor that takes too long
    async def slow_executor(cypher, params, graph_id):
      await asyncio.sleep(10)
      return {"result": "success"}

    queue_manager._query_executor = slow_executor
    queue_manager.query_timeout = 0.1  # Very short timeout

    query = QueuedQuery(
      id="test_query",
      cypher="MATCH (n) RETURN n",
      parameters=None,
      graph_id="test_graph",
      user_id="user_123",
      credits_reserved=5.0,
      priority=5,
    )
    query.started_at = datetime.now(UTC)

    await queue_manager._execute_query(query)

    assert query.status == QueryStatus.FAILED
    assert query.error is not None
    assert "timeout" in query.error

  @pytest.mark.asyncio
  async def test_execute_query_error(self, queue_manager, mock_metrics):
    """Test query execution with error."""

    # Mock executor that raises error
    async def failing_executor(cypher, params, graph_id):
      raise ValueError("Invalid query")

    queue_manager._query_executor = failing_executor

    query = QueuedQuery(
      id="test_query",
      cypher="MATCH (n) RETURN n",
      parameters=None,
      graph_id="test_graph",
      user_id="user_123",
      credits_reserved=5.0,
      priority=5,
    )
    query.started_at = datetime.now(UTC)

    await queue_manager._execute_query(query)

    assert query.status == QueryStatus.FAILED
    assert query.error == "Invalid query"

  @pytest.mark.asyncio
  async def test_execute_query_no_executor(self, queue_manager, mock_metrics):
    """Test query execution without executor set."""
    query = QueuedQuery(
      id="test_query",
      cypher="MATCH (n) RETURN n",
      parameters=None,
      graph_id="test_graph",
      user_id="user_123",
      credits_reserved=5.0,
      priority=5,
    )
    query.started_at = datetime.now(UTC)

    await queue_manager._execute_query(query)

    assert query.status == QueryStatus.FAILED
    assert query.error is not None
    assert "Query executor not configured" in query.error

  def test_cleanup_completed_queries(self, queue_manager):
    """Test cleanup of completed queries cache."""
    queue_manager._max_completed = 3

    # Add 5 queries
    for i in range(5):
      query = QueuedQuery(
        id=f"query_{i}",
        cypher="MATCH (n) RETURN n",
        parameters=None,
        graph_id="test_graph",
        user_id="user_123",
        credits_reserved=5.0,
        priority=5,
      )
      queue_manager._completed_queries[f"query_{i}"] = query

    queue_manager._cleanup_completed_queries()

    # Should only keep last 3
    assert len(queue_manager._completed_queries) == 3
    assert "query_0" not in queue_manager._completed_queries
    assert "query_1" not in queue_manager._completed_queries
    assert "query_4" in queue_manager._completed_queries

  def test_estimate_queue_position(self, queue_manager):
    """Test queue position estimation."""
    # Mock the queue size
    with patch.object(queue_manager._queue, "qsize", return_value=5):
      position = queue_manager._estimate_queue_position("any_query")
      assert position == 5

  def test_estimate_wait_time(self, queue_manager):
    """Test wait time estimation."""
    wait_time = queue_manager._estimate_wait_time(20)
    # 20 queries / 10 concurrent * 2 seconds per query = 4 seconds
    assert wait_time == 4.0

  def test_get_stats(self, queue_manager):
    """Test getting queue statistics."""
    # Add some test data
    queue_manager._running_queries = {"q1": Mock(), "q2": Mock()}
    queue_manager._completed_queries = {"q3": Mock()}
    queue_manager._user_query_counts = {"user1": 2, "user2": 1}

    with patch.object(queue_manager._queue, "qsize", return_value=5):
      stats = queue_manager.get_stats()

      assert stats["queue_size"] == 5
      assert stats["running_queries"] == 2
      assert stats["completed_queries"] == 1
      assert stats["users_with_queries"] == 2
      assert stats["capacity_used"] == 0.05  # 5/100

  def test_get_deep_health_status(self, queue_manager, mock_admission_controller):
    """Test getting comprehensive health status."""
    # Set up some queue state
    queue_manager._running_queries = {"q1": Mock()}

    with patch.object(queue_manager._queue, "qsize", return_value=10):
      health = queue_manager.get_deep_health_status()

      assert "queue" in health
      assert "system" in health
      assert "limits" in health
      assert health["queue"]["queue_size"] == 10
      assert health["queue"]["running_queries"] == 1
      assert health["limits"]["max_queue_size"] == 100
      assert health["system"]["state"] == "healthy"

  def test_get_queue_metrics_by_priority(self, queue_manager):
    """Test getting queue metrics by priority."""
    # Add queries with different priorities
    for i, priority in enumerate([1, 5, 5, 10]):
      query = QueuedQuery(
        id=f"query_{i}",
        cypher="MATCH (n) RETURN n",
        parameters=None,
        graph_id="test_graph",
        user_id="user_123",
        credits_reserved=5.0,
        priority=priority,
      )
      queue_manager._queries[f"query_{i}"] = query

    metrics = queue_manager.get_queue_metrics_by_priority()

    assert metrics[1] == 1
    assert metrics[5] == 2
    assert metrics[10] == 1


class TestGetQueryQueue:
  """Tests for get_query_queue singleton function."""

  @patch("robosystems.config.query_queue.QueryQueueConfig")
  def test_get_query_queue_singleton(self, mock_config):
    """Test that get_query_queue returns singleton."""
    mock_config.get_queue_config.return_value = {
      "max_queue_size": 100,
      "max_concurrent_queries": 10,
      "max_queries_per_user": 5,
      "query_timeout": 60,
    }

    # Reset global state
    import robosystems.middleware.graph.query_queue as qq

    qq._queue_manager = None

    queue1 = get_query_queue()
    queue2 = get_query_queue()

    assert queue1 is queue2
    assert mock_config.get_queue_config.call_count == 1

  @patch("robosystems.config.query_queue.QueryQueueConfig")
  def test_get_query_queue_configuration(self, mock_config):
    """Test that queue is configured correctly."""
    mock_config.get_queue_config.return_value = {
      "max_queue_size": 200,
      "max_concurrent_queries": 20,
      "max_queries_per_user": 10,
      "query_timeout": 120,
    }

    # Reset global state
    import robosystems.middleware.graph.query_queue as qq

    qq._queue_manager = None

    queue = get_query_queue()

    assert queue.max_queue_size == 200
    assert queue.max_concurrent_queries == 20
    assert queue.max_queries_per_user == 10
    assert queue.query_timeout == 120
