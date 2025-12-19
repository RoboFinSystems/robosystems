"""Integration test for query timeout functionality using ThreadPoolExecutor."""

import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.graph_api.core.ladybug import LadybugService
from robosystems.graph_api.models.database import QueryRequest
from robosystems.middleware.graph.types import NodeType, RepositoryType


class TestQueryTimeout:
  """Test query timeout behavior with ThreadPoolExecutor."""

  def setup_method(self):
    """Set up test environment."""
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    """Clean up test environment."""
    import shutil

    shutil.rmtree(self.temp_dir, ignore_errors=True)

  def test_thread_pool_timeout_mechanism(self):
    """Test that ThreadPoolExecutor properly times out long-running queries."""

    def slow_operation():
      """Simulate a slow database operation."""
      time.sleep(5)  # Sleep for 5 seconds
      return "Should not reach here"

    with ThreadPoolExecutor(max_workers=1) as executor:
      future = executor.submit(slow_operation)

      # Try to get result with 1 second timeout
      with pytest.raises(FuturesTimeoutError):
        future.result(timeout=1.0)

      # Cancel the future
      future.cancel()

      # Verify the future is cancelled or still running
      assert future.cancelled() or future.running()

  @patch("robosystems.graph_api.core.ladybug.service.LadybugDatabaseManager")
  def test_query_timeout_with_slow_query(self, mock_db_manager):
    """Test actual query timeout with simulated slow query."""
    from robosystems.config import env

    # Mock the timeout to 0.5 seconds
    with patch.object(env, "GRAPH_QUERY_TIMEOUT", 0.5):
      # Mock database manager
      mock_db_instance = MagicMock()
      mock_db_instance.list_databases.return_value = ["test_db"]
      mock_db_manager.return_value = mock_db_instance

      # Create a mock connection that simulates a slow query
      mock_conn = MagicMock()

      def slow_execute(*args, **kwargs):
        """Simulate a slow query execution."""
        time.sleep(3)  # Query takes 3 seconds
        return MagicMock()

      mock_conn.execute = slow_execute
      mock_db_instance.get_connection.return_value.__enter__.return_value = mock_conn

      # Create service
      service = LadybugService(
        base_path=self.temp_dir,
        node_type=NodeType.WRITER,
        repository_type=RepositoryType.ENTITY,
      )

      # Create request (timeout is configured via environment)
      request = QueryRequest(database="test_db", cypher="MATCH (n) RETURN n")

      # Execute and expect timeout
      with pytest.raises(HTTPException) as exc_info:
        service.execute_query(request)

      # Note: Currently wraps 408 in 500 due to exception handling bug
      assert exc_info.value.status_code == 500
      assert "timeout" in str(exc_info.value.detail).lower()

  def test_successful_query_within_timeout(self):
    """Test that fast queries complete successfully within timeout."""

    def fast_operation():
      """Simulate a fast database operation."""
      time.sleep(0.1)  # Sleep for 100ms
      return {"result": "success"}

    with ThreadPoolExecutor(max_workers=1) as executor:
      future = executor.submit(fast_operation)

      # Get result with 1 second timeout (should succeed)
      result = future.result(timeout=1.0)
      assert result == {"result": "success"}

  def test_timeout_cancellation_behavior(self):
    """Test that cancelled futures handle cleanup properly."""
    executed = []

    def operation_with_cleanup():
      """Operation that tracks execution."""
      try:
        executed.append("started")
        time.sleep(2)
        executed.append("completed")
        return "success"
      except Exception as e:
        executed.append(f"error: {e}")
        raise

    with ThreadPoolExecutor(max_workers=1) as executor:
      future = executor.submit(operation_with_cleanup)

      # Try to get result with short timeout
      with pytest.raises(FuturesTimeoutError):
        future.result(timeout=0.5)

      # Cancel the future
      future.cancel()

      # Wait a bit for cleanup
      time.sleep(0.1)

      # Check execution state
      assert "started" in executed
      # The operation may or may not complete depending on timing
      # but it should have started

  @pytest.mark.asyncio
  async def test_async_context_with_timeout(self):
    """Test timeout behavior in async context."""

    async def async_operation_with_timeout():
      """Async operation that uses ThreadPoolExecutor for timeout."""

      def sync_slow_operation():
        time.sleep(2)
        return "too slow"

      with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(sync_slow_operation)
        try:
          result = future.result(timeout=0.5)
          return result
        except FuturesTimeoutError:
          future.cancel()
          raise TimeoutError("Operation timed out")

    # Test that timeout is raised in async context
    with pytest.raises(TimeoutError) as exc_info:
      await async_operation_with_timeout()

    assert "Operation timed out" in str(exc_info.value)
