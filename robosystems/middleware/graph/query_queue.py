"""
Lightweight Query Queue Manager

Provides in-memory query queuing with asyncio for the public API layer.
No external dependencies, minimal latency overhead.
"""

import asyncio
import time
import uuid
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from robosystems.logger import logger
from robosystems.middleware.graph.admission_control import (
  AdmissionDecision,
  get_admission_controller,
)
from robosystems.middleware.otel.metrics import record_query_queue_metrics


class QueryStatus(str, Enum):
  """Query execution status."""

  PENDING = "pending"
  RUNNING = "running"
  COMPLETED = "completed"
  FAILED = "failed"
  CANCELLED = "cancelled"


@dataclass
class QueuedQuery:
  """Represents a queued query."""

  id: str
  cypher: str
  parameters: dict[str, Any] | None
  graph_id: str
  user_id: str
  credits_reserved: float
  priority: int = 5
  created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
  started_at: datetime | None = None
  completed_at: datetime | None = None
  status: QueryStatus = QueryStatus.PENDING
  result: Any | None = None
  error: str | None = None

  @property
  def wait_time_seconds(self) -> float:
    """Get time spent waiting in queue."""
    if self.started_at:
      return (self.started_at - self.created_at).total_seconds()
    return (datetime.now(UTC) - self.created_at).total_seconds()

  @property
  def execution_time_seconds(self) -> float | None:
    """Get query execution time."""
    if self.started_at and self.completed_at:
      return (self.completed_at - self.started_at).total_seconds()
    return None


class QueryQueueManager:
  """
  Lightweight query queue manager using asyncio.

  Features:
  - In-memory queue with size limits
  - Priority-based execution
  - Credit reservation before queuing
  - Backpressure handling
  - No external dependencies
  """

  def __init__(
    self,
    max_queue_size: int = 1000,
    max_concurrent_queries: int = 50,
    max_queries_per_user: int = 10,
    query_timeout: int = 300,  # 5 minutes
  ):
    """
    Initialize query queue manager.

    Args:
        max_queue_size: Maximum queries in queue
        max_concurrent_queries: Maximum simultaneous executions
        max_queries_per_user: Maximum queries per user
        query_timeout: Query execution timeout in seconds
    """
    self.max_queue_size = max_queue_size
    self.max_concurrent_queries = max_concurrent_queries
    self.max_queries_per_user = max_queries_per_user
    self.query_timeout = query_timeout

    # Query storage
    self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=max_queue_size)
    self._queries: dict[str, QueuedQuery] = {}
    self._user_query_counts: dict[str, int] = {}

    # Execution tracking
    self._running_queries: dict[str, asyncio.Task] = {}
    self._completed_queries: OrderedDict[str, QueuedQuery] = OrderedDict()
    self._max_completed = 10000  # Keep last N completed queries

    # Executor function (set by router)
    self._query_executor: Callable | None = None

    # Worker task (started on first use)
    self._worker_task: asyncio.Task | None = None
    self._started = False

    logger.info(
      f"Query queue initialized: max_size={max_queue_size}, "
      f"max_concurrent={max_concurrent_queries}"
    )

  async def _ensure_started(self):
    """Ensure the worker task is started."""
    if not self._started:
      self._started = True
      self._worker_task = asyncio.create_task(self._process_queue())
      logger.info("Query queue worker started")

  async def submit_query(
    self,
    cypher: str,
    parameters: dict[str, Any] | None,
    graph_id: str,
    user_id: str,
    credits_required: float,
    priority: int = 5,
  ) -> str:
    """
    Submit a query to the queue.

    Args:
        cypher: Query to execute
        parameters: Query parameters
        graph_id: Target graph
        user_id: User submitting query
        credits_required: Credits needed for query
        priority: Query priority (1-10, higher = more important)

    Returns:
        Query ID for tracking

    Raises:
        Exception: If queue is full or user limit exceeded
    """
    # Ensure worker is started
    await self._ensure_started()

    # Get admission controller
    admission_controller = get_admission_controller()

    # Check admission control first
    queue_depth = self._queue.qsize()
    active_queries = len(self._running_queries)

    decision, reason = admission_controller.check_admission(
      queue_depth=queue_depth,
      max_queue_size=self.max_queue_size,
      active_queries=active_queries,
      priority=priority,
    )

    if decision != AdmissionDecision.ACCEPT:
      # Map decision to rejection reason
      rejection_type = {
        AdmissionDecision.REJECT_MEMORY: "memory",
        AdmissionDecision.REJECT_CPU: "cpu",
        AdmissionDecision.REJECT_QUEUE: "queue_full",
        AdmissionDecision.REJECT_LOAD_SHED: "load_shed",
      }.get(decision, "unknown")

      # Record rejection metric
      record_query_queue_metrics(
        metric_type="submission",
        graph_id=graph_id,
        user_id=user_id,
        priority=priority,
        success=False,
        rejection_reason=rejection_type,
      )

      raise Exception(f"Query rejected: {reason}")

    # Check queue capacity
    if self._queue.qsize() >= self.max_queue_size:
      # Record rejection metric
      record_query_queue_metrics(
        metric_type="submission",
        graph_id=graph_id,
        user_id=user_id,
        priority=priority,
        success=False,
        rejection_reason="queue_full",
      )
      raise Exception(
        f"Query queue is full ({self.max_queue_size} queries). Please retry later."
      )

    # Check per-user limit
    user_count = self._user_query_counts.get(user_id, 0)
    if user_count >= self.max_queries_per_user:
      # Record rejection metric
      record_query_queue_metrics(
        metric_type="submission",
        graph_id=graph_id,
        user_id=user_id,
        priority=priority,
        success=False,
        rejection_reason="user_limit",
      )
      raise Exception(
        f"User query limit exceeded ({self.max_queries_per_user} queries). "
        "Please wait for existing queries to complete."
      )

    # Create query
    query_id = f"q_{uuid.uuid4().hex[:12]}"
    query = QueuedQuery(
      id=query_id,
      cypher=cypher,
      parameters=parameters,
      graph_id=graph_id,
      user_id=user_id,
      credits_reserved=credits_required,
      priority=priority,
    )

    # Store query
    self._queries[query_id] = query
    self._user_query_counts[user_id] = user_count + 1

    # Add to priority queue (negative priority for max heap behavior)
    await self._queue.put((-priority, query.created_at.timestamp(), query_id))

    # Record successful submission metric
    record_query_queue_metrics(
      metric_type="submission",
      graph_id=graph_id,
      user_id=user_id,
      priority=priority,
      success=True,
    )

    logger.info(
      f"Query {query_id} submitted: user={user_id}, "
      f"priority={priority}, queue_size={self._queue.qsize()}"
    )

    return query_id

  async def get_query_status(self, query_id: str) -> dict[str, Any] | None:
    """Get current status of a query."""
    # Check running queries
    if query_id in self._running_queries:
      query = self._queries.get(query_id)
      if query:
        return {
          "id": query_id,
          "status": QueryStatus.RUNNING,
          "wait_time": query.wait_time_seconds,
          "started_at": query.started_at.isoformat() if query.started_at else None,
        }

    # Check completed queries
    if query_id in self._completed_queries:
      query = self._completed_queries[query_id]
      return {
        "id": query_id,
        "status": query.status,
        "wait_time": query.wait_time_seconds,
        "execution_time": query.execution_time_seconds,
        "completed_at": query.completed_at.isoformat() if query.completed_at else None,
        "error": query.error,
      }

    # Check pending queries
    if query_id in self._queries:
      query = self._queries[query_id]
      position = self._estimate_queue_position(query_id)
      return {
        "id": query_id,
        "status": QueryStatus.PENDING,
        "queue_position": position,
        "wait_time": query.wait_time_seconds,
        "estimated_wait": self._estimate_wait_time(position),
      }

    return None

  async def get_query_result(
    self, query_id: str, wait_seconds: int = 0
  ) -> dict[str, Any] | None:
    """
    Get query result, optionally waiting for completion.

    Args:
        query_id: Query to check
        wait_seconds: How long to wait for result (0 = don't wait)

    Returns:
        Query result if available
    """
    start_time = time.time()

    while True:
      # Check if completed
      if query_id in self._completed_queries:
        query = self._completed_queries[query_id]
        if query.status == QueryStatus.COMPLETED:
          return {
            "status": "completed",
            "data": query.result,
            "execution_time": query.execution_time_seconds,
          }
        else:
          return {
            "status": query.status,
            "error": query.error,
          }

      # Check if we should keep waiting
      elapsed = time.time() - start_time
      if elapsed >= wait_seconds:
        # Return current status
        status = await self.get_query_status(query_id)
        return status

      # Wait a bit before checking again
      await asyncio.sleep(0.1)

  async def cancel_query(self, query_id: str, user_id: str) -> bool:
    """
    Cancel a pending query.

    Args:
        query_id: Query to cancel
        user_id: User requesting cancellation

    Returns:
        True if cancelled, False if not found or already running
    """
    query = self._queries.get(query_id)
    if not query or query.user_id != user_id:
      return False

    if query.status != QueryStatus.PENDING:
      return False  # Can't cancel running/completed queries

    # Mark as cancelled
    query.status = QueryStatus.CANCELLED
    query.completed_at = datetime.now(UTC)

    # Record cancellation metric
    record_query_queue_metrics(
      metric_type="execution",
      graph_id=query.graph_id,
      user_id=query.user_id,
      execution_time_seconds=0,  # Never executed
      status="cancelled",
    )

    # Move to completed
    self._completed_queries[query_id] = query
    self._cleanup_completed_queries()

    # Update user count
    self._user_query_counts[user_id] = max(0, self._user_query_counts[user_id] - 1)

    logger.info(f"Query {query_id} cancelled by user {user_id}")
    return True

  def set_query_executor(self, executor: Callable):
    """Set the function to execute queries."""
    self._query_executor = executor

  async def _process_queue(self):
    """Background worker to process queued queries."""
    logger.info("Query queue worker started")

    while True:
      try:
        # Wait for capacity
        while len(self._running_queries) >= self.max_concurrent_queries:
          await asyncio.sleep(0.1)

        # Get next query (with timeout to allow periodic checks)
        try:
          priority, timestamp, query_id = await asyncio.wait_for(
            self._queue.get(), timeout=1.0
          )
        except TimeoutError:
          continue

        # Get query details
        query = self._queries.get(query_id)
        if not query or query.status != QueryStatus.PENDING:
          continue  # Query was cancelled

        # Start execution
        query.status = QueryStatus.RUNNING
        query.started_at = datetime.now(UTC)

        # Record wait time metric
        record_query_queue_metrics(
          metric_type="wait_time",
          graph_id=query.graph_id,
          user_id=query.user_id,
          priority=query.priority,
          wait_time_seconds=query.wait_time_seconds,
        )

        # Update concurrent executions
        record_query_queue_metrics(
          metric_type="concurrent_update",
          graph_id=query.graph_id,
          user_id=query.user_id,
          delta=1,
        )

        # Create execution task
        task = asyncio.create_task(self._execute_query(query))
        self._running_queries[query_id] = task

        logger.info(
          f"Started query {query_id}: wait_time={query.wait_time_seconds:.1f}s, "
          f"running={len(self._running_queries)}"
        )

      except Exception as e:
        logger.error(f"Queue worker error: {e}")
        await asyncio.sleep(1)

  async def _execute_query(self, query: QueuedQuery):
    """Execute a query with timeout and error handling."""
    try:
      if not self._query_executor:
        raise Exception("Query executor not configured")

      # Execute with timeout
      result = await asyncio.wait_for(
        self._query_executor(
          query.cypher,
          query.parameters,
          query.graph_id,
        ),
        timeout=self.query_timeout,
      )

      # Mark successful
      query.status = QueryStatus.COMPLETED
      query.result = result

    except TimeoutError:
      query.status = QueryStatus.FAILED
      query.error = f"Query timeout after {self.query_timeout} seconds"
      logger.error(f"Query {query.id} timed out")

    except Exception as e:
      query.status = QueryStatus.FAILED
      query.error = str(e)
      logger.error(f"Query {query.id} failed: {e}")

    finally:
      # Clean up
      query.completed_at = datetime.now(UTC)

      # Record execution metrics
      if query.execution_time_seconds is not None:
        status_map = {
          QueryStatus.COMPLETED: "completed",
          QueryStatus.FAILED: "failed",
          QueryStatus.CANCELLED: "cancelled",
        }

        error_type = None
        if query.status == QueryStatus.FAILED:
          if "timeout" in (query.error or ""):
            error_type = "timeout"
          else:
            error_type = "execution_error"

        record_query_queue_metrics(
          metric_type="execution",
          graph_id=query.graph_id,
          user_id=query.user_id,
          execution_time_seconds=query.execution_time_seconds,
          status=status_map.get(query.status, "unknown"),
          error_type=error_type,
        )
      self._completed_queries[query.id] = query
      self._cleanup_completed_queries()

      # Remove from running
      self._running_queries.pop(query.id, None)

      # Update concurrent executions
      record_query_queue_metrics(
        metric_type="concurrent_update",
        graph_id=query.graph_id,
        user_id=query.user_id,
        delta=-1,
      )

      # Update user count
      count = self._user_query_counts.get(query.user_id, 0)
      self._user_query_counts[query.user_id] = max(0, count - 1)
      if self._user_query_counts[query.user_id] == 0:
        del self._user_query_counts[query.user_id]

      # Remove from main storage after a delay
      asyncio.create_task(self._cleanup_query(query.id))

  async def _cleanup_query(self, query_id: str, delay: int = 300):
    """Remove query from main storage after delay."""
    await asyncio.sleep(delay)  # Keep for 5 minutes
    self._queries.pop(query_id, None)

  def _cleanup_completed_queries(self):
    """Limit size of completed queries cache."""
    while len(self._completed_queries) > self._max_completed:
      self._completed_queries.popitem(last=False)

  def _estimate_queue_position(self, query_id: str) -> int:
    """Estimate position in queue (approximate)."""
    # This is approximate since we can't efficiently inspect priority queue
    return self._queue.qsize()

  def _estimate_wait_time(self, position: int) -> float:
    """Estimate wait time based on position."""
    # Simple estimate: 2 seconds per query
    avg_query_time = 2.0
    concurrent = self.max_concurrent_queries
    return (position / concurrent) * avg_query_time

  def get_stats(self) -> dict[str, Any]:
    """Get queue statistics."""
    return {
      "queue_size": self._queue.qsize(),
      "running_queries": len(self._running_queries),
      "completed_queries": len(self._completed_queries),
      "users_with_queries": len(self._user_query_counts),
      "capacity_used": self._queue.qsize() / self.max_queue_size,
    }

  def get_deep_health_status(self) -> dict[str, Any]:
    """Get comprehensive health status including system resources."""
    admission_controller = get_admission_controller()

    # Get basic stats
    stats = self.get_stats()

    # Get admission control health
    health = admission_controller.get_health_status(
      queue_depth=stats["queue_size"],
      max_queue_size=self.max_queue_size,
      active_queries=stats["running_queries"],
    )

    # Combine stats and health
    return {
      "queue": stats,
      "system": health,
      "limits": {
        "max_queue_size": self.max_queue_size,
        "max_concurrent_queries": self.max_concurrent_queries,
        "max_queries_per_user": self.max_queries_per_user,
        "query_timeout": self.query_timeout,
      },
    }

  def get_queue_metrics_by_priority(self) -> dict[int, int]:
    """Get queue size broken down by priority (for metrics)."""
    # Note: This is approximate since we can't efficiently inspect priority queue
    # In production, you might want to maintain separate counters
    priority_counts = {}

    # Count priorities from pending queries
    for query_id, query in self._queries.items():
      if query.status == QueryStatus.PENDING:
        priority = query.priority
        priority_counts[priority] = priority_counts.get(priority, 0) + 1

    return priority_counts


# Global instance
_queue_manager: QueryQueueManager | None = None


def get_query_queue() -> QueryQueueManager:
  """Get the global query queue instance."""
  global _queue_manager
  if _queue_manager is None:
    # Load configuration
    from robosystems.config.query_queue import QueryQueueConfig

    _queue_manager = QueryQueueManager(**QueryQueueConfig.get_queue_config())
  return _queue_manager
