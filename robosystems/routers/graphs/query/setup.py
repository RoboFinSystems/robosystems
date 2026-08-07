"""Initialization for the query executor that drains the query queue."""

import asyncio
from typing import Any

from robosystems.logger import logger
from robosystems.middleware.graph.query_queue import get_query_queue
from robosystems.middleware.graph.router import GraphRouter
from robosystems.middleware.robustness import CircuitBreakerManager


def _get_query_operation_type(graph_id: str) -> str:
  """
  Determine the correct operation type for query operations.

  For consistency with distributed LadybugDB architecture:
  - User graphs: Always use 'write' to ensure writer cluster routing
  - Shared repositories (and subgraphs): Use 'read' for reader cluster routing
  """
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph

  if is_shared_repository_or_subgraph(graph_id):
    return "read"
  else:
    return "write"


def setup_query_executor():
  """
  Set up the query executor function for the queue manager.

  This function initializes the query executor that will process
  queued queries asynchronously. It should be called during
  application startup.
  """
  queue_manager = get_query_queue()
  circuit_breaker = CircuitBreakerManager()

  async def executor(
    cypher: str, parameters: dict[str, Any] | None, graph_id: str
  ) -> dict[str, Any]:
    """Execute a queued query, returning results plus metadata."""
    try:
      # Get the appropriate repository
      graph_router = GraphRouter()
      operation_type = _get_query_operation_type(graph_id)
      repository = await graph_router.get_repository(graph_id, operation_type)

      # Execute query with proper async handling
      if hasattr(repository, "execute_query") and asyncio.iscoroutinefunction(
        repository.execute_query
      ):
        # Async repository
        data = await repository.execute_query(cypher, parameters)
      else:
        # Sync repository - run in thread pool
        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(
          None, repository.execute_query, cypher, parameters
        )

      # Extract column names from first row
      columns = list(data[0].keys()) if data else []

      # Record success so circuit breaker can close after recovery
      circuit_breaker.record_success(graph_id, "cypher_query")

      # Return structured result
      return {
        "data": data,
        "columns": columns,
        "execution_time_ms": 0,  # Repository doesn't provide this directly
        "row_count": len(data),
      }

    except Exception as e:
      # Record failure so circuit breaker can open under sustained errors
      circuit_breaker.record_failure(graph_id, "cypher_query", error=e)

      logger.error(
        f"Query executor error for graph {graph_id}: {e}",
        extra={
          "graph_id": graph_id,
          "error_type": type(e).__name__,
          "error_message": str(e),
        },
      )
      # Re-raise to let queue manager handle the failure
      raise

  queue_manager.set_query_executor(executor)

  logger.info("Query executor initialized successfully")
