"""
Query executor setup for queue manager.

This module provides the initialization for the query executor
that processes queued queries.
"""

import asyncio
from typing import Any

from robosystems.logger import logger
from robosystems.middleware.graph.query_queue import get_query_queue
from robosystems.middleware.graph.router import GraphRouter
from robosystems.middleware.graph.utils import MultiTenantUtils


def _get_query_operation_type(graph_id: str) -> str:
  """
  Determine the correct operation type for query operations.

  For consistency with distributed LadybugDB architecture:
  - User graphs: Always use 'write' to ensure writer cluster routing
  - Shared repositories: Use 'read' for reader cluster routing
  """
  if MultiTenantUtils.is_shared_repository(graph_id):
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

  async def executor(
    cypher: str, parameters: dict[str, Any] | None, graph_id: str
  ) -> dict[str, Any]:
    """
    Execute a queued query.

    Args:
        cypher: The Cypher query to execute
        parameters: Optional query parameters
        graph_id: Target graph identifier

    Returns:
        Dictionary with query results and metadata
    """
    try:
      # Get the appropriate repository
      graph_router = GraphRouter()
      operation_type = _get_query_operation_type(graph_id)
      repository = graph_router.get_repository(graph_id, operation_type)

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

      # Return structured result
      return {
        "data": data,
        "columns": columns,
        "execution_time_ms": 0,  # Repository doesn't provide this directly
        "row_count": len(data),
      }

    except Exception as e:
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

  # Set the executor on the queue manager
  queue_manager.set_query_executor(executor)

  logger.info("Query executor initialized successfully")
