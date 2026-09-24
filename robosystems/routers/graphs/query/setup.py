"""Initialization for the query executor that drains the query queue."""

import asyncio
from typing import Any

from robosystems.logger import logger
from robosystems.middleware.graph.query_queue import get_query_queue
from robosystems.middleware.graph.router import GraphRouter
from robosystems.middleware.robustness import CircuitBreakerManager


def _get_query_operation_type(graph_id: str) -> str:
  """User graphs route to the writer ('write'); shared repositories and their
  subgraphs to readers ('read')."""
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph

  if is_shared_repository_or_subgraph(graph_id):
    return "read"
  else:
    return "write"


def setup_query_executor():
  """Install the queue manager's query executor. Call once at startup."""
  queue_manager = get_query_queue()
  circuit_breaker = CircuitBreakerManager()

  async def executor(
    cypher: str, parameters: dict[str, Any] | None, graph_id: str
  ) -> dict[str, Any]:
    """Execute a queued query, returning results plus metadata."""
    try:
      graph_router = GraphRouter()
      operation_type = _get_query_operation_type(graph_id)
      repository = await graph_router.get_repository(graph_id, operation_type)

      if hasattr(repository, "execute_query") and asyncio.iscoroutinefunction(
        repository.execute_query
      ):
        data = await repository.execute_query(cypher, parameters)
      else:
        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(
          None, repository.execute_query, cypher, parameters
        )

      columns = list(data[0].keys()) if data else []

      circuit_breaker.record_success(graph_id, "cypher_query")

      return {
        "data": data,
        "columns": columns,
        "execution_time_ms": 0,  # Repository doesn't provide this directly
        "row_count": len(data),
      }

    except Exception as e:
      circuit_breaker.record_failure(graph_id, "cypher_query", error=e)

      logger.error(
        f"Query executor error for graph {graph_id}: {e}",
        extra={
          "graph_id": graph_id,
          "error_type": type(e).__name__,
          "error_message": str(e),
        },
      )
      raise

  queue_manager.set_query_executor(executor)

  logger.info("Query executor initialized successfully")
