"""
Cypher query execution endpoints for LadybugDB databases.

This module provides endpoints for executing Cypher queries against
specific LadybugDB graph databases with admission control.
"""

import json
import os
import time
from contextlib import contextmanager

from fastapi import APIRouter, Depends, HTTPException, Path, status
from fastapi.responses import StreamingResponse

from robosystems.graph_api.core.admission_control import (
  AdmissionDecision,
  get_admission_controller,
)
from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.graph_api.models.database import QueryRequest
from robosystems.logger import logger
from robosystems.models.core import Graph

router = APIRouter(prefix="/databases", tags=["Graph Query"])

# In-memory cache for graph rebuild status. Avoids hitting RDS on every query
# while still protecting users from reading partial data during rebuilds.
# Cache miss or RDS failure → fail open (assume not rebuilding).
_rebuild_status_cache: dict[str, tuple[bool, float]] = {}
_REBUILD_CACHE_TTL = 30  # seconds


def _is_graph_rebuilding(graph_id: str) -> bool:
  """Check if a graph is currently rebuilding, using a cached RDS lookup.

  Skipped for shared replicas — they serve an independent local copy and should
  not be affected by the master setting rebuilding status on the same graph_id.
  """
  if os.getenv("LBUG_ROLE") == "replica":
    return False

  now = time.monotonic()

  cached = _rebuild_status_cache.get(graph_id)
  if cached is not None:
    is_rebuilding, cached_at = cached
    if now - cached_at < _REBUILD_CACHE_TTL:
      return is_rebuilding

  try:
    from robosystems.database import session as scoped_session

    db = scoped_session()
    try:
      graph = Graph.get_by_id(graph_id, db)
      is_rebuilding = bool(
        graph
        and graph.graph_metadata
        and graph.graph_metadata.get("status") == "rebuilding"
      )
      _rebuild_status_cache[graph_id] = (is_rebuilding, now)
      return is_rebuilding
    finally:
      scoped_session.remove()
  except Exception:
    logger.warning(
      f"Failed to check rebuild status for {graph_id}, assuming not rebuilding"
    )
    return False


@contextmanager
def track_connection(admission_controller, database_name):
  """Context manager to track database connections for admission control."""
  admission_controller.register_connection(database_name)
  try:
    yield
  finally:
    admission_controller.release_connection(database_name)


@router.post("/{graph_id}/query")
async def execute_query(
  request: QueryRequest,
  graph_id: str = Path(..., description="Graph database identifier"),
  streaming: bool = False,
  database: str | None = None,
  service=Depends(get_ladybug_service),
):
  """
  Execute a Cypher query against a specific database with admission control.

  Executes the provided Cypher query against the specified database.
  Can return results as a standard JSON response or as a streaming response
  for large result sets.

  Args:
      graph_id: The database identifier
      request: Query request containing the Cypher query and optional parameters
      streaming: If true, use streaming response (recommended for large results)

  Returns:
      Standard JSON response or streaming NDJSON response based on streaming parameter

  Raises:
      HTTPException: 503 if server is overloaded (admission control)
      HTTPException: 503 if graph is rebuilding
  """
  if _is_graph_rebuilding(graph_id):
    logger.warning(f"Query rejected for {graph_id}: graph is rebuilding")
    raise HTTPException(
      status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
      detail={
        "error": "Graph temporarily unavailable",
        "reason": "Graph database is being rebuilt",
        "status": "rebuilding",
        "retry_after": 30,
      },
    )

  # Get admission controller
  admission_controller = get_admission_controller()

  # Check admission control before executing query
  decision, reason = admission_controller.check_admission(graph_id, "query")

  if decision != AdmissionDecision.ACCEPT:
    logger.warning(f"Query rejected for {graph_id}: {reason}")
    raise HTTPException(
      status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
      detail={
        "error": "Server temporarily unavailable",
        "reason": reason,
        "decision": decision,
        "retry_after": 5,  # Suggest retry after 5 seconds
      },
    )

  # Track the connection for this query
  with track_connection(admission_controller, graph_id):
    # Create a new request with the graph_id from the path
    query_request = QueryRequest(
      database=graph_id, cypher=request.cypher, parameters=request.parameters
    )

    if not streaming:
      return service.execute_query(query_request)

    # Streaming response for large result sets
    def generate_stream():
      for chunk in service.execute_query_streaming(query_request, chunk_size=1000):
        yield json.dumps(chunk) + "\n"

    return StreamingResponse(
      generate_stream(),
      media_type="application/x-ndjson",
      headers={"X-Streaming": "true", "Cache-Control": "no-cache"},
    )
