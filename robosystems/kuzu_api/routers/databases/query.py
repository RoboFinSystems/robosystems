"""
Cypher query execution endpoints for Kuzu databases.

This module provides endpoints for executing Cypher queries against
specific Kuzu graph databases with admission control.
"""

from fastapi import APIRouter, Depends, Path, HTTPException, status
from fastapi.responses import StreamingResponse
import json
from contextlib import contextmanager

from robosystems.kuzu_api.models.database import QueryRequest
from robosystems.kuzu_api.core.cluster_manager import get_cluster_service
from robosystems.kuzu_api.core.admission_control import (
  get_admission_controller,
  AdmissionDecision,
)
from robosystems.logger import logger

router = APIRouter(prefix="/databases", tags=["Database Queries"])


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
  cluster_service=Depends(get_cluster_service),
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
  """
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
      # Standard response - note that execute_query now has internal limits
      # to prevent memory issues (MAX_ROWS = 10000)
      return cluster_service.execute_query(query_request)

    # Streaming response for large result sets
    def generate_stream():
      for chunk in cluster_service.execute_query_streaming(
        query_request, chunk_size=1000
      ):
        yield json.dumps(chunk) + "\n"

    return StreamingResponse(
      generate_stream(),
      media_type="application/x-ndjson",
      headers={"X-Streaming": "true", "Cache-Control": "no-cache"},
    )
