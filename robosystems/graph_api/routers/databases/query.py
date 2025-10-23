"""
Cypher query execution endpoints for Kuzu databases.

This module provides endpoints for executing Cypher queries against
specific Kuzu graph databases with admission control.
"""

from fastapi import APIRouter, Depends, Path, HTTPException, status
from fastapi.responses import StreamingResponse
import json
from contextlib import contextmanager
from sqlalchemy.orm import Session

from robosystems.graph_api.models.database import QueryRequest
from robosystems.graph_api.core.cluster_manager import get_cluster_service
from robosystems.graph_api.core.admission_control import (
  get_admission_controller,
  AdmissionDecision,
)
from robosystems.logger import logger
from robosystems.config import env
from robosystems.database import get_db_session
from robosystems.models.iam import Graph

router = APIRouter(prefix="/databases", tags=["Graph Query"])


@contextmanager
def track_connection(admission_controller, database_name):
  """Context manager to track database connections for admission control."""
  admission_controller.register_connection(database_name)
  try:
    yield
  finally:
    admission_controller.release_connection(database_name)


def _get_cluster_service_for_request():
  backend_type = env.GRAPH_BACKEND_TYPE
  if backend_type in ["neo4j_community", "neo4j_enterprise"]:
    from robosystems.graph_api.core.backend_cluster_manager import BackendClusterService

    return BackendClusterService()
  return get_cluster_service()


@router.post("/{graph_id}/query")
async def execute_query(
  request: QueryRequest,
  graph_id: str = Path(..., description="Graph database identifier"),
  streaming: bool = False,
  database: str = None,
  cluster_service=Depends(_get_cluster_service_for_request),
  db: Session = Depends(get_db_session),
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
  # Check if graph is rebuilding
  graph = Graph.get_by_id(graph_id, db)
  if graph and graph.graph_metadata:
    graph_status = graph.graph_metadata.get("status")
    if graph_status == "rebuilding":
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

    backend_type = env.GRAPH_BACKEND_TYPE

    if backend_type in ["neo4j_community", "neo4j_enterprise"]:
      # Use async backend cluster service
      if not streaming:
        return await cluster_service.execute_query(query_request)
      else:
        # Streaming not yet implemented for Neo4j backend
        raise HTTPException(
          status_code=status.HTTP_501_NOT_IMPLEMENTED,
          detail="Streaming not yet implemented for Neo4j backend",
        )
    else:
      # Use existing Kuzu cluster service (sync)
      if not streaming:
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
