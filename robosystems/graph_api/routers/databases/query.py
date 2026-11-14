"""Cypher query execution against a LadybugDB database, under admission control."""

from contextlib import contextmanager

import pydantic_core
from fastapi import APIRouter, Depends, HTTPException, Path, status
from fastapi.responses import StreamingResponse

from robosystems.graph_api.core.admission_control import (
  AdmissionDecision,
  get_admission_controller,
)
from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.graph_api.models.database import QueryRequest
from robosystems.logger import logger

router = APIRouter(prefix="/databases", tags=["Graph Query"])


@contextmanager
def track_connection(admission_controller, database_name):
  """Context manager to track database connections for admission control."""
  admission_controller.register_connection(database_name)
  try:
    yield
  finally:
    admission_controller.release_connection(database_name)


@router.post("/{graph_id}/query")
def execute_query(
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

  A rebuilding graph is refused by the caller's client before the request is
  sent (``middleware/graph/rebuild_status``); this process holds no database
  connection to ask.

  Deliberately a plain ``def``: the body is entirely synchronous (the engine
  call), so Starlette runs it on the threadpool. As ``async def`` it ran on the single event loop, where a
  long query against the shared repository held every caller on the instance.
  """
  admission_controller = get_admission_controller()
  decision, reason = admission_controller.check_admission(graph_id, "query")

  if decision != AdmissionDecision.ACCEPT:
    logger.warning(f"Query rejected for {graph_id}: {reason}")
    raise HTTPException(
      status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
      detail={
        "error": "Server temporarily unavailable",
        "reason": reason,
        "decision": decision,
        "retry_after": 5,
      },
    )

  # The path's graph_id is authoritative — ignore any database in the body.
  query_request = QueryRequest(
    database=graph_id, cypher=request.cypher, parameters=request.parameters
  )

  if not streaming:
    with track_connection(admission_controller, graph_id):
      return service.execute_query(query_request)

  # The handler returns before the body is produced, so a stream is counted
  # from inside its generator for as long as it runs. Chunks encode the way the
  # non-streaming response does; one that cannot ends the stream with an error
  # chunk, so the caller sees a query error rather than a cut-off stream.
  def generate_stream():
    with track_connection(admission_controller, graph_id):
      for chunk in service.execute_query_streaming(query_request, chunk_size=1000):
        try:
          line = pydantic_core.to_json(chunk)
        except pydantic_core.PydanticSerializationError:
          logger.exception(f"Unserializable value in streaming result on {graph_id}")
          line = pydantic_core.to_json(
            {
              "error": "Query result contains a value that cannot be serialized",
              "error_type": "SerializationError",
              "is_last_chunk": True,
            }
          )
          yield line + b"\n"
          return
        yield line + b"\n"

  return StreamingResponse(
    generate_stream(),
    media_type="application/x-ndjson",
    headers={"X-Streaming": "true", "Cache-Control": "no-cache"},
  )
