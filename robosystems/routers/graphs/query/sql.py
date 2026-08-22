"""SQL statement execution over the graph's columnar tables (DuckDB).

Peer to the Cypher endpoint in the query layer: a relational lens on the same
graph-centric data. Read-only — writes are gated on the DuckDB
write-connection sandbox. Shared repositories are rejected, since they have no
user columnar tables. Authorization runs through the shared StatementKernel.
"""

from datetime import UTC, datetime

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Request, status
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.logger import api_logger, logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph import get_universal_repository
from robosystems.middleware.graph.query_telemetry import (
  api_key_prefix_from_request,
  record_shared_query_outcome,
)
from robosystems.middleware.graph.statement_kernel import (
  StatementEngine,
  statement_kernel,
)
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.middleware.robustness import CircuitBreakerManager
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.api.graphs.tables import (
  SqlStatementRequest,
  SqlStatementResponse,
)
from robosystems.models.core import User
from robosystems.security.error_handling import safe_error_message

router = APIRouter()

circuit_breaker = CircuitBreakerManager()


@router.post(
  "/query/sql",
  response_model=SqlStatementResponse,
  operation_id="executeSql",
  summary="Execute SQL Statement",
  description="SQL over the graph's columnar tables (DuckDB) — a relational lens on the same graph-centric data, often ahead of the materialized graph. Use `?` placeholders with the `parameters` array to prevent injection. Read-only (SELECT only), 30s timeout, 10,000 row limit. Not available on shared repositories.",
  responses={
    **RESOURCE_ERROR_RESPONSES,
    408: {"description": "Query timeout"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/query/sql", business_event_type="table_query_executed"
)
async def execute_sql(
  full_request: Request,
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  request: SqlStatementRequest = Body(..., description="SQL statement request"),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> SqlStatementResponse:
  start_time = datetime.now(UTC)

  circuit_breaker.check_circuit(graph_id, "table_query")

  # Authorize the statement — SQL is read-only and blocked on shared repos.
  # Shared, transport-independent path (also used by /query/cypher, MCP).
  try:
    statement_kernel.authorize(
      engine=StatementEngine.SQL,
      graph_id=graph_id,
      statement=request.sql,
      user=current_user,
      session=db,
    )
  except HTTPException as exc:
    record_shared_query_outcome(
      graph_id,
      current_user.id,
      status_code=exc.status_code,
      api_key_prefix=api_key_prefix_from_request(full_request),
      endpoint="/v1/graphs/{graph_id}/query/sql",
      source="query_sql",
    )
    raise

  try:
    # Verify graph access
    repository = await get_universal_repository(graph_id, "read")

    if not repository:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Graph {graph_id} not found",
      )

    # Log structured query attempt
    api_logger.info(
      "SQL statement execution started",
      extra={
        "component": "query_api",
        "action": "sql_query_started",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "query_length": len(request.sql),
        "metadata": {
          "endpoint": "/v1/graphs/{graph_id}/query/sql",
        },
      },
    )

    # Execute query via graph API
    from robosystems.graph_api.client.factory import get_graph_client

    client = await get_graph_client(graph_id=graph_id, operation_type="read")

    response = await client.query_table(
      graph_id=graph_id, sql=request.sql, parameters=request.parameters
    )

    # Calculate execution time
    execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

    circuit_breaker.record_success(graph_id, "table_query")

    # Record business event
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/query/sql",
      method="POST",
      event_type="table_query_executed_successfully",
      event_data={
        "graph_id": graph_id,
        "query_length": len(request.sql),
        "execution_time_ms": execution_time,
        "row_count": response.get("row_count", 0),
      },
      user_id=current_user.id,
    )

    # Log structured completion
    api_logger.info(
      "SQL statement completed successfully",
      extra={
        "component": "query_api",
        "action": "sql_query_completed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "duration_ms": execution_time,
        "row_count": response.get("row_count", 0),
        "success": True,
      },
    )

    return SqlStatementResponse(**response)

  except HTTPException:
    circuit_breaker.record_failure(graph_id, "table_query")
    raise

  except Exception as e:
    circuit_breaker.record_failure(graph_id, "table_query")

    # Record business event for failure
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/query/sql",
      method="POST",
      event_type="table_query_failed",
      event_data={
        "graph_id": graph_id,
        "query_length": len(request.sql) if request else 0,
        "error_type": type(e).__name__,
        "error_message": str(e),
      },
      user_id=current_user.id,
    )

    logger.error(
      f"SQL statement failed for graph {graph_id}: {e}",
      extra={
        "component": "query_api",
        "action": "sql_query_failed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "error_type": type(e).__name__,
      },
      exc_info=True,
    )

    # The caller's own statement errors keep their message; infrastructure
    # exceptions (boto3/redis/driver text naming hosts and internals) collapse
    # to a generic detail — the full text is in the log line above.
    safe_message = safe_error_message(e)
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"Query failed: {safe_message}" if safe_message else "Query failed",
    )
