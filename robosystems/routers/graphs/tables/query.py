"""SQL query endpoint for DuckDB staging tables."""

from datetime import UTC, datetime

from fastapi import APIRouter, Body, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session

from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
from robosystems.database import get_db_session
from robosystems.logger import api_logger, logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph import get_universal_repository
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.middleware.robustness import CircuitBreakerManager
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.api.graphs.tables import TableQueryRequest, TableQueryResponse
from robosystems.models.core import User

router = APIRouter()

circuit_breaker = CircuitBreakerManager()


@router.post(
  "/tables/query",
  response_model=TableQueryResponse,
  operation_id="queryTables",
  summary="Query Staging Tables with SQL",
  description="Execute SQL against DuckDB staging tables for pre-ingestion validation. Use `?` placeholders with the `parameters` array to prevent injection. Read-only (SELECT only), 30s timeout, 10K row limit. Not allowed on shared repositories.",
  responses={
    **RESOURCE_ERROR_RESPONSES,
    408: {"description": "Query timeout"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/tables/query", business_event_type="table_query_executed"
)
async def query_tables(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  request: TableQueryRequest = Body(..., description="SQL query request"),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> TableQueryResponse:
  start_time = datetime.now(UTC)

  # Check circuit breaker
  circuit_breaker.check_circuit(graph_id, "table_query")

  # Block shared repositories
  if is_shared_repository_or_subgraph(graph_id.lower()):
    logger.warning(
      f"User {current_user.id} attempted SQL query on shared repository {graph_id}"
    )
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Shared repositories do not allow direct SQL table queries. "
      "Use the graph query endpoint (POST /query) to access shared repository data through the structured graph interface.",
    )

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
      "SQL table query execution started",
      extra={
        "component": "tables_api",
        "action": "query_started",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "query_length": len(request.sql),
        "metadata": {
          "endpoint": "/v1/graphs/{graph_id}/tables/query",
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

    # Record success
    circuit_breaker.record_success(graph_id, "table_query")

    # Record business event
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/tables/query",
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
      "SQL table query completed successfully",
      extra={
        "component": "tables_api",
        "action": "query_completed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "duration_ms": execution_time,
        "row_count": response.get("row_count", 0),
        "success": True,
      },
    )

    return TableQueryResponse(**response)

  except HTTPException:
    circuit_breaker.record_failure(graph_id, "table_query")
    raise

  except Exception as e:
    circuit_breaker.record_failure(graph_id, "table_query")

    # Record business event for failure
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/tables/query",
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
      f"SQL query failed for graph {graph_id}: {e}",
      extra={
        "component": "tables_api",
        "action": "query_failed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "error_type": type(e).__name__,
      },
      exc_info=True,
    )

    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"Query failed: {e!s}",
    )
