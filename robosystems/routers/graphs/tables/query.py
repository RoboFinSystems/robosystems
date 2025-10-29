"""
Staging Tables SQL Query Endpoint.

This module provides SQL query capabilities for DuckDB staging tables,
enabling direct data inspection and validation before ingestion into
the Kuzu graph database.

Key Features:
- Direct SQL query execution on DuckDB staging tables
- Row-level data inspection and validation
- Pre-ingestion data quality checks
- Staging table analytics and reporting

Workflow Integration:
1. Upload data files to staging tables
2. Query staging tables to validate data quality
3. Run SQL analytics on raw data
4. Ingest validated data into graph

Security:
- Read-only access to user's staging tables
- Blocked on shared repositories (use graph queries instead)
- Rate limited per subscription tier
- Full audit logging of query patterns
"""

from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, Path, Body, status
from sqlalchemy.orm import Session

from robosystems.models.iam import User
from robosystems.models.api.table import TableQueryRequest, TableQueryResponse
from robosystems.models.api.common import ErrorResponse
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.middleware.graph import get_universal_repository
from robosystems.database import get_db_session
from robosystems.logger import logger, api_logger
from robosystems.middleware.graph.types import GraphTypeRegistry
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from robosystems.middleware.robustness import CircuitBreakerManager

router = APIRouter()

circuit_breaker = CircuitBreakerManager()


@router.post(
  "/tables/query",
  response_model=TableQueryResponse,
  operation_id="queryTables",
  summary="Query Staging Tables with SQL",
  description="""Execute SQL queries on DuckDB staging tables for data inspection and validation.

**Purpose:**
Query raw staging data directly with SQL before ingestion into the graph database.
Useful for data quality checks, validation, and exploratory analysis.

**Use Cases:**
- Validate data quality before graph ingestion
- Inspect row-level data for debugging
- Run analytics on staging tables
- Check for duplicates, nulls, or data issues
- Preview data transformations

**Workflow:**
1. Upload data files via `POST /tables/{table_name}/files`
2. Query staging tables to validate: `POST /tables/query`
3. Fix any data issues by re-uploading
4. Ingest validated data: `POST /tables/ingest`

**Supported SQL:**
- Full DuckDB SQL syntax
- SELECT, JOIN, WHERE, GROUP BY, ORDER BY
- Aggregations, window functions, CTEs
- Multiple table joins across staging area

**Example Queries:**
```sql
-- Count rows in staging table
SELECT COUNT(*) FROM Entity;

-- Check for nulls
SELECT * FROM Entity WHERE name IS NULL LIMIT 10;

-- Find duplicates
SELECT identifier, COUNT(*) as cnt
FROM Entity
GROUP BY identifier
HAVING COUNT(*) > 1;

-- Join across tables
SELECT e.name, COUNT(t.id) as transaction_count
FROM Entity e
LEFT JOIN Transaction t ON e.identifier = t.entity_id
GROUP BY e.name
ORDER BY transaction_count DESC;
```

**Limits:**
- Query timeout: 30 seconds
- Result limit: 10,000 rows (use LIMIT clause)
- Read-only: No INSERT, UPDATE, DELETE
- User's tables only: Cannot query other users' data

**Shared Repositories:**
Shared repositories (SEC, etc.) do not allow direct SQL queries.
Use the graph query endpoint instead: `POST /v1/graphs/{graph_id}/query`

**Note:**
Staging table queries are included - no credit consumption.""",
  responses={
    200: {
      "description": "Query executed successfully",
      "content": {
        "application/json": {
          "example": {
            "success": True,
            "data": [
              {"name": "Acme Corp", "count": 150},
              {"name": "TechCo", "count": 89},
            ],
            "columns": ["name", "count"],
            "row_count": 2,
            "execution_time_ms": 45.2,
          }
        }
      },
    },
    400: {
      "description": "Invalid SQL query syntax or execution error",
      "model": ErrorResponse,
    },
    403: {
      "description": "Access denied - shared repositories or insufficient permissions",
      "model": ErrorResponse,
    },
    404: {"description": "Graph not found", "model": ErrorResponse},
    408: {"description": "Query timeout exceeded"},
    500: {"description": "Internal server error"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/tables/query", business_event_type="table_query_executed"
)
async def query_tables(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern="^[a-zA-Z][a-zA-Z0-9_]{2,62}$",
  ),
  request: TableQueryRequest = Body(..., description="SQL query request"),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> TableQueryResponse:
  """
  Execute SQL query on DuckDB staging tables.

  This endpoint provides direct SQL access to staging tables for data
  inspection and validation before ingestion into the graph database.
  """
  start_time = datetime.now(timezone.utc)

  # Check circuit breaker
  circuit_breaker.check_circuit(graph_id, "table_query")

  # Block shared repositories
  if graph_id.lower() in GraphTypeRegistry.SHARED_REPOSITORIES:
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

    response = await client.query_table(graph_id=graph_id, sql=request.sql)

    # Calculate execution time
    execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

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
    )

    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"Query failed: {str(e)}",
    )
