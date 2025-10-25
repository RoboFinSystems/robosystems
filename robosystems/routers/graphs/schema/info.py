"""Schema info endpoint."""

from typing import Dict, Any
import asyncio
import time
from fastapi import APIRouter, Depends, HTTPException, status, Path
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.iam import User
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.middleware.graph.dependencies import get_universal_repository_with_auth
from robosystems.database import get_async_db_session
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from robosystems.middleware.robustness import (
  OperationType,
  OperationStatus,
  record_operation_metric,
  get_operation_logger,
)

from .utils import get_schema_info, circuit_breaker, timeout_coordinator

router = APIRouter()


@router.get(
  "/info",
  summary="Get Runtime Graph Schema Information",
  description="""Get runtime schema information for the specified graph database.

This endpoint inspects the actual graph database structure and returns:
- **Node Labels**: All node types currently in the database
- **Relationship Types**: All relationship types currently in the database
- **Node Properties**: Properties for each node type (limited to first 10 for performance)

This is different from custom schema management - it shows what actually exists in the database,
useful for understanding the current graph structure before writing queries.

This operation is included - no credit consumption required.""",
  operation_id="getGraphSchemaInfo",
  responses={
    200: {"description": "Schema information retrieved successfully"},
    403: {"description": "Access denied to graph"},
    500: {"description": "Failed to retrieve schema"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/schema/info", business_event_type="schema_info_retrieved"
)
async def get_graph_schema_info(
  graph_id: str = Path(..., description="The graph database to get schema for"),
  current_user: User = Depends(get_current_user),
  session: Session = Depends(get_async_db_session),
  _: None = Depends(subscription_aware_rate_limit_dependency),
) -> Dict[str, Any]:
  """
  Get runtime schema information for the specified graph.

  Returns node labels, relationship types, and property information by inspecting
  the actual database structure.

  Args:
      graph_id: The graph to get schema for
      current_user: The authenticated user
      session: Database session

  Returns:
      Dictionary containing schema information
  """
  # Initialize robustness components
  operation_logger = get_operation_logger()

  # Record operation start and get timing
  operation_start_time = time.time()

  # Record operation start metrics
  record_operation_metric(
    operation_type=OperationType.SCHEMA_OPERATION,
    status=OperationStatus.SUCCESS,  # Will be updated on completion
    duration_ms=0.0,  # Will be updated on completion
    endpoint="/v1/graphs/{graph_id}/schema/info",
    graph_id=graph_id,
    user_id=current_user.id,
    operation_name="get_schema_info",
    metadata={},
  )

  # Initialize timeout for error handling
  operation_timeout = None

  try:
    # Check circuit breaker before processing
    circuit_breaker.check_circuit(graph_id, "schema_info")

    # Set up timeout coordination for schema operations
    operation_timeout = timeout_coordinator.calculate_timeout(
      operation_type="database_query",
      complexity_factors={
        "operation": "schema_introspection",
        "expected_complexity": "medium",
      },
    )
    # Schema operations are included - no credit consumption

    # Log the request with operation logger
    operation_logger.log_external_service_call(
      endpoint="/v1/graphs/{graph_id}/schema/info",
      service_name="graph_repository",
      operation="get_schema_info",
      duration_ms=0.0,  # Will be updated on completion
      status="processing",
      graph_id=graph_id,
      user_id=current_user.id,
      metadata={},
    )

    # Get repository with unified authentication and authorization
    repository = await get_universal_repository_with_auth(
      graph_id, current_user, "read", session
    )

    # Get schema information with timeout coordination
    schema = await asyncio.wait_for(
      get_schema_info(repository), timeout=operation_timeout
    )

    # Record business event for successful schema retrieval
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/schema/info",
      method="GET",
      event_type="schema_info_retrieved_successfully",
      event_data={
        "graph_id": graph_id,
        "node_labels_count": len(schema.get("node_labels", [])),
        "relationship_types_count": len(schema.get("relationship_types", [])),
        "node_properties_count": len(schema.get("node_properties", {})),
      },
      user_id=current_user.id,
    )

    # Record successful operation
    operation_duration_ms = (time.time() - operation_start_time) * 1000
    circuit_breaker.record_success(graph_id, "schema_info")

    # Record success metrics
    record_operation_metric(
      operation_type=OperationType.SCHEMA_OPERATION,
      status=OperationStatus.SUCCESS,
      duration_ms=operation_duration_ms,
      endpoint="/v1/graphs/{graph_id}/schema/info",
      graph_id=graph_id,
      user_id=current_user.id,
      operation_name="get_schema_info",
      metadata={
        "node_labels_count": len(schema.get("node_labels", [])),
        "relationship_types_count": len(schema.get("relationship_types", [])),
      },
    )

    return {"graph_id": graph_id, "schema": schema}

  except asyncio.TimeoutError:
    # Record circuit breaker failure and timeout metrics
    circuit_breaker.record_failure(graph_id, "schema_info")
    operation_duration_ms = (time.time() - operation_start_time) * 1000

    # Record timeout failure metrics
    record_operation_metric(
      operation_type=OperationType.SCHEMA_OPERATION,
      status=OperationStatus.FAILURE,
      duration_ms=operation_duration_ms,
      endpoint="/v1/graphs/{graph_id}/schema/info",
      graph_id=graph_id,
      user_id=current_user.id,
      operation_name="get_schema_info",
      metadata={
        "error_type": "timeout",
        "timeout_seconds": operation_timeout,
      },
    )

    timeout_str = f" after {operation_timeout}s" if operation_timeout else ""
    logger.error(
      f"Schema info operation timeout{timeout_str} for user {current_user.id}"
    )
    raise HTTPException(
      status_code=status.HTTP_504_GATEWAY_TIMEOUT,
      detail="Schema info operation timed out",
    )
  except HTTPException:
    # Record circuit breaker failure for HTTP exceptions
    circuit_breaker.record_failure(graph_id, "schema_info")
    operation_duration_ms = (time.time() - operation_start_time) * 1000

    # Record failure metrics
    record_operation_metric(
      operation_type=OperationType.SCHEMA_OPERATION,
      status=OperationStatus.FAILURE,
      duration_ms=operation_duration_ms,
      endpoint="/v1/graphs/{graph_id}/schema/info",
      graph_id=graph_id,
      user_id=current_user.id,
      operation_name="get_schema_info",
      metadata={
        "error_type": "http_exception",
      },
    )
    raise
  except Exception as e:
    # Record circuit breaker failure for general exceptions
    circuit_breaker.record_failure(graph_id, "schema_info")
    operation_duration_ms = (time.time() - operation_start_time) * 1000

    # Record failure metrics
    record_operation_metric(
      operation_type=OperationType.SCHEMA_OPERATION,
      status=OperationStatus.FAILURE,
      duration_ms=operation_duration_ms,
      endpoint="/v1/graphs/{graph_id}/schema/info",
      graph_id=graph_id,
      user_id=current_user.id,
      operation_name="get_schema_info",
      metadata={
        "error_type": type(e).__name__,
        "error_message": str(e),
      },
    )

    # Record business event for unexpected errors
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/schema/info",
      method="GET",
      event_type="schema_info_retrieval_unexpected_error",
      event_data={
        "graph_id": graph_id,
        "error_type": type(e).__name__,
        "error_message": str(e),
      },
      user_id=current_user.id,
    )
    logger.error(f"Error getting graph schema info: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to retrieve graph schema information",
    )
