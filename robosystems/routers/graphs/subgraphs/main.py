"""
Main subgraph routes (list and create operations).
"""

import asyncio
import json
from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from robosystems.database import get_async_db_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.models.api.graphs.subgraphs import (
  CreateSubgraphRequest,
  SubgraphResponse,
  SubgraphSummary,
  ListSubgraphsResponse,
  SubgraphType,
)
from robosystems.models.iam.graph import Graph
from robosystems.models.iam.user import User
from robosystems.security import SecurityAuditLogger, SecurityEventType
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.logger import logger, api_logger, log_metric

from .utils import (
  verify_parent_graph_access,
  verify_subgraph_tier_support,
  verify_parent_graph_active,
  check_subgraph_quota,
  validate_subgraph_name_unique,
  get_subgraph_service,
  record_operation_start,
  record_operation_metrics,
  handle_circuit_breaker_check,
)
from robosystems.config.graph_tier import get_tier_max_subgraphs
from robosystems.config import env
from robosystems.middleware.graph.types import GRAPH_ID_PATTERN

router = APIRouter()


async def get_database_size_mb(graph_id: str) -> float | None:
  """Get the size of a database in MB from Graph API metrics.

  Returns None if size cannot be determined. This is expected for some graphs
  and the function gracefully handles errors without failing the listing operation.
  """
  try:
    from robosystems.graph_api.client.factory import GraphClientFactory

    # Create client using factory for endpoint discovery
    graph_client = await GraphClientFactory.create_client(
      graph_id=graph_id, operation_type="read"
    )

    # Get database metrics from Graph API
    metrics = await graph_client.get_database_metrics(graph_id=graph_id)

    if metrics and "size_mb" in metrics:
      return metrics["size_mb"]

    logger.debug(f"Size metric not available for database {graph_id}")
    return None
  except Exception as e:
    logger.warning(f"Failed to get size for database {graph_id}: {e}")
    return None


@router.get(
  "",
  response_model=ListSubgraphsResponse,
  operation_id="listSubgraphs",
  summary="List Subgraphs",
  description="""List all subgraphs for a parent graph.

**Requirements:**
- Valid authentication
- Parent graph must exist and be accessible to the user
- User must have at least 'read' permission on the parent graph

**Returns:**
- List of all subgraphs for the parent graph
- Each subgraph includes its ID, name, description, type, status, and creation date
""",
  status_code=status.HTTP_200_OK,
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/graphs/{graph_id}/subgraphs",
  business_event_type="subgraph_list",
)
async def list_subgraphs(
  graph_id: str = Path(
    ...,
    description="Parent graph ID (e.g., 'kg1a2b3c4d5')",
    pattern=GRAPH_ID_PATTERN,
  ),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_async_db_session),
) -> ListSubgraphsResponse:
  """List all subgraphs for a parent graph."""
  operation_start_time = record_operation_start()

  try:
    # Circuit breaker check
    handle_circuit_breaker_check(graph_id, "list_subgraphs")

    # Verify access to parent graph
    parent_graph = verify_parent_graph_access(graph_id, current_user, db, "read")

    # Log access event
    api_logger.info(
      f"User {current_user.id} listing subgraphs for graph {graph_id}",
      extra={
        "user_id": current_user.id,
        "graph_id": graph_id,
        "operation": "list_subgraphs",
      },
    )

    # Get all subgraphs for the parent graph
    subgraphs = (
      db.query(Graph)
      .filter(Graph.parent_graph_id == parent_graph.graph_id)
      .order_by(Graph.created_at.desc())
      .all()
    )

    # Get all sizes concurrently for better performance
    size_tasks = [get_database_size_mb(sg.graph_id) for sg in subgraphs]
    sizes = await asyncio.gather(*size_tasks)

    subgraph_summaries = []
    total_size_mb = 0.0
    for subgraph, size_mb in zip(subgraphs, sizes):
      # Extract subgraph name from graph_id (format: {parent_id}_{subgraph_name})
      subgraph_name = subgraph.subgraph_name
      if not subgraph_name and "_" in subgraph.graph_id:
        # Fallback: extract from graph_id if subgraph_name is not set
        subgraph_name = subgraph.graph_id.split("_", 1)[1]

      # Sum sizes
      if size_mb:
        total_size_mb += size_mb

      # Determine status from graph_stale field
      subgraph_status = "stale" if subgraph.graph_stale else "active"

      # Extract subgraph_type from metadata, default to "static"
      subgraph_type_str = "static"
      if subgraph.subgraph_metadata and isinstance(subgraph.subgraph_metadata, dict):
        subgraph_type_str = subgraph.subgraph_metadata.get("subgraph_type", "static")

      # Convert string to SubgraphType enum
      try:
        subgraph_type = SubgraphType(subgraph_type_str)
      except ValueError:
        subgraph_type = SubgraphType.STATIC

      subgraph_summaries.append(
        SubgraphSummary(
          graph_id=subgraph.graph_id,
          subgraph_name=subgraph_name or subgraph.graph_name,
          display_name=subgraph.graph_name,
          subgraph_type=subgraph_type,
          status=subgraph_status,
          created_at=subgraph.created_at,
          size_mb=size_mb,
          last_accessed=None,
        )
      )

    # Log metrics
    log_metric(
      "subgraph_list_count",
      len(subgraph_summaries),
      {"graph_id": graph_id, "user_id": str(current_user.id)},
    )

    # Record success metrics
    record_operation_metrics(
      start_time=operation_start_time,
      operation_name="list_subgraphs",
      parent_graph_id=graph_id,
      additional_tags={
        "success": True,
        "entity_count": len(subgraph_summaries),
      },
    )

    max_subgraphs = get_tier_max_subgraphs(parent_graph.graph_tier)
    subgraphs_enabled = max_subgraphs is None or max_subgraphs > 0

    return ListSubgraphsResponse(
      parent_graph_id=graph_id,
      parent_graph_name=parent_graph.graph_name,
      parent_graph_tier=parent_graph.graph_tier,
      subgraphs_enabled=subgraphs_enabled,
      subgraphs=subgraph_summaries,
      subgraph_count=len(subgraph_summaries),
      max_subgraphs=max_subgraphs,
      total_size_mb=round(total_size_mb, 2) if total_size_mb > 0 else None,
    )

  except HTTPException:
    record_operation_metrics(
      start_time=operation_start_time,
      operation_name="list_subgraphs",
      parent_graph_id=graph_id,
      additional_tags={"success": False},
    )
    raise
  except SQLAlchemyError as e:
    record_operation_metrics(
      start_time=operation_start_time,
      operation_name="list_subgraphs",
      parent_graph_id=graph_id,
      additional_tags={"success": False, "error_type": "db"},
    )
    logger.error(f"Database error listing subgraphs: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to list subgraphs",
    )
  except Exception as e:
    record_operation_metrics(
      start_time=operation_start_time,
      operation_name="list_subgraphs",
      parent_graph_id=graph_id,
      additional_tags={"success": False, "error_type": "unexpected"},
    )
    logger.error(f"Unexpected error listing subgraphs: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="An unexpected error occurred",
    )


@router.post(
  "",
  operation_id="createSubgraph",
  summary="Create Subgraph",
  description="""Create a new subgraph within a parent graph, with optional data forking.

**Requirements:**
- Valid authentication
- Parent graph must exist and be accessible to the user
- User must have 'admin' permission on the parent graph
- Parent graph tier must support subgraphs (LadybugDB Large/XLarge or Neo4j Enterprise XLarge)
- Must be within subgraph quota limits
- Subgraph name must be unique within the parent graph

**Fork Mode:**
When `fork_parent=true`, the operation:
- Returns immediately with an operation_id for SSE monitoring
- Copies data from parent graph to the new subgraph
- Supports selective forking via metadata.fork_options
- Tracks progress in real-time via SSE

**Returns:**
- Without fork: Immediate SubgraphResponse with created subgraph details
- With fork: Operation response with SSE monitoring endpoint

**Subgraph ID format:** `{parent_id}_{subgraph_name}` (e.g., kg1234567890abcdef_dev)

**Usage:**
- Subgraphs share parent's credit pool
- Subgraph ID can be used in all standard `/v1/graphs/{graph_id}/*` endpoints
- Permissions inherited from parent graph
""",
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/graphs/{graph_id}/subgraphs",
  business_event_type="subgraph_create",
)
async def create_subgraph(
  request: CreateSubgraphRequest,
  graph_id: str = Path(
    ...,
    description="Parent graph ID (e.g., 'kg1a2b3c4d5')",
    pattern=GRAPH_ID_PATTERN,
  ),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_async_db_session),
) -> SubgraphResponse:
  """Create a new subgraph within a parent graph."""
  operation_start_time = record_operation_start()
  audit_logger = SecurityAuditLogger()

  try:
    # Circuit breaker check
    handle_circuit_breaker_check(graph_id, "create_subgraph")

    # Check if subgraph creation is enabled
    if not env.SUBGRAPH_CREATION_ENABLED:
      logger.warning(
        f"Subgraph creation blocked by feature flag for user {current_user.id} on graph {graph_id}"
      )
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Subgraph creation is currently disabled. Please contact support if you need assistance.",
      )

    # 1. Verify parent graph access (requires admin)
    parent_graph = verify_parent_graph_access(graph_id, current_user, db, "admin")

    # 2. Verify tier supports subgraphs
    verify_subgraph_tier_support(parent_graph)

    # 3. Verify parent graph is active
    verify_parent_graph_active(parent_graph)

    # 4. Check subgraph quota
    current_count, max_subgraphs, existing_subgraphs = check_subgraph_quota(
      parent_graph, db
    )

    # 5. Validate name uniqueness
    validate_subgraph_name_unique(request.name, existing_subgraphs, graph_id)

    # Log creation attempt
    api_logger.info(
      f"User {current_user.id} creating subgraph '{request.name}' for graph {graph_id}",
      extra={
        "user_id": current_user.id,
        "graph_id": graph_id,
        "subgraph_name": request.name,
        "operation": "create_subgraph",
      },
    )

    # 6. Check if we need SSE for forking
    if request.fork_parent:
      # Use SSE for fork operations (like graph creation)
      from robosystems.middleware.sse.operation_manager import create_operation_response
      from robosystems.tasks.graph_operations.create_subgraph import (
        create_subgraph_with_fork_sse_task,
      )

      # Create SSE operation
      operation_response = await create_operation_response(
        operation_type="subgraph_fork",
        user_id=str(current_user.id),
        graph_id=graph_id,
      )

      # Prepare task data
      task_data = {
        "parent_graph_id": graph_id,
        "user_id": str(current_user.id),
        "name": request.name,
        "description": request.display_name,
        "subgraph_type": request.subgraph_type.value
        if request.subgraph_type
        else "static",
        "metadata": request.metadata,
        "fork_parent": True,
        "fork_options": request.metadata.get("fork_options")
        if request.metadata
        else {},
      }

      # Queue the task with operation ID for SSE progress tracking
      task = create_subgraph_with_fork_sse_task.delay(  # type: ignore[reportFunctionMemberAccess]
        task_data, operation_response["operation_id"]
      )

      logger.info(
        f"Created SSE operation {operation_response['operation_id']} and queued task {task.id} "
        f"for subgraph creation with fork"
      )

      # Record success metrics
      record_operation_metrics(
        start_time=operation_start_time,
        operation_name="create_subgraph",
        parent_graph_id=graph_id,
        additional_tags={
          "success": True,
          "entity_count": 1,
          "fork": True,
        },
      )

      # Return operation response for SSE monitoring (202 Accepted)
      from fastapi import Response

      return Response(
        content=json.dumps(operation_response),
        status_code=status.HTTP_202_ACCEPTED,
        media_type="application/json",
      )

    # Non-fork path: Create immediately and return SubgraphResponse
    service = get_subgraph_service()
    subgraph_result = await service.create_subgraph(
      parent_graph=parent_graph,
      user=current_user,
      name=request.name,
      description=request.display_name,
      subgraph_type=request.subgraph_type.value if request.subgraph_type else "static",
      metadata=request.metadata,
      fork_parent=False,  # Immediate creation doesn't fork
      fork_options=None,
    )

    # Log security event
    audit_logger.log_security_event(
      SecurityEventType.SUBGRAPH_CREATED,
      user_id=str(current_user.id),
      details={
        "resource_id": subgraph_result["graph_id"],
        "parent_graph_id": graph_id,
        "subgraph_name": request.name,
        "subgraph_type": request.subgraph_type.value
        if request.subgraph_type
        else "static",
      },
    )

    # Log metrics
    log_metric(
      "subgraph_created",
      1,
      {
        "graph_id": graph_id,
        "subgraph_id": subgraph_result["graph_id"],
        "user_id": str(current_user.id),
        "tier": parent_graph.graph_tier,
      },
    )

    # Record success metrics
    record_operation_metrics(
      start_time=operation_start_time,
      operation_name="create_subgraph",
      parent_graph_id=graph_id,
      additional_tags={
        "success": True,
        "entity_count": 1,
      },
    )

    return SubgraphResponse(
      graph_id=subgraph_result["graph_id"],
      parent_graph_id=graph_id,
      subgraph_index=subgraph_result.get("subgraph_index", 1),
      subgraph_name=request.name,
      display_name=request.display_name,
      description=request.description,
      subgraph_type=request.subgraph_type or SubgraphType.STATIC,
      status=subgraph_result.get("status", "active"),
      created_at=subgraph_result.get("created_at"),
      updated_at=subgraph_result.get("updated_at", subgraph_result.get("created_at")),
      size_mb=None,
      node_count=None,
      edge_count=None,
      last_accessed=None,
      metadata=request.metadata,
    )

  except HTTPException:
    record_operation_metrics(
      start_time=operation_start_time,
      operation_name="create_subgraph",
      parent_graph_id=graph_id,
      additional_tags={"success": False},
    )
    raise
  except SQLAlchemyError as e:
    record_operation_metrics(
      start_time=operation_start_time,
      operation_name="create_subgraph",
      parent_graph_id=graph_id,
      additional_tags={"success": False, "error_type": "db"},
    )
    logger.error(f"Database error creating subgraph: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to create subgraph due to database error",
    )
  except Exception as e:
    record_operation_metrics(
      start_time=operation_start_time,
      operation_name="create_subgraph",
      parent_graph_id=graph_id,
      additional_tags={"success": False, "error_type": "unexpected"},
    )
    logger.error(f"Unexpected error creating subgraph: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="An unexpected error occurred while creating the subgraph",
    )
