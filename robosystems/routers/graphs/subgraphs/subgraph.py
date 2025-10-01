"""
Main subgraph routes (list and create operations).
"""

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from robosystems.database import get_async_db_session
from robosystems.middleware.auth import get_current_user
from robosystems.models.api.subgraph import (
  CreateSubgraphRequest,
  SubgraphResponse,
  SubgraphSummary,
  ListSubgraphsResponse,
  SubgraphType,
)
from robosystems.models.iam.graph import Graph
from robosystems.models.iam.user import User
from robosystems.models.iam.user_graph import UserGraph
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
from robosystems.config.tier_config import get_tier_max_subgraphs
from robosystems.config import env

router = APIRouter()


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
    pattern="^(kg[a-f0-9]{10}|sec)$",
  ),
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_async_db_session),
) -> ListSubgraphsResponse:
  """List all subgraphs for a parent graph."""
  operation_start_time = record_operation_start()

  try:
    # Circuit breaker check
    handle_circuit_breaker_check(graph_id, "list_subgraphs")

    # Verify access to parent graph
    parent_graph = verify_parent_graph_access(db, current_user, graph_id, "read")

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
      .filter(Graph.parent_id == parent_graph.id, ~Graph.is_deleted)
      .order_by(Graph.created_at.desc())
      .all()
    )

    subgraph_summaries = []
    for subgraph in subgraphs:
      # Get usage stats from UserGraph
      user_graph = (
        db.query(UserGraph)
        .filter(UserGraph.user_id == current_user.id, UserGraph.graph_id == subgraph.id)
        .first()
      )

      subgraph_summaries.append(
        SubgraphSummary(
          graph_id=subgraph.graph_id,
          subgraph_name=subgraph.name,
          display_name=subgraph.description or subgraph.name,
          subgraph_type=SubgraphType(subgraph.graph_type),
          status=subgraph.status,
          created_at=subgraph.created_at,
          size_mb=None,  # Not available in current model
          last_accessed=user_graph.last_accessed if user_graph else None,
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

    return ListSubgraphsResponse(
      parent_graph_id=graph_id,
      parent_graph_name=parent_graph.name,
      parent_graph_tier=parent_graph.graph_tier,
      subgraphs=subgraph_summaries,
      subgraph_count=len(subgraph_summaries),
      max_subgraphs=get_tier_max_subgraphs(parent_graph.graph_tier),
      total_size_mb=None,  # Not calculated yet
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
  response_model=SubgraphResponse,
  operation_id="createSubgraph",
  summary="Create Subgraph",
  description="""Create a new subgraph within a parent graph.

**Requirements:**
- Valid authentication
- Parent graph must exist and be accessible to the user
- User must have 'admin' permission on the parent graph
- Parent graph tier must support subgraphs (Enterprise or Premium only)
- Must be within subgraph quota limits
- Subgraph name must be unique within the parent graph

**Returns:**
- Created subgraph details including its unique ID
""",
  status_code=status.HTTP_201_CREATED,
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
    pattern="^(kg[a-f0-9]{10}|sec)$",
  ),
  current_user: User = Depends(get_current_user),
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
    parent_graph = verify_parent_graph_access(db, current_user, graph_id, "admin")

    # 2. Verify tier supports subgraphs
    verify_subgraph_tier_support(parent_graph)

    # 3. Verify parent graph is active
    verify_parent_graph_active(parent_graph)

    # 4. Check subgraph quota
    check_subgraph_quota(db, parent_graph)

    # 5. Validate name uniqueness
    validate_subgraph_name_unique(db, parent_graph, request.name)

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

    # 6. Create the subgraph using service
    service = get_subgraph_service()
    subgraph_result = service.create_subgraph(
      parent_graph=parent_graph,
      user=current_user,
      name=request.name,
      description=request.description,
      subgraph_type=request.type.value if request.type else "time_series",
      metadata=request.metadata,
    )

    # Log security event
    audit_logger.log_security_event(
      SecurityEventType.SUBGRAPH_CREATED,
      user_id=str(current_user.id),
      details={
        "resource_id": subgraph_result["graph_id"],
        "parent_graph_id": graph_id,
        "subgraph_name": request.name,
        "subgraph_type": request.type.value if request.type else "time_series",
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
      display_name=request.description or request.name,
      description=request.description,
      subgraph_type=SubgraphType(
        subgraph_result.get("graph_type", request.type or SubgraphType.TIME_SERIES)
      ),
      status=subgraph_result.get("status", "active"),
      created_at=subgraph_result.get("created_at"),
      updated_at=subgraph_result.get("updated_at", subgraph_result.get("created_at")),
      size_mb=None,  # Not available yet
      node_count=None,  # Not available yet
      edge_count=None,  # Not available yet
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
