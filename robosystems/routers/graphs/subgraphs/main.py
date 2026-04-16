"""
Subgraph read routes (list).

Write operations (create, delete) live at
``POST /v1/graphs/{graph_id}/operations/{create-subgraph,delete-subgraph}``.
"""

import asyncio

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from robosystems.config.graph_tier import get_tier_max_subgraphs
from robosystems.database import get_async_db_session
from robosystems.logger import api_logger, log_metric, logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_ID_PATTERN
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.api.graphs.subgraphs import (
  CreateSubgraphRequest,
  ListSubgraphsResponse,
  SubgraphResponse,
  SubgraphSummary,
  SubgraphType,
)
from robosystems.models.core.graph import Graph
from robosystems.models.core.user import User
from robosystems.security import SecurityAuditLogger, SecurityEventType

from .utils import (
  check_subgraph_quota,
  get_subgraph_service,
  handle_circuit_breaker_check,
  record_operation_metrics,
  record_operation_start,
  validate_subgraph_name_unique,
  verify_parent_graph_access,
  verify_parent_graph_active,
  verify_subgraph_tier_support,
)

router = APIRouter()


async def get_database_size_mb(graph_id: str) -> float | None:
  """Returns None if Graph API has no size data — expected for empty or new databases."""
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
  responses={**RESOURCE_ERROR_RESPONSES},
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
    for subgraph, size_mb in zip(subgraphs, sizes, strict=False):
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


# ---------------------------------------------------------------------------
# create_subgraph — not routed here; called by the graph operations router
# at POST /v1/graphs/{graph_id}/operations/create-subgraph
# ---------------------------------------------------------------------------


async def create_subgraph(
  request: CreateSubgraphRequest,
  graph_id: str,
  current_user: User,
  db: Session,
):
  from robosystems.config import env

  operation_start_time = record_operation_start()
  audit_logger = SecurityAuditLogger()

  # Circuit breaker check
  handle_circuit_breaker_check(graph_id, "create_subgraph")

  # Check if subgraph creation is enabled
  if not env.SUBGRAPH_CREATION_ENABLED:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Subgraph creation is currently disabled.",
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

  # 6. Fork path: enqueue to worker for background execution
  if request.fork_parent:
    from robosystems.worker.client import enqueue_task

    response = await enqueue_task(
      task_type="subgraph_creation",
      graph_id=graph_id,
      user_id=str(current_user.id),
      params={
        "parent_graph_id": graph_id,
        "subgraph_name": request.name,
        "description": request.display_name,
        "fork_data": True,
      },
    )

    audit_logger.log_security_event(
      SecurityEventType.SUBGRAPH_CREATED,
      user_id=str(current_user.id),
      details={
        "operation_id": response["operation_id"],
        "parent_graph_id": graph_id,
        "subgraph_name": request.name,
        "fork": True,
      },
    )

    record_operation_metrics(
      start_time=operation_start_time,
      operation_name="create_subgraph",
      parent_graph_id=graph_id,
      additional_tags={"success": True, "fork": True},
    )

    return response

  # Non-fork path: Create immediately
  service = get_subgraph_service()
  subgraph_result = await service.create_subgraph(
    parent_graph=parent_graph,
    user=current_user,
    name=request.name,
    description=request.display_name,
    subgraph_type=request.subgraph_type.value if request.subgraph_type else "static",
    metadata=request.metadata,
    fork_parent=False,
    fork_options=None,
  )

  audit_logger.log_security_event(
    SecurityEventType.SUBGRAPH_CREATED,
    user_id=str(current_user.id),
    details={
      "resource_id": subgraph_result["graph_id"],
      "parent_graph_id": graph_id,
      "subgraph_name": request.name,
    },
  )

  record_operation_metrics(
    start_time=operation_start_time,
    operation_name="create_subgraph",
    parent_graph_id=graph_id,
    additional_tags={"success": True},
  )

  from robosystems.dagster.reporting import report_asset_materialization

  await report_asset_materialization(
    asset_key="user_subgraph_creation",
    description=f"Subgraph {subgraph_result['graph_id']} created from {graph_id}",
    metadata={
      "graph_id": subgraph_result["graph_id"],
      "parent_graph_id": graph_id,
      "user_id": str(current_user.id),
      "provisioning_method": "direct",
      "subgraph_name": request.name,
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
