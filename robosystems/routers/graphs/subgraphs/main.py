"""
Subgraph read routes (list).

Write operations (create, delete) live at
``POST /v1/graphs/{graph_id}/operations/{create-subgraph,delete-subgraph}``.
"""

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from robosystems.config.graph_tier import get_tier_max_subgraphs
from robosystems.database import get_async_db_session
from robosystems.logger import api_logger, log_metric, logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_ID_PATTERN
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
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

router = APIRouter(dependencies=[Depends(subscription_aware_rate_limit_dependency)])


async def get_subgraph_sizes(parent_graph_id: str) -> dict[str, int]:
  """On-disk bytes per subgraph, from one instance-wide storage breakdown.

  Replaces an N+1 of `get_database_metrics` calls that also measured
  something different. Two problems came from that path, and both are fixed
  by reading the breakdown the storage cap already reads:

  - **It undercounted.** `get_database_metrics` reports `db_info.size_bytes`,
    the primary `.lbug` only. Writes land in the write-ahead log first, so
    during a write burst this page sat frozen while `/usage` — which scans
    disk and counts `.lbug` + `.lbug.wal` — climbed. Same subgraph, two
    numbers, and the smaller one looked stuck.
  - **It rounded to a floor.** `round(bytes / 1024**2, 2)` cannot represent
    less than ~10.24 KB, so a subgraph under ~5 KB reported 0.0 and rendered
    as nothing at all.

  Returns an empty mapping when the breakdown is unavailable; callers report
  `None` rather than zero, because "not measured" is not "empty".
  """
  try:
    from robosystems.graph_api.client.factory import GraphClientFactory

    graph_client = await GraphClientFactory.create_client(
      graph_id=parent_graph_id, operation_type="read"
    )
    try:
      breakdown = await graph_client.get_storage_breakdown(parent_graph_id)
    finally:
      await graph_client.close()

    # A subgraph's footprint is spread across item types — its database
    # (with WAL folded in) and its vector index share the subgraph's id, so
    # summing by id rather than filtering to type="subgraph" is what makes
    # this agree with the instance total.
    sizes: dict[str, int] = {}
    for item in breakdown.get("items", []):
      item_id = item.get("id")
      if item_id:
        sizes[item_id] = sizes.get(item_id, 0) + int(item.get("bytes") or 0)
    return sizes
  except Exception as e:
    logger.warning(f"Failed to get subgraph sizes for {parent_graph_id}: {e}")
    return {}


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

    # One instance-wide breakdown covers every subgraph — they all live on
    # the parent's box. Absent means "could not measure", which is reported
    # as None per subgraph rather than 0.
    sizes = await get_subgraph_sizes(parent_graph.graph_id)

    subgraph_summaries = []
    total_size_bytes = 0
    measured_any = False
    for subgraph in subgraphs:
      size_bytes = sizes.get(subgraph.graph_id)
      # Extract subgraph name from graph_id (format: {parent_id}_{subgraph_name})
      subgraph_name = subgraph.subgraph_name
      if not subgraph_name and "_" in subgraph.graph_id:
        # Fallback: extract from graph_id if subgraph_name is not set
        subgraph_name = subgraph.graph_id.split("_", 1)[1]

      if size_bytes is not None:
        total_size_bytes += size_bytes
        measured_any = True

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
          size_bytes=size_bytes,
          # Enough precision to stay non-zero at subgraph scale; the old
          # 2-decimal rounding bottomed out at ~10.24 KB.
          size_mb=(
            round(size_bytes / (1024 * 1024), 6) if size_bytes is not None else None
          ),
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
      # Null when nothing could be measured — distinct from a genuine zero,
      # which is what a graph with empty subgraphs legitimately reports.
      total_size_bytes=total_size_bytes if measured_any else None,
      total_size_mb=(
        round(total_size_bytes / (1024 * 1024), 6) if measured_any else None
      ),
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
