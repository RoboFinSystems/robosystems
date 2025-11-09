"""Admin API for graph management."""

from datetime import datetime, timezone, timedelta
from typing import Optional, List
from fastapi import APIRouter, Request, HTTPException, status, Query
from sqlalchemy import func

from ...database import get_db_session
from ...models.iam import User, Graph
from ...models.iam.graph_backup import GraphBackup
from ...models.iam.graph_usage import GraphUsage, UsageEventType
from ...models.iam.graph_credits import GraphCredits
from ...models.api.admin import (
  GraphResponse,
  GraphStorageResponse,
  GraphBackupResponse,
  GraphInfrastructureResponse,
  GraphAnalyticsResponse,
)
from ...middleware.auth.admin import require_admin
from ...logger import get_logger
from ...config.graph_tier import GraphTierConfig

logger = get_logger(__name__)

router = APIRouter(prefix="/admin/v1/graphs", tags=["admin-graphs"])


def _get_graph_backend(graph: Graph) -> str:
  """Get the backend for a graph from its tier configuration.

  Args:
      graph: The graph model instance

  Returns:
      Backend type (e.g., "kuzu", "neo4j")
  """
  tier_config = GraphTierConfig.get_tier_config(graph.graph_tier)
  return tier_config.get("backend", "kuzu")


def _get_graph_status(graph: Graph) -> str:
  """Get the operational status for a graph.

  Args:
      graph: The graph model instance

  Returns:
      Status string (e.g., "active", "syncing", "error")
  """
  if graph.is_repository and graph.sync_status:
    return graph.sync_status
  return "active"


@router.get("", response_model=List[GraphResponse])
@require_admin(permissions=["graphs:read"])
async def list_graphs(
  request: Request,
  user_email: Optional[str] = Query(None, description="Filter by owner email"),
  tier: Optional[str] = Query(None, description="Filter by tier"),
  backend: Optional[str] = Query(None, description="Filter by backend"),
  status_filter: Optional[str] = Query(None, description="Filter by status"),
  limit: int = Query(100, ge=1, le=1000),
  offset: int = Query(0, ge=0),
):
  """List all graphs with optional filters."""
  session = next(get_db_session())
  try:
    query = session.query(Graph)

    if tier:
      query = query.filter(Graph.graph_tier == tier)

    if backend:
      query = query.filter(Graph.backend == backend)

    if status_filter:
      query = query.filter(Graph.status == status_filter)

    if user_email:
      query = query.join(User, Graph.user_id == User.id)
      query = query.filter(User.email.ilike(f"%{user_email}%"))

    total = query.count()
    graphs = query.offset(offset).limit(limit).all()

    results = []
    for graph in graphs:
      latest_usage = (
        session.query(GraphUsage)
        .filter(
          GraphUsage.graph_id == graph.graph_id,
          GraphUsage.event_type == UsageEventType.STORAGE_SNAPSHOT.value,
        )
        .order_by(GraphUsage.recorded_at.desc())
        .first()
      )

      subgraph_count = (
        session.query(func.count(Graph.graph_id))
        .filter(Graph.parent_graph_id == graph.graph_id)
        .scalar()
      )

      tier_config = GraphTierConfig.get_tier_config(graph.graph_tier)

      results.append(
        GraphResponse(
          graph_id=graph.graph_id,
          user_id=graph.org_id or "system",
          org_id=graph.org_id or "system",
          name=graph.graph_name,
          description=None,
          graph_tier=graph.graph_tier,
          backend=_get_graph_backend(graph),
          status=_get_graph_status(graph),
          storage_gb=float(latest_usage.storage_gb)
          if latest_usage and latest_usage.storage_gb
          else None,
          storage_limit_gb=tier_config.get("storage_limit_gb"),
          subgraph_count=subgraph_count or 0,
          subgraph_limit=tier_config.get("max_subgraphs"),
          created_at=graph.created_at,
          updated_at=graph.updated_at,
        )
      )

    logger.info(
      f"Admin listed {len(results)} graphs",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "total": total,
        "filters": {
          "user_email": user_email,
          "tier": tier,
          "backend": backend,
          "status": status_filter,
        },
      },
    )

    return results
  finally:
    session.close()


@router.get("/{graph_id}", response_model=GraphResponse)
@require_admin(permissions=["graphs:read"])
async def get_graph(request: Request, graph_id: str):
  """Get detailed information about a specific graph."""
  session = next(get_db_session())
  try:
    graph = session.query(Graph).filter(Graph.graph_id == graph_id).first()

    if not graph:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Graph {graph_id} not found",
      )

    latest_usage = (
      session.query(GraphUsage)
      .filter(
        GraphUsage.graph_id == graph.graph_id,
        GraphUsage.event_type == UsageEventType.STORAGE_SNAPSHOT.value,
      )
      .order_by(GraphUsage.recorded_at.desc())
      .first()
    )

    subgraph_count = (
      session.query(func.count(Graph.graph_id))
      .filter(Graph.parent_graph_id == graph.graph_id)
      .scalar()
    )

    tier_config = GraphTierConfig.get_tier_config(graph.graph_tier)

    logger.info(
      f"Admin retrieved graph {graph_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "graph_id": graph_id,
      },
    )

    return GraphResponse(
      graph_id=graph.graph_id,
      user_id=graph.org_id or "system",
      org_id=graph.org_id or "system",
      name=graph.graph_name,
      description=None,
      graph_tier=graph.graph_tier,
      backend=_get_graph_backend(graph),
      status=_get_graph_status(graph),
      storage_gb=float(latest_usage.storage_gb)
      if latest_usage and latest_usage.storage_gb
      else None,
      storage_limit_gb=tier_config.get("storage_limit_gb"),
      subgraph_count=subgraph_count or 0,
      subgraph_limit=tier_config.get("max_subgraphs"),
      created_at=graph.created_at,
      updated_at=graph.updated_at,
    )
  finally:
    session.close()


@router.get("/{graph_id}/storage", response_model=GraphStorageResponse)
@require_admin(permissions=["graphs:read"])
async def get_graph_storage(request: Request, graph_id: str):
  """Get storage usage details for a graph."""
  session = next(get_db_session())
  try:
    graph = session.query(Graph).filter(Graph.graph_id == graph_id).first()

    if not graph:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Graph {graph_id} not found",
      )

    latest_usage = (
      session.query(GraphUsage)
      .filter(
        GraphUsage.graph_id == graph.graph_id,
        GraphUsage.event_type == UsageEventType.STORAGE_SNAPSHOT.value,
      )
      .order_by(GraphUsage.recorded_at.desc())
      .first()
    )

    week_ago_usage = (
      session.query(GraphUsage)
      .filter(
        GraphUsage.graph_id == graph.graph_id,
        GraphUsage.event_type == UsageEventType.STORAGE_SNAPSHOT.value,
        GraphUsage.recorded_at <= datetime.now(timezone.utc) - timedelta(days=7),
      )
      .order_by(GraphUsage.recorded_at.desc())
      .first()
    )

    current_storage = (
      float(latest_usage.storage_gb)
      if latest_usage and latest_usage.storage_gb
      else 0.0
    )

    credits = GraphCredits.get_by_graph_id(graph_id, session)
    from ...config.billing import BillingConfig

    tier_config = BillingConfig.get_subscription_plan(graph.graph_tier)
    storage_limit = (
      float(credits.storage_limit_override_gb)
      if credits and credits.storage_limit_override_gb
      else float(tier_config.get("storage_limit_gb", 500))
    )

    usage_percentage = (
      (current_storage / storage_limit * 100) if storage_limit > 0 else 0
    )
    within_limit = current_storage <= storage_limit
    approaching_limit = usage_percentage >= 80

    recent_growth = None
    estimated_days_to_limit = None
    if week_ago_usage and week_ago_usage.storage_gb:
      week_ago_storage = float(week_ago_usage.storage_gb)
      recent_growth = current_storage - week_ago_storage

      if recent_growth > 0:
        daily_growth = recent_growth / 7
        remaining_storage = storage_limit - current_storage
        estimated_days_to_limit = (
          int(remaining_storage / daily_growth) if daily_growth > 0 else None
        )

    logger.info(
      f"Admin retrieved storage for graph {graph_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "graph_id": graph_id,
        "current_storage_gb": current_storage,
      },
    )

    return GraphStorageResponse(
      graph_id=graph_id,
      current_storage_gb=current_storage,
      storage_limit_gb=storage_limit,
      usage_percentage=usage_percentage,
      within_limit=within_limit,
      approaching_limit=approaching_limit,
      recent_growth_gb=recent_growth,
      estimated_days_to_limit=estimated_days_to_limit,
    )
  finally:
    session.close()


@router.get("/{graph_id}/backups", response_model=GraphBackupResponse)
@require_admin(permissions=["graphs:read"])
async def get_graph_backups(request: Request, graph_id: str):
  """Get backup status for a graph."""
  session = next(get_db_session())
  try:
    graph = session.query(Graph).filter(Graph.graph_id == graph_id).first()

    if not graph:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Graph {graph_id} not found",
      )

    backups = (
      session.query(GraphBackup)
      .filter(GraphBackup.graph_id == graph_id)
      .order_by(GraphBackup.created_at.desc())
      .all()
    )

    last_backup = backups[0] if backups else None
    backup_count = len(backups)
    total_size = sum(float(b.size_bytes or 0) for b in backups) / (1024**3)

    backup_status = "healthy"
    if not backups:
      backup_status = "no_backups"
    elif last_backup and (datetime.now(timezone.utc) - last_backup.created_at).days > 7:
      backup_status = "stale"

    logger.info(
      f"Admin retrieved backups for graph {graph_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "graph_id": graph_id,
        "backup_count": backup_count,
      },
    )

    return GraphBackupResponse(
      graph_id=graph_id,
      last_backup_at=last_backup.created_at if last_backup else None,
      backup_count=backup_count,
      total_backup_size_gb=total_size,
      backup_enabled=True,
      backup_status=backup_status,
    )
  finally:
    session.close()


@router.get("/{graph_id}/infrastructure", response_model=GraphInfrastructureResponse)
@require_admin(permissions=["graphs:read"])
async def get_graph_infrastructure(request: Request, graph_id: str):
  """Get infrastructure details for a graph."""
  session = next(get_db_session())
  try:
    graph = session.query(Graph).filter(Graph.graph_id == graph_id).first()

    if not graph:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Graph {graph_id} not found",
      )

    from ...middleware.graph.multitenant_utils import MultiTenantUtils

    identity = MultiTenantUtils.get_graph_identity(graph_id)

    from ...config.billing import BillingConfig

    tier_config = BillingConfig.get_subscription_plan(graph.graph_tier)

    instance_type = None
    cluster_type = None
    if identity.is_shared_repository:
      cluster_type = "shared"
      instance_type = tier_config.get("infrastructure", {}).get("instance_type")
    elif identity.is_user_graph:
      cluster_type = tier_config.get("infrastructure", {}).get(
        "cluster_type", "multi-tenant"
      )
      instance_type = tier_config.get("infrastructure", {}).get("instance_type")

    logger.info(
      f"Admin retrieved infrastructure for graph {graph_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "graph_id": graph_id,
        "tier": graph.graph_tier,
      },
    )

    return GraphInfrastructureResponse(
      graph_id=graph_id,
      tier=graph.graph_tier,
      instance_type=instance_type,
      cluster_type=cluster_type,
      writer_endpoint=None,
      reader_endpoint=None,
      connection_status="connected",
      health_status="healthy",
    )
  finally:
    session.close()


@router.get("/analytics", response_model=GraphAnalyticsResponse)
@require_admin(permissions=["graphs:read"])
async def get_graph_analytics(
  request: Request,
  tier: Optional[str] = Query(None, description="Filter by tier"),
):
  """Get cross-graph analytics."""
  session = next(get_db_session())
  try:
    query = session.query(Graph)

    if tier:
      query = query.filter(Graph.graph_tier == tier)

    all_graphs = query.all()

    total_graphs = len(all_graphs)

    by_tier = {}
    by_backend = {}
    by_status = {}

    for graph in all_graphs:
      tier_name = graph.graph_tier
      by_tier[tier_name] = by_tier.get(tier_name, 0) + 1

      backend = _get_graph_backend(graph)
      by_backend[backend] = by_backend.get(backend, 0) + 1

      status = _get_graph_status(graph)
      by_status[status] = by_status.get(status, 0) + 1

    storage_query = (
      session.query(
        GraphUsage.graph_id,
        func.max(GraphUsage.storage_gb).label("storage_gb"),
      )
      .filter(GraphUsage.event_type == UsageEventType.STORAGE_SNAPSHOT.value)
      .group_by(GraphUsage.graph_id)
      .all()
    )

    storage_map = {g.graph_id: float(g.storage_gb or 0) for g in storage_query}
    total_storage_gb = sum(storage_map.values())

    largest_graphs = sorted(
      [
        {"graph_id": gid, "storage_gb": size}
        for gid, size in storage_map.items()
        if size > 0
      ],
      key=lambda x: x["storage_gb"],
      reverse=True,
    )[:10]

    most_recently_updated = sorted(
      [
        {
          "graph_id": g.graph_id,
          "name": g.graph_name,
          "updated_at": g.updated_at.isoformat() if g.updated_at else None,
        }
        for g in all_graphs
      ],
      key=lambda x: x["updated_at"] or "",
      reverse=True,
    )[:10]

    logger.info(
      "Admin retrieved graph analytics",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "total_graphs": total_graphs,
      },
    )

    return GraphAnalyticsResponse(
      total_graphs=total_graphs,
      by_tier=by_tier,
      by_backend=by_backend,
      by_status=by_status,
      total_storage_gb=total_storage_gb,
      largest_graphs=largest_graphs,
      most_active_graphs=most_recently_updated,
    )
  finally:
    session.close()
