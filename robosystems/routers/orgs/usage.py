"""Organization usage and limits endpoints."""

from datetime import datetime, timezone, timedelta
from decimal import Decimal
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import func

from ...database import get_db_session
from ...middleware.auth.dependencies import get_current_user
from ...middleware.rate_limits import general_api_rate_limit_dependency
from ...models.iam import (
  User,
  OrgUser,
  OrgLimits,
  Graph,
  GraphCredits,
  GraphUsage,
  UsageEventType,
)
from ...models.api.orgs import (
  OrgLimitsResponse,
  OrgUsageResponse,
  OrgUsageSummary,
)
from ...logger import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Org Usage"])


@router.get(
  "/orgs/{org_id}/limits",
  response_model=OrgLimitsResponse,
  summary="Get Organization Limits",
  description="Get the current limits and quotas for an organization.",
  operation_id="getOrgLimits",
)
async def get_org_limits(
  org_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> OrgLimitsResponse:
  """Get organization limits and current usage against those limits."""
  try:
    # Check if user is a member of the org
    membership = OrgUser.get_by_org_and_user(org_id, current_user.id, db)
    if not membership:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="You are not a member of this organization",
      )

    # Get org limits
    limits = OrgLimits.get_by_org_id(org_id, db)
    if not limits:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Organization limits not found",
      )

    # Get current usage
    usage = limits.get_current_usage(db)

    # Calculate if any limits are approaching or exceeded
    warnings = []
    current_graphs = usage["graphs"]["current"]
    if current_graphs >= limits.max_graphs * 0.9:
      warnings.append(f"Approaching graph limit ({current_graphs}/{limits.max_graphs})")

    return OrgLimitsResponse(
      org_id=org_id,
      max_graphs=limits.max_graphs,
      current_usage=usage,
      warnings=warnings,
      can_create_graph=limits.can_create_graph(db)[0],
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Error getting organization limits: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to get organization limits",
    )


@router.get(
  "/orgs/{org_id}/usage",
  response_model=OrgUsageResponse,
  summary="Get Organization Usage",
  description="Get detailed usage statistics for an organization aggregated across all graphs.",
  operation_id="getOrgUsage",
)
async def get_org_usage(
  org_id: str,
  days: int = 30,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> OrgUsageResponse:
  """Get organization usage statistics aggregated across all graphs."""
  try:
    # Check if user is a member of the org
    membership = OrgUser.get_by_org_and_user(org_id, current_user.id, db)
    if not membership:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="You are not a member of this organization",
      )

    # Get all graphs for the org
    graphs = db.query(Graph).filter(Graph.org_id == org_id).all()
    graph_ids = [g.graph_id for g in graphs]

    # Calculate time range
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days)

    # Aggregate credit usage across all graphs
    total_credits_used = Decimal(0)
    total_ai_operations = 0
    total_storage_gb = Decimal(0)
    total_api_calls = 0

    graph_usage_details = []

    for graph in graphs:
      # Get graph credits
      credits = GraphCredits.get_by_graph_id(graph.graph_id, db)
      if not credits:
        continue

      # Get usage records for this graph in the time period
      usage_records = (
        db.query(GraphUsage)
        .filter(
          GraphUsage.graph_id == graph.graph_id,
          GraphUsage.recorded_at >= start_date,
          GraphUsage.recorded_at <= end_date,
        )
        .all()
      )

      graph_credits_used = sum(
        r.credits_consumed or 0 for r in usage_records if r.credits_consumed
      )
      graph_ai_ops = sum(
        1 for r in usage_records if r.event_type == UsageEventType.AI_OPERATION
      )
      graph_api_calls = len(usage_records)

      # Get latest storage snapshot
      latest_storage = (
        db.query(GraphUsage)
        .filter(
          GraphUsage.graph_id == graph.graph_id,
          GraphUsage.event_type == UsageEventType.STORAGE_SNAPSHOT,
        )
        .order_by(GraphUsage.recorded_at.desc())
        .first()
      )

      graph_storage = latest_storage.storage_gb if latest_storage else Decimal(0)

      graph_usage_details.append(
        {
          "graph_id": graph.graph_id,
          "graph_name": graph.graph_name,
          "credits_used": float(graph_credits_used),
          "ai_operations": graph_ai_ops,
          "storage_gb": float(graph_storage),
          "api_calls": graph_api_calls,
          "credits_available": float(credits.current_balance) if credits else 0,
          "credits_allocated": float(credits.monthly_allocation) if credits else 0,
        }
      )

      total_credits_used += graph_credits_used
      total_ai_operations += graph_ai_ops
      total_storage_gb += graph_storage
      total_api_calls += graph_api_calls

    # Calculate daily averages
    daily_avg_credits = float(total_credits_used) / max(days, 1)
    daily_avg_api_calls = total_api_calls / max(days, 1)

    # Project monthly usage
    projected_monthly_credits = daily_avg_credits * 30
    projected_monthly_api_calls = daily_avg_api_calls * 30

    summary = OrgUsageSummary(
      total_credits_used=float(total_credits_used),
      total_ai_operations=total_ai_operations,
      total_storage_gb=float(total_storage_gb),
      total_api_calls=total_api_calls,
      daily_avg_credits=daily_avg_credits,
      daily_avg_api_calls=daily_avg_api_calls,
      projected_monthly_credits=projected_monthly_credits,
      projected_monthly_api_calls=int(projected_monthly_api_calls),
      credits_limit=None,  # We'll expand this later
      api_calls_limit=None,  # We'll expand this later
      storage_limit_gb=None,  # We'll expand this later
    )

    # Get historical usage trend (daily aggregates)
    daily_usage = []
    for i in range(min(days, 30)):
      day_start = start_date + timedelta(days=i)
      day_end = day_start + timedelta(days=1)

      day_records = (
        db.query(
          func.sum(GraphUsage.credits_consumed).label("credits"),
          func.count(GraphUsage.id).label("api_calls"),
        )
        .filter(
          GraphUsage.graph_id.in_(graph_ids) if graph_ids else False,
          GraphUsage.recorded_at >= day_start,
          GraphUsage.recorded_at < day_end,
        )
        .first()
      )

      daily_usage.append(
        {
          "date": day_start.date().isoformat(),
          "credits_used": float(day_records.credits or 0) if day_records else 0,
          "api_calls": day_records.api_calls if day_records else 0,
        }
      )

    return OrgUsageResponse(
      org_id=org_id,
      period_days=days,
      start_date=start_date,
      end_date=end_date,
      summary=summary,
      graph_details=graph_usage_details,
      daily_trend=daily_usage,
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Error getting organization usage: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to get organization usage",
    )
