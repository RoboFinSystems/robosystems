"""User analytics and usage monitoring API endpoints."""

from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Depends, HTTPException, status, Query

from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.middleware.rate_limits import analytics_rate_limit_dependency
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from robosystems.models.iam import User, UserGraph, UserLimits, UserAPIKey
from robosystems.models.iam.user_usage_tracking import UserUsageTracking, UsageType
from robosystems.models.api.user import (
  UserGraphSummary,
  UserUsageSummaryResponse,
  UserAnalyticsResponse,
)
from robosystems.operations.graph.metrics_service import GraphMetricsService
from robosystems.database import get_db_session
from sqlalchemy.orm import Session
from robosystems.logger import logger

# Create router for user analytics endpoints
router = APIRouter(tags=["User Analytics"])

# Initialize services
graph_metrics_service = GraphMetricsService()


@router.get(
  "/overview",
  response_model=UserUsageSummaryResponse,
  summary="Get User Usage Overview",
  description="Get a high-level overview of usage statistics for the current user.",
  status_code=status.HTTP_200_OK,
  operation_id="getUserUsageOverview",
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/user/analytics/overview",
  business_event_type="user_overview_accessed",
)
async def get_user_overview(
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(analytics_rate_limit_dependency),
) -> UserUsageSummaryResponse:
  """
  Get a high-level overview of usage statistics for the current user.

  This endpoint provides a summary of:
  - Number of accessible graphs vs limits
  - Total nodes and relationships across all graphs
  - Usage compared to subscription limits
  - Basic information about each graph
  """
  try:
    # Get user limits and usage
    user_limits = UserLimits.get_or_create_for_user(current_user.id, db)
    usage_stats = user_limits.get_current_usage(db)

    # Get all user graphs
    user_graphs = UserGraph.get_by_user_id(current_user.id, db)

    # Collect metrics for each graph
    graph_summaries = []
    total_nodes = 0
    total_relationships = 0

    for ug in user_graphs:
      try:
        # Get basic metrics for each graph
        metrics = await graph_metrics_service.collect_metrics_for_graph_async(
          ug.graph_id
        )

        if metrics and "error" not in metrics:
          nodes = metrics.get("total_nodes", 0)
          relationships = metrics.get("total_relationships", 0)
          size_mb = metrics.get("estimated_size", {}).get("total_mb", 0.0)

          total_nodes += nodes
          total_relationships += relationships

          graph_summaries.append(
            UserGraphSummary(
              graph_id=ug.graph_id,
              graph_name=ug.graph_name,
              role=ug.role,
              total_nodes=nodes,
              total_relationships=relationships,
              estimated_size_mb=size_mb,
              last_accessed=ug.updated_at.isoformat() if ug.updated_at else None,
            )
          )
      except Exception as e:
        logger.warning(f"Failed to get metrics for graph {ug.graph_id}: {e}")
        # Add graph with zero metrics if we can't fetch them
        graph_summaries.append(
          UserGraphSummary(
            graph_id=ug.graph_id,
            graph_name=ug.graph_name,
            role=ug.role,
            total_nodes=0,
            total_relationships=0,
            estimated_size_mb=0.0,
            last_accessed=ug.updated_at.isoformat() if ug.updated_at else None,
          )
        )

    # Combine usage vs limits information
    usage_vs_limits = {
      "graphs": usage_stats["graphs"],
    }

    # Record business event
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/user/analytics/overview",
      method="GET",
      event_type="user_overview_accessed",
      event_data={
        "user_id": current_user.id,
        "graph_count": len(user_graphs),
        "total_nodes": total_nodes,
      },
      user_id=current_user.id,
    )

    return UserUsageSummaryResponse(
      user_id=current_user.id,
      graph_count=len(user_graphs),
      total_nodes=total_nodes,
      total_relationships=total_relationships,
      usage_vs_limits=usage_vs_limits,
      graphs=graph_summaries,
      timestamp=datetime.now(timezone.utc).isoformat(),
    )

  except Exception as e:
    logger.error(f"Error getting usage overview for user {current_user.id}: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to retrieve usage overview. Please try again later.",
    )


@router.get(
  "/detailed",
  response_model=UserAnalyticsResponse,
  summary="Get Detailed User Analytics",
  description="Get comprehensive analytics for the current user including API usage and recent activity.",
  status_code=status.HTTP_200_OK,
  operation_id="getDetailedUserAnalytics",
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/user/analytics/detailed",
  business_event_type="user_detailed_analytics_accessed",
)
async def get_detailed_analytics(
  include_api_stats: bool = Query(True, description="Include API usage statistics"),
  include_recent_activity: bool = Query(True, description="Include recent activity"),
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(analytics_rate_limit_dependency),
) -> UserAnalyticsResponse:
  """
  Get comprehensive analytics for the current user.

  This endpoint provides detailed information including:
  - User profile and subscription info
  - Graph usage statistics across all accessible graphs
  - API usage patterns and statistics
  - Current limits and restrictions
  - Recent activity (optional)
  """
  try:
    # Get user info
    user_info = {
      "user_id": current_user.id,
      "email": current_user.email,
      "name": current_user.name,
      "role": current_user.role,
      "is_active": current_user.is_active,
      "created_at": current_user.created_at.isoformat()
      if current_user.created_at
      else None,
    }

    # Get user limits
    user_limits = UserLimits.get_or_create_for_user(current_user.id, db)
    limits = {
      "max_user_graphs": user_limits.max_user_graphs,
    }

    # Get graph usage statistics
    user_graphs = UserGraph.get_by_user_id(current_user.id, db)
    graph_usage = {
      "total_graphs": len(user_graphs),
      "graphs_by_role": {},
      "total_storage_mb": 0.0,
      "total_nodes": 0,
      "total_relationships": 0,
    }

    for ug in user_graphs:
      # Count graphs by role
      role = ug.role
      graph_usage["graphs_by_role"][role] = (
        graph_usage["graphs_by_role"].get(role, 0) + 1
      )

      # Try to get metrics
      try:
        metrics = await graph_metrics_service.collect_metrics_for_graph_async(
          ug.graph_id
        )
        if metrics and "error" not in metrics:
          graph_usage["total_nodes"] += metrics.get("total_nodes", 0)
          graph_usage["total_relationships"] += metrics.get("total_relationships", 0)
          graph_usage["total_storage_mb"] += metrics.get("estimated_size", {}).get(
            "total_mb", 0.0
          )
      except Exception:
        pass

    # Get API usage statistics
    api_usage = {}
    if include_api_stats:
      # Get API keys for the user
      api_keys = UserAPIKey.get_by_user_id(current_user.id, db)
      api_usage = {
        "total_api_keys": len(api_keys),
        "active_api_keys": len([k for k in api_keys if k.is_active]),
        "api_calls_today": _get_api_calls_today(current_user.id, db),
        "api_calls_this_hour": _get_api_calls_this_hour(current_user.id, db),
        "most_used_endpoints": _get_most_used_endpoints(current_user.id, db),
      }

    # Get recent activity
    recent_activity = []
    if include_recent_activity:
      recent_activity = _get_recent_activity(current_user.id, db)

    # Record business event
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/user/analytics/detailed",
      method="GET",
      event_type="user_detailed_analytics_accessed",
      event_data={
        "user_id": current_user.id,
        "include_api_stats": include_api_stats,
        "include_recent_activity": include_recent_activity,
        "total_graphs": len(user_graphs),
      },
      user_id=current_user.id,
    )

    return UserAnalyticsResponse(
      user_info=user_info,
      graph_usage=graph_usage,
      api_usage=api_usage,
      limits=limits,
      recent_activity=recent_activity,
      timestamp=datetime.now(timezone.utc).isoformat(),
    )

  except Exception as e:
    logger.error(
      f"Error getting detailed analytics for user {current_user.id}: {str(e)}"
    )
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to retrieve detailed analytics. Please try again later.",
    )


# Helper functions for analytics data collection


def _get_api_calls_today(user_id: str, db: Session) -> int:
  """Get the number of API calls made by user today."""
  try:
    today_start = datetime.now(timezone.utc).replace(
      hour=0, minute=0, second=0, microsecond=0
    )

    return UserUsageTracking.get_usage_count(
      user_id=user_id, usage_type=UsageType.API_CALL, session=db, since=today_start
    )
  except Exception as e:
    logger.error(f"Error getting API calls today for user {user_id}: {e}")
    return 0


def _get_api_calls_this_hour(user_id: str, db: Session) -> int:
  """Get the number of API calls made by user this hour."""
  try:
    hour_start = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    return UserUsageTracking.get_usage_count(
      user_id=user_id, usage_type=UsageType.API_CALL, session=db, since=hour_start
    )
  except Exception as e:
    logger.error(f"Error getting API calls this hour for user {user_id}: {e}")
    return 0


def _get_most_used_endpoints(user_id: str, db: Session, limit: int = 5) -> list:
  """Get the most frequently used endpoints by user."""
  try:
    from sqlalchemy import func

    # Get endpoint usage counts from the last 30 days
    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)

    results = (
      db.query(
        UserUsageTracking.endpoint, func.count(UserUsageTracking.id).label("count")
      )
      .filter(
        UserUsageTracking.user_id == user_id,
        UserUsageTracking.usage_type == UsageType.API_CALL.value,
        UserUsageTracking.endpoint.isnot(None),
        UserUsageTracking.occurred_at >= thirty_days_ago,
      )
      .group_by(UserUsageTracking.endpoint)
      .order_by(func.count(UserUsageTracking.id).desc())
      .limit(limit)
      .all()
    )

    return [{"endpoint": endpoint, "count": count} for endpoint, count in results]

  except Exception as e:
    logger.error(f"Error getting most used endpoints for user {user_id}: {e}")
    return []


def _get_recent_activity(user_id: str, db: Session, limit: int = 20) -> list:
  """Get recent activity for user."""
  try:
    # Get recent usage events from the last 7 days
    seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)

    activities = (
      db.query(UserUsageTracking)
      .filter(
        UserUsageTracking.user_id == user_id,
        UserUsageTracking.occurred_at >= seven_days_ago,
      )
      .order_by(UserUsageTracking.occurred_at.desc())
      .limit(limit)
      .all()
    )

    activity_list = []
    for activity in activities:
      activity_data = {
        "type": activity.usage_type,
        "timestamp": activity.occurred_at.isoformat(),
        "resource_count": activity.resource_count,
      }

      # Add type-specific data
      if activity.endpoint:
        activity_data["endpoint"] = activity.endpoint
      if activity.graph_id:
        activity_data["graph_id"] = activity.graph_id

      activity_list.append(activity_data)

    return activity_list

  except Exception as e:
    logger.error(f"Error getting recent activity for user {user_id}: {e}")
    return []
