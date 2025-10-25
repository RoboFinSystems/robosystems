"""
User limits management API endpoints.

This module provides endpoints for:
- User graph limits and usage statistics
- Shared repository rate limit status and usage
"""

from typing import Dict
from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.models.iam import User
from robosystems.middleware.rate_limits import (
  user_management_rate_limit_dependency,
  DualLayerRateLimiter,
  SharedRepositoryRateLimits,
)
from robosystems.middleware.otel.metrics import (
  get_endpoint_metrics,
  endpoint_metrics_decorator,
)
from robosystems.models.iam import UserLimits
from robosystems.models.iam.user_repository import UserRepository
from robosystems.models.api.user import UserLimitsResponse, UserUsageResponse
from robosystems.config.billing.repositories import SharedRepository
from robosystems.config.valkey_registry import ValkeyDatabase

router = APIRouter(tags=["User Limits"])


# =============================================================================
# User Graph Limits
# =============================================================================


@router.get(
  "",
  response_model=UserLimitsResponse,
  operation_id="getUserLimits",
  summary="Get user limits",
  description="Retrieve current limits and restrictions for the authenticated user",
  responses={
    200: {"description": "User limits retrieved successfully"},
    404: {"description": "User limits not found"},
  },
)
@endpoint_metrics_decorator(
  "/v1/user/limits", business_event_type="user_limits_retrieved"
)
async def get_user_limits(
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
):
  """
  Get current limits and restrictions for the authenticated user.

  This endpoint returns the user's current subscription limits including:
  - Maximum number of user graphs allowed
  - API usage limits
  - Import restrictions

  Args:
      current_user: Authenticated user (automatically injected)
      db: Database session (automatically injected)

  Returns:
      UserLimitsResponse: Current user limits and subscription information

  Raises:
      HTTPException: 404 if user limits not found
  """
  try:
    user_limits = UserLimits.get_or_create_for_user(current_user.id, db)

    # Record business event details
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/user/limits",
      method="GET",
      event_type="user_limits_retrieved_details",
      event_data={
        "user_id": current_user.id,
        "max_user_graphs": user_limits.max_user_graphs,
      },
      user_id=current_user.id,
    )

    return UserLimitsResponse(
      id=user_limits.id,
      user_id=user_limits.user_id,
      max_user_graphs=user_limits.max_user_graphs,
      created_at=user_limits.created_at.isoformat(),
      updated_at=user_limits.updated_at.isoformat(),
    )

  except Exception as e:
    # Record business event for limits retrieval failure
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/user/limits",
      method="GET",
      event_type="user_limits_retrieval_failed",
      event_data={
        "user_id": current_user.id,
        "error": str(e),
      },
      user_id=current_user.id,
    )
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to retrieve user limits: {str(e)}",
    )


@router.get(
  "/usage",
  response_model=UserUsageResponse,
  operation_id="getUserUsage",
  summary="Get user usage statistics",
  description="Retrieve current usage statistics and remaining limits for the authenticated user",
  responses={
    200: {"description": "User usage statistics retrieved successfully"},
  },
)
@endpoint_metrics_decorator(
  "/v1/user/limits/usage", business_event_type="user_usage_retrieved"
)
async def get_user_usage(
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
):
  """
  Get current usage statistics and remaining limits for the authenticated user.

  This endpoint provides a comprehensive view of the user's current resource
  usage compared to their limits, including:
  - User graphs: current count, limit, and remaining
  - Current limit settings

  Args:
      current_user: Authenticated user (automatically injected)
      db: Database session (automatically injected)

  Returns:
      UserUsageResponse: Comprehensive usage statistics and limits information
  """
  try:
    user_limits = UserLimits.get_or_create_for_user(current_user.id, db)
    usage_stats = user_limits.get_current_usage(db)

    # Record business event details
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/user/limits/usage",
      method="GET",
      event_type="user_usage_retrieved_details",
      event_data={
        "user_id": current_user.id,
        "current_graphs": usage_stats["graphs"]["current"],
        "remaining_graphs": usage_stats["graphs"]["remaining"],
        "graph_limit": usage_stats["graphs"]["limit"],
      },
      user_id=current_user.id,
    )

    return UserUsageResponse(
      user_id=current_user.id,
      graphs=usage_stats["graphs"],
      limits=UserLimitsResponse(
        id=user_limits.id,
        user_id=user_limits.user_id,
        max_user_graphs=user_limits.max_user_graphs,
        created_at=user_limits.created_at.isoformat(),
        updated_at=user_limits.updated_at.isoformat(),
      ),
    )

  except Exception as e:
    # Record business event for usage retrieval failure
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/user/limits/usage",
      method="GET",
      event_type="user_usage_retrieval_failed",
      event_data={
        "user_id": current_user.id,
        "error": str(e),
      },
      user_id=current_user.id,
    )
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to retrieve user usage: {str(e)}",
    )


# =============================================================================
# Shared Repository Limits
# =============================================================================


@router.get(
  "/shared-repositories/summary",
  operation_id="getAllSharedRepositoryLimits",
  summary="Get all shared repository limits",
  description="Get rate limit status for all shared repositories the user has access to.",
  response_model=Dict,
)
async def get_all_shared_repository_limits(
  current_user: User = Depends(get_current_user),
  session: Session = Depends(get_db_session),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
) -> Dict:
  """Get rate limit status for all accessible shared repositories."""

  repositories = {}

  for repo in SharedRepository:
    # Check if user has access
    repo_access = UserRepository.get_by_user_and_repository(
      current_user.id, repo.value, session
    )

    if repo_access:
      # Get limits for this repository
      limits = SharedRepositoryRateLimits.get_limits(
        repo.value, repo_access.repository_plan
      )

      repositories[repo.value] = {
        "access": True,
        "plan": repo_access.repository_plan.value,
        "key_limits": {
          "queries_per_hour": limits.get("queries_per_hour", 0),
          "mcp_queries_per_hour": limits.get("mcp_queries_per_hour", 0),
          "agent_calls_per_hour": limits.get("agent_calls_per_hour", 0),
        },
      }
    else:
      repositories[repo.value] = {
        "access": False,
        "plan": None,
        "message": "No access - subscription required",
      }

  return {
    "user_id": current_user.id,
    "user_tier": getattr(current_user, "subscription_tier", "standard"),
    "repositories": repositories,
    "pricing_model": "No credit consumption for queries, rate-limited by subscription tier",
    "upgrade_url": "https://roboledger.ai/upgrade",
  }


def _get_tier_benefits(plan: str) -> Dict:
  """Get benefits description for a subscription plan."""
  benefits = {
    "starter": {
      "description": "Basic access for individuals and small teams",
      "highlights": [
        "500 queries per hour",
        "200 MCP queries per hour",
        "Basic rate limits",
      ],
    },
    "advanced": {
      "description": "Advanced access for professionals",
      "highlights": [
        "2,000 queries per hour",
        "1,000 MCP queries per hour",
        "Professional rate limits",
        "Priority support",
      ],
    },
    "unlimited": {
      "description": "Unlimited access for enterprise users",
      "highlights": [
        "Unlimited queries",
        "Unlimited MCP queries",
        "No rate limits",
        "Dedicated support",
        "Custom integrations",
      ],
    },
  }

  return benefits.get(
    plan.lower(),
    {
      "description": "Custom plan",
      "highlights": ["Contact support for details"],
    },
  )


@router.get(
  "/shared-repositories/{repository}",
  operation_id="getSharedRepositoryLimits",
  summary="Get shared repository rate limit status",
  description="""
    Get current rate limit status and usage for a shared repository.

    Returns:
    - Current usage across different time windows
    - Rate limits based on subscription tier
    - Remaining quota
    - Reset times

    Note: All queries are included - this only shows rate limit status.
    """,
  response_model=Dict,
)
async def get_shared_repository_limits(
  repository: str = Path(..., description="Repository name (e.g., 'sec')"),
  current_user: User = Depends(get_current_user),
  session: Session = Depends(get_db_session),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
) -> Dict:
  """Get rate limit status for a shared repository."""

  # Validate repository
  if repository not in [repo.value for repo in SharedRepository]:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"Repository '{repository}' not found or not a shared repository",
    )

  # Get user's repository access
  repo_access = UserRepository.get_by_user_and_repository(
    current_user.id, repository, session
  )

  if not repo_access:
    return {
      "repository": repository,
      "access": False,
      "message": f"No access to {repository} repository. Subscribe at https://roboledger.ai/upgrade",
      "plan": None,
      "limits": None,
      "usage": None,
    }

  # Get async Redis client for rate limiting with authentication in prod/staging
  from robosystems.config.valkey_registry import create_async_redis_client

  # Use async factory method to handle SSL params correctly
  redis_client = create_async_redis_client(
    ValkeyDatabase.RATE_LIMITING, decode_responses=True
  )

  try:
    limiter = DualLayerRateLimiter(redis_client)

    # Get usage statistics
    usage_stats = await limiter.get_usage_stats(
      user_id=current_user.id, repository=repository, plan=repo_access.repository_plan
    )

    # Get limits for the plan
    limits = SharedRepositoryRateLimits.get_limits(
      repository, repo_access.repository_plan
    )

    # Calculate remaining quotas
    remaining = {}

    for operation in ["queries", "mcp_queries", "agent_calls"]:
      operation_remaining = {}

      # Check each time window
      for window in ["minute", "hour", "day"]:
        limit_key = f"{operation}_per_{window}"
        if limit_key in limits:
          limit = limits[limit_key]
          if limit == -1:
            operation_remaining[window] = "unlimited"
          else:
            usage_key = (
              operation.replace("_queries", "")
              .replace("queries", "query")
              .replace("_calls", "")
            )
            current_usage = (
              usage_stats.get("usage", {}).get(usage_key, {}).get(window, 0)
            )
            operation_remaining[window] = max(0, limit - current_usage)

      if operation_remaining:
        remaining[operation] = operation_remaining

    return {
      "repository": repository,
      "access": True,
      "plan": repo_access.repository_plan.value,
      "subscription_active": repo_access.is_active,
      "pricing": "Included - No credits consumed for queries",
      "limits": limits,
      "usage": usage_stats.get("usage", {}),
      "remaining": remaining,
      "reset_times": {
        "minute": "60 seconds",
        "hour": "3600 seconds",
        "day": "86400 seconds",
      },
      "tier_benefits": _get_tier_benefits(repo_access.repository_plan.value),
    }

  finally:
    await redis_client.close()
