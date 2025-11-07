"""User limits management endpoints."""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from ...middleware.auth.dependencies import get_current_user
from ...middleware.rate_limits import user_management_rate_limit_dependency
from ...middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from ...models.iam import User, UserLimits
from ...models.api.user import UserLimitsResponse, UserUsageResponse
from ...database import get_db_session

router = APIRouter(tags=["User"])


@router.get(
  "/user/limits",
  response_model=UserUsageResponse,
  operation_id="getUserLimits",
  summary="Get user limits and usage",
  description="Retrieve current limits and usage statistics for the authenticated user (simple safety valve for graph creation)",
  responses={
    200: {"description": "User limits and usage retrieved successfully"},
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
  Get current limits and usage statistics for the authenticated user.

  This endpoint returns the user's graph limits and current usage.
  UserLimits is now a simple safety valve to prevent runaway graph creation.

  Args:
      current_user: Authenticated user (automatically injected)
      db: Database session (automatically injected)

  Returns:
      UserUsageResponse: Comprehensive usage statistics and limits information

  Raises:
      HTTPException: 500 if unable to retrieve limits
  """
  try:
    user_limits = UserLimits.get_or_create_for_user(current_user.id, db)
    usage_stats = user_limits.get_current_usage(db)

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/user/limits",
      method="GET",
      event_type="user_limits_retrieved_details",
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

  except HTTPException:
    raise
  except Exception as e:
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
