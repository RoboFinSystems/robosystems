"""User profile management endpoints."""

from datetime import datetime, timezone
from fastapi import APIRouter, Depends, status, Request, HTTPException
from sqlalchemy.orm import Session

from ...middleware.auth.dependencies import get_current_user
from ...middleware.rate_limits import user_management_rate_limit_dependency
from ...middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from ...models.iam import User
from ...models.api.user import (
  UserResponse,
  UpdateUserRequest,
)
from ...models.api.common import (
  ErrorCode,
  create_error_response,
)
from ...database import get_db_session
from ...logger import logger
from ...security import SecurityAuditLogger, SecurityEventType
from ...security.input_validation import (
  validate_email,
  sanitize_string,
)

router = APIRouter(tags=["User"])


@router.get(
  "/user",
  response_model=UserResponse,
  summary="Get Current User",
  description="Returns information about the currently authenticated user.",
  status_code=status.HTTP_200_OK,
  operation_id="getCurrentUser",
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/user", business_event_type="user_info_accessed"
)
async def get_current_user_info(
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
) -> UserResponse:
  """
  Get information about the currently authenticated user.

  Args:
      current_user: The authenticated user from authentication

  Returns:
      UserResponse: Response with user information

  Raises:
      HTTPException: If there's an error retrieving the user information
  """
  user_id = getattr(current_user, "id", None) if current_user else None

  try:
    user_response = UserResponse(
      id=current_user.id,
      name=current_user.name,
      email=current_user.email,
      accounts=[],
    )

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/user",
      method="GET",
      event_type="user_info_accessed",
      event_data={
        "user_id": user_id,
        "has_name": bool(current_user.name),
        "has_email": bool(current_user.email),
      },
      user_id=user_id,
    )

    return user_response

  except Exception as e:
    logger.error(f"Error retrieving user info: {str(e)}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Error retrieving user information",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.put(
  "/user",
  response_model=UserResponse,
  summary="Update User Profile",
  description="Update the current user's profile information.",
  status_code=status.HTTP_200_OK,
  operation_id="updateUser",
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/user", business_event_type="user_profile_updated"
)
async def update_user_profile(
  request: UpdateUserRequest,
  fastapi_request: Request,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
) -> UserResponse:
  """
  Update the current user's profile information.

  Args:
      request: The update request with new profile data
      current_user: The authenticated user from authentication

  Returns:
      UserResponse: Updated user information

  Raises:
      HTTPException: If there's an error updating the user or email conflict
  """
  user_id = getattr(current_user, "id", None) if current_user else None

  try:
    update_data = request.model_dump(exclude_unset=True)
    if not update_data:
      metrics_instance = get_endpoint_metrics()
      metrics_instance.record_business_event(
        endpoint="/v1/user",
        method="PUT",
        event_type="user_update_empty",
        event_data={"user_id": user_id},
        user_id=user_id,
      )
      raise create_error_response(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="No fields provided for update",
        code=ErrorCode.INVALID_INPUT,
      )

    fields_updated = list(update_data.keys())
    sanitized_email = None

    if "email" in update_data and update_data["email"] != current_user.email:
      if not validate_email(update_data["email"]):
        raise create_error_response(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail="Invalid email format",
          code=ErrorCode.INVALID_INPUT,
        )

      sanitized_email = sanitize_string(update_data["email"], max_length=254)
      existing_user = User.get_by_email(sanitized_email, db)
      if existing_user:
        metrics_instance = get_endpoint_metrics()
        metrics_instance.record_business_event(
          endpoint="/v1/user",
          method="PUT",
          event_type="user_update_email_conflict",
          event_data={"user_id": user_id, "attempted_email": update_data["email"]},
          user_id=user_id,
        )
        raise create_error_response(
          status_code=status.HTTP_409_CONFLICT,
          detail="Email already in use by another account",
          code=ErrorCode.ALREADY_EXISTS,
        )

    user_in_session = User.get_by_id(user_id, db)
    if not user_in_session:
      raise create_error_response(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="User not found",
        code=ErrorCode.NOT_FOUND,
      )

    if "name" in update_data:
      user_in_session.name = sanitize_string(update_data["name"], max_length=100)
    if "email" in update_data:
      user_in_session.email = (
        sanitized_email if sanitized_email else update_data["email"]
      )

    user_in_session.updated_at = datetime.now(timezone.utc)

    db.commit()
    db.refresh(user_in_session)

    try:
      from ...middleware.auth.cache import api_key_cache

      api_key_cache.invalidate_user_data(user_id)
    except Exception as e:
      logger.error(f"Failed to invalidate user cache after profile update: {e}")

    current_user = user_in_session

    client_ip = fastapi_request.client.host if fastapi_request.client else None
    user_agent = fastapi_request.headers.get("user-agent")

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_SUCCESS,
      user_id=str(user_id),
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint="/v1/user",
      details={
        "action": "profile_updated",
        "fields_changed": fields_updated,
        "email_changed": "email" in fields_updated,
        "name_changed": "name" in fields_updated,
        "old_email": current_user.email if "email" in fields_updated else None,
      },
      risk_level="low",
    )

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/user",
      method="PUT",
      event_type="user_profile_updated",
      event_data={
        "user_id": user_id,
        "fields_updated": ",".join(fields_updated),
        "email_changed": "email" in fields_updated,
        "name_changed": "name" in fields_updated,
      },
      user_id=user_id,
    )

    return UserResponse(
      id=current_user.id,
      name=current_user.name,
      email=current_user.email,
      accounts=[],
    )

  except HTTPException:
    raise
  except Exception as e:
    db.rollback()
    logger.error(f"Error updating user profile: {str(e)}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Error updating user profile",
      code=ErrorCode.INTERNAL_ERROR,
    )
