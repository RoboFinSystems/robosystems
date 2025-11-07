"""User password management endpoints."""

from datetime import datetime, timezone
from fastapi import APIRouter, Depends, status, HTTPException
from sqlalchemy.orm import Session
import bcrypt

from ...middleware.auth.dependencies import get_current_user
from ...middleware.rate_limits import user_management_rate_limit_dependency
from ...middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from ...models.iam import User
from ...models.api.user import UpdatePasswordRequest
from ...models.api.common import (
  SuccessResponse,
  ErrorResponse,
  ErrorCode,
  create_error_response,
)
from ...database import get_db_session
from ...logger import logger
from ...security.input_validation import validate_password_strength

router = APIRouter(tags=["User"])


@router.put(
  "/user/password",
  response_model=SuccessResponse,
  summary="Update Password",
  description="Update the current user's password.",
  status_code=status.HTTP_200_OK,
  operation_id="updateUserPassword",
  responses={
    200: {"description": "Password updated successfully", "model": SuccessResponse},
    400: {
      "description": "Invalid password or validation error",
      "model": ErrorResponse,
    },
    404: {"description": "User not found", "model": ErrorResponse},
    500: {"description": "Error updating password", "model": ErrorResponse},
  },
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/user/password", business_event_type="password_updated"
)
async def update_user_password(
  request: UpdatePasswordRequest,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
):
  """
  Update the current user's password.

  Args:
      request: The password update request
      current_user: The authenticated user from authentication

  Returns:
      Success message

  Raises:
      HTTPException: If current password is wrong or passwords don't match
  """
  user_id = getattr(current_user, "id", None) if current_user else None

  try:
    if not current_user.password_hash:
      metrics_instance = get_endpoint_metrics()
      metrics_instance.record_business_event(
        endpoint="/v1/user/password",
        method="PUT",
        event_type="password_update_no_password_set",
        event_data={"user_id": user_id},
        user_id=user_id,
      )
      raise create_error_response(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="User has no password set",
        code=ErrorCode.INVALID_INPUT,
      )

    if not bcrypt.checkpw(
      request.current_password.encode("utf-8"),
      current_user.password_hash.encode("utf-8"),
    ):
      metrics_instance = get_endpoint_metrics()
      metrics_instance.record_business_event(
        endpoint="/v1/user/password",
        method="PUT",
        event_type="password_update_incorrect_current",
        event_data={"user_id": user_id},
        user_id=user_id,
      )
      raise create_error_response(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Current password is incorrect",
        code=ErrorCode.INVALID_CREDENTIALS,
      )

    if request.new_password != request.confirm_password:
      metrics_instance = get_endpoint_metrics()
      metrics_instance.record_business_event(
        endpoint="/v1/user/password",
        method="PUT",
        event_type="password_update_confirmation_mismatch",
        event_data={"user_id": user_id},
        user_id=user_id,
      )
      raise create_error_response(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="New password and confirmation do not match",
        code=ErrorCode.INVALID_INPUT,
      )

    password_valid, password_issues = validate_password_strength(request.new_password)
    if not password_valid:
      raise create_error_response(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Password requirements not met: {', '.join(password_issues)}",
        code=ErrorCode.INVALID_INPUT,
      )

    salt = bcrypt.gensalt()
    new_password_hash = bcrypt.hashpw(
      request.new_password.encode("utf-8"), salt
    ).decode("utf-8")

    user_in_session = User.get_by_id(user_id, db)
    if not user_in_session:
      raise create_error_response(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="User not found",
        code=ErrorCode.NOT_FOUND,
      )

    user_in_session.password_hash = new_password_hash
    user_in_session.updated_at = datetime.now(timezone.utc)

    db.commit()
    db.refresh(user_in_session)

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/user/password",
      method="PUT",
      event_type="password_updated",
      event_data={"user_id": user_id},
      user_id=user_id,
    )

    return SuccessResponse(
      success=True, message="Password updated successfully", data=None
    )

  except HTTPException:
    raise
  except Exception as e:
    db.rollback()
    logger.error(f"Error updating password: {str(e)}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Error updating password",
      code=ErrorCode.INTERNAL_ERROR,
    )
