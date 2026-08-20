"""User password management endpoints."""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from ...database import get_db_session
from ...logger import logger
from ...middleware.auth.dependencies import get_current_user
from ...middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from ...middleware.rate_limits import user_management_rate_limit_dependency
from ...models.api.common import (
  AUTHENTICATED_ERROR_RESPONSES,
  ErrorCode,
  SuccessResponse,
  create_error_response,
)
from ...models.api.user import UpdatePasswordRequest
from ...models.core import User
from ...security.password import PasswordSecurity
from ..auth.utils import require_password_auth

router = APIRouter(tags=["User"])


@router.put(
  "/user/password",
  response_model=SuccessResponse,
  summary="Update Password",
  description="Requires current password verification. Not available for SSO-only accounts.",
  operation_id="updateUserPassword",
  responses={**AUTHENTICATED_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/user/password", business_event_type="password_updated"
)
async def update_user_password(
  request: UpdatePasswordRequest,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
  _password_auth: None = Depends(require_password_auth),
):
  user_id = getattr(current_user, "id", None) if current_user else None

  try:
    # Fetch full user from DB — cached user from get_current_user may not have password_hash
    user_with_password = User.get_by_id(user_id, db)
    if not user_with_password:
      raise create_error_response(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="User not found",
        code=ErrorCode.NOT_FOUND,
      )

    if not user_with_password.password_hash:
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

    if not await PasswordSecurity.verify_password_async(
      request.current_password, user_with_password.password_hash
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

    password_result = PasswordSecurity.validate_password(request.new_password)
    if not password_result.is_valid:
      raise create_error_response(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Password requirements not met: {', '.join(password_result.errors)}",
        code=ErrorCode.INVALID_INPUT,
      )

    # Hash through the policy function (BCRYPT_ROUNDS), not a bare gensalt():
    # registration and reset both hash at the policy cost, and a bare
    # bcrypt.gensalt() defaults to cost 12, silently downgrading every account
    # that changes its password below the configured work factor.
    new_password_hash = await PasswordSecurity.hash_password_async(request.new_password)

    user_in_session = User.get_by_id(user_id, db)
    if not user_in_session:
      raise create_error_response(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="User not found",
        code=ErrorCode.NOT_FOUND,
      )

    user_in_session.password_hash = new_password_hash

    # Bump session_version so every JWT minted before this change stops
    # authenticating. invalidate_sessions() commits the new hash alongside the
    # bump, so the two cannot diverge: a failure rolls both back rather than
    # leaving the password changed while prior sessions stay live.
    user_in_session.invalidate_sessions(db)

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
    logger.error(f"Error updating password: {e!s}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Error updating password",
      code=ErrorCode.INTERNAL_ERROR,
    )
