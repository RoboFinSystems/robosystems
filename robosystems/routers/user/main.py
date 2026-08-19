"""User profile management endpoints."""

from datetime import UTC, datetime

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from ...config import env
from ...config.constants import EMAIL_TOKEN_EXPIRY_HOURS
from ...database import get_db_session
from ...logger import logger
from ...middleware.auth.dependencies import (
  get_current_user,
  get_optional_jwt_user,
)
from ...middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from ...middleware.rate_limits import user_management_rate_limit_dependency
from ...middleware.sse import build_email_job_config, run_and_monitor_dagster_job
from ...models.api.common import (
  AUTHENTICATED_ERROR_RESPONSES,
  ErrorCode,
  ErrorResponse,
  create_error_response,
)
from ...models.api.user import (
  UpdateUserRequest,
  UserResponse,
)
from ...models.core import User, UserToken
from ...operations import passkeys as passkey_ops
from ...security import SecurityAuditLogger, SecurityEventType
from ...security.input_validation import (
  sanitize_string,
  validate_email,
)
from ..auth.utils import detect_app_source

router = APIRouter(tags=["User"])


def _mask_email(email: str | None) -> str:
  """``jane@example.com`` -> ``j***@example.com`` for the change notice, which
  goes to the OLD address and should hint at the new one without disclosing it
  in full."""
  if not email or "@" not in email:
    return "a new address"
  local, _, domain = email.partition("@")
  shown = local[0] if local else ""
  return f"{shown}***@{domain}"


@router.get(
  "/user",
  response_model=UserResponse,
  summary="Get Current User",
  operation_id="getCurrentUser",
  responses={**AUTHENTICATED_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/user", business_event_type="user_info_accessed"
)
async def get_current_user_info(
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
) -> UserResponse:
  user_id = getattr(current_user, "id", None) if current_user else None

  try:
    user_response = UserResponse(
      id=current_user.id,
      name=current_user.name,
      email=current_user.email,
      email_verified=current_user.email_verified,
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
    logger.error(f"Error retrieving user info: {e!s}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Error retrieving user information",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.put(
  "/user",
  response_model=UserResponse,
  summary="Update User Profile",
  operation_id="updateUser",
  responses={
    **AUTHENTICATED_ERROR_RESPONSES,
    409: {"model": ErrorResponse, "description": "Email already in use"},
  },
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/user", business_event_type="user_profile_updated"
)
async def update_user_profile(
  request: UpdateUserRequest,
  fastapi_request: Request,
  background_tasks: BackgroundTasks,
  current_user: User | None = Depends(get_optional_jwt_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
) -> UserResponse:
  # Interactive session only. A programmatic API key must never reach a route
  # that can change the sign-in address — changing email plus the public
  # password-reset flow would otherwise be a full account takeover from a key
  # that legitimately lives in CI configs and connector URLs. Same rule the
  # passkey surfaces enforce (middleware/auth/dependencies.py get_optional_jwt_user).
  if current_user is None:
    raise create_error_response(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="An interactive session is required to update your profile",
      code=ErrorCode.UNAUTHORIZED,
    )
  user_id = getattr(current_user, "id", None)

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
    email_changed = False

    if "email" in update_data and update_data["email"] != current_user.email:
      if not validate_email(update_data["email"]):
        raise create_error_response(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail="Invalid email format",
          code=ErrorCode.INVALID_INPUT,
        )

      # Re-authenticate before changing the sign-in address, exactly as passkey
      # enrollment/removal do. A live proof (password or a fresh mgmt-flow
      # passkey assertion) is required; a valid session alone is not enough.
      try:
        passkey_ops.verify_reauth(
          db,
          current_user,
          password=request.reauth_password,
          assertion=request.reauth_assertion,
        )
      except passkey_ops.ReauthInvalidError:
        client_ip = fastapi_request.client.host if fastapi_request.client else None
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.AUTH_FAILURE,
          user_id=str(user_id),
          ip_address=client_ip,
          user_agent=fastapi_request.headers.get("user-agent"),
          endpoint="/v1/user",
          details={"action": "email_change_reauth_failed"},
          risk_level="medium",
        )
        raise create_error_response(
          status_code=status.HTTP_401_UNAUTHORIZED,
          detail="Re-authentication is required to change your email",
          code=ErrorCode.UNAUTHORIZED,
        )

      # Store lowercase: User.create/update and get_by_email all normalize, and
      # a mixed-case address written here would silently never match at login
      # or password reset (both lowercase their input before the lookup).
      sanitized_email = sanitize_string(update_data["email"], max_length=254).lower()
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
    # Only write email on a real change (sanitized_email is set only when the
    # requested address differs from the current one and passed reauth). This
    # is also where the previous address is captured for the change notice.
    previous_email: str | None = None
    if sanitized_email:
      previous_email = str(user_in_session.email) if user_in_session.email else None
      user_in_session.email = sanitized_email
      email_changed = True
      if env.EMAIL_VERIFICATION_ENABLED:
        user_in_session.email_verified = False

    user_in_session.updated_at = datetime.now(UTC)

    db.commit()
    db.refresh(user_in_session)

    # Tell the PREVIOUS address its account's email was changed — the one
    # signal the account owner still receives if this change was hostile. Sent
    # regardless of EMAIL_VERIFICATION_ENABLED; a fire-and-forget notice whose
    # failure never fails the request (the Dagster op's own send is best-effort).
    if email_changed and previous_email:
      app = detect_app_source(fastapi_request)
      background_tasks.add_task(
        run_and_monitor_dagster_job,
        job_name="send_email_job",
        operation_id=None,
        run_config=build_email_job_config(
          email_type="email_changed",
          to_email=previous_email,
          user_name=user_in_session.name or "there",
          new_email=_mask_email(user_in_session.email),
          app=app,
        ),
      )

    # Queue verification email for new email address
    if email_changed and env.EMAIL_VERIFICATION_ENABLED:
      client_ip = fastapi_request.client.host if fastapi_request.client else None
      user_agent = fastapi_request.headers.get("user-agent")

      token = UserToken.create_token(
        user_id=user_id,
        token_type="email_verification",
        hours=EMAIL_TOKEN_EXPIRY_HOURS,
        session=db,
        ip_address=client_ip,
        user_agent=user_agent,
      )

      app = detect_app_source(fastapi_request)

      run_config = build_email_job_config(
        email_type="email_verification",
        to_email=user_in_session.email,
        user_name=user_in_session.name,
        token=token,
        app=app,
      )
      background_tasks.add_task(
        run_and_monitor_dagster_job,
        job_name="send_email_job",
        operation_id=None,
        run_config=run_config,
      )

      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.EMAIL_SENT,
        user_id=str(user_id),
        ip_address=client_ip,
        user_agent=user_agent,
        endpoint="/v1/user",
        details={
          "email_type": "verification",
          "reason": "email_changed",
          "app_source": app,
        },
        risk_level="low",
      )

      logger.info(
        f"Queued verification email for changed email: {user_in_session.email}"
      )

    try:
      from ...middleware.auth.cache import api_key_cache

      api_key_cache.invalidate_user_data(user_id)
    except Exception as e:
      logger.error(f"Failed to invalidate user cache after profile update: {e}")

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
        "email_changed": email_changed,
        "name_changed": "name" in fields_updated,
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
        "email_changed": email_changed,
        "name_changed": "name" in fields_updated,
      },
      user_id=user_id,
    )

    return UserResponse(
      id=user_in_session.id,
      name=user_in_session.name,
      email=user_in_session.email,
      email_verified=user_in_session.email_verified,
      accounts=[],
    )

  except HTTPException:
    raise
  except Exception as e:
    db.rollback()
    logger.error(f"Error updating user profile: {e!s}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Error updating user profile",
      code=ErrorCode.INTERNAL_ERROR,
    )
