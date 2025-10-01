"""User endpoints for v1 API."""

from datetime import datetime, timezone
from fastapi import APIRouter, Depends, status, HTTPException, Request
from sqlalchemy.orm import Session

from ...middleware.auth.dependencies import get_current_user
from ...middleware.rate_limits import user_management_rate_limit_dependency
from ...middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from ...models.iam import User, UserAPIKey
from ...models.api.user import (
  UserResponse,
  UpdateUserRequest,
  UpdatePasswordRequest,
  CreateAPIKeyRequest,
  APIKeyInfo,
  CreateAPIKeyResponse,
  APIKeysResponse,
  UpdateAPIKeyRequest,
)
from ...models.api.common import (
  ErrorResponse,
  SuccessResponse,
  ErrorCode,
  create_error_response,
)
from ...database import session, get_db_session
from ...logger import logger
from ...security import SecurityAuditLogger, SecurityEventType
from ...security.input_validation import (
  validate_email,
  sanitize_string,
  validate_password_strength,
)
from ...operations.graph.credit_service import CreditService

# Create router for user endpoints (prefix handled by parent)
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
    # For now, return simplified user info (accounts will be handled separately)
    user_response = UserResponse(
      id=current_user.id,
      name=current_user.name,
      email=current_user.email,
      accounts=[],  # Empty for now, can be extended later
    )

    # Record business event for user info access with additional details
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
    # Log the error
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
    # Check if any fields were provided for update
    update_data = request.model_dump(exclude_unset=True)
    if not update_data:
      # Record business event for empty update attempt
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

    # Track what fields are being updated
    fields_updated = list(update_data.keys())

    # Initialize sanitized_email variable
    sanitized_email = None

    # Check for email conflicts if email is being changed
    if "email" in update_data and update_data["email"] != current_user.email:
      # Validate and sanitize email
      if not validate_email(update_data["email"]):
        raise create_error_response(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail="Invalid email format",
          code=ErrorCode.INVALID_INPUT,
        )

      sanitized_email = sanitize_string(update_data["email"], max_length=254)
      existing_user = User.get_by_email(sanitized_email, db)
      if existing_user:
        # Record business event for email conflict
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

    # Reload user from current session to ensure it's attached to the session
    user_in_session = User.get_by_id(user_id, db)
    if not user_in_session:
      raise create_error_response(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="User not found",
        code=ErrorCode.NOT_FOUND,
      )

    # Update user fields
    if "name" in update_data:
      user_in_session.name = sanitize_string(update_data["name"], max_length=100)
    if "email" in update_data:
      # Use already validated and sanitized email if available
      user_in_session.email = (
        sanitized_email if sanitized_email else update_data["email"]
      )

    # Update timestamp
    user_in_session.updated_at = datetime.now(timezone.utc)

    # Save changes
    db.commit()
    db.refresh(user_in_session)

    # Invalidate cached user data since profile was updated
    try:
      from ...middleware.auth.cache import api_key_cache

      api_key_cache.invalidate_user_data(user_id)
    except Exception as e:
      logger.error(f"Failed to invalidate user cache after profile update: {e}")
      # Don't fail the request if cache invalidation fails

    # Update current_user for response (use the session-attached user)
    # NOTE: There's a session mismatch here - current_user was fetched via get_current_user
    # dependency which uses the global session, while user_in_session uses the DI session.
    # This means the response might show stale data. Requires auth refactoring to fix.
    current_user = user_in_session

    # Log user profile update for security audit
    client_ip = fastapi_request.client.host if fastapi_request.client else None
    user_agent = fastapi_request.headers.get("user-agent")

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_SUCCESS,  # Could add PROFILE_UPDATED
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

    # Record business event for successful user update with additional details
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


@router.get(
  "/user/credits",
  summary="Get All Credit Summaries",
  description="""Get credit summaries for all graphs owned by the user.

This endpoint provides a consolidated view of credit usage across
all graphs where the user has access, helping to monitor overall
credit consumption and plan usage.

No credits are consumed for viewing summaries.""",
  operation_id="getAllCreditSummaries",
  responses={
    200: {
      "description": "Credit summaries retrieved successfully",
      "content": {
        "application/json": {
          "example": {
            "user_id": "user_123",
            "total_graphs": 3,
            "graphs": [
              {
                "graph_id": "kg1a2b3c",
                "graph_name": "Acme Corp",
                "role": "admin",
                "current_balance": 950.0,
                "monthly_allocation": 1000.0,
                "consumed_this_month": 50.0,
                "graph_tier": "standard",
              }
            ],
          }
        }
      },
    },
    500: {"description": "Failed to retrieve credit summaries", "model": ErrorResponse},
  },
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/user/credits",
  business_event_type="user_credits_summary_accessed",
)
async def get_all_credit_summaries(
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
) -> dict:
  """
  Get credit summaries for all graphs owned by the user.

  Provides an overview of credit usage across all accessible graphs.

  Args:
      current_user: The authenticated user
      db: Database session

  Returns:
      Dict with summaries for all user's graphs

  Raises:
      HTTPException: If unable to retrieve summaries
  """
  try:
    credit_service = CreditService(db)
    summaries = credit_service.get_all_credit_summaries(current_user.id)

    return {
      "user_id": current_user.id,
      "total_graphs": len(summaries),
      "graphs": summaries,
    }

  except Exception as e:
    logger.error(f"Failed to get all credit summaries for user {current_user.id}: {e}")
    raise create_error_response(
      status_code=500,
      detail="Failed to retrieve credit summaries",
      code=ErrorCode.INTERNAL_ERROR,
    )


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
    # Import password utilities from auth router
    import bcrypt

    # Verify current password
    if not current_user.password_hash:
      # Record business event for no password set
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
      # Record business event for incorrect current password
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

    # Verify new password confirmation
    if request.new_password != request.confirm_password:
      # Record business event for password mismatch
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

    # Validate password strength
    password_valid, password_issues = validate_password_strength(request.new_password)
    if not password_valid:
      raise create_error_response(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Password requirements not met: {', '.join(password_issues)}",
        code=ErrorCode.INVALID_INPUT,
      )

    # Hash the new password
    salt = bcrypt.gensalt()
    new_password_hash = bcrypt.hashpw(
      request.new_password.encode("utf-8"), salt
    ).decode("utf-8")

    # Reload user from current session to ensure it's attached to the session
    user_in_session = User.get_by_id(user_id, session)
    if not user_in_session:
      raise create_error_response(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="User not found",
        code=ErrorCode.NOT_FOUND,
      )

    # Update user password
    user_in_session.password_hash = new_password_hash
    user_in_session.updated_at = datetime.now(timezone.utc)

    # Save changes
    session.commit()
    session.refresh(user_in_session)

    # Record business event for successful password update with additional details
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
    session.rollback()
    logger.error(f"Error updating password: {str(e)}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Error updating password",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.get(
  "/user/api-keys",
  response_model=APIKeysResponse,
  summary="List API Keys",
  description="Get all API keys for the current user.",
  status_code=status.HTTP_200_OK,
  operation_id="listUserAPIKeys",
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/user/api-keys", business_event_type="api_keys_listed"
)
async def list_api_keys(
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
) -> APIKeysResponse:
  """
  Get all API keys for the current user.

  Args:
      current_user: The authenticated user

  Returns:
      APIKeysResponse: List of user's API keys

  Raises:
      HTTPException: If there's an error retrieving the API keys
  """
  user_id = getattr(current_user, "id", None) if current_user else None

  try:
    # Get all API keys for the user
    api_keys = UserAPIKey.get_by_user_id(current_user.id, session)

    # Convert to response format
    api_key_infos = []
    active_keys = 0
    inactive_keys = 0

    for api_key in api_keys:
      if api_key.is_active:
        active_keys += 1
      else:
        inactive_keys += 1

      api_key_infos.append(
        APIKeyInfo(
          id=api_key.id,
          name=api_key.name,
          description=api_key.description,
          prefix=api_key.prefix,
          is_active=api_key.is_active,
          last_used_at=api_key.last_used_at.isoformat()
          if api_key.last_used_at
          else None,
          expires_at=api_key.expires_at.isoformat() if api_key.expires_at else None,
          created_at=api_key.created_at.isoformat(),
        )
      )

    # Record business event for API keys listing with additional details
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/user/api-keys",
      method="GET",
      event_type="api_keys_listed",
      event_data={
        "user_id": user_id,
        "total_keys": len(api_keys),
        "active_keys": active_keys,
        "inactive_keys": inactive_keys,
      },
      user_id=user_id,
    )

    return APIKeysResponse(api_keys=api_key_infos)

  except Exception as e:
    logger.error(f"Error retrieving API keys: {str(e)}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Error retrieving API keys",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.post(
  "/user/api-keys",
  response_model=CreateAPIKeyResponse,
  summary="Create API Key",
  description="Create a new API key for the current user.",
  status_code=status.HTTP_201_CREATED,
  operation_id="createUserAPIKey",
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/user/api-keys", business_event_type="api_key_created"
)
async def create_api_key(
  request: CreateAPIKeyRequest,
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
) -> CreateAPIKeyResponse:
  """
  Create a new API key for the current user.

  Args:
      request: API key creation request
      current_user: The authenticated user

  Returns:
      CreateAPIKeyResponse: Created API key information with the actual key

  Raises:
      HTTPException: If there's an error creating the API key
  """
  user_id = getattr(current_user, "id", None) if current_user else None

  try:
    # Sanitize API key name and description
    sanitized_name = sanitize_string(request.name, max_length=100)
    sanitized_description = (
      sanitize_string(request.description, max_length=500)
      if request.description
      else None
    )

    # Parse expiration date if provided
    expires_at = None
    if request.expires_at:
      try:
        from datetime import datetime, timezone

        expires_at = datetime.fromisoformat(request.expires_at.replace("Z", "+00:00"))
        # Ensure expiration is in the future
        if expires_at <= datetime.now(timezone.utc):
          raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expiration date must be in the future",
          )
      except ValueError:
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail="Invalid expiration date format. Use ISO format (e.g. 2024-12-31T23:59:59Z)",
        )

    # Create the API key
    api_key, plain_key = UserAPIKey.create(
      user_id=current_user.id,
      name=sanitized_name,
      description=sanitized_description,
      expires_at=expires_at,
      session=session,
    )

    # Convert to response format
    api_key_info = APIKeyInfo(
      id=api_key.id,
      name=api_key.name,
      description=api_key.description,
      prefix=api_key.prefix,
      is_active=api_key.is_active,
      last_used_at=None,  # Just created, so never used
      expires_at=api_key.expires_at.isoformat() if api_key.expires_at else None,
      created_at=api_key.created_at.isoformat(),
    )

    # Record business event for API key creation with additional details
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/user/api-keys",
      method="POST",
      event_type="api_key_created",
      event_data={
        "user_id": user_id,
        "api_key_id": api_key.id,
        "api_key_name": request.name,
        "has_description": bool(request.description),
      },
      user_id=user_id,
    )

    return CreateAPIKeyResponse(api_key=api_key_info, key=plain_key)

  except Exception as e:
    logger.error(f"Error creating API key: {str(e)}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Error creating API key",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.put(
  "/user/api-keys/{api_key_id}",
  response_model=APIKeyInfo,
  summary="Update API Key",
  description="Update an API key's name or description.",
  status_code=status.HTTP_200_OK,
  operation_id="updateUserAPIKey",
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/user/api-keys/{api_key_id}", business_event_type="api_key_updated"
)
async def update_api_key(
  api_key_id: str,
  request: UpdateAPIKeyRequest,
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
) -> APIKeyInfo:
  """
  Update an API key's name or description.

  Args:
      api_key_id: The API key ID to update
      request: Update request with new values
      current_user: The authenticated user

  Returns:
      APIKeyInfo: Updated API key information

  Raises:
      HTTPException: If API key not found or access denied
  """
  user_id = getattr(current_user, "id", None) if current_user else None

  try:
    # Get the API key and verify ownership
    api_keys = UserAPIKey.get_by_user_id(current_user.id, session)
    api_key = None
    for key in api_keys:
      if key.id == api_key_id:
        api_key = key
        break

    if not api_key:
      # Record business event for API key not found
      metrics_instance = get_endpoint_metrics()
      metrics_instance.record_business_event(
        endpoint="/v1/user/api-keys/{api_key_id}",
        method="PUT",
        event_type="api_key_update_not_found",
        event_data={"user_id": user_id, "requested_api_key_id": api_key_id},
        user_id=user_id,
      )
      raise create_error_response(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="API key not found or access denied",
        code=ErrorCode.NOT_FOUND,
      )

    # Track what fields are being updated
    update_data = request.model_dump(exclude_unset=True)
    fields_updated = list(update_data.keys())

    # Update the API key with sanitized values
    if request.name is not None:
      api_key.name = sanitize_string(request.name, max_length=100)
    if request.description is not None:
      api_key.description = sanitize_string(request.description, max_length=500)

    session.commit()
    session.refresh(api_key)

    # Record business event for API key update with additional details
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/user/api-keys/{api_key_id}",
      method="PUT",
      event_type="api_key_updated",
      event_data={
        "user_id": user_id,
        "api_key_id": api_key_id,
        "fields_updated": ",".join(fields_updated),
      },
      user_id=user_id,
    )

    # Return updated info
    return APIKeyInfo(
      id=api_key.id,
      name=api_key.name,
      description=api_key.description,
      prefix=api_key.prefix,
      is_active=api_key.is_active,
      last_used_at=api_key.last_used_at.isoformat() if api_key.last_used_at else None,
      created_at=api_key.created_at.isoformat(),
      expires_at=api_key.expires_at.isoformat() if api_key.expires_at else None,
    )

  except HTTPException:
    raise

  except Exception as e:
    logger.error(f"Error updating API key: {str(e)}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Error updating API key",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.delete(
  "/user/api-keys/{api_key_id}",
  response_model=SuccessResponse,
  summary="Revoke API Key",
  description="Revoke (deactivate) an API key.",
  status_code=status.HTTP_200_OK,
  operation_id="revokeUserAPIKey",
  responses={
    200: {"description": "API key revoked successfully", "model": SuccessResponse},
    404: {"description": "API key not found", "model": ErrorResponse},
    500: {"description": "Error revoking API key", "model": ErrorResponse},
  },
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/user/api-keys/{api_key_id}", business_event_type="api_key_revoked"
)
async def revoke_api_key(
  api_key_id: str,
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
):
  """
  Revoke (deactivate) an API key.

  Args:
      api_key_id: The API key ID to revoke
      current_user: The authenticated user

  Returns:
      Success status

  Raises:
      HTTPException: If API key not found or access denied
  """
  user_id = getattr(current_user, "id", None) if current_user else None

  try:
    # Get the API key and verify ownership
    api_keys = UserAPIKey.get_by_user_id(current_user.id, session)
    api_key = None
    for key in api_keys:
      if key.id == api_key_id:
        api_key = key
        break

    if not api_key:
      # Record business event for API key not found
      metrics_instance = get_endpoint_metrics()
      metrics_instance.record_business_event(
        endpoint="/v1/user/api-keys/{api_key_id}",
        method="DELETE",
        event_type="api_key_revoke_not_found",
        event_data={"user_id": user_id, "requested_api_key_id": api_key_id},
        user_id=user_id,
      )
      raise create_error_response(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="API key not found or access denied",
        code=ErrorCode.NOT_FOUND,
      )

    # Track if the key was already inactive
    was_already_inactive = not api_key.is_active

    # Deactivate the API key
    api_key.deactivate(session)

    # Record business event for API key revocation with additional details
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/user/api-keys/{api_key_id}",
      method="DELETE",
      event_type="api_key_revoked",
      event_data={
        "user_id": user_id,
        "api_key_id": api_key_id,
        "was_already_inactive": was_already_inactive,
      },
      user_id=user_id,
    )

    return SuccessResponse(
      success=True,
      message="API key revoked successfully",
      data={"api_key_id": api_key_id},
    )

  except HTTPException:
    raise

  except Exception as e:
    logger.error(f"Error revoking API key: {str(e)}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Error revoking API key",
      code=ErrorCode.INTERNAL_ERROR,
    )
