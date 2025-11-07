"""User API key management endpoints."""

from datetime import datetime, timezone
from fastapi import APIRouter, Depends, status, HTTPException
from sqlalchemy.orm import Session

from ...middleware.auth.dependencies import get_current_user
from ...middleware.rate_limits import user_management_rate_limit_dependency
from ...middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from ...models.iam import User, UserAPIKey
from ...models.api.user import (
  CreateAPIKeyRequest,
  APIKeyInfo,
  CreateAPIKeyResponse,
  APIKeysResponse,
  UpdateAPIKeyRequest,
)
from ...models.api.common import (
  SuccessResponse,
  ErrorResponse,
  ErrorCode,
  create_error_response,
)
from ...database import get_db_session
from ...logger import logger
from ...security.input_validation import sanitize_string

router = APIRouter(tags=["User"])


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
  db: Session = Depends(get_db_session),
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
    api_keys = UserAPIKey.get_by_user_id(current_user.id, db)

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

  except HTTPException:
    raise
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
  db: Session = Depends(get_db_session),
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
    sanitized_name = sanitize_string(request.name, max_length=100)
    sanitized_description = (
      sanitize_string(request.description, max_length=500)
      if request.description
      else None
    )

    expires_at = None
    if request.expires_at:
      try:
        expires_at = datetime.fromisoformat(request.expires_at.replace("Z", "+00:00"))
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

    api_key, plain_key = UserAPIKey.create(
      user_id=current_user.id,
      name=sanitized_name,
      description=sanitized_description,
      expires_at=expires_at,
      session=db,
    )

    api_key_info = APIKeyInfo(
      id=api_key.id,
      name=api_key.name,
      description=api_key.description,
      prefix=api_key.prefix,
      is_active=api_key.is_active,
      last_used_at=None,
      expires_at=api_key.expires_at.isoformat() if api_key.expires_at else None,
      created_at=api_key.created_at.isoformat(),
    )

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

  except HTTPException:
    raise
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
  db: Session = Depends(get_db_session),
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
    api_keys = UserAPIKey.get_by_user_id(current_user.id, db)
    api_key = None
    for key in api_keys:
      if key.id == api_key_id:
        api_key = key
        break

    if not api_key:
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

    update_data = request.model_dump(exclude_unset=True)
    fields_updated = list(update_data.keys())

    if request.name is not None:
      api_key.name = sanitize_string(request.name, max_length=100)
    if request.description is not None:
      api_key.description = sanitize_string(request.description, max_length=500)

    db.commit()
    db.refresh(api_key)

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
  db: Session = Depends(get_db_session),
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
    api_keys = UserAPIKey.get_by_user_id(current_user.id, db)
    api_key = None
    for key in api_keys:
      if key.id == api_key_id:
        api_key = key
        break

    if not api_key:
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

    was_already_inactive = not api_key.is_active

    api_key.deactivate(db)

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
