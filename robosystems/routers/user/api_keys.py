"""User API key management endpoints."""

from datetime import UTC, datetime

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
  RESOURCE_ERROR_RESPONSES,
  ErrorCode,
  SuccessResponse,
  create_error_response,
)
from ...models.api.user import (
  APIKeyInfo,
  APIKeysResponse,
  CreateAPIKeyRequest,
  CreateAPIKeyResponse,
  UpdateAPIKeyRequest,
)
from ...models.core import User, UserAPIKey
from ...security.input_validation import sanitize_string

router = APIRouter(tags=["User"])


@router.get(
  "/user/api-keys",
  response_model=APIKeysResponse,
  summary="List API Keys",
  operation_id="listUserAPIKeys",
  responses={**AUTHENTICATED_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/user/api-keys", business_event_type="api_keys_listed"
)
async def list_api_keys(
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
) -> APIKeysResponse:
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
          graph_id=api_key.graph_id,
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
    logger.error(f"Error retrieving API keys: {e!s}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Error retrieving API keys",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.post(
  "/user/api-keys",
  response_model=CreateAPIKeyResponse,
  summary="Create API Key",
  description="The raw key value is only returned once at creation time and cannot be retrieved again.",
  status_code=status.HTTP_201_CREATED,
  operation_id="createUserAPIKey",
  responses={**AUTHENTICATED_ERROR_RESPONSES},
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
        if expires_at <= datetime.now(UTC):
          raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expiration date must be in the future",
          )
      except ValueError:
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail="Invalid expiration date format. Use ISO format (e.g. 2024-12-31T23:59:59Z)",
        )

    # A graph-scoped key can only be minted for a graph the user can access.
    if request.graph_id is not None:
      import re as _re

      from ...config.shared_repositories import is_shared_repository_or_subgraph
      from ...middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
      from ...middleware.graph.utils import MultiTenantUtils
      from ...models.core import GraphUser

      if not _re.fullmatch(GRAPH_OR_SUBGRAPH_ID_PATTERN, request.graph_id):
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail="Invalid graph_id format",
        )
      if is_shared_repository_or_subgraph(request.graph_id):
        has_access = MultiTenantUtils.validate_repository_access(
          request.graph_id, current_user.id, "read"
        )
      else:
        has_access = GraphUser.user_has_access(current_user.id, request.graph_id, db)
      if not has_access:
        raise HTTPException(
          status_code=status.HTTP_403_FORBIDDEN,
          detail="Access denied to graph",
        )

    api_key, plain_key = UserAPIKey.create(
      user_id=current_user.id,
      name=sanitized_name,
      description=sanitized_description,
      expires_at=expires_at,
      session=db,
      graph_id=request.graph_id,
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
      graph_id=api_key.graph_id,
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
    logger.error(f"Error creating API key: {e!s}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Error creating API key",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.put(
  "/user/api-keys/{api_key_id}",
  response_model=APIKeyInfo,
  summary="Update API Key",
  operation_id="updateUserAPIKey",
  responses={**RESOURCE_ERROR_RESPONSES},
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
      graph_id=api_key.graph_id,
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Error updating API key: {e!s}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Error updating API key",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.delete(
  "/user/api-keys/{api_key_id}",
  response_model=SuccessResponse,
  summary="Revoke API Key",
  description="Deactivates the key immediately. Requests using the revoked key will fail with 401.",
  operation_id="revokeUserAPIKey",
  responses={**RESOURCE_ERROR_RESPONSES},
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
    logger.error(f"Error revoking API key: {e!s}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Error revoking API key",
      code=ErrorCode.INTERNAL_ERROR,
    )
