"""Admin API for pre-registered MCP OAuth clients (support-plane, ALB-isolated)."""

from fastapi import APIRouter, HTTPException, Query, Request, status

from ...db.platform import get_db_session
from ...logger import get_logger
from ...middleware.auth.admin import require_admin
from ...models.api.admin import (
  OAuthClientCreateRequest,
  OAuthClientCreateResponse,
  OAuthClientDeactivateResponse,
  OAuthClientListResponse,
  OAuthClientSummary,
)
from ...models.core import OAuthClient
from ...operations.oauth_server.clients import validate_redirect_uri

logger = get_logger(__name__)

router = APIRouter(prefix="/admin/v1/oauth", tags=["admin-oauth"])


def _summary(client: OAuthClient) -> OAuthClientSummary:
  return OAuthClientSummary(
    oauth_client_id=str(client.id),
    client_id=str(client.client_id),
    client_name=str(client.client_name),
    registration_source=str(client.registration_source),
    token_endpoint_auth_method=str(client.token_endpoint_auth_method),
    redirect_uris=list(client.redirect_uris or []),
    is_active=bool(client.is_active),
    is_trusted=bool(client.is_trusted),
    created_at=client.created_at.isoformat(),
    last_used_at=client.last_used_at.isoformat() if client.last_used_at else None,
    expires_at=client.expires_at.isoformat() if client.expires_at else None,
  )


@router.post("/clients", response_model=OAuthClientCreateResponse)
@require_admin(permissions=["orgs:write"])
async def create_oauth_client(
  request: Request, data: OAuthClientCreateRequest
) -> OAuthClientCreateResponse:
  """Mint a trusted, pre-registered client. The secret (if any) is returned
  once and never recoverable."""
  for uri in data.redirect_uris:
    reason = validate_redirect_uri(uri)
    if reason:
      raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=reason)

  session = next(get_db_session())
  try:
    client, secret = OAuthClient.register_preregistered(
      client_name=data.client_name,
      redirect_uris=list(dict.fromkeys(data.redirect_uris)),
      confidential=data.confidential,
      client_uri=data.client_uri,
      logo_uri=data.logo_uri,
      session=session,
    )
    logger.info(
      "Admin pre-registered OAuth client",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "oauth_client_id": client.id,
        "confidential": data.confidential,
      },
    )
    return OAuthClientCreateResponse(
      oauth_client_id=str(client.id),
      client_id=str(client.client_id),
      client_secret=secret,
      client_name=str(client.client_name),
      redirect_uris=list(client.redirect_uris),
      token_endpoint_auth_method=str(client.token_endpoint_auth_method),
    )
  finally:
    session.close()


@router.get("/clients", response_model=OAuthClientListResponse)
@require_admin(permissions=["orgs:read"])
async def list_oauth_clients(
  request: Request,
  source: str | None = Query(None, description="dcr | cimd | preregistered"),
  include_inactive: bool = Query(False),
  limit: int = Query(100, ge=1, le=500),
) -> OAuthClientListResponse:
  session = next(get_db_session())
  try:
    query = session.query(OAuthClient)
    if source:
      query = query.filter(OAuthClient.registration_source == source)
    if not include_inactive:
      query = query.filter(OAuthClient.is_active)
    rows = query.order_by(OAuthClient.created_at.desc()).limit(limit).all()
    return OAuthClientListResponse(clients=[_summary(row) for row in rows])
  finally:
    session.close()


@router.post(
  "/clients/{oauth_client_id}/deactivate", response_model=OAuthClientDeactivateResponse
)
@require_admin(permissions=["orgs:write"])
async def deactivate_oauth_client(
  request: Request, oauth_client_id: str
) -> OAuthClientDeactivateResponse:
  """Deactivate a client: no new consents, and its access tokens stop
  validating at the next cache miss (the validator checks the client)."""
  session = next(get_db_session())
  try:
    client = OAuthClient.get_by_id(oauth_client_id, session)
    if client is None:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"OAuth client {oauth_client_id} not found",
      )
    client.deactivate(session)
    logger.info(
      "Admin deactivated OAuth client",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "oauth_client_id": oauth_client_id,
      },
    )
    return OAuthClientDeactivateResponse(
      oauth_client_id=oauth_client_id, deactivated=True
    )
  finally:
    session.close()
