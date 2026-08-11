"""Auth posture configuration endpoint."""

from fastapi import APIRouter

from ...config import env
from ...models.api.auth import AuthProvidersResponse, OIDCProviderInfo
from ...models.api.common import COMMON_ERROR_RESPONSES

router = APIRouter()


@router.get(
  "/providers",
  response_model=AuthProvidersResponse,
  summary="Get Auth Providers",
  description="Returns which authentication methods this deployment offers. The login surface renders its posture from this instead of hardcoding methods.",
  operation_id="getAuthProviders",
  responses={**COMMON_ERROR_RESPONSES},
)
async def get_auth_providers() -> AuthProvidersResponse:
  return AuthProvidersResponse(
    password_auth=env.PASSWORD_AUTH_ENABLED,
    oidc=OIDCProviderInfo(
      enabled=env.SSO_OIDC_ENABLED,
      provider_label=env.SSO_OIDC_PROVIDER_LABEL if env.SSO_OIDC_ENABLED else None,
    ),
    registration=env.USER_REGISTRATION_ENABLED,
    passkeys=env.PASSKEYS_ENABLED,
  )
