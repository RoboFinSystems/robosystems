"""Shared OAuth models for connection providers."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class OAuthProvider(BaseModel):
  """OAuth provider configuration."""

  name: str = Field(..., description="Provider name (e.g., 'quickbooks')")
  client_id: str = Field(..., description="OAuth client ID")
  client_secret: str = Field(..., description="OAuth client secret")
  authorize_url: str = Field(..., description="OAuth authorization URL")
  token_url: str = Field(..., description="OAuth token exchange URL")
  scopes: list[str] = Field(..., description="Required OAuth scopes")
  redirect_uri: str = Field(..., description="OAuth callback URL")


class OAuthInitRequest(BaseModel):
  """Request to initiate OAuth flow."""

  connection_id: str = Field(..., description="Connection ID to link OAuth to")
  redirect_uri: str | None = Field(None, description="Override default redirect URI")
  additional_params: dict[str, str] | None = Field(
    None, description="Provider-specific parameters"
  )


class OAuthInitResponse(BaseModel):
  """Response with OAuth authorization URL."""

  auth_url: str = Field(..., description="URL to redirect user for authorization")
  state: str = Field(..., description="OAuth state for security")
  expires_at: datetime = Field(..., description="When this OAuth request expires")


class OAuthCallbackRequest(BaseModel):
  """OAuth callback parameters."""

  code: str = Field(..., description="Authorization code from OAuth provider")
  state: str = Field(..., description="OAuth state for verification")
  realm_id: str | None = Field(None, description="QuickBooks-specific realm ID")
  error: str | None = Field(None, description="OAuth error if authorization failed")
  error_description: str | None = Field(None, description="OAuth error details")


class OAuthTokens(BaseModel):
  """OAuth tokens from provider."""

  access_token: str = Field(..., description="OAuth access token")
  refresh_token: str | None = Field(None, description="OAuth refresh token")
  token_type: str = Field("Bearer", description="Token type")
  expires_in: int | None = Field(None, description="Token expiry in seconds")
  expires_at: datetime | None = Field(None, description="Calculated expiry time")
  scope: str | None = Field(None, description="Granted scopes")


class OAuthConnectionUpdate(BaseModel):
  """Update connection with OAuth credentials."""

  tokens: OAuthTokens
  provider_data: dict[str, object] = Field(
    ..., description="Provider-specific data (e.g., realm_id)"
  )
  status: Literal["connected", "error"] = Field(
    ..., description="Connection status after OAuth"
  )
