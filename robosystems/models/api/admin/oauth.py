"""Admin models for pre-registered MCP OAuth clients."""

from pydantic import BaseModel, Field


class OAuthClientCreateRequest(BaseModel):
  """Mint an operator-registered OAuth client (trusted, never expires).

  For the Connectors Directory's held credentials and enterprise gateways —
  clients that cannot or should not register dynamically.
  """

  client_name: str = Field(..., min_length=1, max_length=100)
  redirect_uris: list[str] = Field(..., min_length=1, max_length=10)
  confidential: bool = Field(
    default=False,
    description="Issue a client_secret (client_secret_post). Public clients "
    "authenticate with PKCE alone.",
  )
  client_uri: str | None = Field(default=None, max_length=2048)
  logo_uri: str | None = Field(default=None, max_length=2048)


class OAuthClientCreateResponse(BaseModel):
  oauth_client_id: str = Field(..., description="Row id (oac_…)")
  client_id: str = Field(..., description="The wire client_id to configure")
  client_secret: str | None = Field(
    None, description="Shown once; never recoverable. None for public clients."
  )
  client_name: str
  redirect_uris: list[str]
  token_endpoint_auth_method: str


class OAuthClientSummary(BaseModel):
  oauth_client_id: str
  client_id: str
  client_name: str
  registration_source: str
  token_endpoint_auth_method: str
  redirect_uris: list[str]
  is_active: bool
  is_trusted: bool
  created_at: str
  last_used_at: str | None
  expires_at: str | None


class OAuthClientListResponse(BaseModel):
  clients: list[OAuthClientSummary]


class OAuthClientDeactivateResponse(BaseModel):
  oauth_client_id: str
  deactivated: bool
