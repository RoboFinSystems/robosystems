"""SCIM administration API models (admin surface, not the SCIM wire format)."""

from pydantic import BaseModel, Field


class ScimBootstrapRequest(BaseModel):
  """Create-or-reuse the enterprise org and mint a SCIM token for it."""

  org_id: str | None = Field(
    default=None, description="Attach the token to this existing org."
  )
  org_name: str | None = Field(
    default=None,
    description="Create a new ENTERPRISE org with this name (when org_id is omitted).",
  )
  token_name: str = Field(default="scim-provisioning")


class ScimBootstrapResponse(BaseModel):
  org_id: str
  org_name: str
  scim_token_id: str
  # The raw bearer token, shown exactly once — paste it into the IdP now.
  token: str


class ScimTokenRevokeResponse(BaseModel):
  scim_token_id: str
  revoked: bool
