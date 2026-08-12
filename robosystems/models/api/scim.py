"""SCIM 2.0 request/response envelopes (RFC 7643 / 7644).

Minimal Users-only surface: enough for Okta's Create/Read/Update/Deactivate
and its filtered dedupe. Field names follow the SCIM schema verbatim (camelCase
via aliases) so the wire format is spec-compliant regardless of our internal
snake_case.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

USER_SCHEMA = "urn:ietf:params:scim:schemas:core:2.0:User"
LIST_RESPONSE_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:ListResponse"
ERROR_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:Error"
PATCH_OP_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:PatchOp"


class ScimName(BaseModel):
  model_config = ConfigDict(populate_by_name=True, extra="ignore")

  formatted: str | None = None
  given_name: str | None = Field(default=None, alias="givenName")
  family_name: str | None = Field(default=None, alias="familyName")


class ScimEmail(BaseModel):
  model_config = ConfigDict(populate_by_name=True, extra="ignore")

  value: str
  primary: bool = True
  type: str | None = "work"


class ScimMeta(BaseModel):
  model_config = ConfigDict(populate_by_name=True)

  resource_type: str = Field(default="User", alias="resourceType")
  location: str | None = None
  created: str | None = None
  last_modified: str | None = Field(default=None, alias="lastModified")


class ScimUserResource(BaseModel):
  """A SCIM User resource as returned to the IdP."""

  model_config = ConfigDict(populate_by_name=True)

  schemas: list[str] = Field(default_factory=lambda: [USER_SCHEMA])
  id: str
  external_id: str | None = Field(default=None, alias="externalId")
  user_name: str = Field(alias="userName")
  name: ScimName | None = None
  emails: list[ScimEmail] = Field(default_factory=list)
  active: bool = True
  meta: ScimMeta | None = None


class ScimUserCreateRequest(BaseModel):
  """Inbound POST/PUT body. Liberal in what it accepts (extra ignored)."""

  model_config = ConfigDict(populate_by_name=True, extra="ignore")

  schemas: list[str] | None = None
  user_name: str = Field(alias="userName")
  external_id: str | None = Field(default=None, alias="externalId")
  name: ScimName | None = None
  emails: list[ScimEmail] = Field(default_factory=list)
  active: bool = True
  # displayName / other core attrs are accepted-and-ignored via extra="ignore".


class ScimListResponse(BaseModel):
  model_config = ConfigDict(populate_by_name=True)

  schemas: list[str] = Field(default_factory=lambda: [LIST_RESPONSE_SCHEMA])
  total_results: int = Field(alias="totalResults")
  start_index: int = Field(default=1, alias="startIndex")
  items_per_page: int = Field(alias="itemsPerPage")
  resources: list[ScimUserResource] = Field(default_factory=list, alias="Resources")


class ScimPatchOperation(BaseModel):
  model_config = ConfigDict(populate_by_name=True, extra="ignore")

  op: str
  path: str | None = None
  value: Any = None


class ScimPatchRequest(BaseModel):
  model_config = ConfigDict(populate_by_name=True, extra="ignore")

  schemas: list[str] | None = None
  operations: list[ScimPatchOperation] = Field(alias="Operations")


class ScimError(BaseModel):
  model_config = ConfigDict(populate_by_name=True)

  schemas: list[str] = Field(default_factory=lambda: [ERROR_SCHEMA])
  detail: str
  status: str
  scim_type: str | None = Field(default=None, alias="scimType")
