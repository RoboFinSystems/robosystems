"""Graph member management API models."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from robosystems.models.core.graph import GraphRole


class AddGraphMemberRequest(BaseModel):
  """Request to grant an org member access to a graph."""

  user_id: str = Field(
    ..., description="User to grant access to (must belong to the graph's organization)"
  )
  role: GraphRole = Field(
    default=GraphRole.MEMBER,
    description="Graph role: viewer (read-only), member (read/write), admin (full control)",
  )


class UpdateGraphMemberRoleRequest(BaseModel):
  """Request to change a graph member's role."""

  role: GraphRole


class GraphMemberResponse(BaseModel):
  """A user with access to a graph."""

  user_id: str
  name: str
  email: str
  role: GraphRole
  source: Literal["explicit", "org_role"] = Field(
    ...,
    description="'explicit' for a direct grant; 'org_role' for implicit admin held by org owners/admins",
  )
  granted_at: datetime | None = Field(
    None, description="When the explicit grant was created (null for implicit access)"
  )


class GraphMemberListResponse(BaseModel):
  """All users with access to a graph."""

  members: list[GraphMemberResponse]
  total: int
  graph_id: str
