"""Publish list request and response models."""

from datetime import datetime

from pydantic import BaseModel, Field

from robosystems.models.api.common import PaginationInfo

# ── Requests ───────────────────────────────────────────────────────────────


class CreatePublishListRequest(BaseModel):
  name: str = Field(..., max_length=100, description="List name")
  description: str | None = Field(
    None, max_length=500, description="Optional description"
  )


class UpdatePublishListRequest(BaseModel):
  name: str | None = Field(None, max_length=100)
  description: str | None = Field(None, max_length=500)


class AddMembersRequest(BaseModel):
  target_graph_ids: list[str] = Field(
    ..., min_length=1, description="Graph IDs to add to this list"
  )


# ── Responses ──────────────────────────────────────────────────────────────


class PublishListMemberResponse(BaseModel):
  id: str
  target_graph_id: str
  target_graph_name: str | None = None
  target_org_name: str | None = None
  added_by: str
  added_at: datetime


class PublishListResponse(BaseModel):
  id: str
  name: str
  description: str | None = None
  member_count: int = 0
  created_by: str
  created_at: datetime
  updated_at: datetime


class PublishListDetailResponse(PublishListResponse):
  """Full detail including members."""

  members: list[PublishListMemberResponse] = Field(default_factory=list)


class PublishListListResponse(BaseModel):
  publish_lists: list[PublishListResponse]
  pagination: PaginationInfo
