"""Publish list request and response models.

A publish list is a saved set of recipient graph IDs that
``share-report`` distributes to. Members can be other tenant graphs
inside the same RoboSystems deployment — typically downstream
consumers (parent companies, audit firms, lender portals).

Lifecycle: create the list, add members (one per recipient graph),
share reports against the list. Membership is managed via the
add/remove member operations; create/update only touches name +
description.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from robosystems.models.api.common import PaginationInfo

# ── Requests ───────────────────────────────────────────────────────────────


class CreatePublishListRequest(BaseModel):
  """Create a new publish list. Members are added separately via
  `add-publish-list-members`.
  """

  name: str = Field(
    ...,
    max_length=100,
    description="Human-readable list name (must be unique per graph).",
  )
  description: str | None = Field(
    None,
    max_length=500,
    description="Free-form description shown to list owners.",
  )

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"name": "Investors", "description": "Quarterly distribution list."},
        {"name": "Auditors"},
      ]
    }
  )


class UpdatePublishListRequest(BaseModel):
  """Update a publish list's name and/or description.

  Membership is managed separately via add/remove member operations —
  this endpoint only touches list metadata. Pass only the fields you
  want to change; omitted fields are left as-is.
  """

  name: str | None = Field(
    None,
    max_length=100,
    description="New list name. Omit to leave unchanged.",
  )
  description: str | None = Field(
    None,
    max_length=500,
    description="New description. Omit to leave unchanged.",
  )

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"name": "Senior Lenders"},
        {"description": "Updated cadence — monthly starting Q3."},
      ]
    }
  )


class AddMembersRequest(BaseModel):
  """Add one or more recipient graphs to a publish list.

  Each ``target_graph_id`` must reference an existing graph in the
  same deployment that has the same extension enabled (e.g.,
  roboledger). Adding your own graph is rejected (422). Adding a
  graph that is already a member is rejected (409).
  """

  target_graph_ids: list[str] = Field(
    ...,
    min_length=1,
    max_length=100,
    description=(
      "Graph IDs to add. Each must exist and have the same extension "
      "(roboledger / roboinvestor) enabled. Self-graph rejected."
    ),
  )

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"target_graph_ids": ["kg_abc12345"]},
        {"target_graph_ids": ["kg_abc12345", "kg_def67890", "kg_ghi13579"]},
      ]
    }
  )


# ── Responses ──────────────────────────────────────────────────────────────


class PublishListMemberResponse(BaseModel):
  """One recipient graph in a publish list."""

  id: str = Field(..., description="Membership row identifier (ULID).")
  target_graph_id: str = Field(..., description="Recipient graph ID.")
  target_graph_name: str | None = Field(
    None, description="Display name of the recipient graph (if known)."
  )
  target_org_name: str | None = Field(
    None,
    description="Display name of the org that owns the recipient graph.",
  )
  added_by: str = Field(..., description="User ID that added this member.")
  added_at: datetime = Field(..., description="When the member was added.")


class PublishListResponse(BaseModel):
  """Publish list summary — header metadata, no members."""

  id: str = Field(..., description="List identifier (ULID).")
  name: str = Field(..., description="Human-readable list name.")
  description: str | None = Field(None, description="Free-form description.")
  member_count: int = Field(
    0, description="Number of recipient graphs currently on the list."
  )
  created_by: str = Field(..., description="User ID that created the list.")
  created_at: datetime = Field(..., description="When the list was created.")
  updated_at: datetime = Field(
    ..., description="Last metadata update (name/description)."
  )


class PublishListDetailResponse(PublishListResponse):
  """Full detail including members."""

  members: list[PublishListMemberResponse] = Field(
    default_factory=list,
    description="All recipient graphs on the list.",
  )


class PublishListListResponse(BaseModel):
  """Paginated list of publish lists owned by the current graph."""

  publish_lists: list[PublishListResponse] = Field(
    ..., description="Publish list summaries, newest first."
  )
  pagination: PaginationInfo
