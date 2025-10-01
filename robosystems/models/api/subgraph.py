"""API models for subgraph management.

This module defines Pydantic models for subgraph-related API requests and responses.
Subgraphs are only available for Enterprise and Premium tier graphs.
"""

from datetime import datetime
from pydantic import BaseModel, Field, field_validator
from enum import Enum
import re


class SubgraphType(str, Enum):
  """Types of subgraphs."""

  STATIC = "static"  # Phase 1: Traditional environment-based subgraphs
  TEMPORAL = "temporal"  # Phase 2: Short-lived memory contexts (future)
  VERSIONED = "versioned"  # Phase 3: Git-like version control (future)
  MEMORY = "memory"  # Phase 3: Memory layer subgraphs (future)


class CreateSubgraphRequest(BaseModel):
  """Request model for creating a subgraph."""

  name: str = Field(
    ...,
    min_length=1,
    max_length=20,
    description="Alphanumeric name for the subgraph (e.g., dev, staging, prod1)",
    examples=["dev", "staging", "prod", "test1"],
  )

  display_name: str = Field(
    ...,
    min_length=1,
    max_length=100,
    description="Human-readable display name for the subgraph",
    examples=["Development Environment", "Staging Environment", "Production"],
  )

  description: str | None = Field(
    None,
    max_length=500,
    description="Optional description of the subgraph's purpose",
    examples=["Development environment for testing new features"],
  )

  schema_extensions: list[str] | None = Field(
    default_factory=list,
    description="Schema extensions to include (inherits from parent by default)",
    examples=[["roboledger", "roboinvestor"]],
  )

  subgraph_type: SubgraphType = Field(
    SubgraphType.STATIC,
    description="Type of subgraph (currently only 'static' is supported)",
  )

  metadata: dict[str, object] | None = Field(
    None,
    description="Additional metadata for the subgraph",
    examples=[{"environment": "development", "team": "engineering"}],
  )

  @field_validator("name")
  @classmethod
  def validate_name(cls, v: str) -> str:
    """Validate that the name is alphanumeric."""
    if not re.match(r"^[a-zA-Z0-9]{1,20}$", v):
      raise ValueError(
        "Subgraph name must be alphanumeric (letters and numbers only) and between 1-20 characters"
      )
    return v.lower()  # Normalize to lowercase


class SubgraphResponse(BaseModel):
  """Response model for a subgraph."""

  graph_id: str = Field(
    ...,
    description="Full subgraph identifier (e.g., kg123_dev)",
    examples=["kg5f2e5e0da65d45d69645_dev"],
  )

  parent_graph_id: str = Field(
    ..., description="Parent graph identifier", examples=["kg5f2e5e0da65d45d69645"]
  )

  subgraph_index: int = Field(
    ..., description="Numeric index of the subgraph", examples=[1, 2, 3]
  )

  subgraph_name: str = Field(
    ...,
    description="Alphanumeric name of the subgraph",
    examples=["dev", "staging", "prod"],
  )

  display_name: str = Field(
    ..., description="Human-readable display name", examples=["Development Environment"]
  )

  description: str | None = Field(
    None, description="Description of the subgraph's purpose"
  )

  subgraph_type: SubgraphType = Field(..., description="Type of subgraph")

  status: str = Field(
    ...,
    description="Current status of the subgraph",
    examples=["active", "creating", "deleting", "failed"],
  )

  created_at: datetime = Field(..., description="When the subgraph was created")

  updated_at: datetime = Field(..., description="When the subgraph was last updated")

  size_mb: float | None = Field(
    None, description="Size of the subgraph database in megabytes"
  )

  node_count: int | None = Field(None, description="Number of nodes in the subgraph")

  edge_count: int | None = Field(None, description="Number of edges in the subgraph")

  last_accessed: datetime | None = Field(
    None, description="When the subgraph was last accessed"
  )

  metadata: dict[str, object] | None = Field(
    None, description="Additional metadata for the subgraph"
  )


class SubgraphSummary(BaseModel):
  """Summary model for listing subgraphs."""

  graph_id: str = Field(
    ..., description="Full subgraph identifier", examples=["kg5f2e5e0da65d45d69645_dev"]
  )

  subgraph_name: str = Field(..., description="Alphanumeric name", examples=["dev"])

  display_name: str = Field(
    ..., description="Human-readable name", examples=["Development Environment"]
  )

  subgraph_type: SubgraphType = Field(..., description="Type of subgraph")

  status: str = Field(..., description="Current status", examples=["active"])

  size_mb: float | None = Field(None, description="Size in megabytes")

  created_at: datetime = Field(..., description="Creation timestamp")

  last_accessed: datetime | None = Field(None, description="Last access timestamp")


class ListSubgraphsResponse(BaseModel):
  """Response model for listing subgraphs."""

  parent_graph_id: str = Field(
    ..., description="Parent graph identifier", examples=["kg5f2e5e0da65d45d69645"]
  )

  parent_graph_name: str = Field(
    ..., description="Parent graph name", examples=["My Company Graph"]
  )

  parent_graph_tier: str = Field(
    ..., description="Parent graph tier", examples=["enterprise", "premium"]
  )

  subgraph_count: int = Field(..., description="Total number of subgraphs", ge=0)

  max_subgraphs: int | None = Field(
    None, description="Maximum allowed subgraphs for this tier (None = unlimited)"
  )

  total_size_mb: float | None = Field(
    None, description="Combined size of all subgraphs in megabytes"
  )

  subgraphs: list[SubgraphSummary] = Field(..., description="List of subgraphs")


class DeleteSubgraphRequest(BaseModel):
  """Request model for deleting a subgraph."""

  force: bool = Field(
    False, description="Force deletion even if subgraph contains data"
  )

  backup_first: bool = Field(True, description="Create a backup before deletion")

  backup_location: str | None = Field(
    None,
    description="S3 location for backup (uses default if not specified)",
    examples=["s3://my-bucket/backups/"],
  )


class DeleteSubgraphResponse(BaseModel):
  """Response model for subgraph deletion."""

  graph_id: str = Field(..., description="Deleted subgraph identifier")

  status: str = Field(
    ..., description="Deletion status", examples=["deleted", "deletion_scheduled"]
  )

  backup_location: str | None = Field(None, description="Location of backup if created")

  deleted_at: datetime = Field(..., description="When deletion occurred")

  message: str | None = Field(
    None, description="Additional information about the deletion"
  )


class SubgraphQuotaResponse(BaseModel):
  """Response model for subgraph quota information."""

  parent_graph_id: str = Field(..., description="Parent graph identifier")

  tier: str = Field(..., description="Graph tier", examples=["enterprise", "premium"])

  current_count: int = Field(..., description="Current number of subgraphs", ge=0)

  max_allowed: int | None = Field(
    None, description="Maximum allowed subgraphs (None = unlimited)"
  )

  remaining: int | None = Field(
    None, description="Remaining subgraphs that can be created"
  )

  total_size_mb: float | None = Field(None, description="Total size of all subgraphs")

  max_size_mb: float | None = Field(None, description="Maximum allowed total size")


class SubgraphAccessRequest(BaseModel):
  """Request model for granting subgraph access."""

  user_id: str = Field(..., description="User ID to grant access to")

  role: str = Field(
    ..., description="Role to grant", examples=["read", "write", "admin"]
  )

  subgraph_ids: list[str] | None = Field(
    None, description="Specific subgraphs to grant access to (None = all)"
  )


class SubgraphAccessResponse(BaseModel):
  """Response model for subgraph access management."""

  user_id: str = Field(..., description="User ID")

  parent_graph_id: str = Field(..., description="Parent graph ID")

  role: str = Field(..., description="Granted role")

  subgraph_access: list[str] = Field(
    ..., description="List of subgraph IDs user has access to"
  )

  granted_at: datetime = Field(..., description="When access was granted")

  granted_by: str = Field(..., description="Who granted the access")
