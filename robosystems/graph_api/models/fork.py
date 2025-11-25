"""
Subgraph fork operation models for the Graph API.

These models are used for forking data from parent graphs to subgraphs.
"""

from pydantic import BaseModel, Field


class ForkFromParentRequest(BaseModel):
  """Request to fork data from parent graph's DuckDB to subgraph's LadybugDB."""

  tables: list[str] = Field(
    default_factory=list,
    description="List of table names to copy from parent, or empty for all tables",
  )
  ignore_errors: bool = Field(
    default=True, description="Continue materialization on row errors"
  )

  class Config:
    extra = "forbid"


class ForkFromParentResponse(BaseModel):
  """Response from fork operation."""

  status: str = Field(..., description="Fork operation status")
  parent_graph_id: str = Field(..., description="Parent graph identifier")
  subgraph_id: str = Field(..., description="Subgraph identifier")
  tables_copied: list[str] = Field(..., description="Tables successfully copied")
  total_rows: int = Field(..., description="Total rows copied")
  execution_time_ms: float = Field(..., description="Total fork time in milliseconds")
