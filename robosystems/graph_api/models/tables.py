"""
DuckDB table-related Pydantic models for the Graph API.

These models are used for staging table operations (create, query, materialize).
"""

from typing import Dict, List, Any, Optional, Union
from pydantic import BaseModel, Field, field_validator


class TableInfo(BaseModel):
  """Information about a DuckDB staging table."""

  graph_id: str = Field(..., description="Graph database identifier")
  table_name: str = Field(..., description="Table name")
  row_count: int = Field(..., description="Approximate row count")
  size_bytes: int = Field(..., description="Table size in bytes")
  s3_location: Optional[str] = Field(
    None, description="S3 location for external tables"
  )


class TableCreateRequest(BaseModel):
  """Request to create a DuckDB staging table."""

  graph_id: str = Field(..., description="Graph database identifier")
  table_name: str = Field(..., description="Table name")
  s3_pattern: Union[str, List[str]] = Field(
    ..., description="S3 glob pattern or list of S3 file paths"
  )
  file_id_map: Optional[Dict[str, str]] = Field(
    default=None,
    description="Optional map of s3_key -> file_id for provenance tracking",
  )

  @field_validator("s3_pattern")
  @classmethod
  def validate_s3_pattern(cls, v):
    """Validate s3_pattern is either a non-empty string or a non-empty list of strings."""
    if isinstance(v, str):
      if not v or not v.strip():
        raise ValueError("s3_pattern string cannot be empty")
      if not v.startswith("s3://"):
        raise ValueError("s3_pattern must start with s3://")
    elif isinstance(v, list):
      if not v:
        raise ValueError("s3_pattern list cannot be empty")
      if not all(isinstance(item, str) for item in v):
        raise ValueError("s3_pattern list must contain only strings")
      if not all(item.startswith("s3://") for item in v):
        raise ValueError("All s3_pattern list items must start with s3://")
    else:
      raise ValueError("s3_pattern must be either a string or a list of strings")
    return v

  class Config:
    extra = "forbid"


class TableCreateResponse(BaseModel):
  """Response from DuckDB table creation."""

  status: str = Field(..., description="Creation status")
  graph_id: str = Field(..., description="Graph database identifier")
  table_name: str = Field(..., description="Table name")
  execution_time_ms: float = Field(..., description="Creation time in milliseconds")


class TableQueryRequest(BaseModel):
  """Request to execute SQL query on DuckDB staging tables."""

  graph_id: str = Field(..., description="Graph database identifier")
  sql: str = Field(..., description="SQL query to execute")
  parameters: Optional[List[Any]] = Field(
    default=None, description="Query parameters for safe value substitution"
  )

  class Config:
    extra = "forbid"


class TableQueryResponse(BaseModel):
  """Response from DuckDB table query."""

  columns: List[str] = Field(..., description="Column names")
  rows: List[List[Any]] = Field(..., description="Query results")
  row_count: int = Field(..., description="Number of rows returned")
  execution_time_ms: float = Field(..., description="Query execution time")


class TableMaterializationRequest(BaseModel):
  """Request to materialize DuckDB table data into LadybugDB."""

  ignore_errors: bool = Field(
    default=True, description="Continue materialization on row errors"
  )
  file_ids: list[str] | None = Field(
    default=None,
    description="Optional list of file IDs to materialize. If None, materializes all files (full materialization).",
  )

  class Config:
    extra = "forbid"


class TableMaterializationResponse(BaseModel):
  """Response from table materialization operation."""

  status: str = Field(..., description="Materialization status")
  graph_id: str = Field(..., description="Graph database identifier")
  table_name: str = Field(..., description="Table name")
  rows_ingested: int = Field(..., description="Number of rows materialized")
  execution_time_ms: float = Field(
    ..., description="Materialization time in milliseconds"
  )
