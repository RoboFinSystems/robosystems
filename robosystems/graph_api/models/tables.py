"""
DuckDB table-related Pydantic models for the Graph API.

These models are used for staging table operations (create, query, materialize).
"""

import re
from typing import Any

from pydantic import BaseModel, Field, field_validator


class TableInfo(BaseModel):
  """Information about a DuckDB staging table."""

  graph_id: str = Field(..., description="Graph database identifier")
  table_name: str = Field(..., description="Table name")
  row_count: int = Field(..., description="Approximate row count")
  size_bytes: int = Field(..., description="Table size in bytes")
  s3_location: str | None = Field(None, description="S3 location for external tables")


class TableCreateRequest(BaseModel):
  """Request to create a DuckDB staging table."""

  graph_id: str = Field(..., description="Graph database identifier")
  table_name: str = Field(
    ...,
    description="Table name",
    pattern=r"^[A-Za-z_][A-Za-z0-9_]*$",
  )
  s3_pattern: str | list[str] = Field(
    ..., description="S3 glob pattern or list of S3 file paths"
  )
  file_id_map: dict[str, str] | None = Field(
    default=None,
    description="Optional map of s3_key -> file_id for provenance tracking",
  )
  null_columns: list[str] | None = Field(
    default=None,
    description="Columns to NULL out in the staged table (e.g., ['embedding'] "
    "to skip large vector data). Column remains in schema as NULL. "
    "Data stays in parquet source files for future use.",
  )
  deduplicate: bool = Field(
    default=True,
    description="Deduplicate rows on insert using NOT EXISTS on dedup key "
    "(identifier for nodes, src/dst for relationships). "
    "Safe for tables with wide columns like FLOAT[384] embeddings.",
  )
  timeout_seconds: int = Field(
    default=1800,
    ge=60,
    le=14400,
    description="Timeout for table creation in seconds (default 30 min, max 4 hours)",
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
  row_count: int | None = Field(
    None, description="Number of rows in table after creation"
  )


class TableQueryRequest(BaseModel):
  """Request to execute SQL query on DuckDB staging tables."""

  graph_id: str = Field(..., description="Graph database identifier")
  sql: str = Field(..., description="SQL query to execute")
  parameters: list[Any] | None = Field(
    default=None, description="Query parameters for safe value substitution"
  )

  class Config:
    extra = "forbid"


class TableQueryResponse(BaseModel):
  """Response from DuckDB table query."""

  columns: list[str] = Field(..., description="Column names")
  rows: list[list[Any]] = Field(..., description="Query results")
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

  @field_validator("file_ids")
  @classmethod
  def validate_file_ids(cls, v):
    """Validate that each file_id is a valid UUID format."""
    if v is not None:
      uuid_pattern = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        re.IGNORECASE,
      )
      for fid in v:
        if not uuid_pattern.match(fid):
          raise ValueError(f"Invalid file_id format: {fid}. Must be a valid UUID.")
    return v

  batch_num: int | None = Field(
    default=None,
    description="Current batch number (0-indexed) for hash-based batching. Use with num_batches.",
  )
  num_batches: int | None = Field(
    default=None,
    description="Total number of batches for hash-based partitioning. Each row goes to exactly one batch based on hash(identifier) % num_batches.",
  )
  materialize_embeddings: bool = Field(
    default=False,
    description="Whether to materialize embedding columns to LadybugDB. Default false — embeddings stay in DuckDB staging only.",
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
