"""Graph query API models."""

from typing import Optional, List
from pydantic import BaseModel, Field


class DatabaseHealthResponse(BaseModel):
  """Response model for database health check."""

  graph_id: str = Field(
    ..., description="Graph database identifier", examples=["kg1a2b3c4d5"]
  )
  status: str = Field(..., description="Overall health status", examples=["healthy"])
  connection_status: str = Field(
    ..., description="Database connection status", examples=["connected"]
  )
  uptime_seconds: float = Field(
    ..., description="Database uptime in seconds", examples=[3600.5]
  )
  last_query_time: Optional[str] = Field(
    None,
    description="Timestamp of last query execution",
    examples=["2024-01-15T10:30:00Z"],
  )
  query_count_24h: int = Field(
    ..., description="Number of queries executed in last 24 hours", examples=[150]
  )
  avg_query_time_ms: float = Field(
    ..., description="Average query execution time in milliseconds", examples=[45.2]
  )
  error_rate_24h: float = Field(
    ..., description="Error rate in last 24 hours (percentage)", examples=[0.5]
  )
  memory_usage_mb: Optional[float] = Field(
    None, description="Memory usage in MB", examples=[512.3]
  )
  storage_usage_mb: Optional[float] = Field(
    None, description="Storage usage in MB", examples=[1024.7]
  )
  alerts: List[str] = Field(
    default_factory=list,
    description="Active alerts or warnings",
    examples=[["High memory usage detected"]],
  )


class DatabaseInfoResponse(BaseModel):
  """Response model for database information and statistics."""

  graph_id: str = Field(
    ..., description="Graph database identifier", examples=["kg1a2b3c4d5"]
  )
  database_name: str = Field(..., description="Database name", examples=["kg1a2b3c4d5"])
  # database_path removed for security - no need to expose file system paths
  database_size_bytes: int = Field(
    ..., description="Database size in bytes", examples=[1048576]
  )
  database_size_mb: float = Field(
    ..., description="Database size in MB", examples=[1.0]
  )
  node_count: int = Field(..., description="Total number of nodes", examples=[1250])
  relationship_count: int = Field(
    ..., description="Total number of relationships", examples=[2340]
  )
  node_labels: List[str] = Field(
    ..., description="List of node labels", examples=[["Entity", "Report", "Fact"]]
  )
  relationship_types: List[str] = Field(
    ...,
    description="List of relationship types",
    examples=[["HAS_REPORT", "REPORTED_IN", "HAS_ELEMENT"]],
  )
  created_at: str = Field(
    ..., description="Database creation timestamp", examples=["2024-01-15T10:00:00Z"]
  )
  last_modified: str = Field(
    ..., description="Last modification timestamp", examples=["2024-01-15T10:30:00Z"]
  )
  schema_version: Optional[str] = Field(
    None, description="Schema version", examples=["1.0.0"]
  )
  read_only: bool = Field(
    ..., description="Whether database is read-only", examples=[False]
  )
  backup_count: int = Field(
    ..., description="Number of available backups", examples=[5]
  )
  last_backup_date: Optional[str] = Field(
    None, description="Date of last backup", examples=["2024-01-15T09:00:00Z"]
  )
