"""
Database-related Pydantic models for the Kuzu API.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
  """Request model for Cypher query execution."""

  database: str = Field(
    ..., description="Target database name", pattern=r"^[a-zA-Z0-9_-]+$", max_length=64
  )
  cypher: str = Field(..., description="Cypher query to execute", max_length=10000)
  parameters: Optional[Dict[str, Any]] = Field(
    default=None, description="Query parameters"
  )

  class Config:
    # Prevent additional fields to avoid injection
    extra = "forbid"


class QueryResponse(BaseModel):
  """Response model for query results."""

  data: List[Dict[str, Any]] = Field(..., description="Query result rows")
  columns: List[str] = Field(..., description="Column names")
  execution_time_ms: float = Field(
    ..., description="Query execution time in milliseconds"
  )
  row_count: int = Field(..., description="Number of rows returned")
  database: str = Field(..., description="Database that executed the query")


class SchemaInstallRequest(BaseModel):
  """Request to install schema on existing database."""

  type: str = Field(..., description="Schema type (custom, ddl)")
  ddl: Optional[str] = Field(None, description="DDL commands to execute")
  metadata: Optional[Dict[str, Any]] = Field(
    None, description="Schema metadata (name, version, etc)"
  )


class SchemaInstallResponse(BaseModel):
  """Response from schema installation."""

  success: bool = Field(..., description="Whether schema was installed successfully")
  message: str = Field(..., description="Success or error message")
  statements_executed: int = Field(0, description="Number of DDL statements executed")


class BackupRequest(BaseModel):
  """Request model for database backup."""

  backup_format: str = Field(
    default="full_dump",
    description="Backup format: 'full_dump' (complete .kuzu database file)",
    pattern="^full_dump$",  # Only allow full_dump for now
  )
  include_metadata: bool = Field(
    default=True, description="Include database metadata and schema information"
  )
  compression: bool = Field(
    default=True, description="Enable compression (always recommended)"
  )
  encryption: bool = Field(
    default=False, description="Enable encryption for secure storage"
  )


class BackupResponse(BaseModel):
  """Response from backup operation."""

  task_id: str = Field(
    ..., description="Background task ID for monitoring backup progress"
  )
  status: str = Field(..., description="Initial backup status")
  message: str = Field(..., description="Backup initiation message")
  database: str = Field(..., description="Database being backed up")
  backup_format: str = Field(..., description="Backup format requested")
  monitor_url: Optional[str] = Field(
    None, description="SSE endpoint URL for monitoring backup progress"
  )
  estimated_completion_time: Optional[str] = Field(
    None, description="Estimated completion time (ISO format)"
  )


class DatabaseInfo(BaseModel):
  """Information about a database on this node."""

  graph_id: str = Field(..., description="Graph database identifier")
  database_path: str = Field(..., description="Full path to database files")
  created_at: str = Field(..., description="Database creation timestamp")
  size_bytes: int = Field(..., description="Database size in bytes")
  read_only: bool = Field(..., description="Whether database is read-only")
  is_healthy: bool = Field(..., description="Database health status")
  last_accessed: Optional[str] = Field(None, description="Last access timestamp")


class DatabaseCreateRequest(BaseModel):
  """Request to create a new database."""

  graph_id: str = Field(
    ...,
    description="Graph database identifier",
    pattern=r"^[a-zA-Z0-9_-]+$",
    max_length=64,
  )
  schema_type: str = Field(
    default="entity",
    description="Schema type (entity, shared, custom)",
    pattern=r"^(entity|shared|custom)$",
  )
  repository_name: Optional[str] = Field(
    None,
    description="Repository name for shared schemas (e.g., 'sec', 'industry')",
  )
  custom_schema_ddl: Optional[str] = Field(
    None,
    description="Custom DDL commands for custom schema type",
  )
  is_subgraph: bool = Field(
    default=False,
    description="Whether this is a subgraph (bypasses max_databases check for Enterprise/Premium)",
  )
  tenant_tier: str = Field(
    default="standard",
    description="Tenant tier affecting resource allocation",
    pattern=r"^(standard|enterprise|premium)$",
  )


class DatabaseCreateResponse(BaseModel):
  """Response from database creation."""

  status: str = Field(..., description="Creation status")
  graph_id: str = Field(..., description="Graph database identifier")
  database_path: str = Field(..., description="Path to created database")
  schema_applied: bool = Field(..., description="Whether schema was applied")
  execution_time_ms: float = Field(..., description="Creation time in milliseconds")


class DatabaseListResponse(BaseModel):
  """Response listing all databases."""

  databases: List[DatabaseInfo] = Field(..., description="List of databases")
  total_databases: int = Field(..., description="Total number of databases")
  total_size_bytes: int = Field(..., description="Total size of all databases")
  node_capacity: Dict[str, Any] = Field(..., description="Node capacity information")


class DatabaseHealthResponse(BaseModel):
  """Health status for all databases."""

  healthy_databases: int = Field(..., description="Number of healthy databases")
  unhealthy_databases: int = Field(..., description="Number of unhealthy databases")
  databases: List[DatabaseInfo] = Field(..., description="Database health details")


class RestoreRequest(BaseModel):
  """Request model for database restore."""

  backup_data: bytes = Field(..., description="Backup data to restore from")
  create_system_backup: bool = Field(
    default=True, description="Create system backup before restore"
  )
  force_overwrite: bool = Field(
    default=False, description="Force overwrite existing database"
  )


class RestoreResponse(BaseModel):
  """Response from restore operation."""

  task_id: str = Field(
    ..., description="Background task ID for monitoring restore progress"
  )
  status: str = Field(..., description="Initial restore status")
  message: str = Field(..., description="Restore initiation message")
  database: str = Field(..., description="Database being restored")
  monitor_url: Optional[str] = Field(
    None, description="SSE endpoint URL for monitoring restore progress"
  )
  system_backup_created: bool = Field(
    ..., description="Whether system backup was created"
  )
