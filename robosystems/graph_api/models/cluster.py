"""
Cluster-related Pydantic models for the Graph API.
"""


from pydantic import BaseModel, Field


class ClusterHealthResponse(BaseModel):
  """Response model for cluster health checks."""

  status: str = Field(..., description="Overall cluster status")
  uptime_seconds: float = Field(..., description="Cluster uptime in seconds")
  node_type: str = Field(..., description="Node type (writer/reader)")
  base_path: str = Field(..., description="Base path for databases")
  max_databases: int = Field(..., description="Maximum database capacity")
  current_databases: int = Field(..., description="Current number of databases")
  capacity_remaining: int = Field(..., description="Remaining database capacity")
  read_only: bool = Field(..., description="Whether node is in read-only mode")
  last_activity: str | None = Field(None, description="Last activity timestamp")


class MemoryConfiguration(BaseModel):
  """Memory configuration settings."""

  instance_max_mb: int = Field(..., description="Maximum memory for instance in MB")
  per_database_max_mb: int = Field(..., description="Maximum memory per database in MB")
  admission_threshold_percent: float = Field(
    ..., description="Memory threshold for admission control"
  )


class QueryConfiguration(BaseModel):
  """Query execution configuration."""

  timeout_seconds: int = Field(..., description="Query execution timeout in seconds")
  max_connections_per_db: int = Field(
    ..., description="Maximum connections per database"
  )
  connection_ttl_minutes: float = Field(..., description="Connection TTL in minutes")
  health_check_interval_minutes: float = Field(
    ..., description="Health check interval in minutes"
  )


class AdmissionControlConfig(BaseModel):
  """Admission control configuration."""

  memory_threshold: float = Field(..., description="Memory usage threshold percentage")
  cpu_threshold: float = Field(..., description="CPU usage threshold percentage")
  queue_threshold: float = Field(..., description="Queue depth threshold")
  check_interval: float = Field(..., description="Check interval in seconds")


class NodeConfiguration(BaseModel):
  """Complete node configuration."""

  base_path: str = Field(..., description="Base path for databases")
  read_only: bool = Field(..., description="Whether node is in read-only mode")
  max_databases: int = Field(..., description="Maximum database capacity")
  memory_limits: MemoryConfiguration = Field(..., description="Memory configuration")
  query_limits: QueryConfiguration = Field(..., description="Query execution limits")
  admission_control: AdmissionControlConfig = Field(
    ..., description="Admission control settings"
  )


class ClusterInfoResponse(BaseModel):
  """Response model for cluster information."""

  node_id: str = Field(..., description="Node identifier")
  node_type: str = Field(..., description="Node type (writer/reader)")
  cluster_version: str = Field(..., description="Cluster software version")
  base_path: str = Field(..., description="Base path for databases")
  max_databases: int = Field(..., description="Maximum database capacity")
  databases: list[str] = Field(..., description="List of database names")
  uptime_seconds: float = Field(..., description="Node uptime")
  read_only: bool = Field(..., description="Read-only mode status")

  # New comprehensive configuration section
  configuration: NodeConfiguration | None = Field(
    None, description="Complete node configuration"
  )
