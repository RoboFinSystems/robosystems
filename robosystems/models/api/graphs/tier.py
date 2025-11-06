"""Graph tier API response models.

This module contains Pydantic models for graph tier configuration responses.
"""

from pydantic import BaseModel, Field, ConfigDict


class GraphTierCopyOperations(BaseModel):
  """Copy operation limits for a tier."""

  max_file_size_gb: float = Field(..., description="Maximum file size in GB")
  timeout_seconds: int = Field(..., description="Operation timeout in seconds")
  concurrent_operations: int = Field(..., description="Maximum concurrent operations")
  max_files_per_operation: int = Field(..., description="Maximum files per operation")
  daily_copy_operations: int = Field(..., description="Daily operation limit")


class GraphTierBackup(BaseModel):
  """Backup configuration for a tier."""

  max_backup_size_gb: int = Field(..., description="Maximum backup size in GB")
  backup_retention_days: int = Field(..., description="Backup retention period in days")
  max_backups_per_day: int = Field(..., description="Maximum backups per day")


class GraphTierLimits(BaseModel):
  """Resource limits for a tier."""

  storage_gb: int = Field(..., description="Storage limit in GB")
  monthly_credits: int = Field(..., description="Monthly credit allocation")
  max_subgraphs: int | None = Field(
    ..., description="Maximum subgraphs (null for unlimited)"
  )
  copy_operations: GraphTierCopyOperations = Field(
    ..., description="Copy operation limits"
  )
  backup: GraphTierBackup = Field(..., description="Backup configuration")


class GraphTierInstance(BaseModel):
  """Instance specifications for a tier."""

  type: str = Field(..., description="Instance type identifier")
  memory_mb: int = Field(..., description="Memory allocated to your graph in megabytes")
  is_multitenant: bool = Field(
    ..., description="Whether this tier shares infrastructure with other graphs"
  )


class GraphTierInfo(BaseModel):
  """Complete information about a graph database tier."""

  model_config = ConfigDict(json_schema_mode_override="serialization")

  tier: str = Field(..., description="Tier identifier")
  name: str = Field(..., description="Tier name")
  display_name: str = Field(..., description="Display name for UI")
  description: str = Field(..., description="Tier description")
  backend: str = Field(..., description="Database backend (kuzu or neo4j)")
  enabled: bool = Field(..., description="Whether tier is available")
  max_subgraphs: int | None = Field(..., description="Maximum subgraphs allowed")
  storage_limit_gb: int = Field(..., description="Storage limit in GB")
  monthly_credits: int = Field(..., description="Monthly AI credits")
  api_rate_multiplier: float = Field(..., description="API rate limit multiplier")
  monthly_price: float | None = Field(default=None, description="Monthly price in USD")
  features: list[str] = Field(..., description="List of tier features")
  instance: GraphTierInstance = Field(..., description="Instance specifications")
  limits: GraphTierLimits = Field(..., description="Resource limits")


class AvailableGraphTiersResponse(BaseModel):
  """Response containing available graph tiers."""

  tiers: list[GraphTierInfo] = Field(..., description="List of available tiers")
