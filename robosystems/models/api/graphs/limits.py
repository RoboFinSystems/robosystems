"""Graph query API models."""

from typing import Optional, List
from pydantic import BaseModel, Field, ConfigDict


class StorageLimits(BaseModel):
  """Storage limits information."""

  current_usage_gb: Optional[float] = Field(
    None, description="Current storage usage in GB"
  )
  max_storage_gb: float = Field(..., description="Maximum storage limit in GB")
  approaching_limit: bool = Field(
    ..., description="Whether approaching storage limit (>80%)"
  )


class QueryLimits(BaseModel):
  """Query operation limits."""

  max_timeout_seconds: int = Field(..., description="Maximum query timeout in seconds")
  chunk_size: int = Field(..., description="Maximum chunk size for result streaming")
  max_rows_per_query: int = Field(..., description="Maximum rows returned per query")
  concurrent_queries: int = Field(..., description="Maximum concurrent queries allowed")


class CopyOperationLimits(BaseModel):
  """Copy/ingestion operation limits."""

  max_file_size_gb: float = Field(..., description="Maximum file size in GB")
  timeout_seconds: int = Field(..., description="Operation timeout in seconds")
  concurrent_operations: int = Field(..., description="Maximum concurrent operations")
  max_files_per_operation: int = Field(..., description="Maximum files per operation")
  daily_copy_operations: int = Field(..., description="Daily operation limit")
  supported_formats: List[str] = Field(..., description="Supported file formats")


class BackupLimits(BaseModel):
  """Backup operation limits."""

  max_backup_size_gb: float = Field(..., description="Maximum backup size in GB")
  backup_retention_days: int = Field(..., description="Backup retention period in days")
  max_backups_per_day: int = Field(..., description="Maximum backups per day")


class RateLimits(BaseModel):
  """API rate limits."""

  requests_per_minute: int = Field(..., description="Requests per minute limit")
  requests_per_hour: int = Field(..., description="Requests per hour limit")
  burst_capacity: int = Field(..., description="Burst capacity for short spikes")


class CreditLimits(BaseModel):
  """AI credit limits (optional)."""

  monthly_ai_credits: int = Field(..., description="Monthly AI credits allocation")
  current_balance: int = Field(..., description="Current credit balance")
  storage_billing_enabled: bool = Field(
    ..., description="Whether storage billing is enabled"
  )
  storage_rate_per_gb_per_day: int = Field(
    ..., description="Storage billing rate per GB per day"
  )


class GraphLimitsResponse(BaseModel):
  """Response model for comprehensive graph operational limits."""

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "summary": "Standard tier user graph limits",
          "description": "Operational limits for a ladybug-standard tier user graph with full details",
          "value": {
            "graph_id": "kg1a2b3c4d5",
            "subscription_tier": "ladybug-standard",
            "graph_tier": "ladybug-standard",
            "is_shared_repository": False,
            "storage": {
              "current_usage_gb": 2.45,
              "max_storage_gb": 500,
              "approaching_limit": False,
            },
            "queries": {
              "max_timeout_seconds": 60,
              "chunk_size": 1000,
              "max_rows_per_query": 10000,
              "concurrent_queries": 1,
            },
            "copy_operations": {
              "max_file_size_gb": 1.0,
              "timeout_seconds": 300,
              "concurrent_operations": 1,
              "max_files_per_operation": 100,
              "daily_copy_operations": 10,
              "supported_formats": ["parquet", "csv", "json", "delta", "iceberg"],
            },
            "backups": {
              "max_backup_size_gb": 10,
              "backup_retention_days": 7,
              "max_backups_per_day": 2,
            },
            "rate_limits": {
              "requests_per_minute": 60,
              "requests_per_hour": 1000,
              "burst_capacity": 10,
            },
            "credits": {
              "monthly_ai_credits": 10000,
              "current_balance": 7500,
              "storage_billing_enabled": True,
              "storage_rate_per_gb_per_day": 10,
            },
          },
        },
        {
          "summary": "Shared repository limits (SEC)",
          "description": "Operational limits for SEC shared repository (read-only, no credits)",
          "value": {
            "graph_id": "sec",
            "subscription_tier": "ladybug-standard",
            "graph_tier": "ladybug-shared",
            "is_shared_repository": True,
            "storage": {
              "current_usage_gb": 125.3,
              "max_storage_gb": 1000,
              "approaching_limit": False,
            },
            "queries": {
              "max_timeout_seconds": 120,
              "chunk_size": 2000,
              "max_rows_per_query": 10000,
              "concurrent_queries": 1,
            },
            "copy_operations": {
              "max_file_size_gb": 5.0,
              "timeout_seconds": 600,
              "concurrent_operations": 2,
              "max_files_per_operation": 200,
              "daily_copy_operations": 50,
              "supported_formats": ["parquet", "csv", "json", "delta", "iceberg"],
            },
            "backups": {
              "max_backup_size_gb": 50,
              "backup_retention_days": 30,
              "max_backups_per_day": 4,
            },
            "rate_limits": {
              "requests_per_minute": 120,
              "requests_per_hour": 2000,
              "burst_capacity": 20,
            },
          },
        },
        {
          "summary": "Enterprise tier limits",
          "description": "Operational limits for ladybug-large tier with enhanced capabilities",
          "value": {
            "graph_id": "kg9f8e7d6c5",
            "subscription_tier": "ladybug-large",
            "graph_tier": "ladybug-large",
            "is_shared_repository": False,
            "storage": {
              "current_usage_gb": 450.8,
              "max_storage_gb": 2000,
              "approaching_limit": False,
            },
            "queries": {
              "max_timeout_seconds": 300,
              "chunk_size": 5000,
              "max_rows_per_query": 10000,
              "concurrent_queries": 1,
            },
            "copy_operations": {
              "max_file_size_gb": 10.0,
              "timeout_seconds": 900,
              "concurrent_operations": 5,
              "max_files_per_operation": 500,
              "daily_copy_operations": 100,
              "supported_formats": ["parquet", "csv", "json", "delta", "iceberg"],
            },
            "backups": {
              "max_backup_size_gb": 100,
              "backup_retention_days": 90,
              "max_backups_per_day": 10,
            },
            "rate_limits": {
              "requests_per_minute": 300,
              "requests_per_hour": 5000,
              "burst_capacity": 50,
            },
            "credits": {
              "monthly_ai_credits": 50000,
              "current_balance": 42300,
              "storage_billing_enabled": True,
              "storage_rate_per_gb_per_day": 10,
            },
          },
        },
      ]
    }
  )

  graph_id: str = Field(..., description="Graph database identifier")
  subscription_tier: str = Field(..., description="User's subscription tier")
  graph_tier: str = Field(..., description="Graph's database tier")
  is_shared_repository: bool = Field(
    ..., description="Whether this is a shared repository"
  )
  storage: StorageLimits = Field(..., description="Storage limits and usage")
  queries: QueryLimits = Field(..., description="Query operation limits")
  copy_operations: CopyOperationLimits = Field(
    ..., description="Copy/ingestion operation limits"
  )
  backups: BackupLimits = Field(..., description="Backup operation limits")
  rate_limits: RateLimits = Field(..., description="API rate limits")
  credits: Optional[CreditLimits] = Field(
    None, description="AI credit limits (if applicable)"
  )
