"""
Background task-related Pydantic models for the Graph API.

These models are used for tracking long-running background operations
like ingestion, backup, restore, and export.
"""

from enum import Enum

from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
  """Task execution status."""

  PENDING = "pending"
  RUNNING = "running"
  COMPLETED = "completed"
  FAILED = "failed"


class TaskType(Enum):
  """Types of background tasks that support SSE monitoring."""

  INGESTION = "ingestion"
  BACKUP = "backup"
  RESTORE = "restore"
  EXPORT = "export"
  MIGRATION = "migration"


class BackgroundIngestRequest(BaseModel):
  """Request for background ingestion with SSE monitoring."""

  s3_pattern: str = Field(
    ...,
    description="S3 glob pattern for bulk loading (e.g., s3://bucket/path/*.parquet)",
  )
  table_name: str = Field(..., description="Target table name")
  s3_credentials: dict | None = Field(
    None, description="S3 credentials for LocalStack/MinIO"
  )
  ignore_errors: bool = Field(
    True, description="Use IGNORE_ERRORS for duplicate handling"
  )

  class Config:
    json_schema_extra = {
      "example": {
        "s3_pattern": "s3://robosystems-sec-processed/consolidated/nodes/Fact/batch_*.parquet",
        "table_name": "Fact",
        "ignore_errors": True,
        "s3_credentials": {
          "aws_access_key_id": "test",
          "aws_secret_access_key": "test",
          "endpoint_url": "http://localhost:4566",
          "region": "us-east-1",
        },
      }
    }
