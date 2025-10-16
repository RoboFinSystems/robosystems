"""
Ingestion-related Pydantic models for the Kuzu API.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field


class CopyIngestRequest(BaseModel):
  """Request model for COPY-based data ingestion (fast bulk load)."""

  pipeline_run_id: str = Field(..., description="Pipeline run identifier")
  bucket: str = Field(..., description="S3 bucket containing parquet files")
  files: List[str] = Field(..., description="List of S3 file keys to ingest")
  priority: int = Field(
    default=5, ge=1, le=10, description="Task priority (1-10, higher is more important)"
  )


class IngestResponse(BaseModel):
  """Response model for ingestion results."""

  status: str = Field(..., description="Ingestion status")
  files_processed: int = Field(..., description="Number of files processed")
  execution_time_ms: float = Field(..., description="Total ingestion time")
  error: Optional[str] = Field(None, description="Error message if failed")
  task_id: Optional[str] = Field(None, description="Celery task ID for tracking")
  message: Optional[str] = Field(None, description="Additional status message")


class TaskStatusResponse(BaseModel):
  """Response model for task status queries."""

  task_id: str = Field(..., description="Celery task ID")
  status: str = Field(..., description="Task status")
  progress: Optional[Dict[str, Any]] = Field(
    None, description="Task progress information"
  )
  result: Optional[Dict[str, Any]] = Field(None, description="Task result if completed")
  error: Optional[str] = Field(None, description="Error message if failed")


class BatchIngestRequest(BaseModel):
  """Request model for batch ingestion operations."""

  file_path: str = Field(..., description="Path to parquet file to ingest")
  table_name: str = Field(..., description="Target table name")
  ignore_errors: bool = Field(default=True, description="Ignore duplicate key errors")
  batch_size: int = Field(default=1000, description="Batch size for operations")


class BatchIngestResponse(BaseModel):
  """Response model for batch ingestion operations."""

  status: str = Field(..., description="Operation status")
  rows_processed: Optional[int] = Field(
    None, description="Total rows processed (omitted for S3 bulk operations)"
  )
  rows_inserted: Optional[int] = Field(
    None, description="Rows inserted (omitted for S3 bulk operations)"
  )
  rows_skipped: Optional[int] = Field(
    None, description="Rows skipped (duplicates, omitted for S3 bulk operations)"
  )
  execution_time_ms: float = Field(..., description="Execution time in milliseconds")
  error: Optional[str] = Field(None, description="Error message if failed")
  message: Optional[str] = Field(None, description="Additional status message")
