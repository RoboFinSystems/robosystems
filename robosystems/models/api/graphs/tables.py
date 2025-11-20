from typing import List, Any, Optional
from enum import Enum
from pydantic import BaseModel, Field


class FileUploadStatus(str, Enum):
  """File upload status enumeration.

  Status lifecycle:
  - PENDING: Upload URL generated, awaiting file upload
  - UPLOADED: File successfully uploaded to S3 and validated
  - DISABLED: File excluded from ingestion (soft exclusion)
  - ARCHIVED: File soft-deleted (not shown in listings)
  """

  PENDING = "pending"
  UPLOADED = "uploaded"
  DISABLED = "disabled"
  ARCHIVED = "archived"


class TableCreate(BaseModel):
  table_name: str = Field(..., description="Table name")
  s3_pattern: str = Field(..., description="S3 glob pattern for parquet/csv files")

  class Config:
    extra = "forbid"


class TableInfo(BaseModel):
  table_name: str = Field(..., description="Table name")
  row_count: int = Field(..., description="Approximate row count")
  file_count: int = Field(0, description="Number of files")
  total_size_bytes: int = Field(0, description="Total size in bytes")
  s3_location: Optional[str] = Field(
    None, description="S3 location for external tables"
  )


class TableListResponse(BaseModel):
  tables: List[TableInfo] = Field(..., description="List of tables")
  total_count: int = Field(..., description="Total number of tables")


class TableQueryRequest(BaseModel):
  sql: str = Field(
    ...,
    description="SQL query to execute on staging tables. Use ? placeholders or $param_name for dynamic values to prevent SQL injection.",
    examples=[
      "SELECT * FROM Entity WHERE entity_type = ? LIMIT ?",
      "SELECT COUNT(*) FROM Transaction WHERE amount > ? AND date >= ?",
      "SELECT * FROM Entity LIMIT 10",
    ],
  )
  parameters: Optional[List[Any]] = Field(
    default=None,
    description="Query parameters for safe value substitution. ALWAYS use parameters instead of string concatenation.",
    examples=[
      ["Company", 100],
      [1000, "2024-01-01"],
      None,
    ],
  )

  class Config:
    extra = "forbid"
    json_schema_extra = {
      "examples": [
        {
          "sql": "SELECT * FROM Entity WHERE entity_type = ? LIMIT ?",
          "parameters": ["Company", 100],
        },
        {
          "sql": "SELECT COUNT(*) FROM Transaction WHERE amount > ? AND date >= ?",
          "parameters": [1000, "2024-01-01"],
        },
        {
          "sql": "SELECT * FROM Entity LIMIT 10",
          "parameters": None,
        },
      ]
    }


class TableQueryResponse(BaseModel):
  columns: List[str] = Field(..., description="Column names")
  rows: List[List[Any]] = Field(..., description="Query results")
  row_count: int = Field(..., description="Number of rows returned")
  execution_time_ms: float = Field(..., description="Query execution time")


class TableIngestRequest(BaseModel):
  ignore_errors: bool = Field(
    default=True,
    description="Continue ingestion on row errors",
    examples=[True, False],
  )
  rebuild: bool = Field(
    default=False,
    description="Rebuild graph database from scratch before ingestion. "
    "Safe operation - staged data is the source of truth, graph can always be regenerated.",
    examples=[False, True],
  )

  class Config:
    extra = "forbid"
    json_schema_extra = {
      "examples": [
        {"ignore_errors": True, "rebuild": False},
        {"ignore_errors": False, "rebuild": True},
      ]
    }


class TableIngestResponse(BaseModel):
  status: str = Field(..., description="Ingestion status")
  graph_id: str = Field(..., description="Graph database identifier")
  table_name: str = Field(..., description="Table name")
  rows_ingested: int = Field(..., description="Number of rows ingested")
  execution_time_ms: float = Field(..., description="Ingestion time in milliseconds")


class FileUploadRequest(BaseModel):
  file_name: str = Field(
    ...,
    description="File name to upload",
    examples=["entities.parquet", "transactions.csv", "data.json"],
  )
  content_type: str = Field(
    default="application/x-parquet",
    description="File MIME type",
    examples=["application/x-parquet", "text/csv", "application/json"],
  )
  table_name: Optional[str] = Field(
    default=None,
    description="Table name to associate file with (required for first-class /files endpoint)",
    examples=["Entity", "Fact", "PERSON_WORKS_FOR_COMPANY"],
  )

  class Config:
    extra = "forbid"
    json_schema_extra = {
      "examples": [
        {
          "file_name": "entities.parquet",
          "content_type": "application/x-parquet",
          "table_name": "Entity",
        },
        {
          "file_name": "transactions.csv",
          "content_type": "text/csv",
          "table_name": "Fact",
        },
      ]
    }


class FileUploadResponse(BaseModel):
  upload_url: str = Field(..., description="Presigned S3 upload URL")
  expires_in: int = Field(..., description="URL expiration time in seconds")
  file_id: str = Field(..., description="File tracking ID")
  s3_key: str = Field(..., description="S3 object key")


class FileStatusUpdate(BaseModel):
  status: str = Field(
    ...,
    description="File status: 'uploaded' (ready for ingest), 'disabled' (exclude from ingest), 'archived' (soft deleted)",
    examples=["uploaded", "disabled", "archived"],
  )
  ingest_to_graph: bool = Field(
    default=False,
    description="Auto-ingest to graph after DuckDB staging. Default=false (batch mode). Set to true for real-time incremental updates.",
  )

  class Config:
    extra = "forbid"
    json_schema_extra = {
      "examples": [
        {"status": "uploaded"},
        {"status": "uploaded", "ingest_to_graph": True},
        {"status": "disabled"},
      ]
    }


class BulkIngestRequest(BaseModel):
  ignore_errors: bool = Field(
    default=True,
    description="Continue ingestion on row errors",
    examples=[True, False],
  )
  rebuild: bool = Field(
    default=False,
    description="Rebuild graph database from scratch before ingestion. "
    "Safe operation - staged data is the source of truth, graph can always be regenerated.",
    examples=[False, True],
  )

  class Config:
    extra = "forbid"
    json_schema_extra = {
      "examples": [
        {"ignore_errors": True, "rebuild": False},
        {"ignore_errors": False, "rebuild": True},
      ]
    }


class TableIngestResult(BaseModel):
  table_name: str = Field(..., description="Table name")
  status: str = Field(..., description="Ingestion status (success/failed/skipped)")
  rows_ingested: int = Field(0, description="Number of rows ingested")
  execution_time_ms: float = Field(0, description="Ingestion time in milliseconds")
  error: Optional[str] = Field(None, description="Error message if failed")


class BulkIngestResponse(BaseModel):
  status: str = Field(..., description="Overall ingestion status")
  graph_id: str = Field(..., description="Graph database identifier")
  total_tables: int = Field(..., description="Total number of tables processed")
  successful_tables: int = Field(
    ..., description="Number of successfully ingested tables"
  )
  failed_tables: int = Field(..., description="Number of failed table ingestions")
  skipped_tables: int = Field(..., description="Number of skipped tables (no files)")
  total_rows_ingested: int = Field(
    ..., description="Total rows ingested across all tables"
  )
  total_execution_time_ms: float = Field(
    ..., description="Total execution time in milliseconds"
  )
  results: List[TableIngestResult] = Field(
    ..., description="Per-table ingestion results"
  )


class FileInfo(BaseModel):
  file_id: str = Field(..., description="Unique file identifier")
  file_name: str = Field(..., description="Original file name")
  file_format: str = Field(..., description="File format (parquet, csv, etc.)")
  size_bytes: int = Field(..., description="File size in bytes")
  row_count: Optional[int] = Field(None, description="Estimated row count")
  upload_status: str = Field(..., description="Current upload status")
  upload_method: str = Field(..., description="Upload method used")
  created_at: Optional[str] = Field(None, description="File creation timestamp")
  uploaded_at: Optional[str] = Field(
    None, description="File upload completion timestamp"
  )
  s3_key: str = Field(..., description="S3 object key")


class ListTableFilesResponse(BaseModel):
  graph_id: str = Field(..., description="Graph database identifier")
  table_name: Optional[str] = Field(
    None, description="Table name (null if listing all files in graph)"
  )
  files: List[FileInfo] = Field(..., description="List of files in the table")
  total_files: int = Field(..., description="Total number of files")
  total_size_bytes: int = Field(..., description="Total size of all files in bytes")


class FileLayerStatus(BaseModel):
  status: str = Field(..., description="Layer status")
  timestamp: Optional[str] = Field(None, description="Status timestamp")
  row_count: Optional[int] = Field(None, description="Row count (if available)")
  size_bytes: Optional[int] = Field(None, description="Size in bytes (S3 layer only)")


class EnhancedFileStatusLayers(BaseModel):
  s3: FileLayerStatus = Field(..., description="S3 layer status (immutable source)")
  duckdb: FileLayerStatus = Field(
    ..., description="DuckDB layer status (mutable staging)"
  )
  graph: FileLayerStatus = Field(
    ..., description="Graph layer status (immutable materialized view)"
  )


class GetFileInfoResponse(BaseModel):
  file_id: str = Field(..., description="Unique file identifier")
  graph_id: str = Field(..., description="Graph database identifier")
  table_id: str = Field(..., description="Table identifier")
  table_name: Optional[str] = Field(None, description="Table name")
  file_name: str = Field(..., description="Original file name")
  file_format: str = Field(..., description="File format (parquet, csv, etc.)")
  size_bytes: int = Field(..., description="File size in bytes")
  row_count: Optional[int] = Field(None, description="Estimated row count")
  upload_status: str = Field(..., description="Current upload status")
  upload_method: str = Field(..., description="Upload method used")
  created_at: Optional[str] = Field(None, description="File creation timestamp")
  uploaded_at: Optional[str] = Field(
    None, description="File upload completion timestamp"
  )
  s3_key: str = Field(..., description="S3 object key")
  layers: Optional[EnhancedFileStatusLayers] = Field(
    default=None,
    description="Multi-layer pipeline status (S3 → DuckDB → Graph). Shows status, timestamps, and row counts for each layer independently.",
  )


class DeleteFileResponse(BaseModel):
  status: str = Field(..., description="Deletion status")
  file_id: str = Field(..., description="Deleted file ID")
  file_name: str = Field(..., description="Deleted file name")
  message: str = Field(..., description="Operation message")
  cascade_deleted: bool = Field(
    default=False, description="Whether cascade deletion was performed"
  )
  tables_affected: Optional[List[str]] = Field(
    None, description="Tables from which file data was deleted (if cascade=true)"
  )
  graph_marked_stale: bool = Field(
    default=False, description="Whether graph was marked as stale"
  )
