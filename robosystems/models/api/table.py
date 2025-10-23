from typing import List, Any, Optional
from pydantic import BaseModel, Field


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
  sql: str = Field(..., description="SQL query to execute on staging tables")

  class Config:
    extra = "forbid"


class TableQueryResponse(BaseModel):
  columns: List[str] = Field(..., description="Column names")
  rows: List[List[Any]] = Field(..., description="Query results")
  row_count: int = Field(..., description="Number of rows returned")
  execution_time_ms: float = Field(..., description="Query execution time")


class TableIngestRequest(BaseModel):
  table_name: str = Field(..., description="Table name to ingest from DuckDB")
  ignore_errors: bool = Field(
    default=True, description="Continue ingestion on row errors"
  )
  rebuild: bool = Field(
    default=False,
    description="Rebuild graph database from scratch before ingestion. "
    "Deletes existing Kuzu database and recreates it with current schema. "
    "Graph will be unavailable during rebuild.",
  )

  class Config:
    extra = "forbid"


class TableIngestResponse(BaseModel):
  status: str = Field(..., description="Ingestion status")
  graph_id: str = Field(..., description="Graph database identifier")
  table_name: str = Field(..., description="Table name")
  rows_ingested: int = Field(..., description="Number of rows ingested")
  execution_time_ms: float = Field(..., description="Ingestion time in milliseconds")


class FileUploadRequest(BaseModel):
  file_name: str = Field(..., description="File name to upload")
  content_type: str = Field(
    default="application/x-parquet", description="File MIME type"
  )

  class Config:
    extra = "forbid"


class FileUploadResponse(BaseModel):
  upload_url: str = Field(..., description="Presigned S3 upload URL")
  expires_in: int = Field(..., description="URL expiration time in seconds")
  file_id: str = Field(..., description="File tracking ID")
  s3_key: str = Field(..., description="S3 object key")


class FileUploadCompleteRequest(BaseModel):
  file_size_bytes: int = Field(..., description="Actual uploaded file size in bytes")
  row_count: Optional[int] = Field(None, description="Number of rows in the file")

  class Config:
    extra = "forbid"
