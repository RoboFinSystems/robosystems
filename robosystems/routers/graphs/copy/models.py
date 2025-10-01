"""
Request/response models for copy operations.

This module defines the data models for all copy operations,
supporting multiple source types with a unified interface.
"""

from typing import Optional, Dict, Any, Literal, Union
from pydantic import BaseModel, Field, field_validator
import re


class BaseCopyRequest(BaseModel):
  """Base request model for all copy operations."""

  # Target configuration
  table_name: str = Field(
    ..., description="Target Kuzu table name", pattern=r"^[a-zA-Z][a-zA-Z0-9_]{0,62}$"
  )

  # Copy options
  ignore_errors: bool = Field(
    True, description="Skip duplicate/invalid rows (enables upsert-like behavior)"
  )
  extended_timeout: bool = Field(
    False, description="Use extended timeout for large datasets"
  )

  # Security and limits
  validate_schema: bool = Field(
    True, description="Validate source schema against target table"
  )

  @field_validator("table_name")
  @classmethod
  def validate_table_name(cls, v: str) -> str:
    """Validate table name for SQL injection prevention."""
    if not v or not v[0].isalpha():
      raise ValueError("Table name must start with a letter")

    # Check for SQL keywords (basic list)
    sql_keywords = {
      "SELECT",
      "INSERT",
      "UPDATE",
      "DELETE",
      "DROP",
      "CREATE",
      "ALTER",
      "TRUNCATE",
      "EXEC",
      "EXECUTE",
      "UNION",
    }
    if v.upper() in sql_keywords:
      raise ValueError(f"Table name cannot be SQL keyword: {v}")

    return v


class S3CopyRequest(BaseCopyRequest):
  """Request model for S3 copy operations.

  Copies data from S3 buckets into graph database tables using user-provided
  AWS credentials. Supports various file formats and bulk loading options.
  """

  source_type: Literal["s3"] = Field("s3", description="Source type identifier")

  # S3 source configuration
  s3_path: str = Field(
    ...,
    description="Full S3 path (s3://bucket/key or s3://bucket/prefix/*.parquet)",
    examples=["s3://my-bucket/data/*.parquet", "s3://my-bucket/file.csv"],
  )

  # S3 authentication (user-provided credentials)
  s3_access_key_id: str = Field(..., description="AWS access key ID for S3 access")
  s3_secret_access_key: str = Field(
    ..., description="AWS secret access key for S3 access"
  )
  s3_session_token: Optional[str] = Field(
    None, description="AWS session token (for temporary credentials)"
  )
  s3_region: Optional[str] = Field("us-east-1", description="S3 region")
  s3_endpoint: Optional[str] = Field(
    None, description="Custom S3 endpoint (for S3-compatible storage)"
  )
  s3_url_style: Optional[Literal["vhost", "path"]] = Field(
    None, description="S3 URL style (vhost or path)"
  )

  # File format configuration
  file_format: Literal["parquet", "csv", "json", "delta", "iceberg"] = Field(
    "parquet", description="File format of the S3 data"
  )

  # CSV-specific options
  csv_delimiter: Optional[str] = Field(",", description="CSV delimiter")
  csv_header: Optional[bool] = Field(True, description="CSV has header row")
  csv_quote: Optional[str] = Field('"', description="CSV quote character")
  csv_escape: Optional[str] = Field("\\", description="CSV escape character")
  csv_skip: Optional[int] = Field(0, description="Number of rows to skip", ge=0)

  # Delta/Iceberg specific options
  allow_moved_paths: Optional[bool] = Field(
    False, description="Allow moved paths for Iceberg tables"
  )

  # Size limits
  max_file_size_gb: Optional[int] = Field(
    10, description="Maximum total file size limit in GB", ge=1, le=100
  )

  @field_validator("s3_path")
  @classmethod
  def validate_s3_path(cls, v: str) -> str:
    """Validate S3 path format."""
    if not v.startswith("s3://"):
      raise ValueError("S3 path must start with 's3://'")

    # Extract bucket and key
    path_parts = v[5:].split("/", 1)
    if not path_parts[0]:
      raise ValueError("S3 bucket name cannot be empty")

    # Validate bucket name (simplified AWS rules)
    bucket = path_parts[0]
    if not re.match(r"^[a-z0-9][a-z0-9\-\.]{1,61}[a-z0-9]$", bucket):
      raise ValueError(
        "Invalid S3 bucket name. Must be 3-63 characters, "
        "start/end with lowercase letter or number"
      )

    # Validate key/prefix if present
    if len(path_parts) > 1 and path_parts[1]:
      key = path_parts[1]
      # Check for path traversal attempts
      if ".." in key or key.startswith("/"):
        raise ValueError("Invalid S3 key pattern")

    return v

  @field_validator("s3_access_key_id")
  @classmethod
  def validate_access_key(cls, v: str) -> str:
    """Validate AWS access key format to prevent injection."""
    if not v:
      raise ValueError("S3 access key ID cannot be empty")

    # AWS access keys are 20 characters, alphanumeric only
    if not re.match(r"^[A-Z0-9]{20}$", v):
      raise ValueError(
        "Invalid AWS access key ID format. Must be 20 uppercase alphanumeric characters"
      )

    # Check for SQL injection attempts
    if "'" in v or '"' in v or ";" in v or "--" in v:
      raise ValueError("Invalid characters in access key ID")

    return v

  @field_validator("s3_secret_access_key")
  @classmethod
  def validate_secret_key(cls, v: str) -> str:
    """Validate AWS secret key format to prevent injection."""
    if not v:
      raise ValueError("S3 secret access key cannot be empty")

    # AWS secret keys are 40 characters, base64-like
    if len(v) != 40:
      raise ValueError("Invalid AWS secret access key length")

    # Check for SQL injection attempts
    if "'" in v or '"' in v or ";" in v or "--" in v:
      raise ValueError("Invalid characters in secret access key")

    return v

  @field_validator("s3_session_token")
  @classmethod
  def validate_session_token(cls, v: Optional[str]) -> Optional[str]:
    """Validate AWS session token format to prevent injection."""
    if v is None:
      return v

    # Session tokens are long base64 strings
    if not re.match(r"^[A-Za-z0-9+/=]+$", v):
      raise ValueError("Invalid AWS session token format")

    # Check for SQL injection attempts
    if "'" in v or '"' in v or ";" in v or "--" in v:
      raise ValueError("Invalid characters in session token")

    return v

  @field_validator("s3_region")
  @classmethod
  def validate_region(cls, v: Optional[str]) -> Optional[str]:
    """Validate AWS region format to prevent injection."""
    if v is None:
      return v

    # AWS regions follow a specific pattern
    if not re.match(r"^[a-z]{2}-[a-z]+-\d{1,2}$", v):
      raise ValueError("Invalid AWS region format. Example: us-east-1, eu-west-2")

    return v

  @field_validator("s3_endpoint")
  @classmethod
  def validate_endpoint(cls, v: Optional[str]) -> Optional[str]:
    """Validate S3 endpoint URL to prevent injection."""
    if v is None:
      return v

    # Basic URL validation
    if not re.match(r"^https?://[a-zA-Z0-9\-\.]+(\:[0-9]+)?(/.*)?$", v):
      raise ValueError("Invalid S3 endpoint URL format")

    # Check for SQL injection attempts
    if "'" in v or '"' in v or ";" in v or "--" in v:
      raise ValueError("Invalid characters in S3 endpoint URL")

    return v

  class Config:
    json_schema_extra = {
      "examples": [
        {
          "source_type": "s3",
          "s3_path": "s3://my-bucket/data/entities.parquet",
          "table_name": "Entity",
          "s3_access_key_id": "AKIAIOSFODNN7EXAMPLE",
          "s3_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
          "file_format": "parquet",
          "ignore_errors": True,
          "validate_schema": False,
          "max_file_size_gb": 10,
        },
        {
          "source_type": "s3",
          "s3_path": "s3://data-lake/transactions/*.csv",
          "table_name": "Transaction",
          "s3_access_key_id": "AKIAIOSFODNN7EXAMPLE",
          "s3_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
          "s3_region": "us-west-2",
          "file_format": "csv",
          "csv_delimiter": ",",
          "csv_header": True,
          "ignore_errors": False,
          "extended_timeout": True,
        },
      ]
    }


class URLCopyRequest(BaseCopyRequest):
  """Request model for URL copy operations (future)."""

  source_type: Literal["url"] = Field("url", description="Source type identifier")

  url: str = Field(
    ..., description="HTTP(S) URL to the data file", pattern=r"^https?://.+"
  )

  file_format: Literal["parquet", "csv", "json"] = Field(
    ..., description="File format of the URL data"
  )

  # HTTP options
  headers: Optional[Dict[str, str]] = Field(
    None, description="Optional HTTP headers for authentication"
  )


class DataFrameCopyRequest(BaseCopyRequest):
  """Request model for DataFrame copy operations (future)."""

  source_type: Literal["dataframe"] = Field(
    "dataframe", description="Source type identifier"
  )

  # For DataFrames, we'd need a different approach - perhaps upload the data
  # as part of the request body or reference a previously uploaded file
  data_reference: str = Field(..., description="Reference to uploaded DataFrame data")

  format: Literal["pandas", "polars", "arrow"] = Field(
    "pandas", description="DataFrame format"
  )


# Union type for all copy requests
CopyRequest = Union[S3CopyRequest, URLCopyRequest, DataFrameCopyRequest]


class CopyResponse(BaseModel):
  """Response model for copy operations."""

  status: Literal["completed", "failed", "partial", "accepted"] = Field(
    ..., description="Operation status"
  )

  operation_id: Optional[str] = Field(
    None, description="Operation ID for SSE monitoring (for long-running operations)"
  )

  sse_url: Optional[str] = Field(
    None, description="SSE endpoint URL for monitoring operation progress"
  )

  source_type: str = Field(..., description="Type of source that was copied from")

  execution_time_ms: Optional[float] = Field(
    None,
    description="Total execution time in milliseconds (for synchronous operations)",
  )

  message: str = Field(..., description="Human-readable status message")

  rows_imported: Optional[int] = Field(
    None, description="Number of rows successfully imported"
  )

  rows_skipped: Optional[int] = Field(
    None, description="Number of rows skipped due to errors (when ignore_errors=true)"
  )

  warnings: Optional[list[str]] = Field(
    None, description="List of warnings encountered during import"
  )

  error_details: Optional[Dict[str, Any]] = Field(
    None, description="Detailed error information if operation failed"
  )

  bytes_processed: Optional[int] = Field(
    None, description="Total bytes processed from source"
  )

  class Config:
    json_schema_extra = {
      "examples": [
        {
          "status": "accepted",
          "operation_id": "550e8400-e29b-41d4-a716-446655440000",
          "sse_url": "/v1/operations/550e8400-e29b-41d4-a716-446655440000/stream",
          "source_type": "s3",
          "message": "Copy operation started. Monitor progress at /v1/operations/550e8400-e29b-41d4-a716-446655440000/stream",
        },
        {
          "status": "completed",
          "source_type": "s3",
          "execution_time_ms": 5234.56,
          "message": "Successfully imported 150,000 rows from s3://my-bucket/data/*.parquet",
          "rows_imported": 150000,
          "bytes_processed": 52428800,
        },
        {
          "status": "completed",
          "source_type": "s3",
          "message": "Data imported to TestEntity (row count not available with ignore_errors option)",
          "execution_time_ms": 3456.78,
          "rows_imported": None,
        },
        {
          "status": "partial",
          "source_type": "s3",
          "execution_time_ms": 3456.78,
          "message": "Imported 95,000 rows, skipped 5,000 due to errors",
          "rows_imported": 95000,
          "rows_skipped": 5000,
          "warnings": ["5,000 rows skipped due to duplicate primary keys"],
        },
      ]
    }
