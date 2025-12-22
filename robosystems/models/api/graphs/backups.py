"""Graph query API models."""

from pydantic import BaseModel, ConfigDict, Field, field_validator


class BackupCreateRequest(BaseModel):
  """Request model for creating a backup."""

  backup_format: str = Field(
    "full_dump",
    description="Backup format - only 'full_dump' is supported (complete .lbug database file)",
    pattern="^full_dump$",  # Only allow full_dump
  )
  backup_type: str = Field(
    "full",
    description="Backup type - only 'full' is supported",
    pattern="^full$",  # Only allow full backups
  )
  retention_days: int = Field(90, ge=1, le=2555, description="Retention period in days")
  compression: bool = Field(
    True, description="Enable compression (always enabled for optimal storage)"
  )
  encryption: bool = Field(
    False, description="Enable encryption (encrypted backups cannot be downloaded)"
  )
  schedule: str | None = Field(
    None, description="Optional cron schedule for automated backups"
  )

  @field_validator("compression")
  @classmethod
  def compression_must_be_true(cls, v):
    """Ensure compression is always enabled."""
    if v is not True:
      raise ValueError("Compression must be enabled (always True)")
    return True


class BackupResponse(BaseModel):
  """Response model for backup information."""

  backup_id: str
  graph_id: str
  backup_format: str
  backup_type: str
  status: str
  # s3_bucket and s3_key removed for security - infrastructure details not needed by users
  original_size_bytes: int
  compressed_size_bytes: int
  compression_ratio: float
  node_count: int
  relationship_count: int
  backup_duration_seconds: float
  encryption_enabled: bool
  compression_enabled: bool
  allow_export: bool
  created_at: str
  completed_at: str | None
  expires_at: str | None


class BackupListResponse(BaseModel):
  """Response model for backup list."""

  backups: list[BackupResponse]
  total_count: int
  graph_id: str


class BackupStatsResponse(BaseModel):
  """Response model for backup statistics."""

  graph_id: str
  total_backups: int
  successful_backups: int
  failed_backups: int
  success_rate: float
  total_original_size_bytes: int
  total_compressed_size_bytes: int
  storage_saved_bytes: int
  average_compression_ratio: float
  latest_backup_date: str | None
  backup_formats: dict[str, int]


class BackupExportRequest(BaseModel):
  """Request model for exporting a backup."""

  backup_id: str = Field(..., description="ID of backup to export")
  export_format: str = Field(
    "original",
    description="Export format - only 'original' is supported (compressed .lbug file)",
    pattern="^original$",  # Only allow original format
  )


class BackupRestoreRequest(BaseModel):
  """Request model for restoring from a backup."""

  create_system_backup: bool = Field(
    True, description="Create a system backup of existing database before restore"
  )
  verify_after_restore: bool = Field(
    True, description="Verify database integrity after restore"
  )


class BackupDownloadUrlResponse(BaseModel):
  """Response model for backup download URL generation."""

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "summary": "Download URL for recent backup",
          "description": "Generated pre-signed URL for downloading a backup with 1 hour expiration",
          "value": {
            "download_url": "https://s3.amazonaws.com/robosystems-backups/kg1a2b3c4d5/backup_20240115_100000.lbug.tar.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=...",
            "expires_in": 3600,
            "expires_at": 1705315200.0,
            "backup_id": "bk1a2b3c4d5",
            "graph_id": "kg1a2b3c4d5",
          },
        },
        {
          "summary": "Extended expiration download URL",
          "description": "Download URL with 24-hour expiration for large backup files",
          "value": {
            "download_url": "https://s3.amazonaws.com/robosystems-backups/kg9f8e7d6c5/backup_20240114_183000.lbug.tar.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=...",
            "expires_in": 86400,
            "expires_at": 1705401600.0,
            "backup_id": "bk9f8e7d6c5",
            "graph_id": "kg9f8e7d6c5",
          },
        },
        {
          "summary": "Short-lived download URL",
          "description": "Download URL with minimum 5-minute expiration for immediate download",
          "value": {
            "download_url": "https://s3.amazonaws.com/robosystems-backups/sec/backup_20240115_120000.lbug.tar.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=...",
            "expires_in": 300,
            "expires_at": 1705314900.0,
            "backup_id": "bksec123456",
            "graph_id": "sec",
          },
        },
      ]
    }
  )

  download_url: str = Field(
    ...,
    description="Pre-signed S3 URL for downloading the backup file",
    examples=[
      "https://s3.amazonaws.com/robosystems-backups/kg1a2b3c4d5/backup.lbug.tar.gz?X-Amz-Credential=..."
    ],
  )
  expires_in: int = Field(
    ...,
    description="URL expiration time in seconds from now",
    examples=[3600],
    ge=300,
    le=86400,
  )
  expires_at: float = Field(
    ...,
    description="Unix timestamp when the URL expires",
    examples=[1705315200.0],
  )
  backup_id: str = Field(..., description="Backup identifier", examples=["bk1a2b3c4d5"])
  graph_id: str = Field(
    ..., description="Graph database identifier", examples=["kg1a2b3c4d5"]
  )
