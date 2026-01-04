"""Graph database storage configuration.

This module defines the S3 path structure for customer graph databases
and provides helpers for building consistent S3 keys.

All graph storage uses the USER_DATA_BUCKET with organized prefixes:

  s3://robosystems-user-data-{env}/
    user-staging/                    # User file uploads (pre-ingestion)
      {user_id}/
        {graph_id}/
          {table_name}/
            {file_id}/
              {filename}

    graph-backups/                   # Application-level backups (via API)
      databases/
        {graph_id}/
          {backup_type}/             # full, incremental
            backup-{timestamp}.{ext}
      metadata/
        {graph_id}/
          backup-{timestamp}.json

    graph-databases/                 # Instance-level backups (via daemon)
      {environment}/
        {graph_id}/
          {graph_id}_{timestamp}.tar.gz

The graph_id is the primary construct that scopes all storage operations,
ensuring multi-tenant isolation and consistent organization.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class GraphStorageType(Enum):
  """Types of graph-related storage."""

  USER_STAGING = "user-staging"  # Pre-ingestion file uploads
  BACKUPS = "graph-backups"  # Application-level backups
  DATABASES = "graph-databases"  # Instance-level database backups


@dataclass
class GraphStorageConfig:
  """Configuration for a graph storage type."""

  storage_type: GraphStorageType
  prefix: str
  description: str


# Registry of graph storage types
GRAPH_STORAGE: dict[GraphStorageType, GraphStorageConfig] = {
  GraphStorageType.USER_STAGING: GraphStorageConfig(
    storage_type=GraphStorageType.USER_STAGING,
    prefix="user-staging/",
    description="User file uploads awaiting ingestion into graph databases",
  ),
  GraphStorageType.BACKUPS: GraphStorageConfig(
    storage_type=GraphStorageType.BACKUPS,
    prefix="graph-backups/",
    description="Application-level graph database backups with metadata",
  ),
  GraphStorageType.DATABASES: GraphStorageConfig(
    storage_type=GraphStorageType.DATABASES,
    prefix="graph-databases/",
    description="Instance-level database backups from writer nodes",
  ),
}


# =============================================================================
# User Staging Helpers
# =============================================================================


def get_staging_key(
  user_id: str,
  graph_id: str,
  table_name: str,
  file_id: str,
  filename: str,
) -> str:
  """Build S3 key for user file staging.

  Args:
      user_id: User identifier
      graph_id: Graph database identifier
      table_name: Target table name
      file_id: Unique file identifier
      filename: Original filename

  Returns:
      S3 key string (without bucket name)

  Example:
      >>> get_staging_key("user123", "kg456", "Entity", "f789", "data.parquet")
      'user-staging/user123/kg456/Entity/f789/data.parquet'
  """
  config = GRAPH_STORAGE[GraphStorageType.USER_STAGING]
  return f"{config.prefix}{user_id}/{graph_id}/{table_name}/{file_id}/{filename}"


def get_staging_prefix(
  user_id: str | None = None,
  graph_id: str | None = None,
  table_name: str | None = None,
) -> str:
  """Build S3 prefix for listing staged files.

  Args:
      user_id: Optional user filter
      graph_id: Optional graph filter
      table_name: Optional table filter

  Returns:
      S3 prefix for listing

  Example:
      >>> get_staging_prefix("user123", "kg456")
      'user-staging/user123/kg456/'
  """
  config = GRAPH_STORAGE[GraphStorageType.USER_STAGING]
  prefix = config.prefix

  if user_id:
    prefix += f"{user_id}/"
    if graph_id:
      prefix += f"{graph_id}/"
      if table_name:
        prefix += f"{table_name}/"

  return prefix


# =============================================================================
# Application Backup Helpers
# =============================================================================


def get_backup_key(
  graph_id: str,
  backup_type: str,
  timestamp: datetime,
  extension: str = ".lbug.gz",
) -> str:
  """Build S3 key for application-level backup.

  Args:
      graph_id: Graph database identifier
      backup_type: Backup type ('full' or 'incremental')
      timestamp: Backup timestamp
      extension: File extension (default: .lbug.gz)

  Returns:
      S3 key string (without bucket name)

  Example:
      >>> from datetime import datetime, UTC
      >>> ts = datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC)
      >>> get_backup_key("kg456", "full", ts)
      'graph-backups/databases/kg456/full/backup-20240115_123045.lbug.gz'
  """
  config = GRAPH_STORAGE[GraphStorageType.BACKUPS]
  timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
  return f"{config.prefix}databases/{graph_id}/{backup_type}/backup-{timestamp_str}{extension}"


def get_backup_metadata_key(graph_id: str, timestamp: datetime) -> str:
  """Build S3 key for backup metadata.

  Args:
      graph_id: Graph database identifier
      timestamp: Backup timestamp

  Returns:
      S3 key string (without bucket name)

  Example:
      >>> from datetime import datetime, UTC
      >>> ts = datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC)
      >>> get_backup_metadata_key("kg456", ts)
      'graph-backups/metadata/kg456/backup-20240115_123045.json'
  """
  config = GRAPH_STORAGE[GraphStorageType.BACKUPS]
  timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
  return f"{config.prefix}metadata/{graph_id}/backup-{timestamp_str}.json"


def get_backup_prefix(
  graph_id: str | None = None, backup_type: str | None = None
) -> str:
  """Build S3 prefix for listing backups.

  Args:
      graph_id: Optional graph filter
      backup_type: Optional backup type filter ('full' or 'incremental')

  Returns:
      S3 prefix for listing

  Example:
      >>> get_backup_prefix("kg456", "full")
      'graph-backups/databases/kg456/full/'
  """
  config = GRAPH_STORAGE[GraphStorageType.BACKUPS]
  prefix = f"{config.prefix}databases/"

  if graph_id:
    prefix += f"{graph_id}/"
    if backup_type:
      prefix += f"{backup_type}/"

  return prefix


# =============================================================================
# Instance Database Backup Helpers
# =============================================================================


def get_instance_backup_key(
  environment: str,
  graph_id: str,
  timestamp: datetime,
) -> str:
  """Build S3 key for instance-level database backup.

  Args:
      environment: Environment name (dev/staging/prod)
      graph_id: Graph database identifier
      timestamp: Backup timestamp

  Returns:
      S3 key string (without bucket name)

  Example:
      >>> from datetime import datetime, UTC
      >>> ts = datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC)
      >>> get_instance_backup_key("prod", "kg456", ts)
      'graph-databases/prod/kg456/kg456_20240115_123045.tar.gz'
  """
  config = GRAPH_STORAGE[GraphStorageType.DATABASES]
  timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
  return f"{config.prefix}{environment}/{graph_id}/{graph_id}_{timestamp_str}.tar.gz"


def get_instance_backup_prefix(
  environment: str,
  graph_id: str | None = None,
) -> str:
  """Build S3 prefix for listing instance backups.

  Args:
      environment: Environment name (dev/staging/prod)
      graph_id: Optional graph filter

  Returns:
      S3 prefix for listing

  Example:
      >>> get_instance_backup_prefix("prod", "kg456")
      'graph-databases/prod/kg456/'
  """
  config = GRAPH_STORAGE[GraphStorageType.DATABASES]
  prefix = f"{config.prefix}{environment}/"

  if graph_id:
    prefix += f"{graph_id}/"

  return prefix


# =============================================================================
# URI Builders
# =============================================================================


def get_staging_uri(bucket: str, *args, **kwargs) -> str:
  """Build full S3 URI for staged file.

  Args:
      bucket: S3 bucket name
      *args, **kwargs: Arguments passed to get_staging_key

  Returns:
      Full S3 URI string
  """
  key = get_staging_key(*args, **kwargs)
  return f"s3://{bucket}/{key}"


def get_backup_uri(bucket: str, *args, **kwargs) -> str:
  """Build full S3 URI for backup.

  Args:
      bucket: S3 bucket name
      *args, **kwargs: Arguments passed to get_backup_key

  Returns:
      Full S3 URI string
  """
  key = get_backup_key(*args, **kwargs)
  return f"s3://{bucket}/{key}"


def get_instance_backup_uri(bucket: str, *args, **kwargs) -> str:
  """Build full S3 URI for instance backup.

  Args:
      bucket: S3 bucket name
      *args, **kwargs: Arguments passed to get_instance_backup_key

  Returns:
      Full S3 URI string
  """
  key = get_instance_backup_key(*args, **kwargs)
  return f"s3://{bucket}/{key}"
