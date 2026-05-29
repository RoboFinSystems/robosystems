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

    report-bundles/                  # Per-Report serialization artifacts (JSON-LD)
      {graph_id}/
        {report_id}/
          g{generation_count}.jsonld

    shared-repositories/             # Shared repository data
      databases/                     # Published databases (downloaded by replicas on boot)
        {graph_id}.lbug
        {graph_id}.duckdb
      backups/                       # Compressed backups (subscriber downloads)
        {graph_id}/
          backup-{timestamp}.tar.gz

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
  REPORT_BUNDLES = "report-bundles"  # Per-Report serialization artifacts
  SHARED_REPO_DATABASES = "shared-repositories/databases"  # Published snapshots
  SHARED_REPO_BACKUPS = "shared-repositories/backups"  # Subscriber backups
  R2_DOWNLOADS = "downloads"  # R2 zero-egress subscriber downloads


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
  GraphStorageType.REPORT_BUNDLES: GraphStorageConfig(
    storage_type=GraphStorageType.REPORT_BUNDLES,
    prefix="report-bundles/",
    description="Per-Report serialization bundles (JSON-LD) stamped at publish",
  ),
  GraphStorageType.SHARED_REPO_DATABASES: GraphStorageConfig(
    storage_type=GraphStorageType.SHARED_REPO_DATABASES,
    prefix="shared-repositories/databases/",
    description="Published shared repository databases for replica fleet",
  ),
  GraphStorageType.SHARED_REPO_BACKUPS: GraphStorageConfig(
    storage_type=GraphStorageType.SHARED_REPO_BACKUPS,
    prefix="shared-repositories/backups/",
    description="Compressed shared repository backups for subscriber downloads",
  ),
  GraphStorageType.R2_DOWNLOADS: GraphStorageConfig(
    storage_type=GraphStorageType.R2_DOWNLOADS,
    prefix="downloads/",
    description="Uncompressed database files on R2 for zero-egress subscriber downloads",
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
# Report Bundle Helpers
# =============================================================================


def get_report_bundle_key(
  graph_id: str,
  report_id: str,
  generation_count: int,
  extension: str = ".jsonld",
) -> str:
  """Build S3 key for a per-Report serialization bundle.

  Versioned by ``generation_count`` so prior generations stay
  addressable in object storage even after a ``regenerate_report``
  bump. ``Report.bundle_url`` always points at the current version;
  history lives on S3 for restatement audit trails. The ``g`` prefix
  reads as "generation" — distinct from framework-version letters
  (``rs-gaap/v1``, ``fac/v1``) elsewhere in the system.

  Args:
      graph_id: Owning graph identifier.
      report_id: Report row id (ULID, ``rpt_`` prefix).
      generation_count: Monotonic counter from ``Report.generation_count``.
      extension: File extension (``.jsonld`` for the JSON-LD flavor;
          future flavors slot in by passing their own extension).

  Returns:
      S3 key string (without bucket name).

  Example:
      >>> get_report_bundle_key("kg456", "rpt_01K8", 1)
      'report-bundles/kg456/rpt_01K8/g1.jsonld'
  """
  config = GRAPH_STORAGE[GraphStorageType.REPORT_BUNDLES]
  return f"{config.prefix}{graph_id}/{report_id}/g{generation_count}{extension}"


def get_report_bundle_prefix(
  graph_id: str | None = None,
  report_id: str | None = None,
) -> str:
  """Build S3 prefix for listing report bundles.

  Args:
      graph_id: Optional graph filter.
      report_id: Optional report filter (only valid when ``graph_id`` set).

  Returns:
      S3 prefix for listing.

  Example:
      >>> get_report_bundle_prefix("kg456", "rpt_01K8")
      'report-bundles/kg456/rpt_01K8/'
  """
  config = GRAPH_STORAGE[GraphStorageType.REPORT_BUNDLES]
  prefix = config.prefix
  if graph_id:
    prefix += f"{graph_id}/"
    if report_id:
      prefix += f"{report_id}/"
  return prefix


# =============================================================================
# Shared Repository Helpers
# =============================================================================


def get_shared_repo_database_key(graph_id: str, extension: str = ".lbug") -> str:
  """Build S3 key for a published shared repository database snapshot.

  Args:
      graph_id: Graph database identifier (e.g., "sec")
      extension: File extension (".lbug" or ".duckdb")

  Returns:
      S3 key string (without bucket name)

  Example:
      >>> get_shared_repo_database_key("sec")
      'shared-repositories/databases/sec.lbug'
      >>> get_shared_repo_database_key("sec", ".duckdb")
      'shared-repositories/databases/sec.duckdb'
  """
  config = GRAPH_STORAGE[GraphStorageType.SHARED_REPO_DATABASES]
  return f"{config.prefix}{graph_id}{extension}"


def get_shared_repo_database_prefix() -> str:
  """Get S3 prefix for shared repository database snapshots.

  Returns:
      S3 prefix for listing

  Example:
      >>> get_shared_repo_database_prefix()
      'shared-repositories/databases/'
  """
  return GRAPH_STORAGE[GraphStorageType.SHARED_REPO_DATABASES].prefix


def get_shared_repo_backup_key(graph_id: str, timestamp: datetime) -> str:
  """Build S3 key for a shared repository backup (subscriber download).

  Args:
      graph_id: Graph database identifier (e.g., "sec")
      timestamp: Backup timestamp

  Returns:
      S3 key string (without bucket name)

  Example:
      >>> from datetime import datetime, UTC
      >>> ts = datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC)
      >>> get_shared_repo_backup_key("sec", ts)
      'shared-repositories/backups/sec/backup-20240115_123045.tar.gz'
  """
  config = GRAPH_STORAGE[GraphStorageType.SHARED_REPO_BACKUPS]
  timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
  return f"{config.prefix}{graph_id}/backup-{timestamp_str}.tar.gz"


def get_shared_repo_backup_prefix(graph_id: str | None = None) -> str:
  """Build S3 prefix for listing shared repository backups.

  Args:
      graph_id: Optional graph filter

  Returns:
      S3 prefix for listing

  Example:
      >>> get_shared_repo_backup_prefix("sec")
      'shared-repositories/backups/sec/'
  """
  config = GRAPH_STORAGE[GraphStorageType.SHARED_REPO_BACKUPS]
  prefix = config.prefix
  if graph_id:
    prefix += f"{graph_id}/"
  return prefix


# =============================================================================
# R2 Download Helpers
# =============================================================================


def get_r2_download_key(graph_id: str, extension: str = ".lbug") -> str:
  """Build R2 key for a subscriber download file.

  Uses a fixed key (no timestamp) — each publish overwrites the previous copy.

  Args:
      graph_id: Graph database identifier (e.g., "sec")
      extension: File extension (".lbug" or ".duckdb")

  Returns:
      R2 key string (without bucket name)

  Example:
      >>> get_r2_download_key("sec")
      'downloads/sec/sec.lbug'
      >>> get_r2_download_key("sec", ".duckdb")
      'downloads/sec/sec.duckdb'
  """
  config = GRAPH_STORAGE[GraphStorageType.R2_DOWNLOADS]
  return f"{config.prefix}{graph_id}/{graph_id}{extension}"


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


def get_report_bundle_uri(bucket: str, *args, **kwargs) -> str:
  """Build full S3 URI for a report bundle.

  Args:
      bucket: S3 bucket name.
      *args, **kwargs: Arguments forwarded to ``get_report_bundle_key``.

  Returns:
      Full S3 URI string.
  """
  key = get_report_bundle_key(*args, **kwargs)
  return f"s3://{bucket}/{key}"
