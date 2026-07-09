"""Shared data source configuration.

This module defines the data sources available in the shared data buckets
and provides helpers for building consistent S3 paths.

The shared bucket structure uses key prefixes to organize data by source:
  s3://robosystems-shared-raw-{env}/
    sec/          # SEC EDGAR filings
    fred/         # Federal Reserve Economic Data (future)
    bls/          # Bureau of Labor Statistics (future)
    census/       # US Census data (future)
    industry/     # Industry benchmarks (future)

  s3://robosystems-shared-processed-{env}/
    sec/          # SEC processed parquet files
    fred/         # FRED processed data (future)
    ...
"""

from dataclasses import dataclass
from enum import Enum


class DataSourceType(Enum):
  """Supported external data sources."""

  SEC = "sec"  # SEC EDGAR filings
  FRED = "fred"  # Federal Reserve Economic Data
  BLS = "bls"  # Bureau of Labor Statistics
  CENSUS = "census"  # US Census Bureau
  INDUSTRY = "industry"  # Industry benchmarks


@dataclass
class DataSourceConfig:
  """Configuration for an external data source."""

  source_type: DataSourceType
  raw_prefix: str  # Prefix in shared-raw bucket
  processed_prefix: str  # Prefix in shared-processed bucket
  enabled: bool  # Whether this source is currently active
  rate_limit: int  # Max requests per second to source API
  user_agent: str | None = None  # Required user agent for API calls
  description: str = ""  # Human-readable description


# Registry of all data sources
DATA_SOURCES: dict[DataSourceType, DataSourceConfig] = {
  DataSourceType.SEC: DataSourceConfig(
    source_type=DataSourceType.SEC,
    raw_prefix="sec/",
    processed_prefix="sec/",
    enabled=True,
    rate_limit=10,
    user_agent="RoboSystems hello@robosystems.ai",
    description="SEC EDGAR financial filings (10-K, 10-Q, 8-K, etc.)",
  ),
  DataSourceType.FRED: DataSourceConfig(
    source_type=DataSourceType.FRED,
    raw_prefix="fred/",
    processed_prefix="fred/",
    enabled=False,
    rate_limit=120,
    description="Federal Reserve Economic Data (GDP, inflation, employment)",
  ),
  DataSourceType.BLS: DataSourceConfig(
    source_type=DataSourceType.BLS,
    raw_prefix="bls/",
    processed_prefix="bls/",
    enabled=False,
    rate_limit=25,
    description="Bureau of Labor Statistics (CPI, employment, wages)",
  ),
  DataSourceType.CENSUS: DataSourceConfig(
    source_type=DataSourceType.CENSUS,
    raw_prefix="census/",
    processed_prefix="census/",
    enabled=False,
    rate_limit=50,
    description="US Census Bureau data (demographics, business patterns)",
  ),
  DataSourceType.INDUSTRY: DataSourceConfig(
    source_type=DataSourceType.INDUSTRY,
    raw_prefix="industry/",
    processed_prefix="industry/",
    enabled=False,
    rate_limit=100,
    description="Industry benchmarks and SIC/NAICS classifications",
  ),
}


def get_data_source(source: DataSourceType) -> DataSourceConfig:
  """Get configuration for a data source.

  Args:
      source: The data source type

  Returns:
      DataSourceConfig for the source

  Raises:
      KeyError: If the source is not registered
  """
  return DATA_SOURCES[source]


def get_raw_key(source: DataSourceType, *parts: str) -> str:
  """Build an S3 key for raw data.

  Args:
      source: The data source type
      *parts: Path components after the source prefix

  Returns:
      S3 key string (without bucket name)

  Example:
      >>> get_raw_key(DataSourceType.SEC, "year=2024", "320193", "filing.zip")
      'sec/year=2024/320193/filing.zip'
  """
  config = DATA_SOURCES[source]
  if parts:
    return f"{config.raw_prefix}{'/'.join(parts)}"
  return config.raw_prefix.rstrip("/")


def get_processed_key(source: DataSourceType, *parts: str) -> str:
  """Build an S3 key for processed data.

  Args:
      source: The data source type
      *parts: Path components after the source prefix

  Returns:
      S3 key string (without bucket name)

  Example:
      >>> get_processed_key(DataSourceType.SEC, "processed", "filed=2024-01-15", "nodes", "Entity", "file.parquet")
      'sec/processed/filed=2024-01-15/nodes/Entity/file.parquet'
  """
  config = DATA_SOURCES[source]
  if parts:
    return f"{config.processed_prefix}{'/'.join(parts)}"
  return config.processed_prefix.rstrip("/")


def get_cache_key(source: DataSourceType, partition: str, source_file_id: str) -> str:
  """Build an S3 key for a processing cache entry.

  Used for spot instance resilience — caches individual filing results
  so they can be restored after an interruption without reprocessing.

  Args:
      source: The data source type
      partition: Partition identifier (e.g., "2024-Q1")
      source_file_id: SourceFile ID

  Returns:
      S3 key string (without bucket name)

  Example:
      >>> get_cache_key(DataSourceType.SEC, "2024-Q1", "sf_01ABC")
      'sec/cache/2024-Q1/sf_01ABC.zip'
  """
  config = DATA_SOURCES[source]
  return f"{config.processed_prefix}cache/{partition}/{source_file_id}.zip"


def get_raw_uri(bucket: str, source: DataSourceType, *parts: str) -> str:
  """Build a full S3 URI for raw data.

  Args:
      bucket: The S3 bucket name
      source: The data source type
      *parts: Path components after the source prefix

  Returns:
      Full S3 URI string

  Example:
      >>> get_raw_uri("robosystems-shared-raw-staging", DataSourceType.SEC, "year=2024")
      's3://robosystems-shared-raw-staging/sec/year=2024'
  """
  key = get_raw_key(source, *parts)
  return f"s3://{bucket}/{key}"


def get_processed_uri(bucket: str, source: DataSourceType, *parts: str) -> str:
  """Build a full S3 URI for processed data.

  Args:
      bucket: The S3 bucket name
      source: The data source type
      *parts: Path components after the source prefix

  Returns:
      Full S3 URI string

  Example:
      >>> get_processed_uri("robosystems-shared-processed-staging", DataSourceType.SEC, "year=2024", "nodes", "Entity")
      's3://robosystems-shared-processed-staging/sec/year=2024/nodes/Entity'
  """
  key = get_processed_key(source, *parts)
  return f"s3://{bucket}/{key}"


def list_enabled_sources() -> list[DataSourceConfig]:
  """Get all enabled data sources.

  Returns:
      List of enabled DataSourceConfig objects
  """
  return [config for config in DATA_SOURCES.values() if config.enabled]


def is_source_enabled(source: DataSourceType) -> bool:
  """Check if a data source is enabled.

  Args:
      source: The data source type

  Returns:
      True if the source is enabled
  """
  return DATA_SOURCES[source].enabled


# =============================================================================
# Artifact Path Helpers
# =============================================================================


def get_artifact_path(name: str) -> str:
  """Get path to a precomputed artifact Parquet file.

  Args:
      name: Artifact name (e.g., "element_knowledge", "structure_profiles")

  Returns:
      Full path to the artifact Parquet file.

  Example:
      >>> get_artifact_path("element_knowledge")
      './data/artifacts/element_knowledge.parquet'
  """
  from robosystems.config import env

  return f"{env.ARTIFACT_PATH}/{name}.parquet"


def get_artifact_r2_key(name: str) -> str:
  """Get R2 key for a precomputed artifact Parquet file.

  Args:
      name: Artifact name (e.g., "element_knowledge", "structure_profiles")

  Returns:
      R2 key string (without bucket name).

  Example:
      >>> get_artifact_r2_key("element_knowledge")
      'sec/artifacts/element_knowledge.parquet'
  """
  return f"sec/artifacts/{name}.parquet"


# =============================================================================
# Public Data URL Helpers
# =============================================================================


def get_public_data_url(bucket: str, key: str, cdn_url: str | None = None) -> str:
  """Build a browser-reachable URL for an object in the public-data bucket.

  Externalized SEC content (large XBRL text blocks, narrative sections) is
  uploaded to the public-data bucket, and its URL is persisted on the graph
  Fact / OpenSearch document for later retrieval by the frontend. This
  resolves that URL so it works across environments:

    1. CDN (prod/staging): ``{cdn_url}/{key}`` when a CloudFront distribution
       is configured.
    2. LocalStack (dev): path-style against the *browser-reachable* endpoint
       (``AWS_S3_PRESIGN_ENDPOINT_URL`` — e.g. ``http://localhost:4566`` —
       falling back to ``AWS_ENDPOINT_URL``). The pipeline uploads through a
       docker-DNS hostname (``http://localstack:4566``) that isn't reachable
       from the host browser, so the stored URL must use the localhost one.
    3. Real AWS without a CDN: the virtual-hosted-style public S3 URL.

  Args:
      bucket: The public-data bucket name the object was uploaded to.
      key: The object key within the bucket (leading slash optional).
      cdn_url: Optional CDN base URL; when truthy it takes precedence over
          both the LocalStack and raw-S3 forms.

  Returns:
      An absolute URL to the object.

  Example:
      >>> get_public_data_url("robosystems-public-data", "2025/320193/f.html")
      'https://robosystems-public-data.s3.amazonaws.com/2025/320193/f.html'
  """
  from robosystems.config import env

  key = key.lstrip("/")

  if cdn_url:
    return f"{cdn_url.rstrip('/')}/{key}"

  endpoint = env.AWS_S3_PRESIGN_ENDPOINT_URL or env.AWS_ENDPOINT_URL
  if endpoint:
    # LocalStack (and other custom S3 endpoints) use path-style addressing.
    return f"{endpoint.rstrip('/')}/{bucket}/{key}"

  return f"https://{bucket}.s3.amazonaws.com/{key}"


# =============================================================================
# DuckDB Staging Path Helpers
# =============================================================================


def get_staging_duckdb_path(graph_id: str = "sec") -> str:
  """Get persistent DuckDB staging database path on EBS.

  This path is used for DuckDB staging tables that persist between
  job runs, enabling independent retry of LadybugDB materialization
  without re-running the DuckDB staging step.

  Args:
      graph_id: Graph database identifier (default: "sec")

  Returns:
      Full path to the DuckDB database file

  Example:
      >>> get_staging_duckdb_path("sec")
      '/mnt/ladybug-data/staging/sec.duckdb'
  """
  from robosystems.config import env

  base = env.DUCKDB_STAGING_PATH
  return f"{base}/{graph_id}.duckdb"
