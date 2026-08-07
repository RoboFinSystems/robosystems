"""Graph limits API models."""

from pydantic import BaseModel, ConfigDict, Field


class StorageLimits(BaseModel):
  """Storage limits information."""

  current_usage_gb: float | None = Field(
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
  supported_formats: list[str] = Field(..., description="Supported file formats")


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


class DocumentLimits(BaseModel):
  """Knowledge-base document usage against the tier's cap."""

  current_count: int = Field(
    ..., description="Uploaded documents currently stored for this graph"
  )
  max_documents: int | None = Field(
    None,
    description="Maximum uploaded documents for this tier (null when uncapped)",
  )
  approaching_limit: bool = Field(
    ..., description="Whether approaching document limit (>80%)"
  )


class SubgraphLimits(BaseModel):
  """Subgraph count against the parent graph tier's cap.

  Subgraphs are refused at the tier cap regardless of how small they are,
  so this is a count axis independent of the storage one — ``instance``
  already itemizes their footprint.
  """

  current_count: int = Field(
    ..., description="Subgraphs currently provisioned under this graph", ge=0
  )
  max_allowed: int | None = Field(
    None,
    description="Maximum subgraphs for this tier (null when uncapped)",
  )
  remaining: int | None = Field(
    None,
    description="Subgraphs that can still be created (null when uncapped)",
  )
  approaching_limit: bool = Field(
    ..., description="Whether approaching subgraph limit (>80%)"
  )


class ContentLimits(BaseModel):
  """Per-operation materialization limits."""

  max_rows_per_copy: int = Field(
    ..., description="Maximum rows per copy/materialization operation"
  )
  max_single_table_rows: int = Field(..., description="Maximum rows per staging table")
  chunk_size_rows: int = Field(..., description="Rows per materialization chunk")


class DatabaseStorageEntry(BaseModel):
  """Storage for a single database on the instance."""

  graph_id: str = Field(..., description="Database identifier")
  is_parent: bool = Field(False, description="Whether this is the parent graph")
  size_mb: float | None = Field(None, description="Database size in MB")


class StorageItem(BaseModel):
  """One itemized piece of a graph's on-disk footprint."""

  type: str = Field(
    ...,
    description=(
      "One of: graph, memory, subgraph, vectors, staging, transient "
      "(blue-green build artifact), orphan (a `{parent}_*` database, vector "
      "index, or staging file with no row in the graph registry — leftover "
      "of a deleted subgraph). Transient and orphan items are collected by "
      "the platform's daily storage-reclaim job."
    ),
  )
  id: str = Field(..., description="Database or index identifier")
  bytes: int = Field(..., description="Size in bytes")


class InstanceUsage(BaseModel):
  """Aggregate storage usage across the dedicated instance.

  Covers the parent graph, all subgraphs, DuckDB staging, and
  future LanceDB vector indexes.
  """

  node_count: int | None = Field(
    None, description="Current node count (informational, no limit enforced)"
  )
  total_storage_gb: float | None = Field(
    None, description="Total storage used across all databases in GB"
  )
  limit_gb: float = Field(..., description="Soft storage limit for this tier in GB")
  usage_percentage: float | None = Field(
    None,
    description=(
      "Storage usage as percentage of limit (e.g. 105.2). Derived from the "
      "enforced figure — durable bytes only, excluding `transient` build "
      "artifacts — so it can read lower than total_storage_gb/limit_gb "
      "while a blue-green rebuild is in flight."
    ),
  )
  status: str = Field(
    ...,
    description="Instance status: 'healthy' (<80%), 'approaching' (80-100%), 'over_limit' (>100%)",
  )
  databases: list[DatabaseStorageEntry] = Field(
    default_factory=list,
    description="Per-database storage breakdown",
  )
  items: list[StorageItem] = Field(
    default_factory=list,
    description=(
      "Itemized storage by type — graph, memory, subgraph, vectors, staging, "
      "transient, orphan. Sums to total_storage_gb. Only `subgraph` items "
      "correspond to live subgraphs, so this is the type to sum when "
      "reconciling against the subgraph list."
    ),
  )


class GraphLimitsResponse(BaseModel):
  """Response model for comprehensive graph operational limits."""

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "graph_id": "kg1a2b3c4d5",
          "subscription_tier": "ladybug-standard",
          "graph_tier": "ladybug-standard",
          "is_shared_repository": False,
          "storage": {
            "current_usage_gb": 2.45,
            "max_storage_gb": 20,
            "approaching_limit": False,
          },
          "queries": {
            "max_timeout_seconds": 45,
            "chunk_size": 500,
            "max_rows_per_query": 10000,
            "concurrent_queries": 1,
          },
          "copy_operations": {
            "max_file_size_gb": 1.0,
            "timeout_seconds": 900,
            "concurrent_operations": 1,
            "max_files_per_operation": 100,
            "daily_copy_operations": 25,
            "supported_formats": ["parquet", "csv", "json", "delta", "iceberg"],
          },
          "backups": {
            "max_backup_size_gb": 10,
            "backup_retention_days": 7,
            "max_backups_per_day": 2,
          },
          "rate_limits": {
            "requests_per_minute": 60,
            "requests_per_hour": 3600,
            "burst_capacity": 60,
          },
          "credits": {
            "monthly_ai_credits": 8000,
            "current_balance": 7500,
          },
          "documents": {
            "current_count": 12,
            "max_documents": 100,
            "approaching_limit": False,
          },
          "subgraphs": {
            "current_count": 1,
            "max_allowed": 3,
            "remaining": 2,
            "approaching_limit": False,
          },
          "content": {
            "max_rows_per_copy": 1000000,
            "max_single_table_rows": 2500000,
            "chunk_size_rows": 250000,
          },
          "instance": {
            "node_count": 150000,
            "total_storage_gb": 2.45,
            "limit_gb": 20,
            "usage_percentage": 12.3,
            "status": "healthy",
            "databases": [
              {"graph_id": "kg1a2b3c4d5", "is_parent": True, "size_mb": 2150.0},
              {"graph_id": "kg1a2b3c4d5_dev", "is_parent": False, "size_mb": 360.0},
            ],
          },
        },
        {
          "graph_id": "sec",
          "subscription_tier": "ladybug-standard",
          "graph_tier": "ladybug-shared",
          "is_shared_repository": True,
          "storage": {
            "current_usage_gb": None,
            "max_storage_gb": 20.0,
            "approaching_limit": False,
          },
          "queries": {
            "max_timeout_seconds": 300,
            "chunk_size": 2500,
            "max_rows_per_query": 10000,
            "concurrent_queries": 1,
          },
          "copy_operations": {
            "max_file_size_gb": 10.0,
            "timeout_seconds": 3600,
            "concurrent_operations": 3,
            "max_files_per_operation": 10000,
            "daily_copy_operations": -1,
            "supported_formats": ["parquet", "csv", "json", "delta", "iceberg"],
          },
          "backups": {
            "max_backup_size_gb": 100,
            "backup_retention_days": 90,
            "max_backups_per_day": 5,
          },
          "rate_limits": {
            "requests_per_minute": 60,
            "requests_per_hour": 3600,
            "burst_capacity": 60,
          },
        },
      ]
    }
  )

  graph_id: str = Field(..., description="Graph database identifier")
  subscription_tier: str = Field(
    ..., description="Rate-limit tier enforced for requests to this graph"
  )
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
  credits: CreditLimits | None = Field(
    None, description="AI credit limits (if applicable)"
  )
  documents: DocumentLimits | None = Field(
    None, description="Knowledge-base document usage and tier cap (user graphs only)"
  )
  subgraphs: SubgraphLimits | None = Field(
    None,
    description="Subgraph count and tier cap (parent user graphs only)",
  )
  content: ContentLimits | None = Field(
    None, description="Per-operation materialization limits (if applicable)"
  )
  instance: InstanceUsage | None = Field(
    None, description="Aggregate instance storage usage (user graphs only)"
  )
