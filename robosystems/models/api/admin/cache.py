"""Admin API models for cache management."""

from pydantic import BaseModel, Field


class CacheDatabaseInfo(BaseModel):
  """Information about a single Valkey database."""

  name: str = Field(description="Database name (kebab-case)")
  db_number: int = Field(description="Database number (0-15)")
  key_count: int = Field(description="Number of keys in database")
  purpose: str = Field(description="Human-readable purpose description")


class CacheOverviewResponse(BaseModel):
  """Overview of all Valkey databases."""

  databases: list[CacheDatabaseInfo]
  total_keys: int = Field(description="Total keys across all databases")


class CacheDatabaseDetailResponse(BaseModel):
  """Detailed information about a single Valkey database."""

  name: str
  db_number: int
  key_count: int
  purpose: str
  sample_keys: list[str] = Field(
    default_factory=list, description="Sample of keys in the database"
  )


class CacheFlushResponse(BaseModel):
  """Response after flushing a single database."""

  name: str
  db_number: int
  keys_before: int = Field(description="Number of keys before flush")
  flushed: bool = True


class CacheFlushAllResponse(BaseModel):
  """Response after flushing all databases."""

  databases: list[CacheFlushResponse]
  total_keys_flushed: int


class CacheKeySampleResponse(BaseModel):
  """Response with sampled keys from a database."""

  name: str
  db_number: int
  pattern: str
  keys: list[str]
  count: int = Field(description="Number of keys returned")


class CacheKeyDeleteResponse(BaseModel):
  """Response after deleting keys matching a pattern."""

  name: str
  db_number: int
  pattern: str
  keys_deleted: int
