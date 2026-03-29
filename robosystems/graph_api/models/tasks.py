"""
Background task-related Pydantic models for the Graph API.

These models are used for tracking long-running background operations
like ingestion, backup, restore, and export.
"""

from enum import Enum


class TaskStatus(str, Enum):
  """Task execution status."""

  PENDING = "pending"
  RUNNING = "running"
  COMPLETED = "completed"
  FAILED = "failed"


class TaskType(Enum):
  """Types of background tasks that support SSE monitoring."""

  INGESTION = "ingestion"
  BACKUP = "backup"
  RESTORE = "restore"
  EXPORT = "export"
  MIGRATION = "migration"
  STAGING = "staging"  # DuckDB table creation from S3
