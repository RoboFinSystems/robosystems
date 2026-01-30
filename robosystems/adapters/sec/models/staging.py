"""Staging result models for SEC DuckDB-based ingestion.

These models support decoupled staging and materialization:
- StagingResult: Returned by stage_to_duckdb() with staging statistics
- MaterializeResult: Returned by materialize_from_duckdb() with ingestion statistics
- TableInfo: Information about a staged table

The pipeline uses a schema-driven approach (RoboLedgerContext) for table discovery,
eliminating the need for a manifest file. This enables cross-service operation
where Dagster and Graph API run on separate infrastructure.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class TableInfo:
  """Information about a staged table."""

  name: str
  row_count: int
  file_count: int
  staged_at: str  # ISO timestamp
  skipped: bool = False  # True if table was skipped (e.g., no files found)

  def to_dict(self) -> dict[str, Any]:
    """Convert to dictionary for JSON serialization."""
    return {
      "name": self.name,
      "row_count": self.row_count,
      "file_count": self.file_count,
      "staged_at": self.staged_at,
      "skipped": self.skipped,
    }

  @classmethod
  def from_dict(cls, data: dict[str, Any]) -> "TableInfo":
    """Create from dictionary."""
    return cls(
      name=data["name"],
      row_count=data["row_count"],
      file_count=data["file_count"],
      staged_at=data["staged_at"],
      skipped=data.get("skipped", False),
    )


@dataclass
class StagingResult:
  """Result from stage_to_duckdb() operation.

  Contains statistics about the staging operation and the list of
  tables that were successfully staged.
  """

  status: str  # "success", "partial", "error", "no_data", "already_staged"
  table_names: list[str]  # Successfully staged tables
  tables: dict[str, TableInfo] = field(default_factory=dict)
  total_files: int = 0
  total_rows: int = 0
  duration_seconds: float = 0.0
  duckdb_path: str | None = None
  error: str | None = None

  def to_dict(self) -> dict[str, Any]:
    """Convert to dictionary for metadata output."""
    return {
      "status": self.status,
      "table_names": self.table_names,
      "tables": {name: info.to_dict() for name, info in self.tables.items()},
      "total_files": self.total_files,
      "total_rows": self.total_rows,
      "duration_seconds": self.duration_seconds,
      "duckdb_path": self.duckdb_path,
      "error": self.error,
    }


@dataclass
class MaterializeResult:
  """Result from materialize_from_duckdb() operation.

  Contains statistics about the materialization (ingestion) operation.
  """

  status: str  # "success", "error", "no_data"
  total_rows_ingested: int = 0
  total_time_ms: float = 0.0
  tables: list[dict[str, Any]] = field(default_factory=list)
  error: str | None = None

  def to_dict(self) -> dict[str, Any]:
    """Convert to dictionary for metadata output."""
    return {
      "status": self.status,
      "total_rows_ingested": self.total_rows_ingested,
      "total_time_ms": self.total_time_ms,
      "tables": self.tables,
      "error": self.error,
    }
