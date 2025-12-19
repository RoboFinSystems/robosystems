"""DuckDB core services.

This module provides DuckDB-specific functionality for staging data
before ingestion into graph databases.
"""

from .manager import (
  DuckDBTableManager,
  TableCreateRequest,
  TableCreateResponse,
  TableInfo,
  TableQueryRequest,
  TableQueryResponse,
  validate_table_name,
)
from .pool import (
  DuckDBConnectionInfo,
  DuckDBConnectionPool,
  get_duckdb_pool,
  initialize_duckdb_pool,
)

__all__ = [
  "DuckDBConnectionInfo",
  "DuckDBConnectionPool",
  "DuckDBTableManager",
  "TableCreateRequest",
  "TableCreateResponse",
  "TableInfo",
  "TableQueryRequest",
  "TableQueryResponse",
  "get_duckdb_pool",
  "initialize_duckdb_pool",
  "validate_table_name",
]
