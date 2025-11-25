"""DuckDB core services.

This module provides DuckDB-specific functionality for staging data
before ingestion into graph databases.
"""

from .manager import (
  DuckDBTableManager,
  TableInfo,
  TableCreateRequest,
  TableCreateResponse,
  TableQueryRequest,
  TableQueryResponse,
  validate_table_name,
)
from .pool import (
  DuckDBConnectionPool,
  DuckDBConnectionInfo,
  get_duckdb_pool,
  initialize_duckdb_pool,
)

__all__ = [
  "DuckDBTableManager",
  "TableInfo",
  "TableCreateRequest",
  "TableCreateResponse",
  "TableQueryRequest",
  "TableQueryResponse",
  "validate_table_name",
  "DuckDBConnectionPool",
  "DuckDBConnectionInfo",
  "get_duckdb_pool",
  "initialize_duckdb_pool",
]
