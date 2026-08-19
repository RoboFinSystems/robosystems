"""DuckDB staging layer: transform parquet on S3 into tables that LadybugDB
ingestion can COPY from."""

from .manager import (
  DuckDBTableManager,
  TableCreateRequest,
  TableCreateResponse,
  TableInfo,
  TableQueryRequest,
  TableQueryResponse,
  quote_identifier,
  validate_column_names,
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
  "quote_identifier",
  "validate_column_names",
  "validate_table_name",
]
