"""LadybugDB core services.

This module provides LadybugDB-specific functionality including:
- Engine: Low-level database connection and query execution
- ConnectionPool: Connection pooling for LadybugDB
- DatabaseManager: Multi-database lifecycle management
- LadybugService: High-level service orchestration
"""

from .engine import Engine, Repository, ConnectionError, QueryError
from .pool import (
  LadybugConnectionPool,
  get_connection_pool,
  initialize_connection_pool,
)
from .manager import LadybugDatabaseManager
from .service import (
  LadybugService,
  get_ladybug_service,
  init_ladybug_service,
  validate_cypher_query,
)

__all__ = [
  # Engine
  "Engine",
  "Repository",
  "ConnectionError",
  "QueryError",
  # Connection Pool
  "LadybugConnectionPool",
  "get_connection_pool",
  "initialize_connection_pool",
  # Database Manager
  "LadybugDatabaseManager",
  # Service
  "LadybugService",
  "get_ladybug_service",
  "init_ladybug_service",
  "validate_cypher_query",
]
