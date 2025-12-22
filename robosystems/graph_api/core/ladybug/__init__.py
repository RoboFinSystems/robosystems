"""LadybugDB core services.

This module provides LadybugDB-specific functionality including:
- Engine: Low-level database connection and query execution
- ConnectionPool: Connection pooling for LadybugDB
- DatabaseManager: Multi-database lifecycle management
- LadybugService: High-level service orchestration
"""

from .engine import ConnectionError, Engine, QueryError, Repository
from .manager import LadybugDatabaseManager
from .pool import (
  LadybugConnectionPool,
  get_connection_pool,
  initialize_connection_pool,
)
from .service import (
  LadybugService,
  get_ladybug_service,
  init_ladybug_service,
  validate_cypher_query,
)

__all__ = [
  "ConnectionError",
  # Engine
  "Engine",
  # Connection Pool
  "LadybugConnectionPool",
  # Database Manager
  "LadybugDatabaseManager",
  # Service
  "LadybugService",
  "QueryError",
  "Repository",
  "get_connection_pool",
  "get_ladybug_service",
  "init_ladybug_service",
  "initialize_connection_pool",
  "validate_cypher_query",
]
