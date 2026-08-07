"""LadybugDB core services, in layers.

- ``Engine`` / ``Repository`` — one owned connection to one database file
- ``LadybugConnectionPool`` — shared, thread-safe connections per database
- ``LadybugDatabaseManager`` — database lifecycle and the blue-green swap
- ``LadybugService`` — the query, health and metrics surface the routers use
"""

from .config import get_database_memory_config
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
  "get_database_memory_config",
  "get_ladybug_service",
  "init_ladybug_service",
  "initialize_connection_pool",
  "validate_cypher_query",
]
