"""
Graph management routers.

This module contains routers for graph database management operations
including creation, listing, selection, backup, usage analytics, and other
graph-level administrative functions.
"""

from robosystems.config import env

from .backups import router as backups_router
from .connections import router as connections_router
from .credits import router as credits_router
from .files import router as files_router
from .health import router as health_router
from .info import router as info_router
from .limits import router as limits_router
from .main import router as main_router
from .materialize import router as materialize_router
from .query import router as query_router
from .schema import router as schema_router
from .subgraphs import router as subgraphs_router
from .subscriptions import router as subscriptions_router
from .tables import router as tables_router
from .usage import router as usage_router

__all__ = [
  "backups_router",
  "connections_router",
  "credits_router",
  "files_router",
  "health_router",
  "info_router",
  "limits_router",
  "main_router",
  "materialize_router",
  "query_router",
  "schema_router",
  "subgraphs_router",
  "subscriptions_router",
  "tables_router",
  "usage_router",
]

# NOTE: views_router was relocated to
# routers/extensions/roboledger/views.py — the fact grid is roboledger
# schema-specific and now lives on the extensions surface. See main.py
# for the new mount.

# Conditionally export search and documents routers based on feature flag
if env.SEMANTIC_SEARCH_ENABLED:
  from .documents import router as documents_router  # noqa: F401
  from .search import router as search_router  # noqa: F401

  __all__.append("search_router")
  __all__.append("documents_router")
