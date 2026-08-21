"""Graph routers, mounted under `/v1/graphs`.

Covers graph creation and listing, per-graph backups, connections, credits,
files, health, limits, members, query, schema, subgraphs, subscriptions,
tables, and usage.
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
from .members import router as members_router
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
  "members_router",
  "query_router",
  "schema_router",
  "subgraphs_router",
  "subscriptions_router",
  "tables_router",
  "usage_router",
]

# The fact grid is roboledger schema-specific, so it lives on the extensions
# surface: routers/extensions/roboledger/views.py, mounted in main.py.

# Conditionally export search, documents, and memory routers based on flags.
# The search router hosts both document search and memory `recall`, so it mounts
# when EITHER feature is on; documents CRUD is search-only; memory governance is
# memory-only.
if env.SEMANTIC_SEARCH_ENABLED:
  from .documents import router as documents_router  # noqa: F401

  __all__.append("documents_router")

if env.SEMANTIC_SEARCH_ENABLED or env.SEMANTIC_MEMORY_ENABLED:
  from .search import router as search_router  # noqa: F401

  __all__.append("search_router")

if env.SEMANTIC_MEMORY_ENABLED:
  from .memory import router as memory_router  # noqa: F401

  __all__.append("memory_router")
