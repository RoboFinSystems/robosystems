"""
API v1 routers.
"""

from fastapi import APIRouter

from .auth import router as auth_router

# Removed entity router - using query endpoint for all Entity operations
from .user import router as user_router
from .graphs.agent import router as agent_router
from .graphs.connections import router as connections_router
from .status import router as status_router
from .graphs import (
  main_router as graph_router,
  backups_router,
  analytics_router,
  query_router,
  schema_router,
  credits_router,
  health_router,
  info_router,
  limits_router,
  subgraphs_router,
  subscriptions_router,
  tables_router,
)  # Removed allocation_router - too dangerous for public API
from .graphs.mcp import router as mcp_router
from .offering import offering_router
from .operations import router as operations_router

# Graph-scoped routes that require an existing graph_id
router = APIRouter(prefix="/v1/graphs/{graph_id}", tags=[])

# Include routers for graph-scoped endpoints
router.include_router(connections_router, prefix="/connections")
router.include_router(agent_router, prefix="/agent")
router.include_router(mcp_router, prefix="/mcp")
router.include_router(backups_router, prefix="/backups")
router.include_router(analytics_router, prefix="/analytics")
router.include_router(query_router)  # No prefix - handled in the query module itself
router.include_router(schema_router)  # No prefix - handled in the schema module itself
router.include_router(credits_router)  # Already has /credits prefix
router.include_router(health_router)  # No prefix - handles /health internally
router.include_router(info_router)  # No prefix - handles /info internally
router.include_router(limits_router)  # No prefix - handles /limits internally
router.include_router(subgraphs_router, prefix="/subgraphs")
router.include_router(
  subscriptions_router, prefix="/subscriptions"
)  # Unified subscription management
router.include_router(
  tables_router
)  # No prefix - handles all /tables and /files paths internally

# Non-graph-scoped routes that don't require a graph_id
user_router_v1 = APIRouter(prefix="/v1", tags=[])
user_router_v1.include_router(user_router, prefix="")

# Include offering router (non-graph-scoped)
offering_router_v1 = APIRouter(prefix="/v1")
offering_router_v1.include_router(offering_router)  # Already has /offering prefix

# Operations router for unified SSE operations
operations_router_v1 = APIRouter(prefix="/v1", tags=["Operations"])
operations_router_v1.include_router(operations_router)

# Auth routes that don't require a graph_id
auth_router_v1 = APIRouter(prefix="/v1/auth", tags=["Auth"])
auth_router_v1.include_router(auth_router)

# Status routes that don't require a graph_id
status_router_v1 = APIRouter(prefix="/v1", tags=["Status"])
status_router_v1.include_router(status_router)

# Export routers for main application
__all__ = [
  "router",
  "user_router_v1",
  "auth_router_v1",
  "status_router_v1",
  "graph_router",
  "offering_router_v1",
  "operations_router_v1",
]
