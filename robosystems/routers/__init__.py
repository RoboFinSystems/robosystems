"""
API v1 routers.
"""

from fastapi import APIRouter

from robosystems.config import env

from .admin import (
  cache_router as admin_cache_router,
)
from .admin import (
  credits_router as admin_credits_router,
)
from .admin import (
  graphs_router as admin_graphs_router,
)
from .admin import (
  invoice_router as admin_invoice_router,
)
from .admin import (
  orgs_router as admin_orgs_router,
)
from .admin import (
  subscription_router as admin_subscription_router,
)
from .admin import (
  users_router as admin_users_router,
)
from .admin import (
  webhooks_router as admin_webhooks_router,
)
from .auth import router as auth_router
from .billing import (
  checkout_router,
  customer_router,
  invoices_router,
)
from .billing import (
  subscriptions_router as billing_subscriptions_router,
)
from .graphs import (
  backups_router,
  credits_router,
  files_router,
  health_router,
  info_router,
  limits_router,
  members_router,
  query_router,
  schema_router,
  subgraphs_router,
  tables_router,
  usage_router,
)
from .graphs import (
  main_router as graph_router,
)
from .graphs import (
  subscriptions_router as graph_subscriptions_router,
)
from .graphs.content_ops import router as graph_content_ops_router
from .graphs.mcp import agnostic_router as mcp_agnostic_router
from .graphs.mcp import remote_router as mcp_remote_router
from .graphs.operations import router as graph_operations_router
from .graphs.operator import (
  router as operator_router,
)  # AI Operator module with modular structure
from .graphs.schema import validate_router as schema_validate_router
from .offering import offering_router
from .operations import router as operations_router
from .orgs import router as orgs_router
from .status import router as status_router
from .user import router as user_router

# Graph-scoped routes that require an existing graph_id
router = APIRouter(prefix="/v1/graphs/{graph_id}", tags=[])

# Include routers for graph-scoped endpoints
# Conditionally include connections router based on feature flag
if env.CONNECTIONS_ENABLED:
  from .graphs.connections import router as connections_router

  router.include_router(connections_router, prefix="/connections")
router.include_router(
  operator_router
)  # No prefix - handled in the operator module itself
# Streamable-HTTP MCP transport at the bare /mcp path (POST, JSON-RPC 2.0);
# schema-excluded, so it never appears in the generated SDK clients. It is
# the graph's only MCP surface — the REST tool endpoints were removed.
router.include_router(mcp_remote_router, prefix="/mcp")
router.include_router(backups_router, prefix="/backups")
router.include_router(
  usage_router
)  # No prefix - handles /metrics and /usage internally
router.include_router(query_router)  # No prefix - handled in the query module itself
router.include_router(schema_router)  # No prefix - handled in the schema module itself
router.include_router(credits_router)  # Already has /credits prefix
router.include_router(health_router)  # No prefix - handles /health internally
router.include_router(info_router)  # No prefix - handles /info internally
router.include_router(limits_router)  # No prefix - handles /limits internally
router.include_router(members_router)  # Already has /members prefix
router.include_router(subgraphs_router, prefix="/subgraphs")
router.include_router(
  graph_subscriptions_router, prefix="/subscriptions"
)  # Unified subscription management
router.include_router(
  tables_router
)  # No prefix - handles all /tables and /files paths internally

# The fact-grid views router mounts at /extensions/roboledger/{graph_id}/views
# in main.py, not here: the grid is roboledger-schema-specific (XBRL
# hypercube), not part of the schema-agnostic platform graph surface.

# Conditionally include search / documents / memory routers based on flags.
# search_router hosts document search AND memory recall → mount under either flag.
if env.SEMANTIC_SEARCH_ENABLED or env.SEMANTIC_MEMORY_ENABLED:
  from .graphs import search_router

  router.include_router(search_router)  # No prefix - handles /search internally

if env.SEMANTIC_SEARCH_ENABLED:
  from .graphs import documents_router

  router.include_router(documents_router)  # No prefix - handles /documents internally

if env.SEMANTIC_MEMORY_ENABLED:
  from .graphs import memory_router

  router.include_router(memory_router)  # No prefix - handles /memory internally

router.include_router(graph_operations_router, prefix="/operations")
router.include_router(graph_content_ops_router, prefix="/operations")
router.include_router(files_router)  # No prefix - handles /files endpoint

# Non-graph-scoped routes that don't require a graph_id

# Schema VALIDATION is graph-independent (validate a candidate schema BEFORE a
# graph exists) so it gets its own dedicated, Schema-tagged /v1/graphs router
# (single "Schema" tag) — NOT the graph-scoped router (info/export read a
# deployed graph) and NOT the Graphs CRUD router (which would double-tag it).
# Mounted in main.py → POST /v1/graphs/schema/validate.
graph_schema_router_v1 = APIRouter(prefix="/v1/graphs", tags=["Schema"])
graph_schema_router_v1.include_router(schema_validate_router)

user_router_v1 = APIRouter(prefix="/v1", tags=[])
user_router_v1.include_router(user_router, prefix="")

# Organization routes
orgs_router_v1 = APIRouter(prefix="/v1", tags=[])
orgs_router_v1.include_router(orgs_router)

# Include offering router (non-graph-scoped)
offering_router_v1 = APIRouter(prefix="/v1")
offering_router_v1.include_router(offering_router)  # Already has /offering prefix

# Operations router for unified SSE operations
operations_router_v1 = APIRouter(prefix="/v1", tags=["Operations"])
operations_router_v1.include_router(operations_router)

# Graph-agnostic MCP transport: POST /v1/mcp, OAuth-only — the consent
# grant names the graph. Schema-excluded like the per-graph transport.
# (Same empty-path rule as the per-graph transport: the bare path must be
# supplied by the include prefix, not the router's own.)
mcp_agnostic_router_v1 = APIRouter(prefix="/v1")
mcp_agnostic_router_v1.include_router(mcp_agnostic_router, prefix="/mcp")

# Auth routes that don't require a graph_id
auth_router_v1 = APIRouter(prefix="/v1/auth", tags=["Auth"])
auth_router_v1.include_router(auth_router)

# Status routes that don't require a graph_id
status_router_v1 = APIRouter(prefix="/v1", tags=["Status"])
status_router_v1.include_router(status_router)

# Billing routes that don't require a graph_id
billing_router_v1 = APIRouter(prefix="/v1")
billing_router_v1.include_router(customer_router)
billing_router_v1.include_router(billing_subscriptions_router)
billing_router_v1.include_router(invoices_router)
billing_router_v1.include_router(checkout_router)

# Admin routes that don't require a graph_id
admin_router_v1 = APIRouter(prefix="")
admin_router_v1.include_router(admin_cache_router)
admin_router_v1.include_router(admin_subscription_router)
admin_router_v1.include_router(admin_invoice_router)
admin_router_v1.include_router(admin_webhooks_router)
admin_router_v1.include_router(admin_credits_router)
admin_router_v1.include_router(admin_graphs_router)
admin_router_v1.include_router(admin_users_router)
admin_router_v1.include_router(admin_orgs_router)

# Extensions reads live at /extensions/{graph_id}/graphql; writes at
# POST /extensions/{roboledger,roboinvestor}/{graph_id}/operations/{op_name}.
# Both mount directly in main.py, with no router_v1 wrapper.

# Export routers for main application
__all__ = [
  "admin_router_v1",
  "auth_router_v1",
  "billing_router_v1",
  "graph_router",
  "graph_schema_router_v1",
  "mcp_agnostic_router_v1",
  "offering_router_v1",
  "operations_router_v1",
  "orgs_router_v1",
  "router",
  "status_router_v1",
  "user_router_v1",
]
