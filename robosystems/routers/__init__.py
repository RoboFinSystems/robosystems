"""API v1 router assembly."""

from fastapi import APIRouter

from robosystems.config import env

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
from .graphs.mcp import roboledger_router as mcp_roboledger_router
from .graphs.operations import router as graph_operations_router
from .graphs.operator import (
  router as operator_router,
)
from .graphs.schema import validate_router as schema_validate_router
from .offering import offering_router
from .operations import router as operations_router
from .orgs import router as orgs_router
from .status import router as status_router
from .user import router as user_router

router = APIRouter(prefix="/v1/graphs/{graph_id}", tags=[])

if env.CONNECTIONS_ENABLED:
  from .graphs.connections import router as connections_router

  router.include_router(connections_router, prefix="/connections")
router.include_router(operator_router)
# Streamable-HTTP MCP transport (JSON-RPC 2.0), schema-excluded so it stays
# out of the generated SDK clients.
router.include_router(mcp_remote_router, prefix="/mcp")
router.include_router(backups_router, prefix="/backups")
router.include_router(usage_router)
router.include_router(query_router)
router.include_router(schema_router)
router.include_router(credits_router)
router.include_router(health_router)
router.include_router(info_router)
router.include_router(limits_router)
router.include_router(members_router)
router.include_router(subgraphs_router, prefix="/subgraphs")
router.include_router(graph_subscriptions_router, prefix="/subscriptions")
router.include_router(tables_router)

# The fact-grid views router mounts in main.py under /extensions/roboledger:
# it is roboledger-schema-specific, not part of the platform graph surface.

# search_router hosts both document search and memory recall.
if env.SEMANTIC_SEARCH_ENABLED or env.SEMANTIC_MEMORY_ENABLED:
  from .graphs import search_router

  router.include_router(search_router)

if env.SEMANTIC_SEARCH_ENABLED:
  from .graphs import documents_router

  router.include_router(documents_router)

if env.SEMANTIC_MEMORY_ENABLED:
  from .graphs import memory_router

  router.include_router(memory_router)

router.include_router(graph_operations_router, prefix="/operations")
router.include_router(graph_content_ops_router, prefix="/operations")
router.include_router(files_router)


# Schema validation needs no graph, so it gets its own Schema-tagged router
# (mounted in main.py as POST /v1/graphs/schema/validate) rather than the
# graph-scoped or Graphs CRUD router, which would double-tag it.
graph_schema_router_v1 = APIRouter(prefix="/v1/graphs", tags=["Schema"])
graph_schema_router_v1.include_router(schema_validate_router)

user_router_v1 = APIRouter(prefix="/v1", tags=[])
user_router_v1.include_router(user_router, prefix="")

orgs_router_v1 = APIRouter(prefix="/v1", tags=[])
orgs_router_v1.include_router(orgs_router)

offering_router_v1 = APIRouter(prefix="/v1")
offering_router_v1.include_router(offering_router)

operations_router_v1 = APIRouter(prefix="/v1", tags=["Operations"])
operations_router_v1.include_router(operations_router)

# Graph-agnostic MCP transport, OAuth-only: the consent grant names the graph.
# The bare path must come from the include prefix, not the router's own.
mcp_agnostic_router_v1 = APIRouter(prefix="/v1")
mcp_agnostic_router_v1.include_router(mcp_agnostic_router, prefix="/mcp")
# RoboLedger graphs only, with a product tool profile (a directory listing
# freezes one tool list per URL).
mcp_agnostic_router_v1.include_router(mcp_roboledger_router, prefix="/mcp/roboledger")

# No `tags` here: the auth sub-routers tag themselves, and a tag set here
# would win the grouping (consumers read the first tag: the API reference's
# page layout and the Python SDK's module directories).
auth_router_v1 = APIRouter(prefix="/v1/auth")
auth_router_v1.include_router(auth_router)

status_router_v1 = APIRouter(prefix="/v1", tags=["Status"])
status_router_v1.include_router(status_router)

billing_router_v1 = APIRouter(prefix="/v1")
billing_router_v1.include_router(customer_router)
billing_router_v1.include_router(billing_subscriptions_router)
billing_router_v1.include_router(invoices_router)
billing_router_v1.include_router(checkout_router)

# Extensions routers mount directly in main.py.

__all__ = [
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
