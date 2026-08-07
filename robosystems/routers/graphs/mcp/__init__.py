"""MCP (Model Context Protocol) endpoints for a graph.

Mounted under `/v1/graphs/{graph_id}/mcp`:

- `tools.py` lists the tools available on the graph.
- `execute.py` runs a tool, picking an execution strategy from the tool
  type and system load and negotiating the response format (JSON, SSE, or
  NDJSON) with the client.
- `remote.py` serves the Streamable-HTTP JSON-RPC transport for MCP clients
  that connect by URL.
"""

from fastapi import APIRouter

from .execute import router as execute_router
from .remote import router as remote_router
from .tools import router as tools_router

# Create main MCP router
router = APIRouter(
  tags=["MCP"],
)

# Mount sub-routers
router.include_router(tools_router)
router.include_router(execute_router)

# The Streamable-HTTP JSON-RPC transport lives at POST /v1/graphs/{graph_id}/mcp
# (the bare /mcp path). FastAPI rejects an empty path on a prefix-less include,
# so it is exported separately and mounted with the "/mcp" prefix alongside this
# router in robosystems/routers/__init__.py. Schema-excluded so the JSON-RPC
# envelope never reaches the generated SDKs.
__all__ = ["remote_router", "router"]
