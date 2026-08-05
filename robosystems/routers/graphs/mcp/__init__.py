"""
MCP (Model Context Protocol) endpoint module with modular sub-endpoints.

This module provides a comprehensive MCP tool execution system optimized for AI agents with:
- Intelligent strategy selection based on tool type and system load
- Multiple response formats (JSON, SSE, NDJSON) with transparent handling
- Shared queue infrastructure with query endpoints
- Automatic format negotiation for AI agent clients
- Progress monitoring for long-running operations
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
