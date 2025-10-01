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
from .tools import router as tools_router

# Create main MCP router
router = APIRouter(
  tags=["MCP"],
)

# Mount sub-routers
router.include_router(tools_router)
router.include_router(execute_router)

# Export main router
__all__ = ["router"]
