"""
Operator execution module with modular sub-components.

The ``/operator/*`` endpoints power RoboSystems' AI agent features.
"Operator" reflects the internal architecture — each Operator is a
Claude-driven executor that uses MCP tools to do work on a graph.

Capabilities:
- Automatic operator selection based on query intent
- Intelligent strategy selection based on execution profiles
- Multiple response formats (JSON, SSE) with transparent handling
- Progress monitoring for long-running operations
- Background queue integration for extended analysis
"""

from fastapi import APIRouter

from .execute import router as execute_router

# Create main Operator router
router = APIRouter(
  tags=["Operator"],
)

# Mount sub-routers
router.include_router(execute_router)

# Export main router
__all__ = ["router"]
