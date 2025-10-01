"""
Query endpoint module with modular sub-endpoints.

This module provides a comprehensive query execution system with:
- Intelligent strategy selection
- Multiple streaming formats (SSE, NDJSON)
- Queue management with priority
- Testing tool detection and optimization
- Long polling support
"""

from fastapi import APIRouter

from .execute import router as execute_router

# Create main query router without prefix
# The prefix will be added at the main app level as /v1/graphs/{graph_id}/query
router = APIRouter(
  tags=["Query"],
)

# Mount only execute router - monitoring via unified SSE at /v1/operations/{operation_id}/stream
router.include_router(execute_router)

# Export main router
__all__ = ["router"]
