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
from .sql import router as sql_router

# Create main query router without prefix
# The prefix will be added at the main app level as /v1/graphs/{graph_id}
router = APIRouter(
  tags=["Query"],
)

# Cypher (/query/cypher) — monitoring via unified SSE at /v1/operations/{id}/stream
router.include_router(execute_router)
# SQL (/query/sql) — relational lens over the graph's columnar tables
router.include_router(sql_router)

# Export main router
__all__ = ["router"]
