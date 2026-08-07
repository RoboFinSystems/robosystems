"""Query endpoints for a graph, mounted under `/v1/graphs/{graph_id}`.

Both surfaces select an execution strategy per request, stream results as SSE
or NDJSON when the client accepts it, and fall back to a priority queue under
load.
"""

from fastapi import APIRouter

from .execute import router as execute_router
from .sql import router as sql_router

# The /v1/graphs/{graph_id} prefix is applied where this router is mounted.
router = APIRouter(
  tags=["Query"],
)

# Cypher (/query/cypher) — monitoring via unified SSE at /v1/operations/{id}/stream
router.include_router(execute_router)
# SQL (/query/sql) — relational lens over the graph's columnar tables
router.include_router(sql_router)

__all__ = ["router"]
