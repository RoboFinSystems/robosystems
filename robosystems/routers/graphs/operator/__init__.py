"""Operator endpoints, mounted at `/v1/graphs/{graph_id}/operator`.

An Operator is a Claude-driven executor that uses MCP tools to do work on a
graph. The endpoint picks an operator from the query's intent, selects an
execution strategy from that operator's profile, and answers as JSON or SSE
with progress, queueing longer analyses in the background.
"""

from fastapi import APIRouter

from .execute import router as execute_router

router = APIRouter(
  tags=["Operator"],
)

router.include_router(execute_router)

__all__ = ["router"]
