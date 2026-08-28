"""Operator endpoints, mounted at `/v1/graphs/{graph_id}/operator`.

An Operator is a Claude-driven executor that uses MCP tools to do work on a
graph. The endpoint picks an operator from the query's intent, gates the
request, and queues the run on the background worker — answering 202 with
the operation's stream/status/cancel links, or 200 with the result under a
bounded `?mode=sync` wait.
"""

from fastapi import APIRouter

from .execute import router as execute_router

router = APIRouter(
  tags=["Operator"],
)

router.include_router(execute_router)

__all__ = ["router"]
