"""Operator endpoints, mounted at `/v1/graphs/{graph_id}/operator`.

An Operator is a Claude-driven executor that uses MCP tools to do work on a
graph; runs queue on the background worker.
"""

from fastapi import APIRouter

from .execute import router as execute_router

router = APIRouter(
  tags=["Operator"],
)

router.include_router(execute_router)

__all__ = ["router"]
