"""Query endpoints (Cypher, SQL), mounted under `/v1/graphs/{graph_id}`."""

from fastapi import APIRouter

from .execute import router as execute_router
from .sql import router as sql_router

router = APIRouter(
  tags=["Query"],
)

router.include_router(execute_router)
router.include_router(sql_router)

__all__ = ["router"]
