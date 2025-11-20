from fastapi import APIRouter

from . import main, query

router = APIRouter(
  tags=["Tables"],
  responses={
    401: {"description": "Not authenticated"},
    403: {"description": "Access denied to graph"},
    404: {"description": "Graph or table not found"},
  },
)

router.include_router(main.router)
router.include_router(query.router)

__all__ = ["router"]
