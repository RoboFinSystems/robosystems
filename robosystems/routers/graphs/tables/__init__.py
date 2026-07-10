from fastapi import APIRouter

from . import main

router = APIRouter(
  tags=["Tables"],
  responses={
    401: {"description": "Not authenticated"},
    403: {"description": "Access denied to graph"},
    404: {"description": "Graph or table not found"},
  },
)

router.include_router(main.router)

__all__ = ["router"]
