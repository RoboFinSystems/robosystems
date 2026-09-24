"""Schema routers: runtime inspection, validation, and export.

Extension listing lives at the global `GET /v1/graphs/extensions` rather than
per graph.
"""

from fastapi import APIRouter

from .export import router as export_router
from .info import router as info_router
from .validate import router as validate_router

# Graph-scoped: info + export read a deployed graph's schema.
router = APIRouter(tags=["Schema"])
router.include_router(info_router)
router.include_router(export_router)

# validate_router is not graph-scoped (a candidate schema needs no graph); it
# is mounted at /v1/graphs/schema/validate in robosystems/routers/__init__.py.
__all__ = ["router", "validate_router"]
