"""Schema routers: runtime inspection, validation, and export.

Extension listing lives at the global `GET /v1/graphs/extensions` rather than
per graph.
"""

from fastapi import APIRouter

from .export import router as export_router
from .info import router as info_router
from .validate import router as validate_router

# Graph-scoped schema router: info + export operate on a DEPLOYED graph's
# schema, so they mount under /v1/graphs/{graph_id}.
router = APIRouter(tags=["Schema"])
router.include_router(info_router)
router.include_router(export_router)

# validate_router is NOT graph-scoped — validating a candidate schema needs no
# graph to exist (chicken-and-egg otherwise). It is re-exported here and mounted
# at /v1/graphs/schema/validate in robosystems/routers/__init__.py.
__all__ = ["router", "validate_router"]
