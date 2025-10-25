"""
Schema management routers.

This module contains routers for schema operations including runtime inspection,
validation, and export.

Note: Extension listing is available at the global level via GET /v1/graphs/extensions
and is not duplicated at the per-graph level.
"""

from .info import router as info_router
from .validate import router as validate_router
from .export import router as export_router

from fastapi import APIRouter

# Create main schema router
router = APIRouter(tags=["Schema"])

# Include all schema sub-routers
router.include_router(info_router)
router.include_router(validate_router)
router.include_router(export_router)

__all__ = ["router"]
