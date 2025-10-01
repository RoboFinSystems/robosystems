"""
Schema management routers.

This module contains routers for schema operations including runtime inspection,
validation, export, and extension management.
"""

from .info import router as info_router
from .validate import router as validate_router
from .export import router as export_router
from .extensions import router as extensions_router

from fastapi import APIRouter

# Create main schema router
router = APIRouter(tags=["Schema"])

# Include all schema sub-routers
router.include_router(info_router)
router.include_router(validate_router)
router.include_router(export_router)
router.include_router(extensions_router)

__all__ = ["router"]
