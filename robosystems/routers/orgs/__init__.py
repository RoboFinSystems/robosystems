"""Organization management routers."""

from fastapi import APIRouter

from .main import router as main_router
from .members import router as members_router
from .usage import router as usage_router

# Create the main org router that combines all sub-routers
router = APIRouter()

# Include sub-routers with proper prefixes
router.include_router(main_router, prefix="")
router.include_router(members_router, prefix="")
router.include_router(usage_router, prefix="")

__all__ = ["router"]
