"""
Backup management routers.

This module contains routers for backup operations including creation,
restoration, listing, and statistics.
"""

from .backup import router as backup_router
from .export import router as export_router
from .restore import router as restore_router
from .stats import router as stats_router
from .download import router as download_router

from fastapi import APIRouter

# Create main backup router
router = APIRouter(tags=["Backup"])

# For the backup router with empty paths, we need to extend routes directly
for route in backup_router.routes:
  if not hasattr(route, "tags") or not route.tags:
    route.tags = ["Backup"]
  router.routes.append(route)

# Include other sub-routers normally (they have prefixes)
router.include_router(export_router)
router.include_router(download_router)
router.include_router(restore_router)
router.include_router(stats_router)

__all__ = ["router"]
