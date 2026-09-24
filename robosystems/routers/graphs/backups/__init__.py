"""Backup read routers (list, download, stats). Creation is the
``create-backup`` graph operation; restore is not customer-facing (see ``backup.py``)."""

from fastapi import APIRouter

from .backup import router as backup_router
from .download import router as download_router
from .stats import router as stats_router

router = APIRouter(tags=["Backup"])

# Empty-path routes can't be included with a prefix, so append them directly.
for route in backup_router.routes:
  if not hasattr(route, "tags") or not route.tags:
    route.tags = ["Backup"]
  router.routes.append(route)

router.include_router(download_router)
router.include_router(stats_router)

__all__ = ["router"]
