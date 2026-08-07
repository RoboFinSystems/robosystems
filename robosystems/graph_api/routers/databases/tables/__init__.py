"""DuckDB staging table routes, mounted under /databases/{graph_id}/tables.

Staging is writer-only: replicas hold no staging file, so every route here
returns 501 on a replica.
"""

import os

from fastapi import APIRouter, Depends, HTTPException
from fastapi import status as http_status

from . import management, materialize, query


def _require_staging_enabled():
  """Block all staging table endpoints on replicas."""
  if os.getenv("LBUG_ROLE") == "replica":
    raise HTTPException(
      status_code=http_status.HTTP_501_NOT_IMPLEMENTED,
      detail="Staging tables are not available on read-only replicas",
    )


router = APIRouter(
  tags=["Tables"],
  dependencies=[Depends(_require_staging_enabled)],
  responses={
    400: {"description": "Invalid request"},
    404: {"description": "Graph or table not found"},
    500: {"description": "Internal server error"},
    501: {"description": "Not available on replicas"},
  },
)

router.include_router(management.router, tags=["Tables"])
router.include_router(materialize.router, tags=["Tables"])
router.include_router(query.router, tags=["Tables"])

__all__ = ["router"]
