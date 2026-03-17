import os

from fastapi import APIRouter, Depends, HTTPException
from fastapi import status as http_status

from . import management, materialize, query


def _require_staging_writes():
  """Block staging table write operations on replicas.

  DuckDB IS initialized on replicas (pool created in app.py, files downloaded
  from S3 in background), so read-only queries are safe. Only write operations
  (create, delete, materialize) need to be blocked.
  """
  if os.getenv("LBUG_ROLE") == "replica":
    raise HTTPException(
      status_code=http_status.HTTP_501_NOT_IMPLEMENTED,
      detail="Staging table writes are not available on read-only replicas",
    )


router = APIRouter(
  tags=["Tables"],
  responses={
    400: {"description": "Invalid request"},
    404: {"description": "Graph or table not found"},
    500: {"description": "Internal server error"},
    501: {"description": "Not available on replicas"},
  },
)

# Write operations — blocked on replicas
router.include_router(
  management.router, tags=["Tables"], dependencies=[Depends(_require_staging_writes)]
)
router.include_router(
  materialize.router, tags=["Tables"], dependencies=[Depends(_require_staging_writes)]
)

# Read-only queries — allowed on replicas (DuckDB files synced from S3)
router.include_router(query.router, tags=["Tables"])

__all__ = ["router"]
