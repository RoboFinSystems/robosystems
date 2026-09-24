"""File resource reads (list + get).

Files are file_id-keyed resources, not nested under tables. Every write is a
content operation (create-file-upload, ingest-file, delete-file). file_id is
the key across S3 (immutable source), DuckDB (staging) and LadybugDB.
"""

from fastapi import APIRouter

from . import main

router = APIRouter(
  tags=["Files"],
  responses={
    401: {"description": "Not authenticated"},
    403: {"description": "Access denied to graph"},
    404: {"description": "Graph or file not found"},
  },
)

router.include_router(main.router)

__all__ = ["router"]
