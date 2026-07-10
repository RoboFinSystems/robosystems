"""File resource reads (list + get).

Files are first-class, file_id-keyed resources with their own namespace (not
nested under tables). After the content-ops cutover this router is **read-only**
— every file *write* is a content operation under
``POST /v1/graphs/{graph_id}/operations/*``:

- GET  /files, GET /files/{file_id}       — list + inspect (here)
- POST /operations/create-file-upload     — presign an S3 upload + register file
- POST /operations/ingest-file            — stage an uploaded file into DuckDB
- POST /operations/delete-file            — cascade-delete across S3/DuckDB/graph

Architecture: S3 (immutable source) → DuckDB (mutable staging) → LadybugDB
(immutable view); file_id is the primary key across all layers, and operations
work on file_id directly, independent of table context.

File lifecycle:
1. POST /operations/create-file-upload  — get a presigned URL, register the file
2. Upload to S3 using the presigned URL
3. POST /operations/ingest-file         — stage into DuckDB (optionally chain
   materialization into the graph via `ingest_to_graph`)
4. POST /operations/delete-file         — cascade deletion across all layers
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
