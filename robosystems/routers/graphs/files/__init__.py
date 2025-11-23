"""
File Management as First-Class Resources.

This module provides file operations at the graph level, treating files as
first-class citizens with their own namespace (not nested under tables).

Key Features:
- Files as atomic units with file_id tracking
- Multi-layer status tracking (S3, DuckDB, Graph)
- Query parameter filtering by table, status, etc.
- Cascade deletion support across all layers
- Immediate DuckDB staging on upload
- Complete file lifecycle management

Architecture Alignment:
- S3 (Immutable Source) → DuckDB (Mutable Staging) → LadybugDB (Immutable View)
- file_id is the primary key across all layers
- Operations work on file_id directly, independent of table context

File Lifecycle:
1. POST /files - Get presigned URL for upload
2. Upload to S3 using presigned URL
3. PATCH /files/{file_id} status='uploaded' - Triggers DuckDB staging
4. File queryable in DuckDB immediately
5. Background task ingests to graph
6. DELETE /files/{file_id} - Cascade deletion across layers

This clean namespace design enables:
- Better REST semantics (files parallel to tables, not nested)
- File-centric operations without table context
- Cleaner client SDK design (client.files.* methods)
- Independent scaling and access control
"""

from fastapi import APIRouter

from . import main, upload

router = APIRouter(
  tags=["Files"],
  responses={
    401: {"description": "Not authenticated"},
    403: {"description": "Access denied to graph"},
    404: {"description": "Graph or file not found"},
  },
)

router.include_router(main.router)
router.include_router(upload.router)

__all__ = ["router"]
