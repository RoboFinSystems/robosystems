"""
Database management endpoints for Graph API.

This module provides endpoints for creating, listing, retrieving,
and deleting LadybugDB graph databases.
"""

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from fastapi import status as http_status

from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.graph_api.models.database import (
  DatabaseCreateRequest,
  DatabaseCreateResponse,
  DatabaseInfo,
  DatabaseListResponse,
)
from robosystems.logger import logger
from robosystems.middleware.graph.types import NodeType

router = APIRouter(prefix="/databases", tags=["Graph Management"])


@router.get("", response_model=DatabaseListResponse)
async def list_databases(
  ladybug_service=Depends(get_ladybug_service),
) -> DatabaseListResponse:
  """
  List all databases on this cluster node.

  Returns information about all databases including their size,
  health status, and creation time.
  """
  return ladybug_service.db_manager.get_all_databases_info()


@router.post("", response_model=DatabaseCreateResponse)
async def create_database(
  request: DatabaseCreateRequest,
  ladybug_service=Depends(get_ladybug_service),
) -> DatabaseCreateResponse:
  """
  Create a new database with schema.

  Creates a new LadybugDB database with the specified schema type.
  Different node types support different schema types:
  - Writer nodes: entity, custom
  - Shared master nodes: shared (requires repository_name)
  """
  if ladybug_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Database creation not allowed on read-only nodes",
    )

  # With unified architecture, any node can host any database type
  # Shared repositories are identified by metadata, not node type
  # Just validate that shared schema types have a repository name
  if request.schema_type == "shared" and not request.repository_name:
    raise HTTPException(
      status_code=http_status.HTTP_400_BAD_REQUEST,
      detail="Shared schema type requires repository_name",
    )

  return ladybug_service.db_manager.create_database(request)


@router.get("/{graph_id}", response_model=DatabaseInfo)
async def get_database_info(
  graph_id: str = Path(..., description="Graph database identifier"),
  ladybug_service=Depends(get_ladybug_service),
) -> DatabaseInfo:
  """
  Get information about a specific database.

  Returns detailed information about a database including its
  size, health status, and last access time.
  """
  return ladybug_service.db_manager.get_database_info(graph_id)


@router.delete("/{graph_id}")
async def delete_database(
  graph_id: str = Path(..., description="Graph database identifier"),
  preserve_duckdb: bool = Query(
    False,
    description="If true, preserve DuckDB staging database (for retry scenarios)",
  ),
  staging_only: bool = Query(
    False,
    description="If true, delete only DuckDB staging database, preserve LadybugDB graph",
  ),
  ladybug_service=Depends(get_ladybug_service),
) -> dict:
  """
  Delete a database and all its data.

  Permanently removes a database and all associated data.
  This operation cannot be undone.

  Delete modes:
  - Default (both false): Delete both LadybugDB and DuckDB staging
  - preserve_duckdb=true: Delete LadybugDB only, keep DuckDB staging (for materialization retry)
  - staging_only=true: Delete DuckDB staging only, keep LadybugDB graph (for re-staging with different settings)

  Cannot use both preserve_duckdb and staging_only at the same time.
  """
  # Validate mutually exclusive options
  if preserve_duckdb and staging_only:
    raise HTTPException(
      status_code=http_status.HTTP_400_BAD_REQUEST,
      detail="Cannot use both preserve_duckdb and staging_only - these options are mutually exclusive",
    )

  if ladybug_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Database deletion not allowed on read-only nodes",
    )

  # Handle staging_only mode - delete only DuckDB, preserve LadybugDB
  if staging_only:
    from robosystems.graph_api.core.duckdb import get_duckdb_pool

    try:
      duckdb_pool = get_duckdb_pool()
      duckdb_pool.force_database_cleanup(graph_id)
      logger.info(f"Deleted DuckDB staging database for {graph_id} (staging_only mode)")
      return {
        "status": "success",
        "message": f"DuckDB staging for {graph_id} deleted successfully (LadybugDB preserved)",
      }
    except Exception as e:
      logger.error(f"Failed to delete DuckDB staging for {graph_id}: {e}")
      raise HTTPException(
        status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Failed to delete DuckDB staging: {e!s}",
      )

  # Additional validation for shared writer nodes
  if ladybug_service.node_type == NodeType.SHARED_MASTER:
    # Add extra confirmation or restrictions for shared database deletion
    logger.warning(f"Attempting to delete shared database: {graph_id}")

  ladybug_service.db_manager.delete_database(graph_id, preserve_duckdb=preserve_duckdb)
  return {"status": "success", "message": f"Database {graph_id} deleted successfully"}
