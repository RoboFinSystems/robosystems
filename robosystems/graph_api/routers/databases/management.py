"""
Database management endpoints for Graph API.

This module provides endpoints for creating, listing, retrieving,
and deleting LadybugDB graph databases.
"""

from fastapi import APIRouter, Depends, HTTPException, Path
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
  ladybug_service=Depends(get_ladybug_service),
) -> dict:
  """
  Delete a database and all its data.

  Permanently removes a database and all associated data.
  This operation cannot be undone.
  """
  if ladybug_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Database deletion not allowed on read-only nodes",
    )

  # Additional validation for shared writer nodes
  if ladybug_service.node_type == NodeType.SHARED_MASTER:
    # Add extra confirmation or restrictions for shared database deletion
    logger.warning(f"Attempting to delete shared database: {graph_id}")

  ladybug_service.db_manager.delete_database(graph_id)
  return {"status": "success", "message": f"Database {graph_id} deleted successfully"}
