"""
Database-specific metrics endpoints for Graph API.

This module provides endpoints for retrieving metrics for individual databases,
primarily used for billing and monitoring purposes.
"""

from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi import status as http_status

from robosystems.graph_api.core.utils import validate_database_name
from robosystems.graph_api.backends import get_backend
from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.config import env
from robosystems.logger import logger

router = APIRouter(prefix="/databases", tags=["Metrics"])


def _get_service_for_metrics():
  """Get the appropriate service based on backend configuration."""
  backend_type = env.GRAPH_BACKEND_TYPE
  if backend_type in ["neo4j_community", "neo4j_enterprise"]:
    from robosystems.graph_api.core.neo4j import Neo4jService

    return Neo4jService()
  else:
    return get_ladybug_service()


@router.get("/{graph_id}/metrics")
async def get_database_metrics(
  graph_id: str = Path(..., description="Graph database identifier"),
  backend=Depends(get_backend),
  service=Depends(_get_service_for_metrics),
) -> Dict[str, Any]:
  """
  Get metrics for a specific database.

  Returns metrics specifically for billing and monitoring:
  - Database size in bytes
  - Node and relationship counts
  - Last modified timestamp
  - Database tier information

  This endpoint is optimized for per-database billing collection.
  """
  try:
    # Validate database name
    validated_graph_id = validate_database_name(graph_id)

    # Check if database exists
    databases = await backend.list_databases()
    if validated_graph_id not in databases:
      raise HTTPException(
        status_code=http_status.HTTP_404_NOT_FOUND,
        detail=f"Database '{validated_graph_id}' not found",
      )

    # Get database info from backend
    db_info = await backend.get_database_info(validated_graph_id)

    # Get modification time (only available for LadybugDB)
    import datetime
    import os
    from pathlib import Path

    last_modified = None
    if hasattr(backend, "data_path"):
      # LadybugDB backend
      # Safe: validated_graph_id has been sanitized by validate_database_name() on line 49
      db_path = Path(backend.data_path) / f"{validated_graph_id}.lbug"
      if db_path.exists():
        mtime = os.path.getmtime(db_path)
        last_modified = datetime.datetime.fromtimestamp(mtime).isoformat()

    return {
      "graph_id": validated_graph_id,
      "database_name": db_info.name,
      "size_bytes": db_info.size_bytes,
      "size_mb": round(db_info.size_bytes / (1024 * 1024), 2),
      "node_count": db_info.node_count,
      "relationship_count": db_info.relationship_count,
      "last_modified": last_modified,
      "backend_type": backend.__class__.__name__,
      "instance_id": service.node_id,
      "node_type": service.node_type.value
      if hasattr(service.node_type, "value")
      else str(service.node_type),
    }

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to get metrics for database {graph_id}: {str(e)}")
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to retrieve database metrics",
    )
