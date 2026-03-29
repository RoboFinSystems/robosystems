"""
Database-specific metrics endpoints for Graph API.

This module provides endpoints for retrieving metrics for individual databases,
primarily used for billing and monitoring purposes.
"""

import datetime
import os
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi import Path as PathParam
from fastapi import status as http_status

from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.graph_api.core.utils import validate_database_name
from robosystems.logger import logger

router = APIRouter(prefix="/databases", tags=["Metrics"])


@router.get("/{graph_id}/metrics")
async def get_database_metrics(
  graph_id: str = PathParam(..., description="Graph database identifier"),
  service=Depends(get_ladybug_service),
) -> dict[str, Any]:
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
    validated_graph_id = validate_database_name(graph_id)

    # Check if database exists
    databases = service.db_manager.list_databases()
    if validated_graph_id not in databases:
      raise HTTPException(
        status_code=http_status.HTTP_404_NOT_FOUND,
        detail=f"Database '{validated_graph_id}' not found",
      )

    # Get database info from manager
    db_info = service.db_manager.get_database_info(validated_graph_id)

    # Get node and relationship counts via Cypher
    node_count = 0
    relationship_count = 0
    try:
      from robosystems.graph_api.core.ladybug import get_connection_pool

      pool = get_connection_pool()
      with pool.get_connection(validated_graph_id, read_only=True) as conn:
        result = conn.execute("MATCH (n) RETURN count(n) as count")
        if result.has_next():
          node_count = result.get_next()[0]
        result.close()

        result = conn.execute("MATCH ()-[r]->() RETURN count(r) as count")
        if result.has_next():
          relationship_count = result.get_next()[0]
        result.close()
    except Exception as e:
      logger.warning(f"Failed to get graph counts for {validated_graph_id}: {e}")

    # Get modification time
    last_modified = None
    db_path = Path(service.db_manager.base_path) / f"{validated_graph_id}.lbug"
    if db_path.exists():
      mtime = os.path.getmtime(db_path)
      last_modified = datetime.datetime.fromtimestamp(mtime).isoformat()

    return {
      "graph_id": validated_graph_id,
      "database_name": validated_graph_id,
      "size_bytes": db_info.size_bytes,
      "size_mb": round(db_info.size_bytes / (1024 * 1024), 2),
      "node_count": node_count,
      "relationship_count": relationship_count,
      "last_modified": last_modified,
      "backend_type": "LadybugDB",
      "instance_id": service.node_id,
      "node_type": service.node_type.value
      if hasattr(service.node_type, "value")
      else str(service.node_type),
    }

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to get metrics for database {graph_id}: {e!s}")
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to retrieve database metrics",
    )
