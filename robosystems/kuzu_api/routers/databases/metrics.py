"""
Database-specific metrics endpoints for Kuzu API.

This module provides endpoints for retrieving metrics for individual databases,
primarily used for billing and monitoring purposes.
"""

from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi import status as http_status

from robosystems.kuzu_api.core.cluster_manager import get_cluster_service
from robosystems.kuzu_api.core.utils import validate_database_name
from robosystems.logger import logger

router = APIRouter(prefix="/databases", tags=["Database Metrics"])


@router.get("/{graph_id}/metrics")
async def get_database_metrics(
  graph_id: str = Path(..., description="Graph database identifier"),
  cluster_service=Depends(get_cluster_service),
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
    databases = cluster_service.db_manager.list_databases()
    if validated_graph_id not in databases:
      raise HTTPException(
        status_code=http_status.HTTP_404_NOT_FOUND,
        detail=f"Database '{validated_graph_id}' not found",
      )

    # Get database metrics
    db_path = cluster_service.db_manager.get_database_path(validated_graph_id)

    # Get size information
    import os

    size_bytes = 0
    if os.path.exists(db_path):
      if os.path.isfile(db_path):
        size_bytes = os.path.getsize(db_path)
      else:
        # Directory-based database
        for root, dirs, files in os.walk(db_path):
          for file in files:
            size_bytes += os.path.getsize(os.path.join(root, file))

    # Get database stats using a connection
    node_count = 0
    relationship_count = 0

    try:
      with cluster_service.db_manager.get_connection(
        validated_graph_id, read_only=True
      ) as conn:
        # Count nodes
        result = conn.execute("MATCH (n) RETURN count(n) as node_count")
        if result.has_next():
          node_count = result.get_next()[0]

        # Count relationships
        result = conn.execute("MATCH ()-[r]->() RETURN count(r) as rel_count")
        if result.has_next():
          relationship_count = result.get_next()[0]
    except Exception as e:
      logger.warning(f"Could not get stats for database {validated_graph_id}: {e}")

    # Get modification time
    import datetime

    last_modified = None
    if os.path.exists(db_path):
      mtime = os.path.getmtime(db_path)
      last_modified = datetime.datetime.fromtimestamp(mtime).isoformat()

    return {
      "graph_id": validated_graph_id,
      "database_name": validated_graph_id,
      "size_bytes": size_bytes,
      "size_mb": round(size_bytes / (1024 * 1024), 2),
      "node_count": node_count,
      "relationship_count": relationship_count,
      "last_modified": last_modified,
      "instance_id": cluster_service.node_id,
      "node_type": cluster_service.node_type.value,
    }

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to get metrics for database {graph_id}: {str(e)}")
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to retrieve database metrics",
    )
