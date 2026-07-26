"""
Database-specific metrics endpoints for Graph API.

This module provides endpoints for retrieving metrics for individual databases,
primarily used for billing and monitoring purposes.
"""

import datetime
import os
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi import Path as PathParam
from fastapi import status as http_status

from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.graph_api.core.storage_breakdown import compute_storage_breakdown
from robosystems.graph_api.core.utils import validate_database_name
from robosystems.logger import logger

router = APIRouter(prefix="/databases", tags=["Metrics"])


@router.get("/{graph_id}/metrics")
async def get_database_metrics(
  graph_id: str = PathParam(..., description="Graph database identifier"),
  include_counts: bool = Query(
    False,
    description=(
      "Compute node and relationship counts. Off by default: these are full "
      "graph scans, and on a large database they take tens of seconds."
    ),
  ),
  service=Depends(get_ladybug_service),
) -> dict[str, Any]:
  """
  Get metrics for a specific database.

  Returns size and modification metadata, which is what callers on a latency
  path need. `node_count` / `relationship_count` are **opt-in** via
  `include_counts` and are `None` otherwise.

  The counts are `MATCH (n) RETURN count(n)` and its relationship equivalent —
  full scans with no index to lean on. On the SEC repository (~110 GB) that
  exceeded 30s and timed out the caller, so the cost is explicit rather than
  a default anyone can wander into.
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

    # Node and relationship counts are full scans — opt-in only.
    node_count = None
    relationship_count = None
    if include_counts:
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
    # Safe: validated_graph_id has been sanitized by validate_database_name() (alphanumeric + hyphens only)
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


@router.get("/{graph_id}/storage")
async def get_database_storage(
  graph_id: str = PathParam(..., description="Graph database identifier"),
) -> dict[str, Any]:
  """
  Get itemized disk usage for a graph and everything it owns.

  Spans all three instance-local roots — the LadybugDB databases (graph,
  memory, subgraphs, and their write-ahead logs), the LanceDB vector indexes,
  and the DuckDB staging file — so the total reflects real occupied disk
  rather than the primary database alone.

  Returns `total_bytes` plus per-item `{type, id, bytes}`, where type is one
  of graph, memory, subgraph, vectors, staging. S3 backups are off-instance
  and excluded.
  """
  try:
    return compute_storage_breakdown(graph_id)
  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to compute storage breakdown for {graph_id}: {e!s}")
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to retrieve storage breakdown",
    )
