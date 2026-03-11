"""Memory management endpoints for graph databases.

These endpoints allow external orchestrators (like Dagster) to manage memory
allocation for staging and materialization operations.

Endpoints:
- /boost: Increase memory limits before heavy operations
- /restore: Reset memory limits to defaults (config only)
- /release: Close connections and free buffers to OS
- /status: Check current boost status

Usage pattern:
1. Before staging: POST /databases/{graph_id}/memory/boost {"target": "duckdb"}
2. Run all staging table creations
3. After staging: POST /databases/{graph_id}/memory/release {"target": "duckdb"}
4. Before materialization: POST /databases/{graph_id}/memory/boost {"target": "ladybug"}
5. Run all materialization
6. After completion: POST /databases/{graph_id}/memory/release {"target": "both"}

Note: /restore only reconfigures memory limits. To actually free memory back to
the OS, use /release which closes connections and releases buffers.
"""

from enum import Enum
from typing import Literal

from fastapi import APIRouter, Body, Path
from pydantic import BaseModel

from robosystems.graph_api.core.memory_manager import (
  ensure_duckdb_memory_boosted,
  ensure_ladybug_memory_boosted,
  is_duckdb_memory_boosted,
  is_ladybug_memory_boosted,
  release_duckdb_memory,
  release_ladybug_memory,
  restore_duckdb_memory,
  restore_ladybug_memory,
)
from robosystems.logger import logger

router = APIRouter(prefix="/databases/{graph_id}/memory", tags=["Memory"])


class MemoryTarget(str, Enum):
  """Target system for memory boost."""

  DUCKDB = "duckdb"
  LADYBUG = "ladybug"
  BOTH = "both"


class MemoryBoostRequest(BaseModel):
  """Request to boost memory for a specific target."""

  target: Literal["duckdb", "ladybug", "both"] = "both"


class MemoryBoostResponse(BaseModel):
  """Response from memory boost operation."""

  graph_id: str
  target: str
  duckdb_boosted: bool
  duckdb_boost_mb: int | None
  ladybug_boosted: bool
  ladybug_boost_mb: int | None
  message: str


class MemoryRestoreResponse(BaseModel):
  """Response from memory restore operation."""

  graph_id: str
  duckdb_restored: bool
  ladybug_restored: bool
  message: str


class MemoryStatusResponse(BaseModel):
  """Response showing current memory boost status."""

  graph_id: str
  duckdb_boosted: bool
  ladybug_boosted: bool


class MemoryReleaseRequest(BaseModel):
  """Request to release memory for a specific target."""

  target: Literal["duckdb", "ladybug", "both"] = "both"
  aggressive: bool = True  # For LadybugDB: run GC and malloc_trim


class MemoryReleaseResponse(BaseModel):
  """Response from memory release operation."""

  graph_id: str
  target: str
  duckdb_connections_closed: int
  duckdb_released: bool
  ladybug_released: bool
  message: str


@router.post("/boost", response_model=MemoryBoostResponse)
async def boost_memory(
  graph_id: str = Path(..., description="Graph database identifier"),
  request: MemoryBoostRequest = Body(default=MemoryBoostRequest()),
) -> MemoryBoostResponse:
  """
  Boost memory for staging (DuckDB) or materialization (LadybugDB) operations.

  Call this before starting a batch of staging or materialization operations.
  The boost will remain active until restore_memory is called.

  Args:
      graph_id: Graph database identifier
      request: Specifies which system to boost (duckdb, ladybug, or both)

  Returns:
      MemoryBoostResponse with boost status and amounts
  """
  logger.info(f"Boosting {request.target} memory for graph {graph_id}")

  duckdb_boosted = False
  duckdb_boost_mb: int | None = None
  ladybug_boosted = False
  ladybug_boost_mb: int | None = None

  if request.target in ("duckdb", "both"):
    result = ensure_duckdb_memory_boosted(graph_id)
    if result:
      duckdb_boosted = True
      # Parse the boost value (e.g., "55GB" -> 56320)
      try:
        if result.endswith("GB"):
          duckdb_boost_mb = int(result[:-2]) * 1024
        elif result.endswith("MB"):
          duckdb_boost_mb = int(result[:-2])
      except (ValueError, AttributeError) as e:
        # Non-fatal: boost succeeded but we couldn't parse the size for response
        logger.warning(f"Could not parse DuckDB boost value '{result}': {e}")
      logger.info(f"DuckDB memory boosted to {result} for {graph_id}")
    else:
      # Already boosted or no boost configured — look up current override
      duckdb_boosted = is_duckdb_memory_boosted(graph_id)
      if duckdb_boosted:
        from robosystems.graph_api.core.duckdb.pool import get_duckdb_memory_override

        current = get_duckdb_memory_override(graph_id)
        if current:
          try:
            if current.endswith("GB"):
              duckdb_boost_mb = int(current[:-2]) * 1024
            elif current.endswith("MB"):
              duckdb_boost_mb = int(current[:-2])
          except (ValueError, AttributeError):
            pass

  if request.target in ("ladybug", "both"):
    result = ensure_ladybug_memory_boosted(graph_id)
    if result:
      ladybug_boosted = True
      ladybug_boost_mb = result
      logger.info(f"LadybugDB memory boosted to {result}MB for {graph_id}")
    else:
      # Already boosted or no boost configured — look up current override
      ladybug_boosted = is_ladybug_memory_boosted(graph_id)
      if ladybug_boosted:
        from robosystems.graph_api.core.ladybug.config import (
          get_ladybug_memory_override,
        )

        current = get_ladybug_memory_override(graph_id)
        if current:
          ladybug_boost_mb = current

  # Build message
  parts = []
  if duckdb_boosted:
    parts.append(f"DuckDB ({duckdb_boost_mb}MB)" if duckdb_boost_mb else "DuckDB")
  if ladybug_boosted:
    parts.append(
      f"LadybugDB ({ladybug_boost_mb}MB)" if ladybug_boost_mb else "LadybugDB"
    )

  if parts:
    message = f"Memory boosted for: {', '.join(parts)}"
  else:
    message = "No boost applied (already boosted or not configured for this tier)"

  return MemoryBoostResponse(
    graph_id=graph_id,
    target=request.target,
    duckdb_boosted=duckdb_boosted,
    duckdb_boost_mb=duckdb_boost_mb,
    ladybug_boosted=ladybug_boosted,
    ladybug_boost_mb=ladybug_boost_mb,
    message=message,
  )


@router.post("/restore", response_model=MemoryRestoreResponse)
async def restore_memory(
  graph_id: str = Path(..., description="Graph database identifier"),
) -> MemoryRestoreResponse:
  """
  Restore DuckDB and LadybugDB memory to default after staging/materialization.

  This endpoint should be called by Dagster after a staging or materialization
  job completes to release the temporarily boosted memory allocation.

  The restore operation:
  1. Clears the DuckDB memory override and reconfigures connections
  2. Clears the LadybugDB memory override and recreates the database

  Returns:
      MemoryRestoreResponse indicating what was restored
  """
  logger.info(f"Restoring memory for graph {graph_id}")

  duckdb_restored = restore_duckdb_memory(graph_id)
  ladybug_restored = restore_ladybug_memory(graph_id)

  if duckdb_restored or ladybug_restored:
    parts = []
    if duckdb_restored:
      parts.append("DuckDB")
    if ladybug_restored:
      parts.append("LadybugDB")
    message = f"Restored memory for: {', '.join(parts)}"
  else:
    message = "No memory boost was active (nothing to restore)"

  logger.info(f"Memory restore complete for {graph_id}: {message}")

  return MemoryRestoreResponse(
    graph_id=graph_id,
    duckdb_restored=duckdb_restored,
    ladybug_restored=ladybug_restored,
    message=message,
  )


@router.post("/release", response_model=MemoryReleaseResponse)
async def release_memory(
  graph_id: str = Path(..., description="Graph database identifier"),
  request: MemoryReleaseRequest = Body(default=MemoryReleaseRequest()),
) -> MemoryReleaseResponse:
  """
  Release memory by closing connections and freeing buffers.

  Unlike /restore (which only reconfigures memory limits), this endpoint
  actually closes connections to force the database engines to release
  their buffer memory back to the OS.

  Call this after staging or materialization operations complete to
  free memory that would otherwise remain allocated until connection timeout.

  Args:
      graph_id: Graph database identifier
      request: Specifies which system to release (duckdb, ladybug, or both)
               and whether to use aggressive cleanup for LadybugDB

  Returns:
      MemoryReleaseResponse with release statistics
  """
  logger.info(f"Releasing {request.target} memory for graph {graph_id}")

  duckdb_connections_closed = 0
  duckdb_released = False
  ladybug_released = False

  if request.target in ("duckdb", "both"):
    result = release_duckdb_memory(graph_id)
    duckdb_connections_closed = result.get("connections_closed", 0)
    duckdb_released = result.get("success", False)
    if duckdb_released:
      logger.info(
        f"DuckDB memory released: {duckdb_connections_closed} connections closed"
      )

  if request.target in ("ladybug", "both"):
    result = release_ladybug_memory(graph_id, aggressive=request.aggressive)
    ladybug_released = result.get("success", False)
    if ladybug_released:
      logger.info(f"LadybugDB memory released (aggressive={request.aggressive})")

  # Build message
  parts = []
  if duckdb_released:
    parts.append(f"DuckDB ({duckdb_connections_closed} connections)")
  if ladybug_released:
    parts.append(f"LadybugDB (aggressive={request.aggressive})")

  if parts:
    message = f"Memory released for: {', '.join(parts)}"
  else:
    message = "No memory released (connections may not exist)"

  logger.info(f"Memory release complete for {graph_id}: {message}")

  return MemoryReleaseResponse(
    graph_id=graph_id,
    target=request.target,
    duckdb_connections_closed=duckdb_connections_closed,
    duckdb_released=duckdb_released,
    ladybug_released=ladybug_released,
    message=message,
  )


@router.get("/status", response_model=MemoryStatusResponse)
async def memory_status(
  graph_id: str = Path(..., description="Graph database identifier"),
) -> MemoryStatusResponse:
  """
  Check if memory is currently boosted for a graph.

  Returns:
      MemoryStatusResponse with current boost status
  """
  return MemoryStatusResponse(
    graph_id=graph_id,
    duckdb_boosted=is_duckdb_memory_boosted(graph_id),
    ladybug_boosted=is_ladybug_memory_boosted(graph_id),
  )
