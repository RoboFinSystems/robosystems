"""Blue-green swap and rollback endpoints for LadybugDB databases.

POST /databases/{graph_id}/swap     — promote WIP to active
POST /databases/{graph_id}/rollback — restore -prev to active
"""

from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi import status as http_status
from pydantic import BaseModel, Field

from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.logger import logger

router = APIRouter(prefix="/databases", tags=["Graph Management"])


class SwapResponse(BaseModel):
  status: str = Field(..., description="Operation status")
  graph_id: str = Field(..., description="Graph database identifier")
  message: str = Field(..., description="Human-readable status message")


@router.post("/{graph_id}/swap", response_model=SwapResponse)
async def swap_database(
  graph_id: str = Path(..., description="Base graph ID (not the -wip variant)"),
  ladybug_service=Depends(get_ladybug_service),
) -> SwapResponse:
  """Promote a WIP database to active (blue-green swap).

  The active database is renamed to -prev (rollback safety), then
  the WIP database is renamed to the active name. Downtime is
  milliseconds (file rename).

  Requires that a WIP database ({graph_id}-wip.lbug) exists.
  """
  if ladybug_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Swap not allowed on read-only nodes",
    )

  logger.info(f"Swap requested for graph {graph_id}")
  result = ladybug_service.db_manager.swap_database(graph_id)

  return SwapResponse(
    status=result["status"],
    graph_id=result["graph_id"],
    message=result["message"],
  )


@router.post("/{graph_id}/rollback", response_model=SwapResponse)
async def rollback_database(
  graph_id: str = Path(..., description="Base graph ID"),
  ladybug_service=Depends(get_ladybug_service),
) -> SwapResponse:
  """Restore the -prev database to active (manual rollback).

  Requires that a -prev database ({graph_id}-prev.lbug) exists.
  """
  if ladybug_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Rollback not allowed on read-only nodes",
    )

  logger.info(f"Rollback requested for graph {graph_id}")
  result = ladybug_service.db_manager.rollback_database(graph_id)

  return SwapResponse(
    status=result["status"],
    graph_id=result["graph_id"],
    message=result["message"],
  )
