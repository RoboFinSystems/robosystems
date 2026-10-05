"""Blue-green swap: promote ``{graph_id}-wip`` to active and delete the old
active. One-way — there is no rollback once it succeeds.
"""

from fastapi import APIRouter, Depends, Header, HTTPException, Path
from fastapi import status as http_status
from pydantic import BaseModel, Field

from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.graph_api.routers.databases.lock_guard import (
  materialization_lock_held,
)
from robosystems.logger import logger

router = APIRouter(prefix="/databases", tags=["Graph Management"])


class SwapResponse(BaseModel):
  status: str = Field(..., description="Operation status")
  graph_id: str = Field(..., description="Graph database identifier")
  message: str = Field(..., description="Human-readable status message")


@router.post("/{graph_id}/swap", response_model=SwapResponse)
async def swap_database(
  graph_id: str = Path(..., description="Base graph ID (not the -wip variant)"),
  x_materialization_lock_token: str | None = Header(
    default=None,
    description="Lock token from the materialization caller. "
    "If provided, it must be the current holder's token. "
    "If not provided, the swap acquires its own lock.",
  ),
  ladybug_service=Depends(get_ladybug_service),
) -> SwapResponse:
  """Promote a WIP database to active (one-way swap).

  The old active database is deleted after the WIP is promoted.
  The WIP database must exist ({graph_id}-wip.lbug).
  Runs under the per-graph materialization lock: 409 when another run holds
  it, 503 when the lock service is unreachable.
  """
  if ladybug_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Swap not allowed on read-only nodes",
    )

  async with materialization_lock_held(
    graph_id,
    x_materialization_lock_token,
    conflict_detail="Another materialization is in progress for this graph",
  ):
    logger.info(f"Swap requested for graph {graph_id}")
    result = ladybug_service.db_manager.swap_database(graph_id)

    return SwapResponse(
      status=result["status"],
      graph_id=result["graph_id"],
      message=result["message"],
    )
