"""Blue-green swap endpoint for LadybugDB databases.

POST /databases/{graph_id}/swap — promote WIP to active (one-way)

The swap is a one-way promotion: WIP becomes the new active database,
the old active is deleted. There is no rollback mechanism — the goal is
to allow a new graph to be fully materialized without disrupting the
active graph, then replace it atomically when ready.
"""

from fastapi import APIRouter, Depends, Header, HTTPException, Path
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
  x_materialization_lock_token: str | None = Header(
    default=None,
    description="Lock token from the materialization caller. "
    "If provided, the swap verifies against this token. "
    "If not provided, the swap acquires its own lock.",
  ),
  ladybug_service=Depends(get_ladybug_service),
) -> SwapResponse:
  """Promote a WIP database to active (one-way swap).

  The old active database is deleted after the WIP is promoted.
  The WIP database must exist ({graph_id}-wip.lbug).
  Acquires the per-graph materialization lock to prevent races.
  """
  if ladybug_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Swap not allowed on read-only nodes",
    )

  # Acquire or verify the materialization lock
  lock = None
  try:
    from robosystems.config.valkey_registry import (
      ValkeyDatabase,
      create_async_redis_client,
    )
    from robosystems.graph_api.core.ladybug.materialization_lock import (
      MaterializationLock,
    )

    redis_client = create_async_redis_client(ValkeyDatabase.LOCKS)

    if x_materialization_lock_token:
      # Caller already holds the lock — verify by using their token
      lock = MaterializationLock.from_token(
        redis_client, graph_id, x_materialization_lock_token
      )
    else:
      # Acquire our own lock for the swap
      lock = MaterializationLock(redis_client, graph_id)
      acquired = await lock.acquire(timeout_seconds=5)
      if not acquired:
        raise HTTPException(
          status_code=http_status.HTTP_409_CONFLICT,
          detail="Another materialization is in progress for this graph",
        )
  except HTTPException:
    raise
  except Exception as e:
    logger.warning(f"Could not acquire materialization lock for swap: {e}")
    # Proceed without lock if Valkey is unavailable (degraded mode)

  try:
    logger.info(f"Swap requested for graph {graph_id}")
    result = ladybug_service.db_manager.swap_database(graph_id)

    return SwapResponse(
      status=result["status"],
      graph_id=result["graph_id"],
      message=result["message"],
    )
  finally:
    # Release lock only if we acquired it ourselves (not passthrough)
    if lock is not None and not x_materialization_lock_token and lock.acquired:
      await lock.release()
