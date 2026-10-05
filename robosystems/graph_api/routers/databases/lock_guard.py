"""Hold a graph's materialization lock for one Graph API request."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import HTTPException
from fastapi import status as http_status

from robosystems.logger import logger

_LOCK_UNAVAILABLE = HTTPException(
  status_code=http_status.HTTP_503_SERVICE_UNAVAILABLE,
  detail="Materialization lock service unavailable; retry shortly",
  headers={"Retry-After": "30"},
)


@asynccontextmanager
async def materialization_lock_held(
  graph_id: str, token: str | None, conflict_detail: str
) -> AsyncIterator[None]:
  """Run the block under ``graph_id``'s materialization lock, failing closed.

  With ``token`` the caller already holds the lock, and the token must be the
  stored one; without, the lock is acquired here and released after. Either
  way a held or unverified lock is a 409, and an unreachable Valkey a 503.
  """
  from robosystems.config.valkey_registry import (
    ValkeyDatabase,
    create_async_redis_client,
  )
  from robosystems.graph_api.core.ladybug.materialization_lock import (
    MaterializationLock,
  )

  owned = None
  try:
    redis_client = create_async_redis_client(ValkeyDatabase.LOCKS)
    if token:
      if await MaterializationLock.adopt(redis_client, graph_id, token) is None:
        raise HTTPException(
          status_code=http_status.HTTP_409_CONFLICT, detail=conflict_detail
        )
    else:
      owned = MaterializationLock(redis_client, graph_id)
      if not await owned.acquire(timeout_seconds=5):
        if owned.last_backend_error:
          raise _LOCK_UNAVAILABLE
        raise HTTPException(
          status_code=http_status.HTTP_409_CONFLICT, detail=conflict_detail
        )
  except HTTPException:
    raise
  except Exception as e:
    logger.warning(f"Materialization lock unavailable for {graph_id}: {e}")
    raise _LOCK_UNAVAILABLE from e

  try:
    yield
  finally:
    if owned is not None:
      await owned.release()
