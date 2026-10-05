"""Hold a graph's materialization lock for one Graph API request."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import HTTPException
from fastapi import status as http_status

from robosystems.logger import logger


def _lock_unavailable() -> HTTPException:
  return HTTPException(
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

  try:
    redis_client = create_async_redis_client(ValkeyDatabase.LOCKS)
  except Exception as e:
    logger.warning(f"Materialization lock unavailable for {graph_id}: {e}")
    raise _lock_unavailable() from e

  owned = None
  try:
    try:
      if token:
        if await MaterializationLock.adopt(redis_client, graph_id, token) is None:
          raise HTTPException(
            status_code=http_status.HTTP_409_CONFLICT, detail=conflict_detail
          )
      else:
        owned = MaterializationLock(redis_client, graph_id)
        if not await owned.acquire(timeout_seconds=5):
          if owned.last_backend_error:
            raise _lock_unavailable()
          raise HTTPException(
            status_code=http_status.HTTP_409_CONFLICT, detail=conflict_detail
          )
    except HTTPException:
      raise
    except Exception as e:
      logger.warning(f"Materialization lock unavailable for {graph_id}: {e}")
      raise _lock_unavailable() from e

    yield
  finally:
    # A no-op when the acquire failed: only a lock this call holds is released.
    if owned is not None:
      await owned.release()
    try:
      await redis_client.aclose()
    except Exception as e:
      logger.debug(f"Closing the materialization lock client failed: {e}")
