"""Admin API for Valkey cache management."""

from fastapi import APIRouter, HTTPException, Query, Request, status

from ...config.valkey_registry import (
  ValkeyDatabase,
  create_redis_client,
  get_database_purpose,
)
from ...logger import get_logger
from ...middleware.auth.admin import require_admin
from ...models.api.admin.cache import (
  CacheDatabaseDetailResponse,
  CacheDatabaseInfo,
  CacheFlushAllResponse,
  CacheFlushResponse,
  CacheKeyDeleteResponse,
  CacheKeySampleResponse,
  CacheOverviewResponse,
)

logger = get_logger(__name__)

router = APIRouter(prefix="/admin/v1/cache", tags=["admin-cache"])


def _resolve_database(name: str) -> ValkeyDatabase:
  """Convert a kebab-case name ('rate-limits') to a ValkeyDatabase member.

  Raises 404 when the name matches no database.
  """
  for member in ValkeyDatabase:
    if member.name.lower().replace("_", "-") == name:
      return member
  valid_names = [m.name.lower().replace("_", "-") for m in ValkeyDatabase]
  raise HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail=f"Unknown database '{name}'. Valid names: {valid_names}",
  )


@router.get("/info", response_model=CacheOverviewResponse)
@require_admin(permissions=["cache:read"])
async def cache_info(request: Request):
  """Get overview of all Valkey databases with key counts."""
  databases = []
  total_keys = 0

  for db in ValkeyDatabase:
    try:
      client = create_redis_client(db)
      try:
        key_count = client.dbsize()
      finally:
        client.close()
    except Exception:
      key_count = -1

    total_keys += max(key_count, 0)
    databases.append(
      CacheDatabaseInfo(
        name=db.name.lower().replace("_", "-"),
        db_number=db.value,
        key_count=key_count,
        purpose=get_database_purpose(db),
      )
    )

  logger.info(
    "Admin cache info requested",
    extra={"admin_key_id": getattr(request.state, "admin_key_id", None)},
  )

  return CacheOverviewResponse(databases=databases, total_keys=total_keys)


@router.get("/info/{database}", response_model=CacheDatabaseDetailResponse)
@require_admin(permissions=["cache:read"])
async def cache_database_info(request: Request, database: str):
  """Get detailed info for a specific Valkey database."""
  db = _resolve_database(database)

  try:
    client = create_redis_client(db)
    try:
      key_count = client.dbsize()
      sample_keys = [str(k) for k in client.scan_iter(match="*", count=20)][:20]
    finally:
      client.close()
  except Exception as e:
    logger.error(f"Failed to connect to cache database: {e}", exc_info=True)
    raise HTTPException(
      status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
      detail="Failed to connect to database.",
    )

  logger.info(
    "Admin cache database info requested",
    extra={
      "admin_key_id": getattr(request.state, "admin_key_id", None),
      "database": database,
    },
  )

  return CacheDatabaseDetailResponse(
    name=db.name.lower().replace("_", "-"),
    db_number=db.value,
    key_count=key_count,
    purpose=get_database_purpose(db),
    sample_keys=sample_keys,
  )


@router.post("/flush/{database}", response_model=CacheFlushResponse)
@require_admin(permissions=["cache:write"])
async def cache_flush(request: Request, database: str):
  """Flush all keys from a specific Valkey database."""
  db = _resolve_database(database)

  try:
    client = create_redis_client(db)
    try:
      keys_before = client.dbsize()
      client.flushdb()
    finally:
      client.close()
  except Exception as e:
    logger.error(f"Failed to flush cache database: {e}", exc_info=True)
    raise HTTPException(
      status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
      detail="Failed to flush database.",
    )

  logger.warning(
    "Admin flushed cache database",
    extra={
      "admin_key_id": getattr(request.state, "admin_key_id", None),
      "database": database,
      "keys_flushed": keys_before,
    },
  )

  return CacheFlushResponse(
    name=db.name.lower().replace("_", "-"),
    db_number=db.value,
    keys_before=keys_before,
  )


@router.post("/flush-all", response_model=CacheFlushAllResponse)
@require_admin(permissions=["cache:write"])
async def cache_flush_all(request: Request):
  """Flush all keys from all Valkey databases."""
  results = []
  total_keys_flushed = 0

  for db in ValkeyDatabase:
    try:
      client = create_redis_client(db)
      try:
        keys_before = client.dbsize()
        client.flushdb()
      finally:
        client.close()
      total_keys_flushed += keys_before
      results.append(
        CacheFlushResponse(
          name=db.name.lower().replace("_", "-"),
          db_number=db.value,
          keys_before=keys_before,
        )
      )
    except Exception:
      results.append(
        CacheFlushResponse(
          name=db.name.lower().replace("_", "-"),
          db_number=db.value,
          keys_before=-1,
          flushed=False,
        )
      )

  logger.warning(
    "Admin flushed ALL cache databases",
    extra={
      "admin_key_id": getattr(request.state, "admin_key_id", None),
      "total_keys_flushed": total_keys_flushed,
    },
  )

  return CacheFlushAllResponse(databases=results, total_keys_flushed=total_keys_flushed)


@router.get("/keys/{database}", response_model=CacheKeySampleResponse)
@require_admin(permissions=["cache:read"])
async def cache_keys(
  request: Request,
  database: str,
  pattern: str = Query("*", description="Key pattern to match"),
  count: int = Query(100, ge=1, le=1000, description="Maximum keys to return"),
):
  """List keys in a Valkey database matching a pattern."""
  db = _resolve_database(database)

  try:
    client = create_redis_client(db)
    try:
      keys = [str(k) for k in client.scan_iter(match=pattern, count=count)][:count]
    finally:
      client.close()
  except Exception as e:
    logger.error(f"Failed to scan cache database: {e}", exc_info=True)
    raise HTTPException(
      status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
      detail="Failed to scan database.",
    )

  logger.info(
    "Admin listed cache keys",
    extra={
      "admin_key_id": getattr(request.state, "admin_key_id", None),
      "database": database,
      "pattern": pattern,
    },
  )

  return CacheKeySampleResponse(
    name=db.name.lower().replace("_", "-"),
    db_number=db.value,
    pattern=pattern,
    keys=keys,
    count=len(keys),
  )


@router.delete("/keys/{database}", response_model=CacheKeyDeleteResponse)
@require_admin(permissions=["cache:write"])
async def cache_delete_keys(
  request: Request,
  database: str,
  pattern: str = Query(..., description="Key pattern to delete"),
):
  """Delete keys matching a pattern from a Valkey database.

  The wildcard pattern '*' is rejected to prevent accidental full flushes.
  Use the flush endpoint instead.
  """
  if pattern == "*":
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Wildcard pattern '*' is not allowed. Use the flush endpoint instead.",
    )

  db = _resolve_database(database)

  try:
    client = create_redis_client(db)
    try:
      keys_to_delete = list(client.scan_iter(match=pattern, count=1000))
      deleted = 0
      if keys_to_delete:
        deleted = client.delete(*keys_to_delete)
    finally:
      client.close()
  except Exception as e:
    logger.error(f"Failed to delete cache keys: {e}", exc_info=True)
    raise HTTPException(
      status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
      detail="Failed to delete keys.",
    )

  logger.warning(
    "Admin deleted cache keys",
    extra={
      "admin_key_id": getattr(request.state, "admin_key_id", None),
      "database": database,
      "pattern": pattern,
      "keys_deleted": deleted,
    },
  )

  return CacheKeyDeleteResponse(
    name=db.name.lower().replace("_", "-"),
    db_number=db.value,
    pattern=pattern,
    keys_deleted=deleted,
  )
