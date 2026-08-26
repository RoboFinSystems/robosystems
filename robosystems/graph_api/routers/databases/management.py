"""Create, list, inspect, and delete LadybugDB graph databases."""

from fastapi import APIRouter, Depends, Header, HTTPException, Path, Query
from fastapi import status as http_status

from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.graph_api.models.database import (
  DatabaseCreateRequest,
  DatabaseCreateResponse,
  DatabaseInfo,
  DatabaseListResponse,
)
from robosystems.logger import logger
from robosystems.middleware.graph.types import NodeType

router = APIRouter(prefix="/databases", tags=["Graph Management"])


@router.get("", response_model=DatabaseListResponse)
async def list_databases(
  ladybug_service=Depends(get_ladybug_service),
) -> DatabaseListResponse:
  """
  List all databases on this cluster node.

  Returns information about all databases including their size,
  health status, and creation time.
  """
  return ladybug_service.db_manager.get_all_databases_info()


@router.post("", response_model=DatabaseCreateResponse)
async def create_database(
  request: DatabaseCreateRequest,
  ladybug_service=Depends(get_ladybug_service),
) -> DatabaseCreateResponse:
  """
  Create a new database with schema.

  Creates a new LadybugDB database with the specified schema type.
  Different node types support different schema types:
  - Writer nodes: entity, custom
  - Shared master nodes: shared (requires repository_name)
  """
  if ladybug_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Database creation not allowed on read-only nodes",
    )

  # Any node can host any database type — shared repositories are identified by
  # metadata, not by node type. Only the repository name has to be present.
  if request.schema_type == "shared" and not request.repository_name:
    raise HTTPException(
      status_code=http_status.HTTP_400_BAD_REQUEST,
      detail="Shared schema type requires repository_name",
    )

  return ladybug_service.db_manager.create_database(request)


@router.get("/{graph_id}", response_model=DatabaseInfo)
async def get_database_info(
  graph_id: str = Path(..., description="Graph database identifier"),
  ladybug_service=Depends(get_ladybug_service),
) -> DatabaseInfo:
  """
  Get information about a specific database.

  Returns detailed information about a database including its
  size, health status, and last access time.
  """
  return ladybug_service.db_manager.get_database_info(graph_id)


@router.delete("/{graph_id}")
async def delete_database(
  graph_id: str = Path(..., description="Graph database identifier"),
  preserve_duckdb: bool = Query(
    False,
    description="If true, preserve DuckDB staging database (for retry scenarios)",
  ),
  staging_only: bool = Query(
    False,
    description="If true, delete only DuckDB staging database, preserve LadybugDB graph",
  ),
  x_materialization_lock_token: str | None = Header(
    default=None,
    description="Lock token from a materialization caller deleting its own "
    "-wip/-prev artifact. If provided, the delete trusts the caller's lock "
    "instead of acquiring one.",
  ),
  ladybug_service=Depends(get_ladybug_service),
) -> dict:
  """
  Delete a database and all its data.

  Permanently removes a database and all associated data.
  This operation cannot be undone.

  Delete modes:
  - Default (both false): Delete both LadybugDB and DuckDB staging
  - preserve_duckdb=true: Delete LadybugDB only, keep DuckDB staging (for materialization retry)
  - staging_only=true: Delete DuckDB staging only, keep LadybugDB graph (for re-staging with different settings)

  Cannot use both preserve_duckdb and staging_only at the same time.
  """
  if preserve_duckdb and staging_only:
    raise HTTPException(
      status_code=http_status.HTTP_400_BAD_REQUEST,
      detail="Cannot use both preserve_duckdb and staging_only - these options are mutually exclusive",
    )

  if ladybug_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Database deletion not allowed on read-only nodes",
    )

  if staging_only:
    from robosystems.graph_api.core.duckdb import get_duckdb_pool

    try:
      duckdb_pool = get_duckdb_pool()
      duckdb_pool.force_database_cleanup(graph_id)
      logger.info(f"Deleted DuckDB staging database for {graph_id} (staging_only mode)")
      return {
        "status": "success",
        "message": f"DuckDB staging for {graph_id} deleted successfully (LadybugDB preserved)",
      }
    except Exception as e:
      logger.error(f"Failed to delete DuckDB staging for {graph_id}: {e}")
      raise HTTPException(
        status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Failed to delete DuckDB staging: {e!s}",
      )

  if ladybug_service.node_type == NodeType.SHARED_MASTER:
    logger.warning(f"Attempting to delete shared database: {graph_id}")

  # A `-wip`/`-prev` name is a blue-green build artifact, and its base may be
  # mid-materialization right now — deleting the WIP under an active build
  # destroys the copy the build is writing into. Hold the base's
  # materialization lock across the delete, same protocol as `swap_database`:
  # acquiring (not just checking) closes the window where a build starts
  # between the check and the unlink. The materialize flow deletes its own
  # WIP (leftover cleanup, failure cleanup) while already holding this lock —
  # it passes its token through so those deletes don't 409 against itself.
  lock = None
  if graph_id.endswith(("-wip", "-prev")) and not x_materialization_lock_token:
    try:
      from robosystems.config.valkey_registry import (
        ValkeyDatabase,
        create_async_redis_client,
      )
      from robosystems.graph_api.core.ladybug.materialization_lock import (
        MaterializationLock,
      )

      redis_client = create_async_redis_client(ValkeyDatabase.LOCKS)
      lock = MaterializationLock(redis_client, graph_id)
      if not await lock.acquire(timeout_seconds=5):
        raise HTTPException(
          status_code=http_status.HTTP_409_CONFLICT,
          detail=(
            f"A materialization is in progress for {graph_id}'s base database; "
            "the build artifact cannot be deleted while it may be written to."
          ),
        )
    except HTTPException:
      raise
    except Exception as e:
      # Degraded mode mirrors swap_database: an unreachable Valkey should not
      # make artifacts undeletable forever. In that state materialize also
      # runs unlocked, so both sides accept the same residual race — and the
      # active database is never the target either way.
      logger.warning(f"Could not acquire materialization lock for delete: {e}")
      lock = None

  try:
    # The manager's result carries `existed` and `removed`: a delete of a
    # graph whose .lbug was already gone still disposes its side stores and
    # says so, rather than 404ing before any cleanup.
    return ladybug_service.db_manager.delete_database(
      graph_id, preserve_duckdb=preserve_duckdb
    )
  finally:
    if lock is not None and lock.acquired:
      await lock.release()
