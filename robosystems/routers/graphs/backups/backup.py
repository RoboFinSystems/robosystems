"""
Backup read routes (list).

Backup creation lives at ``POST /v1/graphs/{graph_id}/operations/create-backup``.

There is no customer-facing restore. Backups are a *download* capability: every
graph type with an upstream rebuilds from it rather than from a snapshot
(entity graphs re-materialize from the extensions database, generic graphs from
their staged source files, shared repositories re-ingest), and the classes with
no upstream — entity subgraphs and the semantic memory store — are recovered by
downloading the payload and rebuilding, or by an operator-run restore. The
restore machinery is retained for that internal path only; see
``dagster/jobs/graph.py::restore_backup``.
"""

from datetime import UTC, datetime

from fastapi import (
  APIRouter,
  Depends,
  HTTPException,
  Path,
  Query,
  Request,
  status,
)
from sqlalchemy.orm import Session

from robosystems.config.storage.graph import get_download_extension
from robosystems.database import get_async_db_session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import (
  get_current_user_with_deprovisioned_graph,
)
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.graph.utils import MultiTenantUtils
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from robosystems.middleware.rate_limits import (
  DownloadRateLimiter,
  subscription_aware_rate_limit_dependency,
)
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.api.graphs.backups import (
  BackupListResponse,
  BackupResponse,
  DownloadQuota,
)
from robosystems.models.core import User, UserRepository

# Create router
router = APIRouter()


@router.get(
  "",
  response_model=BackupListResponse,
  operation_id="listBackups",
  summary="List graph database backups",
  responses={**RESOURCE_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/graphs/{graph_id}/backups",
  business_event_type="backup_list_accessed",
)
async def list_backups(
  request: Request,
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  limit: int = Query(
    50, ge=1, le=100, description="Maximum number of backups to return"
  ),
  offset: int = Query(0, ge=0, description="Number of backups to skip"),
  # Export grace period: a departing org's OWNER/ADMIN can still list a
  # torn-down graph's backups (get_effective_role allow_deprovisioned).
  current_user: User = Depends(get_current_user_with_deprovisioned_graph),
  db: Session = Depends(get_async_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> BackupListResponse:
  try:
    logger.info(
      f"Starting list_backups for graph_id: {graph_id}, user: {current_user.id}"
    )

    # Access validated by the require_graph_access dependency

    # List backups from database instead of S3
    logger.info(f"Querying database for backups of graph: {graph_id}")

    # Listing shows what the customer can act on: their own backups and the
    # scheduled ones taken on their behalf. System-initiated rows are
    # pre-restore snapshots and migration-export artifacts — internal to an
    # operation they did not request, so surfacing them would be noise.
    from robosystems.models.core import BackupInitiator, BackupStatus, GraphBackup

    # Query database for backups
    backup_records = (
      db.query(GraphBackup)
      .filter(
        GraphBackup.graph_id == graph_id,
        GraphBackup.status.in_([BackupStatus.COMPLETED, BackupStatus.IN_PROGRESS]),
        GraphBackup.initiated_by != BackupInitiator.SYSTEM.value,
      )
      .order_by(GraphBackup.created_at.desc())
      .offset(offset)
      .limit(limit)
      .all()
    )

    total_count = (
      db.query(GraphBackup)
      .filter(
        GraphBackup.graph_id == graph_id,
        GraphBackup.status.in_([BackupStatus.COMPLETED, BackupStatus.IN_PROGRESS]),
        GraphBackup.initiated_by != BackupInitiator.SYSTEM.value,
      )
      .count()
    )

    logger.info(
      f"Found {len(backup_records)} backups in database (total: {total_count})"
    )

    # Convert to response format
    backups = []
    for backup in backup_records:
      # Parse backup format from metadata or filename
      backup_format = "full_dump"  # default
      if backup.backup_metadata and "backup_format" in backup.backup_metadata:
        backup_format = backup.backup_metadata["backup_format"]
      elif backup.s3_key:
        if ".csv.zip" in backup.s3_key:
          backup_format = "csv"
        elif ".json.zip" in backup.s3_key:
          backup_format = "json"
        elif ".parquet.zip" in backup.s3_key:
          backup_format = "parquet"

      backups.append(
        BackupResponse(
          backup_id=str(backup.id),
          graph_id=graph_id,
          backup_format=backup_format,
          backup_type=backup.backup_type,
          initiated_by=backup.initiated_by,
          # Tri-state flattened for the API: the manifest records "included" /
          # "absent", and a backup predating memory support records nothing at
          # all. That last case stays None rather than collapsing to False,
          # because "makes no claim" and "had none" are different answers.
          memory_included=(
            {"included": True, "absent": False}.get(
              (backup.backup_metadata or {}).get("memory")
            )
          ),
          status=backup.status.value
          if hasattr(backup.status, "value")
          else str(backup.status),
          # s3_bucket and s3_key removed - infrastructure details not exposed
          original_size_bytes=backup.original_size_bytes or 0,
          compressed_size_bytes=backup.compressed_size_bytes or 0,
          compression_ratio=backup.backup_metadata.get("compression_ratio", 0.0)
          if backup.backup_metadata
          else 0.0,
          node_count=backup.node_count or 0,
          relationship_count=backup.relationship_count or 0,
          backup_duration_seconds=backup.backup_duration_seconds or 0.0,
          compression_enabled=backup.compression_enabled,
          download_extension=(
            get_download_extension(backup.s3_key) if backup.s3_key else None
          ),
          created_at=backup.created_at.isoformat()
          if backup.created_at
          else datetime.now(UTC).isoformat(),
          completed_at=backup.completed_at.isoformat() if backup.completed_at else None,
          expires_at=backup.expires_at.isoformat() if backup.expires_at else None,
        )
      )

    # Check if this is a shared repository and get download quota.
    # Subgraph-aware, and the quota keys on the parent repository — matching
    # the download endpoint, which enforces against the parent's counter. The
    # exact-only check made list and download disagree about sec_historical.
    is_shared_repo = MultiTenantUtils.is_shared_repository_or_subgraph(graph_id)
    download_quota = None

    if is_shared_repo:
      from robosystems.config.shared_repositories import (
        resolve_shared_repository_parent,
      )

      repository_id = resolve_shared_repository_parent(graph_id)
      user_repo = UserRepository.get_by_user_and_repository(
        str(current_user.id), repository_id, db
      )
      if user_repo:
        plan = user_repo.repository_plan
        quota_info = await DownloadRateLimiter.get_download_quota(
          user_id=str(current_user.id),
          repository=repository_id,
          plan=plan,
        )
        download_quota = DownloadQuota(
          limit_per_month=quota_info["limit_per_month"],
          used_this_month=quota_info["used_this_month"],
          remaining=quota_info["remaining"],
          resets_at=quota_info["resets_at"],
        )

    # Record business event
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graph/backups",
      method="GET",
      event_type="backup_list_accessed",
      event_data={
        "user_id": current_user.id,
        "graph_id": graph_id,
        "backups_returned": len(backups),
        "limit": limit,
        "offset": offset,
        "is_shared_repository": is_shared_repo,
      },
      user_id=current_user.id,
    )

    return BackupListResponse(
      backups=backups,
      total_count=total_count,
      graph_id=graph_id,
      is_shared_repository=is_shared_repo,
      download_quota=download_quota,
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to list backups for graph {graph_id}: {e!s}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to list backups",
    )
