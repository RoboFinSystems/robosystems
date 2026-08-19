"""
Backup download URL generation endpoint.
"""

from datetime import UTC, datetime

from fastapi import (
  APIRouter,
  Depends,
  HTTPException,
  Path,
  Query,
  status,
)
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
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
from robosystems.models.api.graphs.backups import BackupDownloadUrlResponse
from robosystems.models.core import Graph, User, UserRepository

from .utils import get_backup_manager, verify_admin_access

# Create router
router = APIRouter()


@router.get(
  "/{backup_id}/download",
  response_model=BackupDownloadUrlResponse,
  operation_id="getBackupDownloadUrl",
  summary="Get temporary download URL for backup",
  description=(
    "Generate a temporary download URL for a backup. "
    "The filename carries the extension listed as `download_extension` on the "
    "backup: `.lbug.zip` is a ZIP holding the LadybugDB database file "
    "`{graph_id}.lbug`; `.lbug.zst` (shared repository snapshots) is a single "
    "zstd-compressed database file. Decompress the latter with "
    "`zstd -d <file>.lbug.zst` (install zstd first: `brew install zstd`, "
    "`apt-get install zstd`, or `dnf install zstd`) — no `--long` flag is needed."
  ),
  status_code=status.HTTP_200_OK,
  responses={
    200: {"description": "Download URL generated successfully"},
    403: {
      "description": (
        "Access denied — admin role on the graph, or an eligible repository "
        "subscription, is required"
      )
    },
    404: {"description": "Backup not found"},
    500: {"description": "Failed to generate download URL"},
  },
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/graphs/{graph_id}/backups/{backup_id}/download",
  business_event_type="backup_download_url_generated",
)
async def get_backup_download_url(
  backup_id: str = Path(..., description="Backup identifier"),
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  expires_in: int = Query(
    3600, ge=300, le=86400, description="URL expiration time in seconds"
  ),
  # Export grace period: see list_backups. Both the dependency and the
  # in-handler admin check below allow a torn-down graph for org OWNER/ADMIN.
  current_user: User = Depends(get_current_user_with_deprovisioned_graph),
  session: Session = Depends(get_db_session),
  _: None = Depends(subscription_aware_rate_limit_dependency),
) -> BackupDownloadUrlResponse:
  """
  Generate a temporary download URL for a backup.

  This endpoint provides a secure, time-limited URL that allows direct download
  of compressed .lbug backup files without going through the API.

  Requirements:
  - Admin role on the graph (dedicated graphs), or a repository subscription
    whose plan includes downloads (shared repositories)
  - Backup must be in full_dump format (complete .lbug file)
  - File will be compressed

  Unpacking the download:
  - `{graph_id}_{timestamp}.lbug.zip` (backups of a graph you own) — a standard
    ZIP archive holding the LadybugDB database file `{graph_id}.lbug`; `unzip` it.
  - `{graph_id}_{timestamp}.lbug.zst` (shared repository snapshots) — a single
    zstd-compressed LadybugDB file. Install zstd (`brew install zstd` on macOS,
    `apt-get install zstd` on Debian/Ubuntu, `dnf install zstd` on
    Amazon Linux/Fedora), then run `zstd -d <file>.lbug.zst`. Compression uses a
    128MB long window, so plain `zstd -d` suffices — no `--long` flag required.
    Allow disk for roughly 2x the download size.

  Returns the download URL and its expiration. `expires_in` ranges from 5
  minutes to 24 hours.
  """
  try:
    # Access validated by the graph_access_dependency (deprovisioned-tolerant)
    is_shared = MultiTenantUtils.is_shared_repository_or_subgraph(graph_id)
    has_tier_limit = False
    # The single id the monthly counter is keyed on. Both the check and the
    # increment must use it: the shared path resolves a subgraph to its parent
    # for the *check* (`sec_historical` → `sec`), so incrementing under the
    # requested id instead left the checked counter permanently at zero and the
    # quota unenforced for every subgraph download.
    quota_resource_id = graph_id

    # Check download rate limits based on graph type
    if is_shared:
      # Shared repository: check subscription and plan-based limits
      # Resolve subgraph to parent for subscription lookup
      from robosystems.config.shared_repositories import (
        resolve_shared_repository_parent,
      )

      parent_repo_id = resolve_shared_repository_parent(graph_id)
      quota_resource_id = parent_repo_id
      user_repo = UserRepository.get_by_user_and_repository(
        str(current_user.id), parent_repo_id, session
      )
      if not user_repo:
        raise HTTPException(
          status_code=status.HTTP_403_FORBIDDEN,
          detail="Repository subscription required for backup downloads",
        )

      plan = user_repo.repository_plan
      monthly_limit = DownloadRateLimiter.get_shared_repo_monthly_limit(
        parent_repo_id, plan
      )

      # Limit of 0 means downloads are not available on this plan
      if monthly_limit == 0:
        raise HTTPException(
          status_code=status.HTTP_403_FORBIDDEN,
          detail="Backup downloads are not available on your current plan. Please upgrade to Pro.",
        )

      allowed, remaining, resets_at = await DownloadRateLimiter.check_download_limit(
        user_id=str(current_user.id),
        repository=parent_repo_id,
        plan=plan,
      )

      if not allowed:
        raise HTTPException(
          status_code=status.HTTP_429_TOO_MANY_REQUESTS,
          detail=f"Monthly download limit exceeded. Limit resets at {resets_at.isoformat()}.",
          headers={
            "X-RateLimit-Limit": str(monthly_limit),
            "X-RateLimit-Remaining": "0",
            "X-RateLimit-Reset": resets_at.isoformat(),
          },
        )
    else:
      # A dedicated graph's backup is the whole database, unencrypted, behind
      # a URL that lives up to a day. Creating and restoring one already
      # require admin on the graph; taking one out does too. Shared
      # repositories are gated by subscription plan above instead — there is
      # no per-graph role there.
      verify_admin_access(current_user, graph_id, session, allow_deprovisioned=True)

      # Dedicated graph: check tier-based download limits.
      # Deprovisioned graphs are included deliberately. The final backup is
      # taken precisely so a departing customer can retrieve their data during
      # the published export grace period, and the default lookup skips
      # deprovisioned rows — which 404'd the one download that window exists
      # for. Access is unchanged: verify_admin_access above still gates this
      # on graph admin, which after teardown only the org's owners and admins
      # still hold.
      graph_record = Graph.get_by_id(graph_id, session, include_deprovisioned=True)
      if not graph_record:
        logger.warning(
          f"Graph record not found for {graph_id} during download limit check"
        )
        raise HTTPException(
          status_code=status.HTTP_404_NOT_FOUND,
          detail="Graph not found",
        )

      if graph_record.graph_tier:
        has_tier_limit = True
        tier_limit = DownloadRateLimiter.get_graph_tier_monthly_limit(
          str(graph_record.graph_tier)
        )

        (
          allowed,
          remaining,
          resets_at,
        ) = await DownloadRateLimiter.check_graph_download_limit(
          user_id=str(current_user.id),
          graph_id=graph_id,
          graph_tier=str(graph_record.graph_tier),
        )

        if not allowed:
          raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Monthly backup download limit ({tier_limit}) exceeded. Limit resets at {resets_at.isoformat()}.",
            headers={
              "X-RateLimit-Limit": str(tier_limit),
              "X-RateLimit-Remaining": "0",
              "X-RateLimit-Reset": resets_at.isoformat(),
            },
          )

    # Get backup manager and generate download URL
    backup_manager = get_backup_manager()

    download_url = await backup_manager.get_backup_download_url(
      graph_id=graph_id, backup_id=backup_id, expires_in=expires_in
    )

    if not download_url:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Backup not found or cannot be downloaded",
      )

    # Increment download count for rate limiting
    if is_shared or has_tier_limit:
      await DownloadRateLimiter.increment_download_count(
        user_id=str(current_user.id),
        resource_id=quota_resource_id,
      )

    # Record business event
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graph/backup/download",
      method="GET",
      event_type="backup_download_url_generated",
      event_data={
        "user_id": current_user.id,
        "graph_id": graph_id,
        "backup_id": backup_id,
        "expires_in": expires_in,
        "is_shared_repository": is_shared,
      },
      user_id=current_user.id,
    )

    return BackupDownloadUrlResponse(
      download_url=download_url,
      expires_in=expires_in,
      expires_at=(datetime.now(UTC).timestamp() + expires_in),
      backup_id=backup_id,
      graph_id=graph_id,
    )

  except ValueError:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Invalid backup operation",
    )
  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to generate download URL for backup {backup_id}: {e!s}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to generate download URL",
    )
