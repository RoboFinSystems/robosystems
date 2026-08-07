"""Graph database operational limits endpoint."""

import asyncio

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session

from robosystems.database import get_async_db_session
from robosystems.graph_api.client import GraphClient
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph import get_universal_repository
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.middleware.robustness import (
  CircuitBreakerManager,
  TimeoutCoordinator,
)
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.api.graphs.limits import (
  BackupLimits,
  ContentLimits,
  CopyOperationLimits,
  CreditLimits,
  DatabaseStorageEntry,
  DocumentLimits,
  GraphLimitsResponse,
  InstanceUsage,
  QueryLimits,
  RateLimits,
  StorageItem,
  StorageLimits,
  SubgraphLimits,
)
from robosystems.models.core import User

router = APIRouter(tags=["Graph Limits"])

# Initialize robustness components
circuit_breaker = CircuitBreakerManager()
timeout_coordinator = TimeoutCoordinator()


async def _get_graph_client(graph_id: str) -> GraphClient:
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
  from robosystems.graph_api.client.factory import GraphClientFactory

  # Determine operation type based on graph
  # Shared repositories and their subgraphs (e.g. sec_historical) are read-only
  operation_type = "read" if is_shared_repository_or_subgraph(graph_id) else "write"

  # Create client using factory for endpoint discovery
  # Factory automatically handles routing:
  # - Shared repos: Routes to shared_master/shared_replica
  # - User graphs: Looks up tier from database and routes appropriately
  client = await GraphClientFactory.create_client(
    graph_id=graph_id, operation_type=operation_type
  )

  return client


@router.get(
  "/limits",
  response_model=GraphLimitsResponse,
  summary="Get Graph Operational Limits",
  description="Limits vary by subscription tier (ladybug-standard, ladybug-large, ladybug-xlarge). Includes storage, query, backup, rate, credit, document, and instance usage limits.",
  operation_id="getGraphLimits",
  responses={**RESOURCE_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/graphs/{graph_id}/limits",
  business_event_type="graph_limits_retrieved",
)
async def get_graph_limits(
  graph_id: str = Path(
    ...,
    description="Graph database identifier (user graph or shared repository)",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  current_user: User = Depends(get_current_user_with_graph),
  session: Session = Depends(get_async_db_session),
  _: None = Depends(subscription_aware_rate_limit_dependency),
) -> GraphLimitsResponse:
  circuit_breaker.check_circuit(graph_id, "graph_limits")

  try:
    await get_universal_repository(graph_id, "read")

    # Import needed functions
    from robosystems.config.graph_tier import (
      GraphTierConfig,
      get_tier_backup_limits,
      get_tier_copy_operation_limits,
    )
    from robosystems.middleware.graph.utils import MultiTenantUtils
    from robosystems.models.core.graph import Graph
    from robosystems.models.core.graph.graph_credits import GraphCredits

    # Get graph information if it exists
    graph = session.query(Graph).filter(Graph.graph_id == graph_id).first()

    # Determine graph tier:
    # - User graphs: Use tier from database
    # - Shared repositories: Use ladybug-shared tier
    # - Fallback: ladybug-standard (shouldn't happen in practice)
    if graph:
      graph_tier = str(graph.graph_tier)
    elif MultiTenantUtils.is_shared_repository_or_subgraph(graph_id):
      graph_tier = "ladybug-shared"
    else:
      graph_tier = "ladybug-standard"

    # Subgraph-aware, matching _get_graph_client above: sec_historical must
    # take the shared-repository path here too, or the sections below walk the
    # shared master's storage — the exact latency hazard their comments forbid.
    is_shared = MultiTenantUtils.is_shared_repository_or_subgraph(graph_id)

    # The tier whose rate-limit table the limiter enforces for requests to
    # this graph. Shared repositories are user-keyed at the fallback tier, and
    # a tier string without a limits table (ladybug-shared, legacy values)
    # also floors there — without this, the lookup below would fall through to
    # the anonymous "base" table and report 20/min for a graph served 60.
    from ...config.rate_limits import EndpointCategory, RateLimitConfig
    from ...middleware.rate_limits.graph_tier_resolver import FALLBACK_TIER

    enforced_tier = (
      graph_tier
      if not is_shared and graph_tier in RateLimitConfig.SUBSCRIPTION_RATE_LIMITS
      else FALLBACK_TIER
    )

    # Get storage information (instance storage limit from graph.yml).
    # Reads the itemized breakdown so this agrees with `instance_usage` below
    # and with cap enforcement — all three previously disagreed, and this one
    # read a field name the Graph API never emitted, so it was always 0.
    max_storage_gb = GraphTierConfig.get_instance_storage_limit_gb(graph_tier)
    storage_limits = {
      "current_usage_gb": None,
      "max_storage_gb": max_storage_gb,
      "approaching_limit": False,
    }

    # Shared repositories are measured for their tier limit only. Three
    # reasons not to compute their live footprint:
    #
    # - There is nothing to enforce. The tenant does not own a shared
    #   repository's size and cannot act on it, which is why `instance_usage`
    #   below is already skipped for them.
    # - It would be measured on a read replica, which serves the repository
    #   over S3 ATTACH with no data volume — so local disk is a cache, not the
    #   repository. The number would be wrong as well as expensive.
    # - It is expensive in a place that hurts. A shared repository is large
    #   (SEC is ~110 GB) and its replicas serve every tenant, so a recursive
    #   walk per request is a latency hazard on shared infrastructure.
    if not is_shared:
      try:
        graph_client = await _get_graph_client(graph_id)
        breakdown = await asyncio.wait_for(
          graph_client.get_storage_breakdown(graph_id), timeout=10
        )
        await graph_client.close()

        current_storage_gb = breakdown.get("total_bytes", 0) / (1024**3)

        # The warning flag reads durable bytes only, matching cap enforcement:
        # a blue-green `-wip` copy the size of the database would trip it on
        # every rebuild of a graph past ~40% usage.
        from robosystems.graph_api.core.storage_breakdown import TYPE_TRANSIENT

        transient_bytes = sum(
          item.get("bytes", 0)
          for item in breakdown.get("items", [])
          if item.get("type") == TYPE_TRANSIENT
        )
        durable_storage_gb = (breakdown.get("total_bytes", 0) - transient_bytes) / (
          1024**3
        )

        storage_limits = {
          # Byte-level precision, not 2 decimals. Rounding to 0.01 GB quantises
          # this to ~10.7 MB, which for a graph in the tens of megabytes both
          # destroys the figure and makes it contradict the itemized breakdown
          # rendered directly beneath it.
          "current_usage_gb": round(current_storage_gb, 9),
          "max_storage_gb": max_storage_gb,
          "approaching_limit": durable_storage_gb > (max_storage_gb * 0.8),
        }
      except Exception as e:
        logger.warning(f"Could not get storage info for {graph_id}: {e}")

    # Get copy/ingestion limits from tier configuration (based on graph tier)
    copy_limits = get_tier_copy_operation_limits(graph_tier)

    # Define query limits based on graph tier
    query_limits = {
      "max_timeout_seconds": GraphTierConfig.get_query_timeout(graph_tier),
      "chunk_size": GraphTierConfig.get_chunk_size(graph_tier),
      # These are application-level limits not in YAML config
      "max_rows_per_query": 10000,
      "concurrent_queries": 1,
    }

    # Get backup limits from tier configuration (based on graph tier)
    backup_limits = get_tier_backup_limits(graph_tier)

    # Report what the limiter actually enforces, read from the same table the
    # limiter reads. This previously computed 60 x api_rate_multiplier, which
    # told Large 90/min and XLarge 150/min while enforcement gave every tier
    # 60 — the multiplier is read in several places and applied in none.
    # Reporting a limit we do not honour is worse than reporting a lower one.
    query_limit = RateLimitConfig.get_rate_limit(
      enforced_tier, EndpointCategory.GRAPH_QUERY
    )
    requests_per_minute = query_limit[0] if query_limit else 60

    rate_limits = {
      "requests_per_minute": requests_per_minute,
      # Derived, not separately enforced: the limiter uses fixed-window
      # per-minute buckets, so these describe the same budget over a longer
      # span rather than independent ceilings.
      "requests_per_hour": requests_per_minute * 60,
      "burst_capacity": requests_per_minute,
    }

    # Get credit limits if applicable
    credit_limits = {}
    if not is_shared:
      try:
        graph_credits = (
          session.query(GraphCredits).filter(GraphCredits.graph_id == graph_id).first()
        )
        if graph_credits:
          credit_limits = {
            "monthly_ai_credits": int(graph_credits.monthly_allocation),
            "current_balance": int(graph_credits.current_balance),
          }
      except Exception as e:
        # A bare pass here once hid a nonexistent-column read for the life of
        # the endpoint; credits are optional in the response, but the failure
        # must be visible.
        logger.warning(f"Could not get credit limits for {graph_id}: {e}")

    # Document usage against the tier cap. Mirrors upload enforcement
    # (DocumentService._check_tier_limit): only uploaded documents count, and
    # shared repositories have no documents surface at all.
    document_limits = None
    if not is_shared:
      try:
        from robosystems.config.billing.core import get_tier_max_documents
        from robosystems.models.core import Document

        max_documents = get_tier_max_documents(graph_tier)
        document_count = Document.count_by_graph(
          graph_id, session, source_type="uploaded_doc"
        )
        document_limits = DocumentLimits(
          current_count=document_count,
          max_documents=max_documents,
          approaching_limit=(
            max_documents is not None and document_count > max_documents * 0.8
          ),
        )
      except Exception as e:
        logger.warning(f"Could not get document limits for {graph_id}: {e}")

    # Subgraph count against the tier cap. A count axis, not a storage one —
    # creation is refused at the cap however small the subgraphs are, so this
    # cannot be derived from `instance` below. Reported for parent graphs only:
    # subgraphs do not nest, and shared repositories have no tenant-owned cap.
    subgraph_limits = None
    if not is_shared and graph is not None and not bool(graph.is_subgraph):
      try:
        from robosystems.config.graph_tier import get_tier_max_subgraphs

        max_subgraphs = get_tier_max_subgraphs(graph_tier)
        subgraph_count = len(Graph.get_subgraphs(graph_id, session))
        subgraph_limits = SubgraphLimits(
          current_count=subgraph_count,
          max_allowed=max_subgraphs,
          remaining=(
            max(0, max_subgraphs - subgraph_count)
            if max_subgraphs is not None
            else None
          ),
          approaching_limit=(
            max_subgraphs is not None and subgraph_count > max_subgraphs * 0.8
          ),
        )
      except Exception as e:
        logger.warning(f"Could not get subgraph limits for {graph_id}: {e}")

    # Get content limits and instance usage for non-shared graphs.
    # check_instance_storage is a single Graph API call covering the whole
    # instance — subgraphs live on the parent's box, so one breakdown itemizes
    # them all.
    content_limits = None
    instance_usage = None
    if not is_shared:
      graph_limits_config = GraphTierConfig.get_graph_limits(graph_tier)

      # Get node count (informational only — fast, internally tracked by LadybugDB)
      node_count = None
      try:
        graph_client = await _get_graph_client(graph_id)
        db_info = await asyncio.wait_for(
          graph_client.get_database_info(graph_id), timeout=10
        )
        await graph_client.close()
        node_count = db_info.get("node_count")
      except Exception as e:
        logger.debug(f"Could not fetch node count for {graph_id}: {e}")

      content_limits = ContentLimits(
        max_rows_per_copy=graph_limits_config["max_rows_per_copy"],
        max_single_table_rows=graph_limits_config["max_single_table_rows"],
        chunk_size_rows=graph_limits_config["chunk_size_rows"],
      )

      # Get aggregate instance storage usage (parent + subgraphs)
      try:
        from robosystems.middleware.graph.ingestion_limits import IngestionLimitChecker

        # Use parent graph_id for instance-level aggregation
        parent_graph_id = graph_id
        if graph and graph.parent_graph_id:
          parent_graph_id = graph.parent_graph_id

        storage_check = await IngestionLimitChecker.check_instance_storage(
          db=session,
          graph_id=parent_graph_id,
          tier=graph_tier,
        )

        instance_usage = InstanceUsage(
          node_count=node_count,
          total_storage_gb=storage_check["total_storage_gb"],
          limit_gb=storage_check["limit_gb"],
          usage_percentage=storage_check["usage_percentage"],
          status=storage_check["status"],
          databases=[
            DatabaseStorageEntry(**db_entry) for db_entry in storage_check["databases"]
          ],
          items=[StorageItem(**item) for item in storage_check.get("items", [])],
        )
      except Exception as e:
        logger.warning(f"Could not get instance usage for {graph_id}: {e}")

    # Build comprehensive response using typed models
    response = GraphLimitsResponse(
      graph_id=graph_id,
      # Graph subscriptions are per graph, not per user (User has no tier
      # column): report the tier whose limits this graph's requests are
      # actually held to.
      subscription_tier=enforced_tier,
      graph_tier=graph_tier,
      is_shared_repository=is_shared,
      storage=StorageLimits(**storage_limits),
      queries=QueryLimits(**query_limits),
      copy_operations=CopyOperationLimits(
        max_file_size_gb=copy_limits["max_file_size_gb"],
        timeout_seconds=copy_limits["timeout_seconds"],
        concurrent_operations=copy_limits["concurrent_operations"],
        max_files_per_operation=copy_limits["max_files_per_operation"],
        daily_copy_operations=copy_limits["daily_copy_operations"],
        supported_formats=["parquet", "csv", "json", "delta", "iceberg"],
      ),
      backups=BackupLimits(**backup_limits),
      rate_limits=RateLimits(**rate_limits),
      credits=CreditLimits(**credit_limits) if credit_limits else None,
      documents=document_limits,
      subgraphs=subgraph_limits,
      content=content_limits,
      instance=instance_usage,
    )

    # Record success
    circuit_breaker.record_success(graph_id, "graph_limits")

    return response

  except HTTPException:
    raise
  except Exception as e:
    circuit_breaker.record_failure(graph_id, "graph_limits")
    logger.error(f"Failed to get limits for graph {graph_id}: {e!s}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to retrieve graph limits",
    )
