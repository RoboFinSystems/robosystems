"""
Graph database operational limits endpoint.

This module provides REST API endpoints for retrieving operational limits.
"""

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
from robosystems.models.api.graphs.limits import (
  BackupLimits,
  ContentLimits,
  CopyOperationLimits,
  CreditLimits,
  DatabaseStorageEntry,
  GraphLimitsResponse,
  InstanceUsage,
  QueryLimits,
  RateLimits,
  StorageLimits,
)
from robosystems.models.core import User

# Create router
router = APIRouter(tags=["Graph Limits"])

# Initialize robustness components
circuit_breaker = CircuitBreakerManager()
timeout_coordinator = TimeoutCoordinator()


async def _get_graph_client(graph_id: str) -> GraphClient:
  """Get Graph client for the specified graph using factory for endpoint discovery."""
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
  description="""Get comprehensive operational limits for the graph database.

Returns all operational limits that apply to this graph including:
- **Storage Limits**: Maximum storage size and current usage
- **Query Limits**: Timeouts, complexity, row limits
- **Copy/Ingestion Limits**: File sizes, timeouts, concurrent operations
- **Backup Limits**: Frequency, retention, size limits
- **Rate Limits**: Requests per minute/hour based on tier
- **Credit Limits**: AI operation credits (if applicable)
- **Content Limits**: Per-operation materialization limits (if applicable)
- **Instance Usage**: Aggregate storage across parent + subgraphs (user graphs only)

**Note**: Limits vary based on subscription tier (ladybug-standard, ladybug-large, ladybug-xlarge).""",
  operation_id="getGraphLimits",
  responses={
    200: {"description": "Limits retrieved successfully"},
    403: {"description": "Access denied to graph"},
    404: {"description": "Graph not found"},
    500: {"description": "Failed to retrieve limits"},
  },
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
  """
  Get comprehensive operational limits for the graph database.

  This endpoint consolidates all operational limits into a single response,
  making it easier for clients to understand their constraints.

  Args:
      graph_id: Graph database identifier
      current_user: Authenticated user
      session: Database session

  Returns:
      Dictionary containing all operational limits for the graph
  """
  # Check circuit breaker
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

    # Get user's subscription tier
    user_tier = getattr(current_user, "subscription_tier", "ladybug-standard")

    # Get graph information if it exists
    graph = session.query(Graph).filter(Graph.graph_id == graph_id).first()

    # Determine graph tier:
    # - User graphs: Use tier from database
    # - Shared repositories: Use ladybug-shared tier
    # - Fallback: ladybug-standard (shouldn't happen in practice)
    if graph:
      graph_tier = str(graph.graph_tier)
    elif MultiTenantUtils.is_shared_repository(graph_id):
      graph_tier = "ladybug-shared"
    else:
      graph_tier = "ladybug-standard"

    is_shared = MultiTenantUtils.is_shared_repository(graph_id)

    # Get storage information (instance storage limit from graph.yml)
    max_storage_gb = GraphTierConfig.get_instance_storage_limit_gb(graph_tier)
    storage_limits = {}
    try:
      graph_client = await _get_graph_client(graph_id)
      db_info = await asyncio.wait_for(
        graph_client.get_database_info(graph_id), timeout=10
      )
      await graph_client.close()

      current_storage_gb = db_info.get("database_size_bytes", 0) / (1024**3)

      storage_limits = {
        "current_usage_gb": round(current_storage_gb, 2),
        "max_storage_gb": max_storage_gb,
        "approaching_limit": current_storage_gb > (max_storage_gb * 0.8),
      }
    except Exception as e:
      logger.warning(f"Could not get storage info for {graph_id}: {e}")
      storage_limits = {
        "current_usage_gb": None,
        "max_storage_gb": max_storage_gb,
        "approaching_limit": False,
      }

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

    # Define rate limits based on graph tier (using api_rate_multiplier from config)
    base_requests_per_minute = 60
    base_requests_per_hour = 1000
    base_burst_capacity = 10

    multiplier = GraphTierConfig.get_api_rate_multiplier(graph_tier)

    rate_limits = {
      "requests_per_minute": int(base_requests_per_minute * multiplier)
      if multiplier
      else base_requests_per_minute,
      "requests_per_hour": int(base_requests_per_hour * multiplier)
      if multiplier
      else base_requests_per_hour,
      "burst_capacity": int(base_burst_capacity * multiplier)
      if multiplier
      else base_burst_capacity,
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
            "monthly_ai_credits": graph_credits.monthly_credit_limit,
            "current_balance": graph_credits.current_balance,
          }
      except Exception:
        pass

    # Get content limits and instance usage for non-shared graphs.
    # Note: check_instance_storage issues parallel Graph API calls for the parent
    # + each subgraph (N+1 calls). For graphs with many subgraphs this adds
    # latency. Consider adding a short-TTL Valkey cache if this becomes an issue.
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
        )
      except Exception as e:
        logger.warning(f"Could not get instance usage for {graph_id}: {e}")

    # Build comprehensive response using typed models
    response = GraphLimitsResponse(
      graph_id=graph_id,
      subscription_tier=user_tier,
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
