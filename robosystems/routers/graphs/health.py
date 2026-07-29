"""Graph database health endpoint."""

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
from robosystems.models.api.graphs.health import DatabaseHealthResponse
from robosystems.models.core import User

router = APIRouter(tags=["Graph Health"])

# Initialize robustness components
circuit_breaker = CircuitBreakerManager()
timeout_coordinator = TimeoutCoordinator()

# Below this on both axes the instance has obvious headroom.
_IDLE_CEILING_PERCENT = 50.0


def _resources_exposable(graph_id: str, graph_tier: str) -> bool:
  """Whether instance CPU/memory may be shown to this graph's owner.

  Safe only when the instance is genuinely single-tenant, so both checks are
  required and neither is redundant:

  - Shared repositories (e.g. sec) run one database per box but are read by
    every tenant, so instance load leaks cross-tenant activity.
  - A packed tier co-locates unrelated tenants by construction.

  Driving the second check off the tier's ``databases_per_instance`` rather
  than a tier allowlist means a future packed tier closes this automatically.
  """
  from robosystems.config.graph_tier import GraphTierConfig
  from robosystems.middleware.graph.utils import MultiTenantUtils

  # Subgraph-aware: sec_historical serves every tenant from the shared master
  # exactly as sec does, so the exact-only check leaked its instance metrics.
  if MultiTenantUtils.is_shared_repository_or_subgraph(graph_id):
    return False
  return GraphTierConfig.get_databases_per_instance(graph_tier) == 1


def _derive_resource_status(
  cpu_percent: float | None,
  memory_percent: float | None,
  admission_control: dict,
) -> str | None:
  """Interpret raw load, so a normal workload doesn't read as a fault.

  Materialization deliberately boosts to near the whole instance, so a high
  memory reading is usually the engine working as designed. ``constrained``
  is therefore anchored to the admission controller's own thresholds — the
  point at which the node starts shedding work — rather than an arbitrary
  number, so it means "actually worth acting on".
  """
  if cpu_percent is None and memory_percent is None:
    return None

  cpu = cpu_percent or 0.0
  memory = memory_percent or 0.0

  cpu_threshold = admission_control.get("cpu_threshold")
  memory_threshold = admission_control.get("memory_threshold")
  if (cpu_threshold and cpu >= cpu_threshold) or (
    memory_threshold and memory >= memory_threshold
  ):
    return "constrained"

  if cpu < _IDLE_CEILING_PERCENT and memory < _IDLE_CEILING_PERCENT:
    return "idle"
  return "busy"


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
  "/health",
  response_model=DatabaseHealthResponse,
  summary="Database Health Check",
  operation_id="getDatabaseHealth",
  responses={**RESOURCE_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/graphs/{graph_id}/health",
  business_event_type="database_health_checked",
)
async def get_database_health(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  current_user: User = Depends(get_current_user_with_graph),
  session: Session = Depends(get_async_db_session),
  _: None = Depends(subscription_aware_rate_limit_dependency),
) -> DatabaseHealthResponse:
  circuit_breaker.check_circuit(graph_id, "database_health")

  try:
    _repository = await get_universal_repository(graph_id, "read")

    # Get Graph client and health information
    graph_client = await _get_graph_client(graph_id)

    try:
      # Calculate timeout for health check
      health_timeout = timeout_coordinator.calculate_timeout(
        "database_health", {"complexity": "low"}
      )

      # Get database metrics and general health from Graph API
      db_metrics = await asyncio.wait_for(
        graph_client.get_database_metrics(graph_id=graph_id),
        timeout=health_timeout,
      )

      # Also get cluster health for uptime
      cluster_health = await graph_client.health_check()

      # Record successful operation
      circuit_breaker.record_success(graph_id, "database_health")

      logger.debug(f"Database health retrieved for graph {graph_id}")

      # Pull staleness data from the platform DB (Graph model)
      from datetime import UTC, datetime

      from robosystems.models.core import Graph

      graph_record = Graph.get_by_id(graph_id.split("_")[0], session)
      is_stale = False
      stale_reason = None
      stale_since = None
      last_materialized_at = None
      hours_since_materialization = None
      staleness_alert: list[str] = []

      if graph_record:
        is_stale = graph_record.graph_stale or False
        stale_reason = graph_record.graph_stale_reason
        stale_since = (
          graph_record.graph_stale_at.isoformat()
          if graph_record.graph_stale_at
          else None
        )
        metadata = graph_record.graph_metadata or {}
        last_materialized_at = metadata.get("last_materialized_at")
        if last_materialized_at:
          try:
            from dateutil import parser as date_parser

            last_mat_dt = date_parser.isoparse(last_materialized_at)
            hours_since_materialization = (
              datetime.now(UTC) - last_mat_dt
            ).total_seconds() / 3600
          except Exception:
            logger.debug(
              f"Could not parse last_materialized_at for graph {graph_id}: {last_materialized_at!r}"
            )
        if is_stale:
          staleness_alert = ["Graph is stale — materialization recommended"]

      # Instance CPU/memory, dedicated single-tenant instances only.
      # Best-effort: a failure here must not fail the health check.
      cpu_usage_percent = None
      memory_usage_percent = None
      memory_usage_mb = None
      resource_status = None
      if graph_record and _resources_exposable(graph_id, str(graph_record.graph_tier)):
        try:
          node_metrics = await asyncio.wait_for(
            graph_client.get_metrics(), timeout=health_timeout
          )
          system = node_metrics.get("system", {})
          memory = system.get("memory", {})
          cpu = system.get("cpu", {})

          memory_usage_percent = memory.get("usage_percent")
          used_gb = memory.get("used_gb")
          if used_gb is not None:
            memory_usage_mb = round(used_gb * 1024, 1)
          cpu_usage_percent = cpu.get("usage_percent")
          resource_status = _derive_resource_status(
            cpu_usage_percent,
            memory_usage_percent,
            node_metrics.get("admission_control", {}),
          )
        except Exception as e:
          logger.debug(f"Could not fetch instance resources for {graph_id}: {e}")

      return DatabaseHealthResponse(
        graph_id=graph_id,
        status="degraded" if is_stale else ("healthy" if db_metrics else "unknown"),
        connection_status="connected",
        uptime_seconds=cluster_health.get("uptime_seconds", 0.0),
        last_query_time=db_metrics.get("last_modified"),
        query_count_24h=0,
        avg_query_time_ms=0.0,
        error_rate_24h=0.0,
        memory_usage_mb=memory_usage_mb,
        memory_usage_percent=memory_usage_percent,
        cpu_usage_percent=cpu_usage_percent,
        resource_status=resource_status,
        storage_usage_mb=db_metrics.get("size_mb"),
        alerts=staleness_alert,
        is_stale=is_stale,
        stale_reason=stale_reason,
        stale_since=stale_since,
        last_materialized_at=last_materialized_at,
        hours_since_materialization=hours_since_materialization,
      )

    except TimeoutError:
      circuit_breaker.record_failure(graph_id, "database_health")
      raise HTTPException(
        status_code=status.HTTP_408_REQUEST_TIMEOUT,
        detail="Health check timed out",
      )
    except Exception as e:
      circuit_breaker.record_failure(graph_id, "database_health")
      if "not found" in str(e).lower():
        raise HTTPException(
          status_code=status.HTTP_404_NOT_FOUND,
          detail="Database not found",
        )
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Failed to retrieve health information",
      )
    finally:
      await graph_client.close()

  except HTTPException:
    raise
  except Exception as e:
    circuit_breaker.record_failure(graph_id, "database_health")
    logger.error(f"Unexpected error getting health for graph {graph_id}: {e!s}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="An unexpected error occurred while retrieving health information",
    )
