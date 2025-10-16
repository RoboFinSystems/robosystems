"""
Graph database operational limits endpoint.

This module provides REST API endpoints for retrieving operational limits.
"""

import asyncio
from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session

from robosystems.database import get_async_db_session
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.models.iam import User
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.middleware.graph.dependencies import get_universal_repository_with_auth
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.graph_api.client import KuzuClient
from robosystems.logger import logger
from robosystems.middleware.robustness import (
  CircuitBreakerManager,
  TimeoutCoordinator,
)

# Create router
router = APIRouter(tags=["Graph Limits"])

# Initialize robustness components
circuit_breaker = CircuitBreakerManager()
timeout_coordinator = TimeoutCoordinator()


async def _get_kuzu_client(graph_id: str) -> KuzuClient:
  """Get Kuzu client for the specified graph using factory for endpoint discovery."""
  from robosystems.graph_api.client.factory import KuzuClientFactory
  from robosystems.middleware.graph.multitenant_utils import MultiTenantUtils

  # Determine operation type based on graph
  # Shared repositories are read-only from the application perspective
  operation_type = (
    "read" if MultiTenantUtils.is_shared_repository(graph_id) else "write"
  )

  # Create client using factory for endpoint discovery
  # Factory automatically handles routing:
  # - Shared repos: Routes to shared_master/shared_replica
  # - User graphs: Looks up tier from database and routes appropriately
  client = await KuzuClientFactory.create_client(
    graph_id=graph_id, operation_type=operation_type
  )

  return client


@router.get(
  "/limits",
  summary="Get Graph Operational Limits",
  description="""Get comprehensive operational limits for the graph database.

Returns all operational limits that apply to this graph including:
- **Storage Limits**: Maximum storage size and current usage
- **Query Limits**: Timeouts, complexity, row limits
- **Copy/Ingestion Limits**: File sizes, timeouts, concurrent operations
- **Backup Limits**: Frequency, retention, size limits
- **Rate Limits**: Requests per minute/hour based on tier
- **Credit Limits**: AI operation credits (if applicable)

This unified endpoint provides all limits in one place for easier client integration.

**Note**: Limits vary based on subscription tier (Standard, Enterprise, Premium).""",
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
    pattern="^(kg[a-z0-9]{10}|sec|industry|economic|[a-zA-Z][a-zA-Z0-9_]{2,62})$",
  ),
  current_user: User = Depends(get_current_user),
  session: Session = Depends(get_async_db_session),
  _: None = Depends(subscription_aware_rate_limit_dependency),
) -> Dict[str, Any]:
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
    # Verify user has read access to this graph
    await get_universal_repository_with_auth(graph_id, current_user, "read", session)

    # Import needed functions
    from robosystems.routers.graphs.copy.validation import (
      get_tier_limits as get_copy_tier_limits,
    )
    from robosystems.models.iam.graph import Graph
    from robosystems.models.iam.graph_credits import GraphCredits
    from robosystems.middleware.graph.multitenant_utils import MultiTenantUtils

    # Get user's subscription tier
    user_tier = getattr(current_user, "subscription_tier", "standard")

    # Get graph information if it exists
    graph = session.query(Graph).filter(Graph.graph_id == graph_id).first()
    graph_tier = graph.graph_tier if graph else user_tier

    # Get storage information
    storage_limits = {}
    try:
      kuzu_client = await _get_kuzu_client(graph_id)
      db_info = await asyncio.wait_for(
        kuzu_client.get_database_info(graph_id), timeout=10
      )
      await kuzu_client.close()

      current_storage_gb = db_info.get("database_size_bytes", 0) / (1024**3)
      max_storage_gb = {
        "standard": 10,
        "enterprise": 50,
        "premium": 100,
      }.get(user_tier, 10)

      storage_limits = {
        "current_usage_gb": round(current_storage_gb, 2),
        "max_storage_gb": max_storage_gb,
        "approaching_limit": current_storage_gb > (max_storage_gb * 0.8),
      }
    except Exception as e:
      logger.warning(f"Could not get storage info for {graph_id}: {e}")
      storage_limits = {
        "current_usage_gb": None,
        "max_storage_gb": {
          "standard": 10,
          "enterprise": 50,
          "premium": 100,
        }.get(user_tier, 10),
        "approaching_limit": False,
      }

    # Get copy/ingestion limits
    copy_limits = get_copy_tier_limits(current_user)

    # Define query limits based on tier
    query_limits = {
      "max_timeout_seconds": {
        "standard": 60,
        "enterprise": 120,
        "premium": 300,
      }.get(user_tier, 60),
      "max_rows_per_query": {
        "standard": 10000,
        "enterprise": 100000,
        "premium": -1,  # Unlimited
      }.get(user_tier, 10000),
      "chunk_size": {
        "standard": 1000,
        "enterprise": 5000,
        "premium": 10000,
      }.get(user_tier, 1000),
      "concurrent_queries": {
        "standard": 1,
        "enterprise": 3,
        "premium": 5,
      }.get(user_tier, 1),
    }

    # Define backup limits based on tier
    backup_limits = {
      "max_backup_size_gb": {
        "standard": 10,
        "enterprise": 50,
        "premium": 100,
      }.get(user_tier, 10),
      "backup_retention_days": {
        "standard": 7,
        "enterprise": 30,
        "premium": 90,
      }.get(user_tier, 7),
      "max_backups_per_day": {
        "standard": 2,
        "enterprise": 10,
        "premium": -1,  # Unlimited
      }.get(user_tier, 2),
    }

    # Define rate limits based on tier
    rate_limits = {
      "requests_per_minute": {
        "standard": 60,
        "enterprise": 300,
        "premium": 600,
      }.get(user_tier, 60),
      "requests_per_hour": {
        "standard": 1000,
        "enterprise": 10000,
        "premium": 50000,
      }.get(user_tier, 1000),
      "burst_capacity": {
        "standard": 10,
        "enterprise": 50,
        "premium": 100,
      }.get(user_tier, 10),
    }

    # Get credit limits if applicable
    credit_limits = {}
    if not MultiTenantUtils.is_shared_repository(graph_id):
      try:
        graph_credits = (
          session.query(GraphCredits).filter(GraphCredits.graph_id == graph_id).first()
        )
        if graph_credits:
          credit_limits = {
            "monthly_ai_credits": graph_credits.monthly_credit_limit,
            "current_balance": graph_credits.current_balance,
            "storage_billing_enabled": graph_credits.storage_billing_enabled,
            "storage_rate_per_gb_per_day": 10
            if graph_credits.storage_billing_enabled
            else 0,
          }
      except Exception:
        pass

    # Build comprehensive response
    response = {
      "graph_id": graph_id,
      "subscription_tier": user_tier,
      "graph_tier": graph_tier,
      "is_shared_repository": MultiTenantUtils.is_shared_repository(graph_id),
      "storage": storage_limits,
      "queries": query_limits,
      "copy_operations": {
        "max_file_size_gb": copy_limits["max_file_size_gb"],
        "timeout_seconds": copy_limits["timeout_seconds"],
        "concurrent_operations": copy_limits["concurrent_operations"],
        "max_files_per_operation": copy_limits["max_files_per_operation"],
        "daily_copy_operations": copy_limits["daily_copy_operations"],
        "supported_formats": ["parquet", "csv", "json", "delta", "iceberg"],
      },
      "backups": backup_limits,
      "rate_limits": rate_limits,
    }

    # Add credit limits if they exist
    if credit_limits:
      response["credits"] = credit_limits

    # Record success
    circuit_breaker.record_success(graph_id, "graph_limits")

    return response

  except HTTPException:
    raise
  except Exception as e:
    circuit_breaker.record_failure(graph_id, "graph_limits")
    logger.error(f"Failed to get limits for graph {graph_id}: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to retrieve graph limits",
    )
