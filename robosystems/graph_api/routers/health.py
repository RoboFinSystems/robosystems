"""
Simple health check endpoint for load balancers and monitoring.

This module provides a minimal health check endpoint that returns
quickly for infrastructure health monitoring.
"""

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from robosystems.config import env
from robosystems.logger import logger
from robosystems.graph_api.core.ladybug import get_ladybug_service

router = APIRouter(tags=["Health"])


def _get_service_for_health(
  ladybug_service=Depends(get_ladybug_service),
):
  """Get the appropriate service based on backend configuration."""
  backend_type = env.GRAPH_BACKEND_TYPE
  if backend_type in ["neo4j_community", "neo4j_enterprise"]:
    from robosystems.graph_api.core.neo4j import Neo4jService

    return Neo4jService()
  return ladybug_service


@router.get("/health")
async def health_check(
  service=Depends(_get_service_for_health),
) -> JSONResponse:
  """
  Simple health check endpoint for load balancers and monitoring.

  Returns 200 OK if the service is running and can respond to requests.
  This is a lightweight check that doesn't perform deep validation.

  Used by:
  - AWS Application Load Balancer health checks
  - Auto Scaling Group health checks
  - EC2 instance health monitoring
  - Kubernetes liveness probes
  """
  try:
    # Basic check that service is accessible
    uptime = service.get_uptime()

    # Get database count (different for LadybugDB vs Neo4j)
    database_count = 0
    if hasattr(service, "db_manager"):
      # LadybugDB service
      database_count = len(service.db_manager.list_databases())
    elif hasattr(service, "backend"):
      # Neo4j service - get databases from backend
      try:
        databases = await service.backend.list_databases()
        database_count = len(databases)
      except Exception:
        # If listing databases fails, just continue with 0
        pass

    # Include memory usage if psutil is available
    memory_info = {}
    try:
      import psutil

      process = psutil.Process()
      mem = process.memory_info()
      memory_info = {
        "memory_rss_mb": round(mem.rss / (1024 * 1024), 1),
        "memory_vms_mb": round(mem.vms / (1024 * 1024), 1),
        "memory_percent": round(process.memory_percent(), 2),
      }
    except ImportError:
      pass

    return JSONResponse(
      status_code=200,
      content={
        "status": "healthy",
        "uptime_seconds": uptime,
        "database_count": database_count,
        **memory_info,
      },
    )
  except Exception as e:
    # Log the detailed error securely
    logger.error(f"Health check failed: {str(e)}")
    # Return generic error message to avoid information disclosure
    return JSONResponse(
      status_code=503,
      content={"status": "unhealthy", "error": "Service temporarily unavailable"},
    )
