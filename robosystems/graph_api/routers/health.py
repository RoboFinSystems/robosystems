"""
Simple health check endpoint for load balancers and monitoring.

This module provides a minimal health check endpoint that returns
quickly for infrastructure health monitoring.

For S3 ATTACH replicas, the health check returns 503 until the database
is fully loaded and ready to serve queries. This prevents ALB from routing
traffic to instances still warming up (which can take ~10 minutes).
"""

import os

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from robosystems.config import env
from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.logger import logger

router = APIRouter(tags=["Cluster Health"])

# Track S3 ATTACH warmup status
# This is set to True after the first successful query to the attached database
_s3_attach_ready = False


def mark_s3_attach_ready():
  """Mark the S3 ATTACH database as ready to serve queries."""
  global _s3_attach_ready
  _s3_attach_ready = True
  logger.info("S3 ATTACH database marked as ready - health check will now return 200")


def is_s3_attach_mode() -> bool:
  """Check if running in S3 ATTACH replica mode."""
  return bool(os.getenv("LBUG_S3_ATTACH_URI")) and os.getenv("LBUG_ROLE") == "replica"


def _get_service_for_health():
  """Get the appropriate service based on backend configuration."""
  backend_type = env.GRAPH_BACKEND_TYPE
  if backend_type in ["neo4j_community", "neo4j_enterprise"]:
    from robosystems.graph_api.core.neo4j import Neo4jService

    return Neo4jService()
  else:
    return get_ladybug_service()


@router.get("/health")
async def health_check(
  service=Depends(_get_service_for_health),
) -> JSONResponse:
  """
  Simple health check endpoint for load balancers and monitoring.

  Returns 200 OK if the service is running and can respond to requests.
  This is a lightweight check that doesn't perform deep validation.

  For S3 ATTACH replicas, returns 503 until the database is fully loaded.
  This prevents ALB from routing traffic during the ~10 min warmup period.

  Used by:
  - AWS Application Load Balancer health checks
  - Auto Scaling Group health checks
  - EC2 instance health monitoring
  - Kubernetes liveness probes
  """
  # S3 ATTACH mode: return 503 until database is ready
  if is_s3_attach_mode() and not _s3_attach_ready:
    logger.debug("S3 ATTACH warmup in progress - returning 503")
    return JSONResponse(
      status_code=503,
      content={
        "status": "warming",
        "message": "S3 ATTACH database warming up - not ready for traffic",
      },
    )

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
    logger.error(f"Health check failed: {e!s}")
    # Return generic error message to avoid information disclosure
    return JSONResponse(
      status_code=503,
      content={"status": "unhealthy", "error": "Service temporarily unavailable"},
    )
