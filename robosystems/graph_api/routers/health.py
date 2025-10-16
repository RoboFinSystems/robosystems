"""
Simple health check endpoint for load balancers and monitoring.

This module provides a minimal health check endpoint that returns
quickly for infrastructure health monitoring.
"""

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from robosystems.graph_api.core.cluster_manager import get_cluster_service
from robosystems.logger import logger

router = APIRouter(tags=["Cluster Health"])


@router.get("/health")
async def health_check(
  cluster_service=Depends(get_cluster_service),
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
    # Basic check that cluster service is accessible
    uptime = cluster_service.get_uptime()
    databases = len(cluster_service.db_manager.list_databases())

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
        "database_count": databases,
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
