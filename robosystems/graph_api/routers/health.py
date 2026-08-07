"""Health check endpoint for load balancers and monitoring.

Deliberately cheap — no deep validation, so it stays fast under load. Shared
replicas return 503 until their databases finish warming up (see the warmup
task in ``robosystems/graph_api/app.py``), keeping the ALB from routing traffic
to an instance that would answer from cold disk.
"""

import os
import threading

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.logger import logger

router = APIRouter(tags=["Cluster Health"])

# Track replica warmup status (thread-safe for multi-worker setups)
_replica_ready = False
_replica_lock = threading.Lock()


def mark_replica_ready():
  """Mark the replica as ready to serve queries."""
  global _replica_ready
  with _replica_lock:
    _replica_ready = True
  logger.info("Replica marked as ready - health check will now return 200")


def is_warming_up() -> bool:
  """True while this node is a replica whose databases are still loading.

  Callers use it to answer 503 rather than 404 when a database is missing.
  """
  return os.getenv("LBUG_ROLE") == "replica" and not _replica_ready


@router.get("/health")
async def health_check(
  service=Depends(get_ladybug_service),
) -> JSONResponse:
  """
  Simple health check endpoint for load balancers and monitoring.

  Returns 200 OK if the service is running and can respond to requests.
  This is a lightweight check that doesn't perform deep validation.

  For shared replicas, returns 503 until databases are fully warmed up.
  This prevents ALB from routing traffic during the warmup period.

  Used by:
  - AWS Application Load Balancer health checks
  - Auto Scaling Group health checks
  - EC2 instance health monitoring
  - Kubernetes liveness probes
  """
  if os.getenv("LBUG_ROLE") == "replica" and not _replica_ready:
    logger.debug("Replica warmup in progress - returning 503")
    return JSONResponse(
      status_code=503,
      content={
        "status": "warming",
        "message": "Replica warming up - not ready for traffic",
      },
    )

  from robosystems.graph_api.core.migration_service import is_migration_in_progress

  if is_migration_in_progress():
    logger.debug("Migration in progress - returning 503")
    return JSONResponse(
      status_code=503,
      content={
        "status": "migrating",
        "message": "Version migration in progress - not ready for traffic",
      },
    )

  try:
    uptime = service.get_uptime()

    database_count = len(service.db_manager.list_databases())

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
    # Detail goes to the log, not the response — avoid information disclosure.
    logger.error(f"Health check failed: {e!s}")
    return JSONResponse(
      status_code=503,
      content={"status": "unhealthy", "error": "Service temporarily unavailable"},
    )
