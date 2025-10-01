"""
Unprotected status endpoint for load balancers and monitoring.

This endpoint is intentionally unprotected as it's used by AWS resources
for health checks and monitoring.
"""

from datetime import datetime, timezone
from fastapi import APIRouter
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
)
from robosystems.models.api.common import HealthStatus
import tomllib
from pathlib import Path

# Create router without authentication requirements
router = APIRouter()

# Use centralized metrics - no need for global variables


def get_app_version() -> str:
  """Get the application version from pyproject.toml."""
  try:
    # Find pyproject.toml in parent directories
    current_path = Path(__file__).resolve()
    for parent in current_path.parents:
      pyproject_path = parent / "pyproject.toml"
      if pyproject_path.exists():
        with open(pyproject_path, "rb") as f:
          data = tomllib.load(f)
          return data.get("project", {}).get("version", "unknown")
    return "unknown"
  except Exception:
    # If we can't read the version, return unknown
    return "unknown"


@router.get(
  "/status",
  response_model=HealthStatus,
  operation_id="getServiceStatus",
  summary="Health Check",
  description="Service health check endpoint for monitoring and load balancers",
  responses={200: {"description": "Service is healthy", "model": HealthStatus}},
)
@endpoint_metrics_decorator(endpoint_name="/v1/status")
async def service_status():
  """
  Service status endpoint for AWS load balancer and monitoring.

  This endpoint is unprotected and used by:
  - AWS Application Load Balancer health checks
  - AWS ECS service health checks
  - CloudFormation monitoring
  - External monitoring systems

  Returns:
      Simple status message indicating service is healthy
  """
  return HealthStatus(
    status="healthy",
    timestamp=datetime.now(timezone.utc),
    details={"service": "robosystems-api", "version": get_app_version()},
  )
