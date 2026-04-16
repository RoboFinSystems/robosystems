"""Unprotected service status endpoint for load balancers and monitoring."""

from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version

from fastapi import APIRouter

from robosystems.models.api.common import HealthStatus

router = APIRouter()


def get_app_version() -> str:
  """Get the application version from installed package metadata."""
  try:
    return version("robosystems")
  except PackageNotFoundError:
    return "unknown"


@router.get(
  "/status",
  response_model=HealthStatus,
  summary="Health Check",
  description="Unprotected — used by load balancers and monitoring. No authentication required.",
  operation_id="getServiceStatus",
)
async def service_status():
  return HealthStatus(
    status="healthy",
    timestamp=datetime.now(UTC),
    details={"service": "robosystems-api", "version": get_app_version()},
  )
