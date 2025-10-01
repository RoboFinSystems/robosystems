"""
RoboSystems Middleware Package

This package contains middleware components for the RoboSystems service,
including tracing, authentication, graph database operations, and other
cross-cutting concerns.
"""

from .otel.setup import setup_telemetry, get_tracer, shutdown_telemetry

# Authentication middleware
from .auth import (
  get_current_user,
  get_optional_user,
  validate_api_key,
)

# Graph database middleware
from .graph import (
  GraphOperation,
  get_graph_repository,
)

__all__ = [
  # Tracing
  "setup_telemetry",
  "get_tracer",
  "shutdown_telemetry",
  # Authentication
  "get_current_user",
  "get_optional_user",
  "validate_api_key",
  # Graph database
  "GraphOperation",
  "get_graph_repository",
]
