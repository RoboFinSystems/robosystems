"""Cross-cutting middleware: authentication, graph routing, billing,
rate limiting, SSE, MCP, telemetry, and operation dispatch.
"""

from .auth import (
  get_current_user,
  get_optional_user,
  validate_api_key,
)
from .graph import (
  GraphOperation,
  get_graph_repository,
)
from .otel.setup import get_tracer, setup_telemetry, shutdown_telemetry

__all__ = [
  # Graph database
  "GraphOperation",
  # Authentication
  "get_current_user",
  "get_graph_repository",
  "get_optional_user",
  "get_tracer",
  # Tracing
  "setup_telemetry",
  "shutdown_telemetry",
  "validate_api_key",
]
