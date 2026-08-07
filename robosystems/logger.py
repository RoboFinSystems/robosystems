"""
Application-wide loggers and the shortest path to emitting a structured event.

Importing this module configures logging as a side effect: it calls
`setup_logging()` from `config.logging` (structured JSON in production,
CloudWatch log-group routing) and quiets OpenTelemetry, boto, and HTTP client
loggers in development, where their output otherwise drowns the app's own.

Import `logger` for ordinary messages, or one of the component loggers
(`api_logger`, `worker_logger`, `lbug_logger`, `security_logger`) to route an
event to its own log group. The `log_*` helpers below wrap the field-typed
emitters in `config.logging` with this module's loggers already bound.
"""

import logging
from typing import Any

from .config import env
from .config.logging import (
  get_logger,
  log_api_request,
  log_database_query,
  log_error,
  log_performance_metric,
  log_security_event,
  performance_timer,
  setup_logging,
)

setup_logging()

logger = get_logger("robosystems")

if env.is_development():
  logging.getLogger("opentelemetry").setLevel(logging.WARNING)
  logging.getLogger("opentelemetry.sdk").setLevel(logging.WARNING)
  logging.getLogger("opentelemetry.exporter").setLevel(logging.WARNING)
  logging.getLogger("opentelemetry.instrumentation").setLevel(logging.WARNING)

  # httpx/httpcore are noisy because the OTEL exporters run over them
  logging.getLogger("httpx").setLevel(logging.WARNING)
  logging.getLogger("httpcore").setLevel(logging.WARNING)

  logging.getLogger("boto3").setLevel(logging.WARNING)
  logging.getLogger("botocore").setLevel(logging.WARNING)
  logging.getLogger("urllib3").setLevel(logging.WARNING)
  logging.getLogger("requests").setLevel(logging.WARNING)

# Per-component loggers, each routed to its own CloudWatch log group
api_logger = get_logger("robosystems.api")
worker_logger = get_logger("robosystems.workers")
lbug_logger = get_logger("robosystems.lbug")
security_logger = get_logger("robosystems.security")


def log_api(
  method: str,
  path: str,
  status_code: int,
  duration_ms: float,
  user_id: str | None = None,
  entity_id: str | None = None,
  request_id: str | None = None,
) -> None:
  """Log API requests with structured data."""
  log_api_request(
    api_logger, method, path, status_code, duration_ms, user_id, entity_id, request_id
  )


def log_db_query(
  database: str,
  query_type: str,
  duration_ms: float,
  row_count: int | None = None,
  user_id: str | None = None,
  entity_id: str | None = None,
) -> None:
  """Log database queries with performance tracking."""
  log_database_query(
    lbug_logger, database, query_type, duration_ms, row_count, user_id, entity_id
  )


def log_app_error(
  error: Exception,
  component: str,
  action: str,
  error_category: str = "application",
  user_id: str | None = None,
  entity_id: str | None = None,
  metadata: dict[str, Any] | None = None,
) -> None:
  """Log application errors with context."""
  log_error(
    logger, error, component, action, error_category, user_id, entity_id, metadata
  )


def log_auth_event(
  event_type: str,
  user_id: str | None = None,
  ip_address: str | None = None,
  success: bool = True,
  metadata: dict[str, Any] | None = None,
) -> None:
  """Log security/authentication events."""
  log_security_event(
    security_logger, event_type, user_id, ip_address, success, metadata
  )


def log_metric(
  metric_name: str,
  value: int | float,
  unit: str = "count",
  component: str = "system",
  metadata: dict[str, Any] | None = None,
) -> None:
  """Log performance metrics."""
  log_performance_metric(logger, metric_name, value, unit, component, metadata)


__all__ = [
  "api_logger",
  "get_logger",
  "lbug_logger",
  "log_api",
  "log_api_request",
  "log_app_error",
  "log_auth_event",
  "log_database_query",
  "log_db_query",
  "log_error",
  "log_metric",
  "log_performance_metric",
  "log_security_event",
  "logger",
  "performance_timer",
  "security_logger",
  "worker_logger",
]
