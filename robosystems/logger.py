"""
RoboSystems Unified Logging System

This module provides a unified logging interface that integrates:
1. Structured CloudWatch-optimized logging for production
2. OTEL noise suppression for clean development logs
3. Backward compatibility with existing logger usage

Key features preserved:
- OTEL noise suppression (essential for development)
- Environment-based configuration
- Simple logger interface

Enhanced features:
- Structured JSON logging for production
- CloudWatch log group routing
- Cost-optimized tiered logging
"""

import logging
from typing import Optional, Dict, Any, Union

from .config import env
from .config.logging import (
  setup_logging,
  get_logger,
  log_api_request,
  log_database_query,
  log_error,
  log_security_event,
  log_performance_metric,
  performance_timer,
)

# Initialize the advanced structured logging system
setup_logging()

# Create our main application logger with structured capabilities
logger = get_logger("robosystems")

# Preserve OTEL noise suppression - critical for development usability
if env.is_development():
  # Suppress verbose OpenTelemetry logging
  logging.getLogger("opentelemetry").setLevel(logging.WARNING)
  logging.getLogger("opentelemetry.sdk").setLevel(logging.WARNING)
  logging.getLogger("opentelemetry.exporter").setLevel(logging.WARNING)
  logging.getLogger("opentelemetry.instrumentation").setLevel(logging.WARNING)

  # Also suppress httpx/httpcore noise from OTEL exporters
  logging.getLogger("httpx").setLevel(logging.WARNING)
  logging.getLogger("httpcore").setLevel(logging.WARNING)

  # Suppress additional noisy AWS/boto loggers in development
  logging.getLogger("boto3").setLevel(logging.WARNING)
  logging.getLogger("botocore").setLevel(logging.WARNING)
  logging.getLogger("urllib3").setLevel(logging.WARNING)
  logging.getLogger("requests").setLevel(logging.WARNING)

# Specialized loggers for different components (structured logging enabled)
api_logger = get_logger("robosystems.api")
worker_logger = get_logger("robosystems.workers")
kuzu_logger = get_logger("robosystems.kuzu")
security_logger = get_logger("robosystems.security")


# Convenience functions that preserve existing usage patterns while adding structure
def log_api(
  method: str,
  path: str,
  status_code: int,
  duration_ms: float,
  user_id: Optional[str] = None,
  entity_id: Optional[str] = None,
  request_id: Optional[str] = None,
) -> None:
  """Log API requests with structured data."""
  log_api_request(
    api_logger, method, path, status_code, duration_ms, user_id, entity_id, request_id
  )


def log_db_query(
  database: str,
  query_type: str,
  duration_ms: float,
  row_count: Optional[int] = None,
  user_id: Optional[str] = None,
  entity_id: Optional[str] = None,
) -> None:
  """Log database queries with performance tracking."""
  log_database_query(
    kuzu_logger, database, query_type, duration_ms, row_count, user_id, entity_id
  )


def log_app_error(
  error: Exception,
  component: str,
  action: str,
  error_category: str = "application",
  user_id: Optional[str] = None,
  entity_id: Optional[str] = None,
  metadata: Optional[Dict[str, Any]] = None,
) -> None:
  """Log application errors with context."""
  log_error(
    logger, error, component, action, error_category, user_id, entity_id, metadata
  )


def log_auth_event(
  event_type: str,
  user_id: Optional[str] = None,
  ip_address: Optional[str] = None,
  success: bool = True,
  metadata: Optional[Dict[str, Any]] = None,
) -> None:
  """Log security/authentication events."""
  log_security_event(
    security_logger, event_type, user_id, ip_address, success, metadata
  )


def log_metric(
  metric_name: str,
  value: Union[int, float],
  unit: str = "count",
  component: str = "system",
  metadata: Optional[Dict[str, Any]] = None,
) -> None:
  """Log performance metrics."""
  log_performance_metric(logger, metric_name, value, unit, component, metadata)


# Export all logging capabilities
__all__ = [
  "logger",  # Main backward-compatible logger
  "api_logger",  # API-specific structured logger
  "worker_logger",  # Worker-specific structured logger
  "kuzu_logger",  # Database-specific structured logger
  "security_logger",  # Security-specific structured logger
  # Convenience functions
  "log_api",
  "log_db_query",
  "log_app_error",
  "log_auth_event",
  "log_metric",
  # Advanced functions from config.logging
  "log_api_request",
  "log_database_query",
  "log_error",
  "log_security_event",
  "log_performance_metric",
  "performance_timer",
  "get_logger",
]
