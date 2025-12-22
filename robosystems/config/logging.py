"""
Cost-Optimized Structured Logging Configuration for RoboSystems

This module provides structured logging that's optimized for AWS CloudWatch
and designed to be AWS CLI-friendly for debugging production issues.

Key Features:
- Tiered logging (Critical/Operational/Debug) for cost optimization
- Structured JSON output for CloudWatch Insights queries
- AWS CLI-friendly search patterns
- Automatic log level management by environment
- Performance tracking and error categorization
"""

import json
import logging
import logging.config
import time
import traceback
from datetime import datetime
from functools import wraps
from typing import Any

from robosystems.config.env import EnvConfig


class StructuredFormatter(logging.Formatter):
  """
  JSON formatter that creates AWS CLI-searchable structured logs.

  Output format optimized for CloudWatch Insights queries:
  - Timestamp in ISO format
  - Consistent field names for filtering
  - Hierarchical component/action structure
  - Metadata preserved as searchable fields
  """

  def format(self, record: logging.LogRecord) -> str:
    # Base log structure
    log_entry = {
      "timestamp": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
      "level": record.levelname,
      "component": getattr(record, "component", record.name),
      "message": record.getMessage(),
    }

    # Add action if specified (for searching specific operations)
    if hasattr(record, "action"):
      log_entry["action"] = record.action

    # Add user context if available
    if hasattr(record, "user_id"):
      log_entry["user_id"] = record.user_id
    if hasattr(record, "entity_id"):
      log_entry["entity_id"] = record.entity_id
    if hasattr(record, "database"):
      log_entry["database"] = record.database

    # Add performance metrics if available
    if hasattr(record, "duration_ms"):
      log_entry["duration_ms"] = record.duration_ms
    if hasattr(record, "status_code"):
      log_entry["status_code"] = record.status_code

    # Add error details for ERROR/CRITICAL logs
    if record.levelno >= logging.ERROR:
      if record.exc_info:
        log_entry["error"] = {
          "type": record.exc_info[0].__name__ if record.exc_info[0] else "Unknown",
          "message": str(record.exc_info[1]) if record.exc_info[1] else "",
          "traceback": traceback.format_exception(*record.exc_info),
        }

      # Add error category for easier searching
      if hasattr(record, "error_category"):
        log_entry["error_category"] = record.error_category

    # Add any additional metadata
    if hasattr(record, "metadata"):
      log_entry["metadata"] = record.metadata

    # Add request ID for tracing
    if hasattr(record, "request_id"):
      log_entry["request_id"] = record.request_id

    return json.dumps(log_entry, default=str, separators=(",", ":"))


class TieredLogFilter:
  """
  Filter logs by tier to control costs.

  Tier 1 (Critical): ERROR, CRITICAL
  Tier 2 (Operational): INFO, WARNING
  Tier 3 (Debug): DEBUG
  """

  def __init__(self, tier: str):
    self.tier = tier

  def filter(self, record: logging.LogRecord) -> bool:
    if self.tier == "critical":
      return record.levelno >= logging.ERROR
    elif self.tier == "operational":
      return logging.INFO <= record.levelno < logging.ERROR
    elif self.tier == "debug":
      return record.levelno == logging.DEBUG
    return True


def get_logging_config(environment: str | None = None) -> dict[str, Any]:
  """
  Generate logging configuration based on environment.

  Cost optimization by environment:
  - prod: INFO level, structured output, no debug logs
  - staging: INFO level, with debug logs enabled
  - test: WARNING level, minimal output for clean test runs
  - dev: DEBUG level, all logs enabled (unless LOG_LEVEL overrides)
  """
  env = environment or EnvConfig.ENVIRONMENT

  # Check for LOG_LEVEL override
  log_level_override = getattr(EnvConfig, "LOG_LEVEL", None)

  # Environment-specific settings
  if env == "prod":
    default_level = "INFO"
    enable_debug = False
  elif env == "staging":
    default_level = "INFO"
    enable_debug = True
  elif env == "test":
    default_level = "WARNING"  # Quieter tests - only warnings/errors
    enable_debug = False
  else:  # dev
    default_level = log_level_override or "DEBUG"
    enable_debug = default_level == "DEBUG"

  config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
      "structured": {
        "()": StructuredFormatter,
      },
      "simple": {"format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"},
    },
    "filters": {
      "critical_filter": {"()": TieredLogFilter, "tier": "critical"},
      "operational_filter": {"()": TieredLogFilter, "tier": "operational"},
      "debug_filter": {"()": TieredLogFilter, "tier": "debug"},
    },
    "handlers": {
      # Critical logs - errors and failures
      "critical": {
        "class": "logging.StreamHandler",
        "level": "ERROR",
        "formatter": "structured",
        "filters": ["critical_filter"],
        "stream": "ext://sys.stderr",
      },
      # Operational logs - business logic and API requests
      "operational": {
        "class": "logging.StreamHandler",
        "level": "INFO",
        "formatter": "structured",
        "filters": ["operational_filter"],
        "stream": "ext://sys.stdout",
      },
      # Console output for development
      "console": {
        "class": "logging.StreamHandler",
        "level": default_level,
        "formatter": "simple" if env == "dev" else "structured",
        "stream": "ext://sys.stdout",
      },
    },
    "loggers": {
      # Application loggers
      "robosystems": {
        "level": default_level,
        "handlers": ["critical", "operational"] if env != "dev" else ["console"],
        "propagate": False,
      },
      "robosystems.api": {
        "level": default_level,
        "handlers": ["critical", "operational"] if env != "dev" else ["console"],
        "propagate": False,
      },
      "robosystems.workers": {
        "level": default_level,
        "handlers": ["critical", "operational"] if env != "dev" else ["console"],
        "propagate": False,
      },
      "robosystems.lbug": {
        "level": default_level,
        "handlers": ["critical", "operational"] if env != "dev" else ["console"],
        "propagate": False,
      },
      # Third-party loggers (reduced verbosity)
      "uvicorn": {
        "level": "WARNING",
        "handlers": ["operational"] if env != "dev" else ["console"],
        "propagate": False,
      },
      "sqlalchemy": {
        "level": "WARNING",
        "handlers": ["operational"] if env != "dev" else ["console"],
        "propagate": False,
      },
      "boto3": {
        "level": "WARNING",
        "handlers": ["critical"] if env != "dev" else ["console"],
        "propagate": False,
      },
      "botocore": {
        "level": "WARNING",
        "handlers": ["critical"] if env != "dev" else ["console"],
        "propagate": False,
      },
      # Suppress OpenTelemetry export errors
      "opentelemetry.exporter.otlp": {
        "level": "CRITICAL",  # Only show critical errors, suppresses ERROR level
        "handlers": ["critical"] if env != "dev" else ["console"],
        "propagate": False,
      },
    },
    "root": {
      "level": "WARNING",
      "handlers": ["critical"] if env != "dev" else ["console"],
    },
  }

  # Add debug handler for staging/dev
  if enable_debug:
    config["handlers"]["debug"] = {
      "class": "logging.StreamHandler",
      "level": "DEBUG",
      "formatter": "structured",
      "filters": ["debug_filter"],
      "stream": "ext://sys.stdout",
    }

    # Add debug handler to application loggers
    for logger_name in [
      "robosystems",
      "robosystems.api",
      "robosystems.workers",
      "robosystems.lbug",
    ]:
      if env != "dev":
        config["loggers"][logger_name]["handlers"].append("debug")

  return config


def setup_logging(environment: str | None = None) -> None:
  """Initialize structured logging configuration."""
  config = get_logging_config(environment)
  logging.config.dictConfig(config)


# Logging utility functions for structured logging
def get_logger(name: str) -> logging.Logger:
  """Get a logger with structured logging capabilities."""
  return logging.getLogger(name)


def log_api_request(
  logger: logging.Logger,
  method: str,
  path: str,
  status_code: int,
  duration_ms: float,
  user_id: str | None = None,
  entity_id: str | None = None,
  request_id: str | None = None,
) -> None:
  """Log API request with structured data for CloudWatch searching."""
  logger.info(
    f"{method} {path} - {status_code} ({duration_ms:.2f}ms)",
    extra={
      "component": "api",
      "action": "request_completed",
      "method": method,
      "path": path,
      "status_code": status_code,
      "duration_ms": duration_ms,
      "user_id": user_id,
      "entity_id": entity_id,
      "request_id": request_id,
    },
  )


def log_database_query(
  logger: logging.Logger,
  database: str,
  query_type: str,
  duration_ms: float,
  row_count: int | None = None,
  user_id: str | None = None,
  entity_id: str | None = None,
) -> None:
  """Log database query with performance metrics."""
  extra = {
    "component": "database",
    "action": "query_executed",
    "database": database,
    "query_type": query_type,
    "duration_ms": duration_ms,
    "user_id": user_id,
    "entity_id": entity_id,
  }

  if row_count is not None:
    extra["row_count"] = row_count

  if duration_ms > 1000:  # Slow query threshold
    logger.warning(
      f"Slow {query_type} query on {database} ({duration_ms:.2f}ms)", extra=extra
    )
  else:
    logger.info(f"{query_type} query on {database} ({duration_ms:.2f}ms)", extra=extra)


def log_error(
  logger: logging.Logger,
  error: Exception,
  component: str,
  action: str,
  error_category: str = "application",
  user_id: str | None = None,
  entity_id: str | None = None,
  metadata: dict[str, Any] | None = None,
) -> None:
  """Log error with structured data for easy searching."""
  logger.error(
    f"Error in {component}.{action}: {error!s}",
    exc_info=True,
    extra={
      "component": component,
      "action": action,
      "error_category": error_category,
      "user_id": user_id,
      "entity_id": entity_id,
      "metadata": metadata or {},
    },
  )


def log_security_event(
  logger: logging.Logger,
  event_type: str,
  user_id: str | None = None,
  ip_address: str | None = None,
  success: bool = True,
  metadata: dict[str, Any] | None = None,
) -> None:
  """Log security events for monitoring and alerting."""
  level = logging.INFO if success else logging.WARNING

  logger.log(
    level,
    f"Security event: {event_type} - {'Success' if success else 'Failed'}",
    extra={
      "component": "security",
      "action": event_type,
      "user_id": user_id,
      "ip_address": ip_address,
      "success": success,
      "metadata": metadata or {},
    },
  )


def log_performance_metric(
  logger: logging.Logger,
  metric_name: str,
  value: int | float,
  unit: str = "count",
  component: str = "system",
  metadata: dict[str, Any] | None = None,
) -> None:
  """Log performance metrics for monitoring."""
  logger.info(
    f"Performance metric: {metric_name} = {value} {unit}",
    extra={
      "component": "performance",
      "action": "metric_recorded",
      "metric_name": metric_name,
      "metric_value": value,
      "unit": unit,
      "source_component": component,
      "metadata": metadata or {},
    },
  )


def performance_timer(
  logger: logging.Logger,
  component: str,
  action: str,
  user_id: str | None = None,
  entity_id: str | None = None,
):
  """Decorator to automatically log function execution time."""

  def decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
      start_time = time.time()
      try:
        result = func(*args, **kwargs)
        duration_ms = (time.time() - start_time) * 1000

        logger.info(
          f"{component}.{action} completed ({duration_ms:.2f}ms)",
          extra={
            "component": component,
            "action": action,
            "duration_ms": duration_ms,
            "user_id": user_id,
            "entity_id": entity_id,
            "success": True,
          },
        )
        return result
      except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        log_error(
          logger,
          e,
          component,
          action,
          user_id=user_id,
          entity_id=entity_id,
          metadata={"duration_ms": duration_ms},
        )
        raise

    return wrapper

  return decorator


# Initialize logging on import
setup_logging()
