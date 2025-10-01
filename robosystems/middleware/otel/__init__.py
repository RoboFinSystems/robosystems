"""
OpenTelemetry centralized metrics and tracing utilities for RoboSystems.

This module provides consistent observability patterns for all API endpoints.
"""

from .setup import setup_telemetry, get_tracer, shutdown_telemetry
from .metrics import (
  get_endpoint_metrics,
  record_request_metrics,
  record_auth_metrics,
  record_error_metrics,
  endpoint_metrics_decorator,
  endpoint_metrics_context,
)

__all__ = [
  "setup_telemetry",
  "get_tracer",
  "shutdown_telemetry",
  "get_endpoint_metrics",
  "record_request_metrics",
  "record_auth_metrics",
  "record_error_metrics",
  "endpoint_metrics_decorator",
  "endpoint_metrics_context",
]
