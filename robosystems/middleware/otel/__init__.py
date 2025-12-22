"""
OpenTelemetry centralized metrics and tracing utilities for RoboSystems.

This module provides consistent observability patterns for all API endpoints.
"""

from .metrics import (
  endpoint_metrics_context,
  endpoint_metrics_decorator,
  get_endpoint_metrics,
  record_auth_metrics,
  record_error_metrics,
  record_request_metrics,
)
from .setup import get_tracer, setup_telemetry, shutdown_telemetry

__all__ = [
  "endpoint_metrics_context",
  "endpoint_metrics_decorator",
  "get_endpoint_metrics",
  "get_tracer",
  "record_auth_metrics",
  "record_error_metrics",
  "record_request_metrics",
  "setup_telemetry",
  "shutdown_telemetry",
]
