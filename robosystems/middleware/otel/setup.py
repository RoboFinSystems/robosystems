"""
RoboSystems OpenTelemetry Middleware

This module provides a distributed tracing solution for RoboSystems using OpenTelemetry.
It is designed to be a drop-in replacement for the previous AWS X-Ray implementation.

Features:
- Environment-conditional tracing (dev/staging/prod)
- Automatic instrumentation for FastAPI, requests, and psycopg2
- OTLP exporter for sending telemetry data to a collector
- Graceful degradation if the collector is not available
- Resource attributes for better observability
- Comprehensive error handling
"""

import logging
from importlib.metadata import version as pkg_version

from fastapi import FastAPI

# OpenTelemetry imports
from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

from robosystems.config import env

# Initialize logger
logger = logging.getLogger(__name__)

# Configuration
# OTEL enabled by default for staging/prod, otherwise disabled
tracing_enabled = getattr(env, "OTEL_ENABLED", env.is_staging() or env.is_production())
service_name = env.OTEL_SERVICE_NAME
otlp_endpoint = env.OTEL_EXPORTER_OTLP_ENDPOINT.replace("4317", "4318")  # Use HTTP port
resource_attributes = (
  env.OTEL_RESOURCE_ATTRIBUTES if hasattr(env, "OTEL_RESOURCE_ATTRIBUTES") else ""
)

# Global variables to track instrumentation state
_tracer_provider: TracerProvider | None = None
_meter_provider: MeterProvider | None = None
_instrumentation_enabled = False


def _create_resource() -> Resource:
  """Create OpenTelemetry resource with service information."""
  try:
    service_version = pkg_version("robosystems-service")
  except Exception:
    # Fallback if package version can't be determined
    service_version = "unknown"

  attributes = {
    "service.name": service_name,
    "service.version": service_version,
    "deployment.environment": env.ENVIRONMENT,
  }

  # Parse additional resource attributes from environment
  if resource_attributes:
    try:
      for attr in resource_attributes.split(","):
        if "=" in attr:
          key, value = attr.split("=", 1)
          attributes[key.strip()] = value.strip()
    except Exception as e:
      logger.warning(f"Failed to parse OTEL_RESOURCE_ATTRIBUTES: {e}")

  return Resource.create(attributes)


def setup_telemetry(app: FastAPI) -> None:
  """
  Sets up OpenTelemetry for the application.

  Args:
      app: FastAPI application instance
  """
  global _tracer_provider, _meter_provider, _instrumentation_enabled

  if not tracing_enabled:
    logger.info(f"OpenTelemetry tracing disabled for environment: {env.ENVIRONMENT}")
    return

  if _instrumentation_enabled:
    logger.warning("OpenTelemetry already initialized, skipping setup")
    return

  try:
    # Create resource
    resource = _create_resource()

    # Initialize TracerProvider
    _tracer_provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(_tracer_provider)

    # Initialize MeterProvider
    metric_readers = []
    # Allow localhost endpoint in production/staging when using sidecar pattern (ADOT collector)
    should_export_metrics = otlp_endpoint and (
      otlp_endpoint != "http://localhost:4318"
      or env.is_staging()
      or env.is_production()  # Allow localhost in prod/staging (sidecar pattern)
    )

    if should_export_metrics:
      try:
        otlp_metric_exporter = OTLPMetricExporter(
          endpoint=f"{otlp_endpoint}/v1/metrics",
          timeout=30,
        )
        metric_reader = PeriodicExportingMetricReader(
          exporter=otlp_metric_exporter,
          export_interval_millis=60000,  # Export every 60 seconds (reduced from 30s to cut costs)
        )
        metric_readers.append(metric_reader)
        logger.info(f"OTLP metrics exporter configured for endpoint: {otlp_endpoint}")
      except Exception as e:
        logger.error(f"Failed to configure OTLP metrics exporter: {e}")
    elif otlp_endpoint == "http://localhost:4318" and env.is_development():
      logger.info(
        "Skipping OTLP metrics exporter for localhost endpoint in dev environment (use observability profile)"
      )

    _meter_provider = MeterProvider(resource=resource, metric_readers=metric_readers)
    metrics.set_meter_provider(_meter_provider)

    # Configure exporters
    exporters = []

    # Check if traces are explicitly enabled (default to False since only metrics are configured)
    traces_enabled = getattr(env, "OTEL_TRACES_ENABLED", False)

    # Allow localhost endpoint in production/staging when using sidecar pattern (ADOT collector)
    should_export_traces = (
      traces_enabled
      and otlp_endpoint
      and (
        otlp_endpoint != "http://localhost:4318"
        or env.is_staging()
        or env.is_production()  # Allow localhost in prod/staging (sidecar pattern)
      )
    )

    # Add OTLP exporter if endpoint is configured and appropriate for environment
    if should_export_traces:
      try:
        otlp_exporter = OTLPSpanExporter(
          endpoint=f"{otlp_endpoint}/v1/traces",
          timeout=30,
        )
        exporters.append(otlp_exporter)
        logger.info(f"OTLP trace exporter configured for endpoint: {otlp_endpoint}")
      except Exception as e:
        logger.error(f"Failed to configure OTLP trace exporter: {e}")
    elif otlp_endpoint == "http://localhost:4318" and env.is_development():
      logger.info(
        "Skipping OTLP trace exporter for localhost endpoint in dev environment (use observability profile)"
      )
    elif not traces_enabled and (env.is_staging() or env.is_production()):
      logger.info(
        "OTLP trace exporter disabled - only metrics are enabled for this environment"
      )

    # Add console exporter for development only if explicitly enabled
    if env.is_development() and env.OTEL_CONSOLE_EXPORT:
      exporters.append(ConsoleSpanExporter())

    # Add span processors
    for exporter in exporters:
      _tracer_provider.add_span_processor(BatchSpanProcessor(exporter))

    # Instrument FastAPI
    FastAPIInstrumentor.instrument_app(app, tracer_provider=_tracer_provider)

    # Instrument other libraries
    RequestsInstrumentor().instrument()
    Psycopg2Instrumentor().instrument()

    _instrumentation_enabled = True
    logger.info(f"OpenTelemetry tracing enabled for service: {service_name}")

  except Exception as e:
    logger.error(f"Failed to setup OpenTelemetry: {e}")
    # Graceful degradation - continue without tracing


def get_tracer(name: str | None = None):
  """
  Returns a tracer instance.

  Args:
      name: Optional tracer name, defaults to module name

  Returns:
      Tracer instance
  """
  tracer_name = name or __name__
  return trace.get_tracer(tracer_name)


def shutdown_telemetry() -> None:
  """
  Gracefully shutdown OpenTelemetry components.
  """
  global _tracer_provider, _meter_provider, _instrumentation_enabled

  if _instrumentation_enabled:
    try:
      if _tracer_provider:
        _tracer_provider.shutdown()
      if _meter_provider:
        _meter_provider.shutdown()
      logger.info("OpenTelemetry shutdown completed")
    except Exception as e:
      logger.error(f"Error during OpenTelemetry shutdown: {e}")
    finally:
      _instrumentation_enabled = False
      _tracer_provider = None
      _meter_provider = None
