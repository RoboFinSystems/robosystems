"""OpenTelemetry tracing and metrics setup.

Instruments FastAPI, `requests`, and psycopg2, and exports over OTLP.
Tracing is enabled per environment, and every step degrades gracefully:
a missing or unreachable collector leaves the application running.
"""

import logging
import socket
from importlib.metadata import version as pkg_version

from fastapi import FastAPI
from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Span, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

from robosystems.config import env

logger = logging.getLogger(__name__)


class QueryParamRedactionSpanProcessor(SpanProcessor):
  """Redact sensitive query parameters from URL-bearing span attributes at
  span start, using the request-logging list. OTel's own ``redact_url`` does
  not cover our parameter names.
  """

  _FULL_URL_ATTRS = ("http.url", "url.full", "http.target")
  _QUERY_ATTRS = ("url.query",)

  def on_start(self, span: Span, parent_context=None) -> None:
    attributes = span.attributes
    if not attributes:
      return
    from robosystems.middleware.logging import redact_sensitive_query_params

    for attr in self._FULL_URL_ATTRS:
      value = attributes.get(attr)
      if isinstance(value, str) and "?" in value:
        base, _, query = value.partition("?")
        span.set_attribute(attr, f"{base}?{redact_sensitive_query_params(query)}")
    for attr in self._QUERY_ATTRS:
      value = attributes.get(attr)
      if isinstance(value, str) and value:
        span.set_attribute(attr, redact_sensitive_query_params(value))


tracing_enabled = getattr(env, "OTEL_ENABLED", env.is_staging() or env.is_production())
service_name = env.OTEL_SERVICE_NAME
otlp_endpoint = env.OTEL_EXPORTER_OTLP_ENDPOINT.replace("4317", "4318")  # HTTP port
resource_attributes = (
  env.OTEL_RESOURCE_ATTRIBUTES if hasattr(env, "OTEL_RESOURCE_ATTRIBUTES") else ""
)

_tracer_provider: TracerProvider | None = None
_meter_provider: MeterProvider | None = None
_instrumentation_enabled = False


def _create_resource() -> Resource:
  try:
    service_version = pkg_version("robosystems")
  except Exception:
    service_version = "unknown"

  attributes = {
    "service.name": service_name,
    "service.version": service_version,
    "deployment.environment": env.ENVIRONMENT,
    # One series per task: interleaved cumulative counters from several tasks
    # read as constant resets and inflate rate(). Hostname = container ID.
    "service.instance.id": socket.gethostname(),
  }

  # An explicit service.instance.id here overrides the one above.
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
  global _tracer_provider, _meter_provider, _instrumentation_enabled

  if not tracing_enabled:
    logger.info(f"OpenTelemetry tracing disabled for environment: {env.ENVIRONMENT}")
    return

  if _instrumentation_enabled:
    logger.warning("OpenTelemetry already initialized, skipping setup")
    return

  try:
    resource = _create_resource()

    _tracer_provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(_tracer_provider)

    metric_readers = []
    # localhost is the ADOT sidecar in prod/staging, so allowed there.
    should_export_metrics = otlp_endpoint and (
      otlp_endpoint != "http://localhost:4318"
      or env.is_staging()
      or env.is_production()
    )

    if should_export_metrics:
      try:
        otlp_metric_exporter = OTLPMetricExporter(
          endpoint=f"{otlp_endpoint}/v1/metrics",
          timeout=30,
        )
        metric_reader = PeriodicExportingMetricReader(
          exporter=otlp_metric_exporter,
          export_interval_millis=60000,
        )
        metric_readers.append(metric_reader)
        logger.info(f"OTLP metrics exporter configured for endpoint: {otlp_endpoint}")
      except Exception as e:
        logger.error(f"Failed to configure OTLP metrics exporter: {e}")
    elif otlp_endpoint == "http://localhost:4318" and env.is_development():
      logger.info(
        "Skipping OTLP metrics exporter for localhost endpoint in dev environment (use observability profile)"
      )

    from robosystems.middleware.otel.metrics import get_metric_views

    _meter_provider = MeterProvider(
      resource=resource,
      metric_readers=metric_readers,
      views=get_metric_views(),
    )
    metrics.set_meter_provider(_meter_provider)

    exporters = []

    # Span export is its own switch: the collector only carries a traces
    # pipeline once a backend exists, so this stays off by default.
    traces_enabled = env.OTEL_TRACES_ENABLED

    should_export_traces = (
      traces_enabled
      and otlp_endpoint
      and (
        otlp_endpoint != "http://localhost:4318"
        or env.is_staging()
        or env.is_production()
      )
    )

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

    if env.is_development() and env.OTEL_CONSOLE_EXPORT:
      exporters.append(ConsoleSpanExporter())

    # Before any exporter sees the span.
    _tracer_provider.add_span_processor(QueryParamRedactionSpanProcessor())

    for exporter in exporters:
      _tracer_provider.add_span_processor(BatchSpanProcessor(exporter))

    # Excludes ALB health checks.
    FastAPIInstrumentor.instrument_app(
      app,
      tracer_provider=_tracer_provider,
      excluded_urls="status,health",
    )

    RequestsInstrumentor().instrument()
    Psycopg2Instrumentor().instrument()

    _instrumentation_enabled = True
    logger.info(f"OpenTelemetry tracing enabled for service: {service_name}")

  except Exception as e:
    logger.error(f"Failed to setup OpenTelemetry: {e}")


def get_tracer(name: str | None = None):
  tracer_name = name or __name__
  return trace.get_tracer(tracer_name)


def shutdown_telemetry() -> None:
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
