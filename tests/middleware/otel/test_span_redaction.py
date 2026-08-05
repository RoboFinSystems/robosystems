"""Tests for the query-param redaction span processor."""

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from robosystems.middleware.otel.setup import QueryParamRedactionSpanProcessor


@pytest.fixture
def traced():
  provider = TracerProvider()
  exporter = InMemorySpanExporter()
  provider.add_span_processor(QueryParamRedactionSpanProcessor())
  provider.add_span_processor(SimpleSpanProcessor(exporter))
  tracer = provider.get_tracer("test")
  return tracer, exporter


@pytest.mark.unit
class TestQueryParamRedactionSpanProcessor:
  def test_redacts_token_in_http_url(self, traced):
    tracer, exporter = traced
    secret = "rfsc" + "a" * 64

    with tracer.start_as_current_span(
      "POST /v1/graphs/kg123/mcp",
      attributes={
        "http.url": f"http://api.example.com/v1/graphs/kg123/mcp?token={secret}",
        "http.target": f"/v1/graphs/kg123/mcp?token={secret}",
      },
    ):
      pass

    (span,) = exporter.get_finished_spans()
    assert secret not in str(span.attributes)
    assert span.attributes["http.url"].endswith("token=REDACTED")
    assert span.attributes["http.target"].endswith("token=REDACTED")

  def test_redacts_new_semconv_query_attribute(self, traced):
    tracer, exporter = traced
    secret = "rfsc" + "b" * 64

    with tracer.start_as_current_span(
      "POST",
      attributes={"url.query": f"token={secret}&other=1"},
    ):
      pass

    (span,) = exporter.get_finished_spans()
    assert secret not in str(span.attributes)
    assert "token=REDACTED" in span.attributes["url.query"]

  def test_leaves_benign_attributes_alone(self, traced):
    tracer, exporter = traced

    with tracer.start_as_current_span(
      "GET",
      attributes={
        "http.url": "http://api.example.com/v1/status",
        "http.target": "/v1/graphs/kg123/mcp?limit=10",
        "http.method": "GET",
      },
    ):
      pass

    (span,) = exporter.get_finished_spans()
    assert span.attributes["http.url"] == "http://api.example.com/v1/status"
    assert span.attributes["http.target"] == "/v1/graphs/kg123/mcp?limit=10"
    assert span.attributes["http.method"] == "GET"

  def test_spans_without_attributes_are_untouched(self, traced):
    tracer, exporter = traced

    with tracer.start_as_current_span("internal-op"):
      pass

    (span,) = exporter.get_finished_spans()
    assert span.name == "internal-op"
