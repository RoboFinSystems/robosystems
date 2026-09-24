"""Metric values and label sets, read back through a real OTel SDK reader."""

from unittest.mock import patch

import pytest
from fastapi import HTTPException
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from robosystems.middleware.otel import metrics as metrics_module
from robosystems.middleware.otel.metrics import EndpointMetrics


@pytest.fixture
def reader_and_metrics():
  reader = InMemoryMetricReader()
  provider = MeterProvider(metric_readers=[reader])
  endpoint_metrics = EndpointMetrics("test")
  endpoint_metrics.meter = provider.get_meter("test")
  return reader, endpoint_metrics


def _points(reader):
  """One collection, by metric name: a gauge reports only in the next collect."""
  data = reader.get_metrics_data()
  return {
    metric.name: list(metric.data.data_points)
    for resource_metrics in (data.resource_metrics if data else [])
    for scope_metrics in resource_metrics.scope_metrics
    for metric in scope_metrics.metrics
  }


@pytest.mark.unit
class TestMetricCardinality:
  def test_graph_gauges_report_the_latest_value(self, reader_and_metrics):
    """Recorded on every usage-page read; the value must not accumulate."""
    reader, m = reader_and_metrics
    for _ in range(3):
      m.record_graph_metrics("kg123", 1000, 5000, 2048)

    points = _points(reader)
    (nodes,) = points["robosystems_graph_nodes_total"]
    (size,) = points["robosystems_graph_size_bytes"]
    assert nodes.value == 1000
    assert size.value == 2048

  def test_rate_limit_rejection_collapses_graph_ids(self, reader_and_metrics):
    reader, m = reader_and_metrics
    for gid in ("kg0123456789abcdef01", "kg0123456789abcdef02"):
      m.record_rate_limit_rejection(
        endpoint=f"/v1/graphs/{gid}/query", limit_type="burst", identifier_type="user"
      )

    points = _points(reader)["robosystems_rate_limit_rejections_total"]
    assert {p.attributes["endpoint"] for p in points} == {"/v1/graphs/{graph_id}/query"}

  @pytest.mark.asyncio
  async def test_error_label_is_the_status_code_not_the_message(
    self, reader_and_metrics
  ):
    reader, m = reader_and_metrics

    @metrics_module.endpoint_metrics_decorator("/v1/things", "GET")
    async def handler(thing_id: str):
      raise HTTPException(status_code=404, detail=f"Thing {thing_id} not found")

    with patch.object(metrics_module, "get_endpoint_metrics", return_value=m):
      for thing_id in ("a", "b", "c"):
        with pytest.raises(HTTPException):
          await handler(thing_id)

    points = _points(reader)["robosystems_api_errors_total"]
    assert {p.attributes.get("error_code") for p in points} == {"404"}
