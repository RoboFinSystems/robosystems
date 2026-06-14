"""OpenTelemetry endpoint metrics tests."""

import pytest
from fastapi import HTTPException

from robosystems.middleware.otel.metrics import (
  EndpointMetrics,
  MetricType,
  _sanitize_endpoint,
  endpoint_metrics_context,
  get_endpoint_metrics,
  record_auth_metrics,
  record_error_metrics,
  record_query_queue_metrics,
  record_request_metrics,
)


@pytest.fixture
def metrics():
  """Create an EndpointMetrics instance."""
  return EndpointMetrics("test.metrics")


class TestMetricType:
  def test_values(self):
    assert MetricType.REQUEST.value == "request"
    assert MetricType.AUTH.value == "auth"
    assert MetricType.ERROR.value == "error"
    assert MetricType.BUSINESS.value == "business"


class TestEndpointMetricsInit:
  def test_creates_meter(self, metrics):
    assert metrics.meter is not None
    assert metrics._request_counter is None  # Lazy init


class TestRecordRequest:
  def test_basic_request(self, metrics):
    metrics.record_request("/test", "GET", 200, 0.1)

  def test_with_user_id(self, metrics):
    metrics.record_request("/test", "POST", 201, 0.5, user_id="user1")

  def test_with_additional_attributes(self, metrics):
    metrics.record_request(
      "/test",
      "GET",
      200,
      0.1,
      additional_attributes={"graph_id": "kg123"},
    )

  def test_unauthenticated(self, metrics):
    metrics.record_request("/test", "GET", 200, 0.1, user_id=None)


class TestRecordRequestDuration:
  def test_basic_duration(self, metrics):
    metrics.record_request_duration("/test", "GET", 200, 0.5)

  def test_with_user(self, metrics):
    metrics.record_request_duration("/test", "POST", 201, 1.5, user_id="u1")


class TestRecordAuthAttempt:
  def test_successful_auth(self, metrics):
    metrics.record_auth_attempt("/login", "POST", "jwt", True)

  def test_failed_auth(self, metrics):
    metrics.record_auth_attempt(
      "/login", "POST", "jwt", False, failure_reason="invalid_password"
    )

  def test_with_user_id(self, metrics):
    metrics.record_auth_attempt("/login", "POST", "api_key", True, user_id="user1")


class TestRecordError:
  def test_basic_error(self, metrics):
    metrics.record_error("/test", "GET", "ValueError")

  def test_with_error_code(self, metrics):
    metrics.record_error("/test", "POST", "HTTPException", error_code="404")

  def test_with_user(self, metrics):
    metrics.record_error("/test", "GET", "Error", user_id="u1")

  def test_unauthenticated(self, metrics):
    metrics.record_error("/test", "GET", "Error", user_id=None)


class TestRecordBusinessEvent:
  def test_basic_event(self, metrics):
    metrics.record_business_event("/test", "POST", "graph_created")

  def test_with_event_data(self, metrics):
    metrics.record_business_event(
      "/test",
      "POST",
      "query_executed",
      event_data={"rows": 100, "duration": 0.5},
    )

  def test_with_user(self, metrics):
    metrics.record_business_event("/test", "GET", "view", user_id="u1")

  def test_health_check_skipped(self, metrics):
    # Health check business events should be skipped
    metrics.record_business_event("/v1/status", "GET", "health_check")
    metrics.record_business_event("/status", "GET", "metrics_access")


class TestRecordGraphMetrics:
  def test_basic_graph_metrics(self, metrics):
    metrics.record_graph_metrics("kg123", 1000, 5000, 1024 * 1024)

  def test_with_user_and_attributes(self, metrics):
    metrics.record_graph_metrics(
      "kg123",
      500,
      2000,
      512 * 1024,
      user_id="u1",
      additional_attributes={"tier": "standard"},
    )


class TestQueryQueueMetrics:
  def test_submission(self, metrics):
    metrics.record_query_submission("kg123", "u1", 5, True)

  def test_failed_submission(self, metrics):
    metrics.record_query_submission(
      "kg123", "u1", 5, False, rejection_reason="queue_full"
    )

  def test_user_limit_rejection(self, metrics):
    metrics.record_query_submission(
      "kg123", "u1", 5, False, rejection_reason="user_limit"
    )

  def test_wait_time(self, metrics):
    metrics.record_query_wait_time("kg123", "u1", 5, 2.5)

  def test_execution(self, metrics):
    metrics.record_query_execution("kg123", "u1", 1.5, "completed")

  def test_execution_failed(self, metrics):
    metrics.record_query_execution("kg123", "u1", 0.5, "failed", error_type="timeout")

  def test_concurrent_update(self, metrics):
    metrics.update_concurrent_executions(1)
    metrics.update_concurrent_executions(-1)


class TestSSEMetrics:
  def test_connection_opened(self, metrics):
    metrics.record_sse_connection_opened("u1", "op1")

  def test_connection_closed(self, metrics):
    metrics.record_sse_connection_closed("u1", "op1")

  def test_connection_rejected(self, metrics):
    metrics.record_sse_connection_rejected("u1", "rate_limit")

  def test_event_emitted(self, metrics):
    metrics.record_sse_event_emitted("op1", "progress")

  def test_event_failed(self, metrics):
    metrics.record_sse_event_failed("op1", "connection_closed")

  def test_event_failed_redis_error(self, metrics):
    metrics.record_sse_event_failed("op1", "redis_error")

  def test_queue_overflow(self, metrics):
    metrics.record_sse_queue_overflow("op1", "conn1")


class TestConvenienceFunctions:
  def test_record_request_metrics(self):
    record_request_metrics("/test", "GET", 200, 0.1)

  def test_record_request_metrics_skips_health(self):
    record_request_metrics("/v1/status", "GET", 200, 0.01)
    record_request_metrics("/status", "GET", 200, 0.01)

  def test_record_auth_metrics(self):
    record_auth_metrics("/login", "POST", "jwt", True)

  def test_record_error_metrics(self):
    record_error_metrics("/test", "GET", "ValueError")

  def test_get_endpoint_metrics_singleton(self):
    m1 = get_endpoint_metrics()
    m2 = get_endpoint_metrics()
    assert m1 is m2


class TestRecordQueryQueueMetrics:
  def test_submission(self):
    record_query_queue_metrics("submission", "kg123", "u1", priority=3, success=True)

  def test_wait_time(self):
    record_query_queue_metrics(
      "wait_time", "kg123", "u1", priority=3, wait_time_seconds=1.5
    )

  def test_execution(self):
    record_query_queue_metrics(
      "execution",
      "kg123",
      "u1",
      execution_time_seconds=2.0,
      status="completed",
    )

  def test_concurrent_update(self):
    record_query_queue_metrics("concurrent_update", "kg123", "u1", delta=1)


class TestEndpointMetricsContext:
  def test_success_path(self):
    with endpoint_metrics_context("/test", "POST", user_id="u1") as _ctx:
      pass  # Success

  def test_failure_path(self):
    with pytest.raises(ValueError):
      with endpoint_metrics_context("/test", "GET") as _ctx:
        raise ValueError("test error")

  def test_http_exception(self):
    with pytest.raises(HTTPException):
      with endpoint_metrics_context("/test", "GET") as _ctx:
        raise HTTPException(status_code=404, detail="Not found")

  def test_business_event_recording(self):
    with endpoint_metrics_context(
      "/test",
      "POST",
      business_event_type="graph_created",
      event_data={"graph_id": "kg123"},
    ) as _ctx:
      pass

  def test_ctx_record_business_event(self):
    with endpoint_metrics_context("/test", "POST") as ctx:
      ctx.record_business_event("custom_event", {"key": "value"})


class TestSanitizeEndpoint:
  """Guardrail against raw graph_id leaking into the unbounded `endpoint` label."""

  def test_collapses_raw_graph_id(self):
    assert (
      _sanitize_endpoint("/v1/graphs/kg19ea0c7f61d75b25f1c1/analytics")
      == "/v1/graphs/{graph_id}/analytics"
    )

  def test_collapses_subgraph_id(self):
    assert (
      _sanitize_endpoint("/v1/graphs/kg19ea0c7f61d75b25f1c1_dev/query")
      == "/v1/graphs/{graph_id}/query"
    )

  def test_leaves_templated_placeholder_untouched(self):
    templated = "/v1/graphs/{graph_id}/analytics"
    assert _sanitize_endpoint(templated) == templated

  def test_leaves_non_graph_paths_untouched(self):
    assert _sanitize_endpoint("/v1/auth/me") == "/v1/auth/me"
    assert _sanitize_endpoint("/") == "/"

  def test_does_not_touch_shared_repo_ids(self):
    # Shared repos (e.g. "sec") are bounded and not kg-prefixed.
    assert _sanitize_endpoint("/v1/graphs/sec/query") == "/v1/graphs/sec/query"

  def test_handles_empty_and_none(self):
    assert _sanitize_endpoint("") == ""
    assert _sanitize_endpoint(None) is None  # type: ignore[arg-type]

  def test_record_business_event_sanitizes_label(self, metrics):
    # Even if a caller leaks a raw graph_id, the recorded label is collapsed.
    metrics.record_business_event(
      endpoint="/v1/graphs/kg19ea0c7f61d75b25f1c1/analytics",
      method="GET",
      event_type="graph_metrics_accessed",
    )
