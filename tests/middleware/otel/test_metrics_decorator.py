"""Tests for otel metrics decorator and sync wrapper."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
)


class TestEndpointMetricsDecoratorAsync:
  @pytest.mark.asyncio
  async def test_success_records_metrics(self):
    @endpoint_metrics_decorator("/v1/test")
    async def my_endpoint():
      return {"ok": True}

    result = await my_endpoint()
    assert result == {"ok": True}

  @pytest.mark.asyncio
  async def test_error_records_error_metrics(self):
    @endpoint_metrics_decorator("/v1/test")
    async def failing_endpoint():
      raise HTTPException(status_code=404, detail="Not found")

    with pytest.raises(HTTPException):
      await failing_endpoint()

  @pytest.mark.asyncio
  async def test_extracts_method_from_request(self):
    mock_request = MagicMock()
    mock_request.method = "POST"
    mock_request.path_params = {}
    mock_request.headers = MagicMock()
    mock_request.headers.get = MagicMock(return_value=None)
    mock_request.state = MagicMock()

    @endpoint_metrics_decorator("/v1/test")
    async def my_endpoint(request):
      return {"ok": True}

    result = await my_endpoint(mock_request)
    assert result == {"ok": True}

  @pytest.mark.asyncio
  async def test_with_business_event(self):
    @endpoint_metrics_decorator("/v1/test", business_event_type="graph_created")
    async def my_endpoint():
      return {"ok": True}

    result = await my_endpoint()
    assert result == {"ok": True}

  @pytest.mark.asyncio
  async def test_default_endpoint_name(self):
    @endpoint_metrics_decorator()
    async def custom_name_endpoint():
      return {"ok": True}

    result = await custom_name_endpoint()
    assert result == {"ok": True}

  @pytest.mark.asyncio
  async def test_explicit_method_kwarg_used_without_request_scan(self):
    """When `method=` is passed, the decorator must skip the runtime scan
    and use the explicit value — so routes that don't declare
    `request: Request` still get a correct method label in Prometheus.
    """
    captured = {}

    def _capture_request(endpoint, method, status_code, duration, **kw):
      captured["method"] = method
      captured["endpoint"] = endpoint

    with patch(
      "robosystems.middleware.otel.metrics.record_request_metrics",
      side_effect=_capture_request,
    ):

      @endpoint_metrics_decorator(
        "/extensions/roboledger/{graph_id}/operations/foo",
        method="POST",
      )
      async def my_op(body, graph_id):
        return {"ok": True}

      await my_op(body={"x": 1}, graph_id="kg123")

    assert captured["method"] == "POST"
    assert captured["endpoint"] == "/extensions/roboledger/{graph_id}/operations/foo"

  @pytest.mark.asyncio
  async def test_business_event_suppressed_on_idempotent_replay(self):
    """When the decorated function returns an object with
    `idempotent_replay=True`, the business_event counter must not fire.
    Request counter and duration histogram still fire because the HTTP
    call genuinely happened — only the business event is suppressed.
    """

    class _FakeReplayEnvelope:
      idempotent_replay = True

    class _FakeFreshEnvelope:
      idempotent_replay = False

    business_events = []

    def _capture_business_event(
      endpoint, method, event_type, event_data=None, user_id=None
    ):
      business_events.append(event_type)

    fake_metrics = MagicMock()
    fake_metrics.record_business_event.side_effect = _capture_business_event

    with patch(
      "robosystems.middleware.otel.metrics.get_endpoint_metrics",
      return_value=fake_metrics,
    ):

      @endpoint_metrics_decorator(
        "/extensions/roboledger/{graph_id}/operations/foo",
        method="POST",
        business_event_type="ledger_foo",
      )
      async def my_op(replay: bool):
        return _FakeReplayEnvelope() if replay else _FakeFreshEnvelope()

      # Fresh execution: business event fires
      await my_op(replay=False)
      # Replay execution: business event suppressed
      await my_op(replay=True)

    assert business_events == ["ledger_foo"]  # exactly one, from the fresh call


class TestEndpointMetricsDecoratorSync:
  def test_sync_success(self):
    @endpoint_metrics_decorator("/v1/test")
    def my_sync_endpoint():
      return {"ok": True}

    result = my_sync_endpoint()
    assert result == {"ok": True}

  def test_sync_error(self):
    @endpoint_metrics_decorator("/v1/test")
    def failing_sync():
      raise ValueError("boom")

    with pytest.raises(ValueError):
      failing_sync()

  def test_sync_with_request_arg(self):
    mock_request = MagicMock()
    mock_request.method = "GET"

    @endpoint_metrics_decorator("/v1/test")
    def my_endpoint(request):
      return {"ok": True}

    result = my_endpoint(mock_request)
    assert result == {"ok": True}
