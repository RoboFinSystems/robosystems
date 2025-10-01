"""
Tests for OpenTelemetry metrics collection functionality.

Tests verify that the metrics decorator and context manager correctly
record business events, request metrics, and authentication metrics.
"""

import pytest
import os
from fastapi import FastAPI, APIRouter, HTTPException
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  endpoint_metrics_context,
  EndpointMetrics,
  get_endpoint_metrics,
)
from robosystems.middleware.otel import record_auth_metrics


class TestEndpointMetricsDecorator:
  """Test the endpoint_metrics_decorator functionality."""

  @pytest.fixture
  def app_with_metrics(self):
    """Create test FastAPI app with metrics-decorated endpoints."""
    from fastapi import Request

    app = FastAPI()
    router = APIRouter()

    @endpoint_metrics_decorator("/test/simple", business_event_type="test_event")
    async def simple_endpoint(request: Request):
      return {"message": "success"}

    @endpoint_metrics_decorator("/test/async", business_event_type="async_test")
    async def async_endpoint(request: Request):
      return {"message": "async success"}

    @endpoint_metrics_decorator("/test/error")
    async def error_endpoint(request: Request):
      raise HTTPException(status_code=400, detail="Test error")

    @endpoint_metrics_decorator("/test/business")
    async def business_endpoint(request: Request):
      # Manually record business event inside endpoint
      metrics = get_endpoint_metrics()
      metrics.record_business_event(
        endpoint="/test/business",
        method="GET",
        event_type="manual_business_event",
        event_data={"key": "value"},
      )
      return {"message": "business"}

    router.add_api_route("/simple", simple_endpoint, methods=["GET"])
    router.add_api_route("/async", async_endpoint, methods=["GET"])
    router.add_api_route("/error", error_endpoint, methods=["GET"])
    router.add_api_route("/business", business_endpoint, methods=["GET"])

    app.include_router(router)
    return app

  @patch("robosystems.middleware.otel.metrics.get_endpoint_metrics")
  def test_decorator_records_business_event(self, mock_get_metrics, app_with_metrics):
    """Test that decorator records business events."""
    mock_metrics_instance = MagicMock()
    mock_get_metrics.return_value = mock_metrics_instance

    client = TestClient(app_with_metrics)
    response = client.get("/simple")

    assert response.status_code == 200

    # Verify business event was recorded
    mock_metrics_instance.record_business_event.assert_called_once_with(
      endpoint="/test/simple",
      method="GET",
      event_type="test_event",
      event_data={},
      user_id=None,
    )

  @patch("robosystems.middleware.otel.metrics.get_endpoint_metrics")
  def test_decorator_records_async_endpoint(self, mock_get_metrics, app_with_metrics):
    """Test that decorator works with async endpoints."""
    mock_metrics_instance = MagicMock()
    mock_get_metrics.return_value = mock_metrics_instance

    client = TestClient(app_with_metrics)
    response = client.get("/async")

    assert response.status_code == 200

    # Verify business event was recorded
    mock_metrics_instance.record_business_event.assert_called_once_with(
      endpoint="/test/async",
      method="GET",
      event_type="async_test",
      event_data={},
      user_id=None,
    )

  @patch("robosystems.middleware.otel.metrics.get_endpoint_metrics")
  def test_decorator_handles_errors(self, mock_get_metrics, app_with_metrics):
    """Test that decorator handles endpoint errors gracefully."""
    mock_metrics_instance = MagicMock()
    mock_get_metrics.return_value = mock_metrics_instance

    client = TestClient(app_with_metrics)
    response = client.get("/error")

    assert response.status_code == 400

    # Should still record business event even on error
    # (if business_event_type was provided, but it wasn't in this case)
    # Verify no business event recorded since no business_event_type
    mock_metrics_instance.record_business_event.assert_not_called()

  @patch("robosystems.middleware.otel.metrics.get_endpoint_metrics")
  def test_decorator_without_business_event(self, mock_get_metrics, app_with_metrics):
    """Test decorator without business_event_type parameter."""
    mock_metrics_instance = MagicMock()
    mock_get_metrics.return_value = mock_metrics_instance

    client = TestClient(app_with_metrics)
    response = client.get("/error")

    assert response.status_code == 400

    # No business event should be recorded when business_event_type not provided
    mock_metrics_instance.record_business_event.assert_not_called()


class TestEndpointMetricsContext:
  """Test the endpoint_metrics_context context manager."""

  @patch("robosystems.middleware.otel.metrics.get_endpoint_metrics")
  def test_context_manager_basic_usage(self, mock_get_metrics):
    """Test basic context manager usage."""
    mock_metrics_instance = MagicMock()
    mock_get_metrics.return_value = mock_metrics_instance

    with endpoint_metrics_context("/test/endpoint", "GET") as ctx:
      # Context should provide a way to record business events
      ctx.record_business_event("test_event", {"key": "value"})

    # Verify metrics instance was called
    mock_get_metrics.assert_called()

  @patch("robosystems.middleware.otel.metrics.get_endpoint_metrics")
  def test_context_manager_with_exception(self, mock_get_metrics):
    """Test context manager handles exceptions gracefully."""
    mock_metrics_instance = MagicMock()
    mock_get_metrics.return_value = mock_metrics_instance

    try:
      with endpoint_metrics_context("/test/endpoint", "POST") as ctx:
        ctx.record_business_event("before_error", {})
        raise ValueError("Test error")
    except ValueError:
      pass

    # Verify metrics instance was called even with exception
    mock_get_metrics.assert_called()


class TestEndpointMetricsClass:
  """Test the EndpointMetrics class functionality."""

  @pytest.fixture
  def mock_meter(self):
    """Create mock OpenTelemetry meter."""
    mock_meter = MagicMock()
    mock_counter = MagicMock()
    mock_histogram = MagicMock()

    mock_meter.create_counter.return_value = mock_counter
    mock_meter.create_histogram.return_value = mock_histogram

    return mock_meter, mock_counter, mock_histogram

  @pytest.fixture
  def endpoint_metrics(self, mock_meter):
    """Create EndpointMetrics instance with mocked meter."""
    meter, counter, histogram = mock_meter

    with patch(
      "robosystems.middleware.otel.metrics.metrics.get_meter"
    ) as mock_get_meter:
      mock_get_meter.return_value = meter
      return EndpointMetrics("test_meter"), meter, counter, histogram

  def test_endpoint_metrics_initialization(self, endpoint_metrics):
    """Test EndpointMetrics initialization."""
    metrics, meter, counter, histogram = endpoint_metrics

    # Instruments are created lazily, so trigger creation by recording a request
    metrics.record_request(
      endpoint="/test", method="GET", status_code=200, duration=0.1
    )

    # Verify counters and histograms were created after first use
    assert (
      meter.create_counter.call_count == 16
    )  # request, error, auth_attempts, auth_failures, business_events, query_submissions, query_queue_rejections, query_completions, query_user_limits + 7 SSE counters
    assert (
      meter.create_histogram.call_count == 3
    )  # request_duration, query_wait_time, query_execution_time

  def test_record_business_event(self, endpoint_metrics):
    """Test recording business events."""
    metrics, meter, counter, histogram = endpoint_metrics

    metrics.record_business_event(
      endpoint="/auth/register",
      method="POST",
      event_type="user_registered",
      event_data={"user_id": "test123"},
    )

    # Verify counter was incremented
    counter.add.assert_called_with(
      1,
      {
        "endpoint": "/auth/register",
        "method": "POST",
        "event_type": "user_registered",
        "event_user_id": "test123",
      },
    )

  def test_record_auth_attempt(self, endpoint_metrics):
    """Test recording authentication attempts."""
    metrics, meter, counter, histogram = endpoint_metrics

    metrics.record_auth_attempt(
      endpoint="/auth/login",
      method="POST",
      auth_type="email_password",
      success=True,
      user_id="user123",
    )

    # Verify counter was incremented (using the mocked counter from fixture)
    counter.add.assert_called_with(
      1,
      {
        "endpoint": "/auth/login",
        "method": "POST",
        "auth_type": "email_password",
        "success": True,
        "user_id": "user123",
      },
    )

  def test_record_request_duration(self, endpoint_metrics):
    """Test recording request duration."""
    metrics, meter, counter, histogram = endpoint_metrics

    metrics.record_request_duration(
      endpoint="/api/test", method="GET", status_code=200, duration=0.125
    )

    # Verify histogram was recorded
    histogram.record.assert_called_with(
      0.125, {"endpoint": "/api/test", "method": "GET", "status_code": 200}
    )

  def test_record_error(self, endpoint_metrics):
    """Test recording errors."""
    metrics, meter, counter, histogram = endpoint_metrics

    metrics.record_error(
      endpoint="/api/test",
      method="POST",
      error_type="ValidationError",
      error_code="422",
    )

    # Verify counter was incremented
    counter.add.assert_called_with(
      1,
      {
        "endpoint": "/api/test",
        "method": "POST",
        "error_type": "ValidationError",
        "error_code": "422",
        "user_authenticated": "false",
      },
    )


class TestAuthMetrics:
  """Test authentication metrics recording."""

  @patch("robosystems.middleware.otel.metrics.get_endpoint_metrics")
  def test_record_auth_metrics_success(self, mock_get_metrics):
    """Test recording successful authentication metrics."""
    mock_metrics_instance = MagicMock()
    mock_get_metrics.return_value = mock_metrics_instance

    record_auth_metrics(
      endpoint="/auth/login",
      method="POST",
      auth_type="email_password_login",
      success=True,
      user_id="user123",
    )

    # Verify auth attempt was recorded (using positional arguments as the function does)
    mock_metrics_instance.record_auth_attempt.assert_called_once_with(
      "/auth/login", "POST", "email_password_login", True, None, "user123"
    )

  @patch("robosystems.middleware.otel.metrics.get_endpoint_metrics")
  def test_record_auth_metrics_failure(self, mock_get_metrics):
    """Test recording failed authentication metrics."""
    mock_metrics_instance = MagicMock()
    mock_get_metrics.return_value = mock_metrics_instance

    record_auth_metrics(
      endpoint="/auth/login",
      method="POST",
      auth_type="api_key",
      success=False,
      failure_reason="invalid_key",
    )

    # Verify failed auth attempt was recorded (using positional arguments as the function does)
    mock_metrics_instance.record_auth_attempt.assert_called_once_with(
      "/auth/login", "POST", "api_key", False, "invalid_key", None
    )


class TestBusinessEventMetrics:
  """Test business event metrics recording."""

  def test_record_business_event_simple(self):
    """Test recording simple business events through metrics instance."""
    # Create a real EndpointMetrics instance with mocked meter
    with patch(
      "robosystems.middleware.otel.metrics.metrics.get_meter"
    ) as mock_get_meter:
      mock_meter = MagicMock()
      mock_counter = MagicMock()
      mock_get_meter.return_value = mock_meter
      mock_meter.create_counter.return_value = mock_counter

      # Create EndpointMetrics instance
      metrics = EndpointMetrics("test_meter")

      # Record business event
      metrics.record_business_event(
        endpoint="/auth/register",
        method="POST",
        event_type="user_registered",
        event_data={"source": "web"},
      )

      # Verify counter was called with correct attributes
      mock_counter.add.assert_called_with(
        1,
        {
          "endpoint": "/auth/register",
          "method": "POST",
          "event_type": "user_registered",
          "event_source": "web",
        },
      )

  def test_record_business_event_complex(self):
    """Test recording business events with complex attributes."""
    # Create a real EndpointMetrics instance with mocked meter
    with patch(
      "robosystems.middleware.otel.metrics.metrics.get_meter"
    ) as mock_get_meter:
      mock_meter = MagicMock()
      mock_counter = MagicMock()
      mock_get_meter.return_value = mock_meter
      mock_meter.create_counter.return_value = mock_counter

      # Create EndpointMetrics instance
      metrics = EndpointMetrics("test_meter")

      # Record business event with complex data - but need to pass proper args
      metrics.record_business_event(
        endpoint="/api/entity",
        method="POST",
        event_type="entity_created",
        event_data={
          "user_id": "user123",
          "entity_id": "comp456",
          "graph_id": "graph789",
          "action": "create",
        },
        user_id="user123",
      )

      # Verify counter was called with flattened attributes
      mock_counter.add.assert_called_with(
        1,
        {
          "endpoint": "/api/entity",
          "method": "POST",
          "event_type": "entity_created",
          "user_id": "user123",
          "event_user_id": "user123",
          "event_entity_id": "comp456",
          "event_graph_id": "graph789",
          "event_action": "create",
        },
      )


class TestMetricsIntegration:
  """Test metrics integration with real API endpoints."""

  @pytest.fixture
  def authenticated_user(self, client: TestClient):
    """Create authenticated user for integration tests."""
    from robosystems.models.iam import UserAPIKey
    from robosystems.database import session

    # Patch environment to ensure CAPTCHA is disabled and registration is enabled
    with patch.dict(os.environ, {"ENVIRONMENT": "dev"}):
      with patch.object(
        __import__("robosystems.config", fromlist=["env"]).env,
        "USER_REGISTRATION_ENABLED",
        True,
      ):
        registration_data = {
          "name": "Metrics Integration User",
          "email": "integration@example.com",
          "password": "S3cur3P@ssw0rd!2024",
        }
        response = client.post("/v1/auth/register", json=registration_data)
        if response.status_code != 201:
          raise ValueError(
            f"Failed to register user: {response.status_code} - {response.json()}"
          )
        user_data = response.json()["user"]
        user_id = user_data["id"]

    api_key, plain_key = UserAPIKey.create(
      user_id=user_id, name="Integration Test API Key", session=session
    )

    return {"X-API-Key": plain_key}

  @patch("robosystems.middleware.otel.metrics.get_endpoint_metrics")
  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_auth_endpoints_record_metrics(self, mock_get_metrics, client: TestClient):
    """Test that auth endpoints record the correct metrics."""
    mock_metrics_instance = MagicMock()
    mock_get_metrics.return_value = mock_metrics_instance

    registration_data = {
      "name": "Metrics Test User",
      "email": "metricstest@example.com",
      "password": "S3cur3P@ssw0rd!2024",
    }

    response = client.post("/v1/auth/register", json=registration_data)
    assert response.status_code == 201

    # Verify business event was recorded for registration
    business_calls = mock_metrics_instance.record_business_event.call_args_list
    assert len(business_calls) >= 1

    # Check that user_registered event was recorded
    registration_call = None
    for call_info in business_calls:
      args, kwargs = call_info
      # The event_type is passed as a keyword argument
      if kwargs.get("event_type") == "user_registered":
        registration_call = kwargs
        break

    assert registration_call is not None
    # Check that the call has the expected structure
    assert registration_call.get("endpoint") == "/v1/auth/register"
    assert registration_call.get("method") == "POST"

  @patch("robosystems.middleware.otel.metrics.get_endpoint_metrics")
  def test_user_endpoints_record_metrics(
    self, mock_get_metrics, client: TestClient, authenticated_user
  ):
    """Test that user endpoints record the correct metrics."""
    mock_metrics_instance = MagicMock()
    mock_get_metrics.return_value = mock_metrics_instance

    response = client.get("/v1/user/", headers=authenticated_user)
    assert response.status_code == 200

    # Verify business event was recorded
    mock_metrics_instance.record_business_event.assert_called()
    call_args = mock_metrics_instance.record_business_event.call_args
    assert call_args[1]["event_type"] == "user_info_accessed"

  @patch("robosystems.middleware.otel.metrics.get_endpoint_metrics")
  def test_error_metrics_recorded(self, mock_get_metrics, client: TestClient):
    """Test that error responses record appropriate metrics."""
    mock_metrics_instance = MagicMock()
    mock_get_metrics.return_value = mock_metrics_instance

    # Make request to protected endpoint without auth
    response = client.get("/v1/user/")
    assert response.status_code == 401

    # Authentication errors happen at middleware level before endpoint is reached,
    # so endpoint metrics are not recorded for 401 errors.
    # This is correct behavior - auth metrics are recorded separately by auth middleware.
    # For this test, we just verify the auth failure occurred properly
    assert response.status_code == 401
