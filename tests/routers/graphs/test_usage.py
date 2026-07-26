"""Comprehensive unit tests for the graph metrics + usage router."""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi import HTTPException

from robosystems.routers.graphs.usage import (
  _get_days_from_time_range,
  _parse_time_range,
  get_graph_metrics,
  get_graph_usage,
)


def _make_mock_user(user_id="user-123"):
  user = Mock()
  user.id = user_id
  return user


def _patch_robustness():
  """Patch all robustness components used in the usage module."""
  mock_cb = Mock()
  mock_cb.check_circuit = Mock()
  mock_cb.record_success = Mock()
  mock_cb.record_failure = Mock()

  mock_tc = Mock()
  mock_tc.calculate_timeout = Mock(return_value=30.0)

  mock_ol = Mock()
  mock_ol.log_external_service_call = Mock()

  return (
    patch(
      "robosystems.routers.graphs.usage.CircuitBreakerManager",
      return_value=mock_cb,
    ),
    patch(
      "robosystems.routers.graphs.usage.TimeoutCoordinator",
      return_value=mock_tc,
    ),
    patch(
      "robosystems.routers.graphs.usage.get_operation_logger",
      return_value=mock_ol,
    ),
    patch(
      "robosystems.routers.graphs.usage.record_operation_metric",
    ),
  )


@pytest.mark.unit
class TestParseTimeRangeExtended:
  """Extended tests for _parse_time_range helper."""

  def test_current_month(self):
    now = datetime(2024, 6, 15, tzinfo=UTC)
    year, month = _parse_time_range("current_month", now)
    assert year == 2024
    assert month == 6

  def test_last_month_same_year(self):
    now = datetime(2024, 3, 15, tzinfo=UTC)
    year, month = _parse_time_range("last_month", now)
    assert year == 2024
    assert month == 2

  def test_last_month_year_boundary(self):
    now = datetime(2024, 1, 15, tzinfo=UTC)
    year, month = _parse_time_range("last_month", now)
    assert year == 2023
    assert month == 12

  def test_24h_defaults_to_current(self):
    now = datetime(2024, 8, 20, tzinfo=UTC)
    year, month = _parse_time_range("24h", now)
    assert year == 2024
    assert month == 8

  def test_7d_defaults_to_current(self):
    now = datetime(2025, 3, 10, tzinfo=UTC)
    year, month = _parse_time_range("7d", now)
    assert year == 2025
    assert month == 3

  def test_30d_defaults_to_current(self):
    now = datetime(2025, 11, 3, tzinfo=UTC)
    year, month = _parse_time_range("30d", now)
    assert year == 2025
    assert month == 11

  def test_unknown_range_defaults_to_current(self):
    now = datetime(2026, 2, 25, tzinfo=UTC)
    year, month = _parse_time_range("unknown_range", now)
    assert year == 2026
    assert month == 2


@pytest.mark.unit
class TestGetDaysFromTimeRangeExtended:
  """Extended tests for _get_days_from_time_range helper."""

  def test_24h_returns_1(self):
    assert _get_days_from_time_range("24h") == 1

  def test_7d_returns_7(self):
    assert _get_days_from_time_range("7d") == 7

  def test_30d_returns_30(self):
    assert _get_days_from_time_range("30d") == 30

  def test_current_month_returns_day_of_month(self):
    result = _get_days_from_time_range("current_month")
    now = datetime.now(UTC)
    assert result == now.day

  def test_last_month_returns_positive_days(self):
    result = _get_days_from_time_range("last_month")
    assert result > 0
    assert result <= 62  # At most ~2 months of days

  def test_unknown_defaults_30(self):
    assert _get_days_from_time_range("unknown") == 30

  def test_empty_string_defaults_30(self):
    assert _get_days_from_time_range("") == 30


@pytest.mark.unit
class TestGetGraphMetrics:
  """Test the get_graph_metrics endpoint."""

  @pytest.mark.asyncio
  async def test_returns_metrics_successfully(self):
    """Test successful metrics retrieval."""
    mock_metrics = {
      "graph_id": "kg01234567890abcdef",
      "total_nodes": 1000,
      "total_relationships": 5000,
      "timestamp": "2024-06-15T00:00:00Z",
      "storage": {
        "total_bytes": 3101581,
        "total_mb": 2.96,
        "items": [
          {"type": "graph", "id": "kg01234567890abcdef", "bytes": 1159258},
          {"type": "staging", "id": "kg01234567890abcdef", "bytes": 1847296},
        ],
      },
      "health_status": {"status": "healthy", "last_check": "2024-06-15T00:00:00Z"},
      "node_counts": {"Entity": 500, "Report": 300},
      "relationship_counts": {"FILED": 200},
    }

    mock_service = AsyncMock()
    mock_service.collect_metrics_for_graph_async = AsyncMock(return_value=mock_metrics)

    cb_patch, tc_patch, ol_patch, rm_patch = _patch_robustness()

    with cb_patch, tc_patch, ol_patch, rm_patch:
      with patch(
        "robosystems.routers.graphs.usage.graph_metrics_service",
        mock_service,
      ):
        result = await get_graph_metrics(
          graph_id="kg01234567890abcdef",
          current_user=_make_mock_user(),
          db=Mock(),
          _rate_limit=None,
        )

    assert result.graph_id == "kg01234567890abcdef"

  @pytest.mark.asyncio
  async def test_returns_404_when_no_metrics(self):
    """Test 404 when metrics not available."""
    mock_service = AsyncMock()
    mock_service.collect_metrics_for_graph_async = AsyncMock(return_value=None)

    cb_patch, tc_patch, ol_patch, rm_patch = _patch_robustness()

    with cb_patch, tc_patch, ol_patch, rm_patch:
      with patch(
        "robosystems.routers.graphs.usage.graph_metrics_service",
        mock_service,
      ):
        with pytest.raises(HTTPException) as exc_info:
          await get_graph_metrics(
            graph_id="kg01234567890abcdef",
            current_user=_make_mock_user(),
            db=Mock(),
            _rate_limit=None,
          )
        assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_returns_404_when_metrics_has_error(self):
    """Test 404 when metrics contain error key."""
    mock_service = AsyncMock()
    mock_service.collect_metrics_for_graph_async = AsyncMock(
      return_value={"error": "Graph not found"}
    )

    cb_patch, tc_patch, ol_patch, rm_patch = _patch_robustness()

    with cb_patch, tc_patch, ol_patch, rm_patch:
      with patch(
        "robosystems.routers.graphs.usage.graph_metrics_service",
        mock_service,
      ):
        with pytest.raises(HTTPException) as exc_info:
          await get_graph_metrics(
            graph_id="kg01234567890abcdef",
            current_user=_make_mock_user(),
            db=Mock(),
            _rate_limit=None,
          )
        assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_timeout_returns_504(self):
    """Test timeout handling returns 504."""
    mock_service = AsyncMock()

    async def slow_metrics(*args, **kwargs):
      await asyncio.sleep(100)

    mock_service.collect_metrics_for_graph_async = slow_metrics

    cb_patch, tc_patch, ol_patch, rm_patch = _patch_robustness()

    with cb_patch, tc_patch as tc, ol_patch, rm_patch:
      # Force very short timeout
      tc.return_value.calculate_timeout.return_value = 0.001
      with patch(
        "robosystems.routers.graphs.usage.graph_metrics_service",
        mock_service,
      ):
        with pytest.raises(HTTPException) as exc_info:
          await get_graph_metrics(
            graph_id="kg01234567890abcdef",
            current_user=_make_mock_user(),
            db=Mock(),
            _rate_limit=None,
          )
        assert exc_info.value.status_code == 504

  @pytest.mark.asyncio
  async def test_general_exception_returns_500(self):
    """Test general exception handling returns 500."""
    mock_service = AsyncMock()
    mock_service.collect_metrics_for_graph_async = AsyncMock(
      side_effect=RuntimeError("Unexpected error")
    )

    cb_patch, tc_patch, ol_patch, rm_patch = _patch_robustness()

    with cb_patch, tc_patch, ol_patch, rm_patch:
      with patch(
        "robosystems.routers.graphs.usage.graph_metrics_service",
        mock_service,
      ):
        with pytest.raises(HTTPException) as exc_info:
          await get_graph_metrics(
            graph_id="kg01234567890abcdef",
            current_user=_make_mock_user(),
            db=Mock(),
            _rate_limit=None,
          )
        assert exc_info.value.status_code == 500


@pytest.mark.unit
class TestGetGraphUsageAnalytics:
  """Test the get_graph_usage endpoint."""

  @pytest.mark.asyncio
  async def test_basic_usage_analytics_no_data(self):
    """Test usage analytics with empty data."""
    mock_db = Mock()
    mock_query = Mock()
    mock_query.filter.return_value = mock_query
    mock_query.order_by.return_value = mock_query
    mock_query.limit.return_value = mock_query
    mock_query.all.return_value = []
    mock_db.query.return_value = mock_query

    cb_patch, tc_patch, ol_patch, rm_patch = _patch_robustness()

    with cb_patch, tc_patch, ol_patch, rm_patch:
      with (
        patch(
          "robosystems.routers.graphs.usage.GraphUsage.get_monthly_storage_summary",
          return_value={},
        ),
        patch(
          "robosystems.routers.graphs.usage.GraphUsage.get_monthly_credit_summary",
          return_value={},
        ),
      ):
        result = await get_graph_usage(
          graph_id="kg01234567890abcdef",
          time_range="30d",
          include_storage=True,
          include_credits=True,
          include_performance=False,
          include_events=False,
          current_user=_make_mock_user(),
          db=mock_db,
          _rate_limit=None,
        )

    assert result.graph_id == "kg01234567890abcdef"
    assert result.time_range == "30d"
    assert result.storage_summary is None
    assert result.credit_summary is None

  @pytest.mark.asyncio
  async def test_usage_analytics_with_storage_data(self):
    """Test usage analytics with storage data present."""
    mock_db = Mock()
    graph_id = "kg01234567890abcdef"

    storage_data = {
      graph_id: {
        "graph_tier": "ladybug-standard",
        "avg_storage_gb": 5.0,
        "max_storage_gb": 8.0,
        "min_storage_gb": 2.0,
        "total_gb_hours": 120.0,
        "measurement_count": 24,
      }
    }

    cb_patch, tc_patch, ol_patch, rm_patch = _patch_robustness()

    with cb_patch, tc_patch, ol_patch, rm_patch:
      with (
        patch(
          "robosystems.routers.graphs.usage.GraphUsage.get_monthly_storage_summary",
          return_value=storage_data,
        ),
        patch(
          "robosystems.routers.graphs.usage.GraphUsage.get_monthly_credit_summary",
          return_value={},
        ),
      ):
        result = await get_graph_usage(
          graph_id=graph_id,
          time_range="30d",
          include_storage=True,
          include_credits=False,
          include_performance=False,
          include_events=False,
          current_user=_make_mock_user(),
          db=mock_db,
          _rate_limit=None,
        )

    assert result.storage_summary is not None
    assert result.storage_summary.avg_storage_gb == 5.0
    assert result.storage_summary.graph_tier == "ladybug-standard"

  @pytest.mark.asyncio
  async def test_usage_analytics_skips_disabled_sections(self):
    """Test that disabled sections are not populated."""
    mock_db = Mock()

    cb_patch, tc_patch, ol_patch, rm_patch = _patch_robustness()

    with cb_patch, tc_patch, ol_patch, rm_patch:
      result = await get_graph_usage(
        graph_id="kg01234567890abcdef",
        time_range="24h",
        include_storage=False,
        include_credits=False,
        include_performance=False,
        include_events=False,
        current_user=_make_mock_user(),
        db=mock_db,
        _rate_limit=None,
      )

    assert result.storage_summary is None
    assert result.credit_summary is None
    assert result.performance_insights is None
    assert result.recent_events == []

  @pytest.mark.asyncio
  async def test_timeout_returns_504(self):
    """Test timeout handling in usage analytics."""
    cb_patch, tc_patch, ol_patch, rm_patch = _patch_robustness()

    # Make the circuit breaker raise TimeoutError
    mock_cb = Mock()
    mock_cb.check_circuit = Mock(side_effect=TimeoutError("timeout"))
    mock_cb.record_failure = Mock()

    with (
      patch(
        "robosystems.routers.graphs.usage.CircuitBreakerManager",
        return_value=mock_cb,
      ),
      tc_patch,
      ol_patch,
      rm_patch,
    ):
      with pytest.raises(HTTPException) as exc_info:
        await get_graph_usage(
          graph_id="kg01234567890abcdef",
          time_range="30d",
          include_storage=False,
          include_credits=False,
          include_performance=False,
          include_events=False,
          current_user=_make_mock_user(),
          db=Mock(),
          _rate_limit=None,
        )
      assert exc_info.value.status_code == 504
