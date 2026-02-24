"""Tests for usage analytics helper functions."""

from datetime import UTC, datetime

import pytest

from robosystems.routers.graphs.usage import (
  _get_days_from_time_range,
  _parse_time_range,
)


class TestParseTimeRange:
  """Tests for _parse_time_range helper."""

  @pytest.mark.unit
  def test_current_month(self):
    now = datetime(2024, 6, 15, tzinfo=UTC)
    year, month = _parse_time_range("current_month", now)
    assert year == 2024
    assert month == 6

  @pytest.mark.unit
  def test_last_month_same_year(self):
    now = datetime(2024, 3, 15, tzinfo=UTC)
    year, month = _parse_time_range("last_month", now)
    assert year == 2024
    assert month == 2

  @pytest.mark.unit
  def test_last_month_year_boundary(self):
    now = datetime(2024, 1, 15, tzinfo=UTC)
    year, month = _parse_time_range("last_month", now)
    assert year == 2023
    assert month == 12

  @pytest.mark.unit
  def test_24h_defaults_to_current(self):
    now = datetime(2024, 8, 20, tzinfo=UTC)
    year, month = _parse_time_range("24h", now)
    assert year == 2024
    assert month == 8

  @pytest.mark.unit
  def test_30d_defaults_to_current(self):
    now = datetime(2025, 11, 3, tzinfo=UTC)
    year, month = _parse_time_range("30d", now)
    assert year == 2025
    assert month == 11


class TestGetDaysFromTimeRange:
  """Tests for _get_days_from_time_range helper."""

  @pytest.mark.unit
  def test_24h_returns_1(self):
    assert _get_days_from_time_range("24h") == 1

  @pytest.mark.unit
  def test_7d_returns_7(self):
    assert _get_days_from_time_range("7d") == 7

  @pytest.mark.unit
  def test_30d_returns_30(self):
    assert _get_days_from_time_range("30d") == 30

  @pytest.mark.unit
  def test_current_month_returns_day_of_month(self):
    result = _get_days_from_time_range("current_month")
    assert result > 0
    assert result <= 31

  @pytest.mark.unit
  def test_last_month_returns_positive_days(self):
    result = _get_days_from_time_range("last_month")
    assert result > 0

  @pytest.mark.unit
  def test_unknown_defaults_30(self):
    assert _get_days_from_time_range("unknown") == 30
