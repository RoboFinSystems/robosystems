"""Tests for _build_periods helper — JSONB date coercion and legacy mode."""

from __future__ import annotations

from datetime import date

from robosystems.routers.ledger.reports import _build_periods


class TestBuildPeriodsLegacy:
  def test_single_period_no_comparative(self):
    result = _build_periods(date(2026, 1, 1), date(2026, 3, 31), comparative=False)
    assert len(result) == 1
    assert result[0].start == date(2026, 1, 1)
    assert result[0].end == date(2026, 3, 31)
    assert result[0].label == "Current"

  def test_comparative_adds_prior_period(self):
    result = _build_periods(date(2026, 1, 1), date(2026, 3, 31), comparative=True)
    assert len(result) == 2
    assert result[0].label == "Current"
    assert result[1].label == "Prior"
    assert result[1].end == date(2025, 12, 31)


class TestBuildPeriodsJson:
  def test_parses_date_strings_from_jsonb(self):
    """JSONB returns dates as strings — _build_periods must parse them."""
    periods_json = [
      {"start": "2026-01-01", "end": "2026-01-31", "label": "Jan 2026"},
      {"start": "2026-02-01", "end": "2026-02-28", "label": "Feb 2026"},
    ]
    result = _build_periods(None, None, False, periods_json)

    assert len(result) == 2
    assert result[0].start == date(2026, 1, 1)
    assert result[0].end == date(2026, 1, 31)
    assert isinstance(result[0].start, date)
    assert isinstance(result[0].end, date)

  def test_handles_native_date_objects(self):
    """When periods come from a Pydantic model, dates are already native."""
    periods_json = [
      {"start": date(2026, 3, 1), "end": date(2026, 3, 31), "label": "Mar 2026"},
    ]
    result = _build_periods(None, None, False, periods_json)

    assert len(result) == 1
    assert result[0].start == date(2026, 3, 1)

  def test_handles_pydantic_period_spec_objects(self):
    """When periods are PeriodSpec objects (from request body)."""
    from robosystems.models.api.extensions.reports import PeriodSpec

    periods_json = [
      PeriodSpec(start=date(2026, 4, 1), end=date(2026, 4, 30), label="Apr 2026"),
    ]
    result = _build_periods(None, None, False, periods_json)

    assert len(result) == 1
    assert result[0].start == date(2026, 4, 1)
    assert result[0].label == "Apr 2026"

  def test_overrides_legacy_fields(self):
    """When periods_json is provided, period_start/end/comparative are ignored."""
    periods_json = [
      {"start": "2026-06-01", "end": "2026-06-30", "label": "June"},
    ]
    result = _build_periods(
      date(2026, 1, 1), date(2026, 3, 31), comparative=True, periods_json=periods_json
    )

    assert len(result) == 1
    assert result[0].label == "June"

  def test_mixed_string_and_date(self):
    """Edge case: one field is string, other is date."""
    periods_json = [
      {"start": "2026-01-01", "end": date(2026, 1, 31), "label": "Mixed"},
    ]
    result = _build_periods(None, None, False, periods_json)

    assert result[0].start == date(2026, 1, 1)
    assert result[0].end == date(2026, 1, 31)
