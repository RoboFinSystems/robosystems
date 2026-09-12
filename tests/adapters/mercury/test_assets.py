"""The asset's window arithmetic and its failure bookkeeping."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from dagster import MaterializeResult, build_asset_context

from robosystems.adapters.mercury.pipeline.assets import (
  MercurySyncConfig,
  _bootstrap_fiscal_calendar_if_needed,
  _first_period,
  _since_date,
  default_backfill_start,
  get_dagster_components,
  mercury_feed,
  mercury_sync_job,
)

MODULE = "robosystems.adapters.mercury.pipeline.assets"


def _config(**overrides) -> MercurySyncConfig:
  base = {"graph_id": "kg_test", "connection_id": "conn_1", "user_id": "usr_1"}
  base.update(overrides)
  return MercurySyncConfig(**base)


@pytest.mark.unit
class TestSinceDate:
  def test_explicit_since_wins(self):
    cfg = _config(since_date="2026-02-01", full_rebuild=True)
    assert _since_date(cfg, {"since_date": "2025-01-01"}, None) == date(2026, 2, 1)

  def test_first_sync_uses_the_connect_time_start(self):
    assert _since_date(_config(), {"since_date": "2025-06-01"}, None) == date(
      2025, 6, 1
    )

  def test_first_sync_default_is_january_of_last_year(self):
    assert _since_date(_config(), {}, None) == default_backfill_start()
    assert default_backfill_start(date(2026, 9, 12)) == date(2025, 1, 1)

  def test_full_rebuild_ignores_last_sync(self):
    cfg = _config(full_rebuild=True)
    assert _since_date(cfg, {"since_date": "2025-06-01"}, datetime.now(UTC)) == date(
      2025, 6, 1
    )

  def test_incremental_looks_back_but_not_before_the_start(self):
    cfg = _config(lookback_days=30)
    recent = _since_date(cfg, {"since_date": "2020-01-01"}, datetime.now(UTC))
    assert recent == date.today() - timedelta(days=30)
    late_start = (date.today() - timedelta(days=5)).isoformat()
    assert _since_date(
      cfg, {"since_date": late_start}, datetime.now(UTC)
    ) == date.fromisoformat(late_start)


@pytest.mark.unit
class TestFirstPeriod:
  def test_plausible_first_month_opens_the_books_there(self):
    assert _first_period("2026-03-14T15:04:05Z", "2026-09") == "2026-03"

  def test_placeholder_year_one_is_ignored(self):
    assert _first_period("0001-01-01T00:00:00Z", "2026-09") is None

  def test_future_or_current_month_is_ignored(self):
    assert _first_period("2026-09-01T00:00:00Z", "2026-09") is None
    assert _first_period("2027-01-01T00:00:00Z", "2026-09") is None

  def test_missing_or_short_is_ignored(self):
    assert _first_period(None, "2026-09") is None
    assert _first_period("2026", "2026-09") is None


@pytest.mark.unit
class TestDagsterShape:
  def test_components_and_job(self):
    components = get_dagster_components()
    assert components["assets"] == [mercury_feed]
    assert components["jobs"] == [mercury_sync_job]
    assert mercury_sync_job.name == "mercury_sync"
    assert mercury_sync_job.tags["dagster/max_retries"] == "3"

  def test_failure_is_recorded_and_the_lock_released(self):
    cfg = _config(sync_lock_id="lock_1")
    with (
      patch(f"{MODULE}._run_mercury_sync", side_effect=RuntimeError("boom")),
      patch(f"{MODULE}._record_failed_sync_result") as record,
      patch(f"{MODULE}._release_sync_lock") as release,
    ):
      with pytest.raises(RuntimeError, match="boom"):
        mercury_feed(build_asset_context(), config=cfg)
    record.assert_called_once()
    assert isinstance(record.call_args.args[2], RuntimeError)
    release.assert_called_once()
    assert release.call_args.args[1].sync_lock_id == "lock_1"

  def test_success_still_releases_the_lock(self):
    cfg = _config(sync_lock_id="lock_1")
    with (
      patch(f"{MODULE}._run_mercury_sync", return_value=MaterializeResult()) as run,
      patch(f"{MODULE}._record_failed_sync_result") as record,
      patch(f"{MODULE}._release_sync_lock") as release,
    ):
      mercury_feed(build_asset_context(), config=cfg)
    run.assert_called_once()
    record.assert_not_called()
    release.assert_called_once()


@pytest.mark.unit
class TestFiscalCalendarBootstrap:
  def _service(self, existing=None):
    service = MagicMock()
    service.get.return_value = existing
    service.ensure_fiscal_periods.return_value = 7
    return service

  def _session_cm(self):
    session = MagicMock()
    cm = MagicMock()
    cm.__enter__.return_value = session
    cm.__exit__.return_value = False
    return session, cm

  def test_opens_at_the_first_posted_month(self):
    service = self._service()
    session, cm = self._session_cm()
    with (
      patch("robosystems.db.extensions.extensions_session", return_value=cm),
      patch(
        "robosystems.operations.roboledger.fiscal_calendar.FiscalCalendarService",
        return_value=service,
      ),
      patch(
        "robosystems.operations.roboledger.fiscal_calendar.current_month_period",
        return_value="2026-09",
      ),
    ):
      _bootstrap_fiscal_calendar_if_needed(
        MagicMock(), _config(), "2026-03-14T15:04:05Z"
      )
    kwargs = service.initialize.call_args.kwargs
    assert kwargs["closed_through"] == "2026-02"
    assert kwargs["actor_type"] == "system"
    ensure = service.ensure_fiscal_periods.call_args.kwargs
    assert (
      ensure["start_period"] == "2024-10"
    )  # the 24-month window reaches back further
    assert ensure["end_period"] == "2026-09"
    session.commit.assert_called_once()

  def test_no_events_falls_back_to_month_before_last(self):
    service = self._service()
    session, cm = self._session_cm()
    with (
      patch("robosystems.db.extensions.extensions_session", return_value=cm),
      patch(
        "robosystems.operations.roboledger.fiscal_calendar.FiscalCalendarService",
        return_value=service,
      ),
      patch(
        "robosystems.operations.roboledger.fiscal_calendar.current_month_period",
        return_value="2026-09",
      ),
    ):
      _bootstrap_fiscal_calendar_if_needed(MagicMock(), _config(), None)
    assert service.initialize.call_args.kwargs["closed_through"] == "2026-07"

  def test_existing_calendar_is_left_alone(self):
    existing = MagicMock(initialized_at=datetime.now(UTC))
    service = self._service(existing)
    session, cm = self._session_cm()
    with (
      patch("robosystems.db.extensions.extensions_session", return_value=cm),
      patch(
        "robosystems.operations.roboledger.fiscal_calendar.FiscalCalendarService",
        return_value=service,
      ),
    ):
      _bootstrap_fiscal_calendar_if_needed(
        MagicMock(), _config(), "2026-03-01T00:00:00Z"
      )
    service.initialize.assert_not_called()
    session.commit.assert_not_called()

  def test_failure_is_non_fatal(self):
    context = MagicMock()
    with patch(
      "robosystems.db.extensions.extensions_session", side_effect=RuntimeError("db")
    ):
      _bootstrap_fiscal_calendar_if_needed(context, _config(), None)
    context.log.warning.assert_called()
