"""The sync-result discipline: the books' opening month and the calendar bootstrap."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest

from robosystems.adapters.bank_feed.sync import (
  BankFeedSyncConfig,
  bootstrap_fiscal_calendar_if_needed,
  default_backfill_start,
  first_period,
  record_failed_sync_result,
  release_sync_lock,
)


def _config(**overrides) -> BankFeedSyncConfig:
  base = {"graph_id": "kg_test", "connection_id": "conn_1", "user_id": "usr_1"}
  base.update(overrides)
  return BankFeedSyncConfig(**base)


@pytest.mark.unit
def test_default_backfill_start_is_january_of_last_year():
  assert default_backfill_start(date(2026, 9, 12)) == date(2025, 1, 1)


@pytest.mark.unit
class TestFirstPeriod:
  def test_plausible_first_month_opens_the_books_there(self):
    assert first_period("2026-03-14T15:04:05Z", "2026-09") == "2026-03"

  def test_placeholder_year_one_is_ignored(self):
    assert first_period("0001-01-01T00:00:00Z", "2026-09") is None

  def test_future_or_current_month_is_ignored(self):
    assert first_period("2026-09-01T00:00:00Z", "2026-09") is None
    assert first_period("2027-01-01T00:00:00Z", "2026-09") is None

  def test_missing_or_short_is_ignored(self):
    assert first_period(None, "2026-09") is None
    assert first_period("2026", "2026-09") is None


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
      bootstrap_fiscal_calendar_if_needed(
        MagicMock(), _config(), "2026-03-14T15:04:05Z", source_label="Plaid"
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
      bootstrap_fiscal_calendar_if_needed(
        MagicMock(), _config(), None, source_label="Plaid"
      )
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
      bootstrap_fiscal_calendar_if_needed(
        MagicMock(), _config(), "2026-03-01T00:00:00Z", source_label="Plaid"
      )
    service.initialize.assert_not_called()
    session.commit.assert_not_called()

  def test_failure_is_non_fatal(self):
    context = MagicMock()
    with patch(
      "robosystems.db.extensions.extensions_session", side_effect=RuntimeError("db")
    ):
      bootstrap_fiscal_calendar_if_needed(
        context, _config(), None, source_label="Plaid"
      )
    context.log.warning.assert_called()


@pytest.mark.unit
class TestBookkeeping:
  def test_no_lock_id_releases_nothing(self):
    with patch("robosystems.config.valkey_registry.create_redis_client") as client:
      release_sync_lock(MagicMock(), _config())
    client.assert_not_called()

  def test_lock_is_released_by_its_id(self):
    with (
      patch("robosystems.config.valkey_registry.create_redis_client"),
      patch(
        "robosystems.middleware.auth.distributed_lock.release_lock_by_id",
        return_value=True,
      ) as release,
    ):
      release_sync_lock(MagicMock(), _config(sync_lock_id="lock_1"))
    kwargs = release.call_args.kwargs
    assert kwargs == {**kwargs, "lock_key": "qb_sync:conn_1", "lock_id": "lock_1"}

  def test_failure_is_recorded_without_advancing_last_sync(self):
    conn = MagicMock()
    session = MagicMock()
    factory = MagicMock()
    factory.return_value.__enter__.return_value = session
    with (
      patch("robosystems.database.SessionFactory", factory),
      patch(
        "robosystems.models.core.connection.connection.Connection.get_by_id",
        return_value=conn,
      ),
    ):
      record_failed_sync_result(
        MagicMock(), _config(full_rebuild=True), RuntimeError("bank down")
      )
    result = conn.record_sync_result.call_args.args[1]
    assert result["status"] == "failed"
    assert result["error"] == {"code": "RuntimeError", "message": "bank down"}
    assert result["window"]["full_rebuild"] is True
    conn.update_last_sync.assert_not_called()
