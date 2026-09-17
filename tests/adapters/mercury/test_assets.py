"""The asset's window arithmetic and its failure bookkeeping."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from unittest.mock import patch

import pytest
from dagster import MaterializeResult, build_asset_context

from robosystems.adapters.bank_feed.sync import default_backfill_start
from robosystems.adapters.mercury.pipeline.assets import (
  MercurySyncConfig,
  _since_date,
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
      patch(f"{MODULE}.record_failed_sync_result") as record,
      patch(f"{MODULE}.release_sync_lock") as release,
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
      patch(f"{MODULE}.record_failed_sync_result") as record,
      patch(f"{MODULE}.release_sync_lock") as release,
    ):
      mercury_feed(build_asset_context(), config=cfg)
    run.assert_called_once()
    record.assert_not_called()
    release.assert_called_once()
