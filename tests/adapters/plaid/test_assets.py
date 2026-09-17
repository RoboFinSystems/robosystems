"""The asset: the window and cursor, the job's retry policy, and failures."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from dagster import Failure, MaterializeResult, build_asset_context

from robosystems.adapters.bank_feed.sync import default_backfill_start
from robosystems.adapters.plaid.client import PlaidError, TransactionsSync
from robosystems.adapters.plaid.pipeline.assets import (
  PULL_POLL_SECONDS,
  PULL_WAIT_SECONDS,
  PlaidSyncConfig,
  get_dagster_components,
  plaid_feed,
  plaid_sync_job,
  since_date,
  sync_cursor,
)

MODULE = "robosystems.adapters.plaid.pipeline.assets"


def _config(**overrides) -> PlaidSyncConfig:
  base = {"graph_id": "kg_test", "connection_id": "conn_1", "user_id": "usr_1"}
  base.update(overrides)
  return PlaidSyncConfig(**base)


@pytest.mark.unit
class TestWindow:
  def test_explicit_since_wins(self):
    cfg = _config(since_date="2026-02-01")
    assert since_date(cfg, {"since_date": "2025-01-01"}) == date(2026, 2, 1)

  def test_connect_time_start_then_the_default(self):
    assert since_date(_config(), {"since_date": "2025-06-01"}) == date(2025, 6, 1)
    assert since_date(_config(), {}) == default_backfill_start()

  def test_incremental_continues_from_the_stored_cursor(self):
    assert sync_cursor(_config(), {"cursor": "c1"}) == "c1"
    assert sync_cursor(_config(), {}) is None

  def test_rebuild_or_an_explicit_window_replays_the_history(self):
    assert sync_cursor(_config(full_rebuild=True), {"cursor": "c1"}) is None
    assert sync_cursor(_config(since_date="2026-01-01"), {"cursor": "c1"}) is None


@pytest.mark.unit
class TestDagsterShape:
  def test_components_and_job(self):
    components = get_dagster_components()
    assert components["assets"] == [plaid_feed]
    assert components["jobs"] == [plaid_sync_job]
    assert plaid_sync_job.name == "plaid_sync"

  def test_only_a_dead_worker_is_retried(self):
    assert plaid_sync_job.tags["dagster/max_retries"] == "3"
    assert plaid_sync_job.tags["dagster/retry_on_asset_or_op_failure"] == "false"

  def test_failure_is_recorded_and_the_lock_released(self):
    cfg = _config(sync_lock_id="lock_1")
    with (
      patch(f"{MODULE}._run_plaid_sync", side_effect=RuntimeError("boom")),
      patch(f"{MODULE}.record_failed_sync_result") as record,
      patch(f"{MODULE}.release_sync_lock") as release,
    ):
      with pytest.raises(RuntimeError, match="boom"):
        plaid_feed(build_asset_context(), config=cfg)
    record.assert_called_once()
    release.assert_called_once()
    assert release.call_args.args[1].sync_lock_id == "lock_1"

  def test_success_still_releases_the_lock(self):
    with (
      patch(f"{MODULE}._run_plaid_sync", return_value=MaterializeResult()),
      patch(f"{MODULE}.record_failed_sync_result") as record,
      patch(f"{MODULE}.release_sync_lock") as release,
    ):
      plaid_feed(build_asset_context(), config=_config())
    record.assert_not_called()
    release.assert_called_once()


@pytest.mark.unit
class TestBody:
  def test_a_connection_that_never_finished_link_fails(self):
    from robosystems.adapters.plaid.pipeline.assets import _run_plaid_sync

    with patch(f"{MODULE}.load_credentials", return_value={"sync_config": {}}):
      with pytest.raises(Failure, match="has not finished Link"):
        _run_plaid_sync(build_asset_context(), _config())

  def test_a_broken_login_marks_the_connection_and_fails(self):
    from robosystems.adapters.plaid.pipeline.assets import _run_plaid_sync

    client = MagicMock()
    client.get_accounts.side_effect = PlaidError(
      "login required", code="ITEM_LOGIN_REQUIRED"
    )
    with (
      patch(f"{MODULE}.load_credentials", return_value={"access_token": "access-1"}),
      patch(
        "robosystems.operations.providers.plaid_provider.plaid_client",
        return_value=client,
      ),
      patch(f"{MODULE}.mark_needs_reauth") as mark,
    ):
      with pytest.raises(Failure, match="back in Link"):
        _run_plaid_sync(build_asset_context(), _config())
    mark.assert_called_once_with("conn_1")
    client.close.assert_called_once()

  def test_a_transient_error_does_not_touch_the_connection(self):
    from robosystems.adapters.plaid.pipeline.assets import _run_plaid_sync

    client = MagicMock()
    client.get_accounts.side_effect = PlaidError("down", code="INSTITUTION_DOWN")
    with (
      patch(f"{MODULE}.load_credentials", return_value={"access_token": "access-1"}),
      patch(
        "robosystems.operations.providers.plaid_provider.plaid_client",
        return_value=client,
      ),
      patch(f"{MODULE}.mark_needs_reauth") as mark,
    ):
      with pytest.raises(PlaidError):
        _run_plaid_sync(build_asset_context(), _config())
    mark.assert_not_called()

  def test_the_cursor_advances_only_when_plaid_was_ready(self):
    run = _run_body([_sync("HISTORICAL_UPDATE_COMPLETE", next_cursor="c9")])
    run.client.sync_transactions.assert_called_once_with("access-1", "c8")
    run.store.assert_called_once_with("conn_1", "c9")
    run.session.commit.assert_called_once()
    summary = run.update.call_args.args[2]
    assert summary["cursor_stored"] is True
    assert summary["history_complete"] is True
    assert summary["window"]["full_rebuild"] is False
    run.bootstrap.assert_called_once()

  def test_the_body_waits_for_the_historical_pull(self):
    run = _run_body(
      [
        _sync("NOT_READY"),
        _sync("INITIAL_UPDATE_COMPLETE", next_cursor="c1"),
        _sync("HISTORICAL_UPDATE_COMPLETE", next_cursor="c2"),
      ]
    )
    assert run.client.sync_transactions.call_count == 3
    assert run.clock.sleep.call_count == 2
    run.store.assert_called_once_with("conn_1", "c2")
    run.bootstrap.assert_called_once()

  def test_a_first_pull_still_empty_after_the_wait_fails_and_writes_nothing(self):
    run = _run_body(lambda *args: _sync("NOT_READY"), expect=Failure)
    assert "has not finished" in str(run.error)
    polls = PULL_WAIT_SECONDS // PULL_POLL_SECONDS + 1
    assert run.client.sync_transactions.call_count == polls
    run.session.commit.assert_not_called()
    run.store.assert_not_called()
    run.update.assert_not_called()

  def test_a_partial_history_is_captured_but_the_calendar_waits(self):
    run = _run_body(lambda *args: _sync("INITIAL_UPDATE_COMPLETE", next_cursor="c1"))
    run.session.commit.assert_called_once()
    run.store.assert_called_once_with("conn_1", "c1")
    run.bootstrap.assert_not_called()
    assert run.update.call_args.args[2]["history_complete"] is False

  def test_failed_captures_hold_the_cursor_and_fail_the_run(self):
    run = _run_body(
      [_sync("HISTORICAL_UPDATE_COMPLETE", next_cursor="c9")],
      failed=2,
      errors=["plaid_txn_x: boom"],
      expect=Failure,
    )
    assert "failed to capture" in str(run.error) and "plaid_txn_x" in str(run.error)
    run.session.commit.assert_called_once()  # the rows that captured are kept
    run.store.assert_not_called()
    run.update.assert_not_called()
    run.stale.assert_called_once()


def _sync(status: str, *, next_cursor: str = "") -> TransactionsSync:
  return TransactionsSync(next_cursor=next_cursor, update_status=status)


def _run_body(syncs, *, failed=0, errors=(), expect=None):
  """Run the body against a mocked Plaid client and tenant session.

  ``syncs`` is the sequence ``sync_transactions`` answers, or a callable that
  answers every call; the clock is stubbed so the wait loop runs at once.
  """
  from robosystems.adapters.plaid.pipeline.assets import _run_plaid_sync

  client = MagicMock()
  client.get_accounts.return_value = {"accounts": [], "item": {}}
  client.sync_transactions.side_effect = syncs
  session = MagicMock()
  extensions = MagicMock()
  extensions.return_value.__enter__.return_value = session
  report = MagicMock(
    events_created=3,
    events_existing=0,
    events_updated=0,
    events_removed=0,
    transfers_matched=0,
    events_failed=failed,
    errors=list(errors),
    earliest_occurred_at="2026-08-20T00:00:00Z",
  )
  report.as_counts.return_value = {}
  run = SimpleNamespace(client=client, session=session, error=None, result=None)
  credentials = {"access_token": "access-1", "cursor": "c8", "item_id": "i1"}
  with (
    patch(f"{MODULE}.load_credentials", return_value=credentials),
    patch(
      "robosystems.operations.providers.plaid_provider.plaid_client",
      return_value=client,
    ),
    patch("robosystems.db.extensions.extensions_session", extensions),
    patch(
      "robosystems.adapters.bank_feed.accounts.link_bank_accounts",
      return_value=MagicMock(links={}, linked=0, created=0),
    ),
    patch("robosystems.adapters.bank_feed.accounts.build_chart_index"),
    patch("robosystems.adapters.plaid.pipeline.load.load_sync", return_value=report),
    patch(f"{MODULE}.time") as clock,
    patch(f"{MODULE}.store_cursor") as store,
    patch(f"{MODULE}.update_last_sync") as update,
    patch(f"{MODULE}.bootstrap_fiscal_calendar_if_needed") as bootstrap,
    patch(f"{MODULE}.mark_graph_stale") as stale,
  ):
    run.clock, run.store, run.update = clock, store, update
    run.bootstrap, run.stale = bootstrap, stale
    if expect is None:
      run.result = _run_plaid_sync(build_asset_context(), _config())
    else:
      with pytest.raises(expect) as excinfo:
        _run_plaid_sync(build_asset_context(), _config())
      run.error = excinfo.value
  return run
