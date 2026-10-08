"""Tests for QuickBooks load Dagster asset (OLTP version)."""

from unittest.mock import MagicMock, Mock, patch

import pytest
from dagster import build_asset_context

_PATCH_LOAD_WORK_DIR = (
  "robosystems.adapters.quickbooks.pipeline.load.get_pipeline_work_dir"
)
_PATCH_OLTP_LOADER = "robosystems.operations.extensions.loader.OLTPLoader"
_PATCH_BASELINE = "robosystems.adapters.quickbooks.pipeline.load._is_baseline_import"
_PATCH_CONN_SVC = "robosystems.operations.connection_service.ConnectionService"


def _make_config(
  graph_id="kg_test123",
  connection_id="conn_abc",
  user_id="user_xyz",
  realm_id="123456789",
  full_rebuild=False,
  lookback_days=60,
):
  """Create a QBSyncConfig for tests."""
  from robosystems.adapters.quickbooks.pipeline.configs import QBSyncConfig

  return QBSyncConfig(
    graph_id=graph_id,
    connection_id=connection_id,
    user_id=user_id,
    realm_id=realm_id,
    full_rebuild=full_rebuild,
    lookback_days=lookback_days,
  )


def _make_load_result(
  graph_id="kg_test123",
  source="quickbooks",
  connection_id="conn_abc",
  elements=5,
  dimensions=2,
  events_captured=10,
  events_updated=0,
  agents_inserted=0,
  agents_updated=0,
  dropped_unbalanced_entries=0,
  dropped_empty_transactions=0,
):
  """Create a LoadResult for tests."""
  from robosystems.operations.extensions.loader import LoadResult

  return LoadResult(
    graph_id=graph_id,
    source=source,
    connection_id=connection_id,
    elements=elements,
    dimensions=dimensions,
    events_captured=events_captured,
    events_updated=events_updated,
    agents_inserted=agents_inserted,
    agents_updated=agents_updated,
    dropped_unbalanced_entries=dropped_unbalanced_entries,
    dropped_empty_transactions=dropped_empty_transactions,
  )


@pytest.mark.unit
class TestQbLoadAsset:
  """Tests for the qb_load Dagster asset."""

  @pytest.fixture(autouse=True)
  def _books_already_posted(self):
    with patch(_PATCH_BASELINE, return_value=False):
      yield

  def test_load_calls_oltp_loader(self, tmp_path):
    """Test that qb_load creates an OLTPLoader and calls load()."""
    from dagster import MaterializeResult

    from robosystems.adapters.quickbooks.pipeline.load import qb_load

    config = _make_config()
    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)

    mock_loader = MagicMock()
    mock_loader.load.return_value = _make_load_result()

    with (
      patch(_PATCH_LOAD_WORK_DIR, return_value=work_dir),
      patch(_PATCH_OLTP_LOADER, return_value=mock_loader),
      patch(
        "robosystems.adapters.quickbooks.pipeline.load._update_last_sync",
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.load._advance_cdc_watermark",
      ),
    ):
      context = build_asset_context()
      result = qb_load(context, config)

    assert isinstance(result, MaterializeResult)
    mock_loader.load.assert_called_once_with(
      graph_id=config.graph_id,
      source="quickbooks",
      connection_id=config.connection_id,
      duckdb_path=work_dir / "quickbooks.duckdb",
      created_by=config.user_id,
      # Incremental sync passes the empty-string since_date as None (the
      # asset translates the QBSyncConfig default) and the explicit
      # full_rebuild=False so the loader skips the pre-sync wipe.
      full_rebuild=False,
      since_date=None,
      baseline=False,
    )

  def test_a_baseline_import_is_passed_to_the_loader(self, tmp_path):
    """Books with nothing posted yet take the history past the period fence,
    and the sync summary says so."""
    from robosystems.adapters.quickbooks.pipeline.load import qb_load

    config = _make_config()
    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)

    mock_loader = MagicMock()
    mock_loader.load.return_value = _make_load_result()

    with (
      patch(_PATCH_BASELINE, return_value=True),
      patch(_PATCH_LOAD_WORK_DIR, return_value=work_dir),
      patch(_PATCH_OLTP_LOADER, return_value=mock_loader),
      patch(
        "robosystems.adapters.quickbooks.pipeline.load._update_last_sync",
      ) as mock_sync,
      patch(
        "robosystems.adapters.quickbooks.pipeline.load._advance_cdc_watermark",
      ),
    ):
      qb_load(build_asset_context(), config)

    assert mock_loader.load.call_args.kwargs["baseline"] is True
    assert mock_sync.call_args.args[2]["baseline"] is True

  def test_load_returns_row_counts_in_metadata(self, tmp_path):
    """Metadata: elements/dimensions structural + event counters."""
    from robosystems.adapters.quickbooks.pipeline.load import qb_load

    config = _make_config()
    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)

    mock_loader = MagicMock()
    mock_loader.load.return_value = _make_load_result(
      elements=3,
      dimensions=1,
      events_captured=7,
      events_updated=2,
      agents_inserted=4,
      agents_updated=1,
      dropped_unbalanced_entries=1,
      dropped_empty_transactions=0,
    )

    with (
      patch(_PATCH_LOAD_WORK_DIR, return_value=work_dir),
      patch(_PATCH_OLTP_LOADER, return_value=mock_loader),
      patch(
        "robosystems.adapters.quickbooks.pipeline.load._update_last_sync",
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.load._advance_cdc_watermark",
      ),
    ):
      context = build_asset_context()
      result = qb_load(context, config)

    assert result.metadata["elements"] == 3
    assert result.metadata["dimensions"] == 1
    assert result.metadata["events_captured"] == 7
    assert result.metadata["events_updated"] == 2
    assert result.metadata["agents_inserted"] == 4
    assert result.metadata["agents_updated"] == 1
    assert result.metadata["dropped_unbalanced_entries"] == 1
    assert result.metadata["dropped_empty_transactions"] == 0
    # total_rows = elements + dimensions + events_captured + events_updated
    # + agents_inserted + agents_updated (= 3 + 1 + 7 + 2 + 4 + 1 = 18)
    # transactions/entries/line_items are always 0 on the capture path
    assert result.metadata["total_rows"] == 18
    assert "transactions" not in result.metadata
    assert "entries" not in result.metadata
    assert "line_items" not in result.metadata

  def test_load_updates_last_sync(self, tmp_path):
    """Test that _update_last_sync is called after loading."""
    from robosystems.adapters.quickbooks.pipeline.load import qb_load

    config = _make_config(connection_id="conn_sync_test")
    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)

    mock_loader = MagicMock()
    mock_loader.load.return_value = _make_load_result()

    with (
      patch(_PATCH_LOAD_WORK_DIR, return_value=work_dir),
      patch(_PATCH_OLTP_LOADER, return_value=mock_loader),
      patch(
        "robosystems.adapters.quickbooks.pipeline.load._update_last_sync",
      ) as mock_sync,
      patch(
        "robosystems.adapters.quickbooks.pipeline.load._advance_cdc_watermark",
      ),
    ):
      context = build_asset_context()
      qb_load(context, config)

    mock_sync.assert_called_once()

  def test_load_advances_cdc_watermark_to_the_extracts_stamp(self, tmp_path):
    """The watermark the extract took when it asked CDC, not the load's start:
    a change made while the sync ran is asked about next time."""
    from datetime import datetime

    from robosystems.adapters.quickbooks.pipeline.cdc import CdcPlan, write_plan
    from robosystems.adapters.quickbooks.pipeline.load import qb_load

    config = _make_config(connection_id="conn_cdc_test")
    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    write_plan(
      work_dir / "extract",
      CdcPlan(
        checked=True,
        watermark="2026-10-07T00:00:00+00:00",
        next_watermark="2026-10-08T11:55:00+00:00",
      ),
    )

    mock_loader = MagicMock()
    mock_loader.load.return_value = _make_load_result()

    with (
      patch(_PATCH_LOAD_WORK_DIR, return_value=work_dir),
      patch(_PATCH_OLTP_LOADER, return_value=mock_loader),
      patch(
        "robosystems.adapters.quickbooks.pipeline.load._update_last_sync",
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.load._advance_cdc_watermark",
      ) as mock_advance,
    ):
      qb_load(build_asset_context(), config)

    mock_advance.assert_called_once()
    assert mock_advance.call_args[0][2] == datetime.fromisoformat(
      "2026-10-08T11:55:00+00:00"
    )

  def test_no_plan_leaves_the_watermark_put(self, tmp_path):
    from robosystems.adapters.quickbooks.pipeline.load import qb_load

    config = _make_config()
    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)
    mock_loader = MagicMock()
    mock_loader.load.return_value = _make_load_result()

    with (
      patch(_PATCH_LOAD_WORK_DIR, return_value=work_dir),
      patch(_PATCH_OLTP_LOADER, return_value=mock_loader),
      patch("robosystems.adapters.quickbooks.pipeline.load._update_last_sync"),
      patch(
        "robosystems.adapters.quickbooks.pipeline.load._advance_cdc_watermark",
      ) as mock_advance,
    ):
      qb_load(build_asset_context(), config)

    mock_advance.assert_not_called()

  def test_a_failed_deletion_step_is_recorded_and_holds_the_watermark(self, tmp_path):
    from robosystems.adapters.quickbooks.pipeline.load import qb_load

    config = _make_config()
    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)
    mock_loader = MagicMock()
    mock_loader.load.return_value = _make_load_result()
    boom = RuntimeError("deadlock")

    with (
      patch(_PATCH_LOAD_WORK_DIR, return_value=work_dir),
      patch(_PATCH_OLTP_LOADER, return_value=mock_loader),
      patch(
        "robosystems.adapters.quickbooks.pipeline.load._apply_cdc", side_effect=boom
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.load._record_failed_sync_result"
      ) as record,
      patch("robosystems.adapters.quickbooks.pipeline.load._update_last_sync") as last,
      patch(
        "robosystems.adapters.quickbooks.pipeline.load._advance_cdc_watermark",
      ) as mock_advance,
      pytest.raises(RuntimeError),
    ):
      qb_load(build_asset_context(), config)

    assert record.call_args[0][2] is boom
    last.assert_not_called()
    mock_advance.assert_not_called()

  def test_load_reports_errors_in_metadata(self, tmp_path):
    """Test that FK resolution errors are counted in metadata."""
    from robosystems.adapters.quickbooks.pipeline.load import qb_load

    config = _make_config()
    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)

    load_result = _make_load_result(events_captured=8)
    load_result.errors = ["unknown entry: X", "unknown account: Y"]

    mock_loader = MagicMock()
    mock_loader.load.return_value = load_result

    with (
      patch(_PATCH_LOAD_WORK_DIR, return_value=work_dir),
      patch(_PATCH_OLTP_LOADER, return_value=mock_loader),
      patch(
        "robosystems.adapters.quickbooks.pipeline.load._update_last_sync",
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.load._advance_cdc_watermark",
      ),
    ):
      context = build_asset_context()
      result = qb_load(context, config)

    assert result.metadata["errors"] == 2

  def test_load_includes_graph_id_in_metadata(self, tmp_path):
    """Test that the graph_id is included in result metadata."""
    from robosystems.adapters.quickbooks.pipeline.load import qb_load

    config = _make_config(graph_id="kg_meta_check")
    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)

    mock_loader = MagicMock()
    mock_loader.load.return_value = _make_load_result(graph_id="kg_meta_check")

    with (
      patch(_PATCH_LOAD_WORK_DIR, return_value=work_dir),
      patch(_PATCH_OLTP_LOADER, return_value=mock_loader),
      patch(
        "robosystems.adapters.quickbooks.pipeline.load._update_last_sync",
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.load._advance_cdc_watermark",
      ),
    ):
      context = build_asset_context()
      result = qb_load(context, config)

    assert result.metadata["graph_id"] == "kg_meta_check"


@pytest.mark.unit
class TestUpdateLastSync:
  """Tests for the _update_last_sync helper."""

  def test_update_last_sync_calls_connection_service(self):
    """Test that Connection.update_last_sync is called via sync DB access."""
    from robosystems.adapters.quickbooks.pipeline.load import _update_last_sync

    context = Mock()
    config = _make_config(connection_id="conn_sync")

    mock_conn = Mock()
    mock_session = Mock()

    with (
      patch(
        "robosystems.database.SessionFactory",
        return_value=mock_session,
      ),
      patch(
        "robosystems.models.core.connection.connection.Connection"
      ) as MockConnection,
    ):
      MockConnection.get_by_id.return_value = mock_conn
      _update_last_sync(context, config)

    MockConnection.get_by_id.assert_called_once_with("conn_sync", mock_session)
    mock_conn.update_last_sync.assert_called_once_with(mock_session, None)
    context.log.info.assert_called()

  def test_update_last_sync_failure_is_non_fatal(self):
    """Test that a last_sync failure logs a warning but doesn't raise."""
    from robosystems.adapters.quickbooks.pipeline.load import _update_last_sync

    context = Mock()
    config = _make_config()

    with patch(
      "robosystems.database.SessionFactory",
      side_effect=Exception("DB unavailable"),
    ):
      # Should NOT raise
      _update_last_sync(context, config)

    context.log.warning.assert_called()


@pytest.mark.unit
class TestSyncResultPersistence:
  """The loader's outcome summary persists on the Connection instead of
  living only in a worker log line."""

  def test_summary_shape(self):
    from robosystems.adapters.quickbooks.pipeline.load import _sync_result_summary

    config = _make_config()
    result = _make_load_result(events_captured=46, elements=179)
    result.events_drift_detected = 11
    result.events_dispatch_failed = 2
    result.errors = ["w1"]

    summary = _sync_result_summary(config, result)

    assert summary["status"] == "succeeded"
    assert summary["window"] == {"since_date": None, "full_rebuild": False}
    assert summary["counts"]["events_captured"] == 46
    assert summary["counts"]["reconciling_items"] == 11
    assert summary["counts"]["dispatch_failed"] == 2
    assert summary["counts"]["elements"] == 179
    assert summary["errors"] == ["w1"]

  def test_failed_sync_records_outcome_without_advancing_last_sync(self):
    """last_sync feeds the close gate's sync-current check — a failed
    attempt must be legible without pretending the data is current."""
    from robosystems.adapters.quickbooks.pipeline.load import (
      _record_failed_sync_result,
    )

    context = Mock()
    config = _make_config(connection_id="conn_sync")
    mock_conn = Mock()
    mock_session = Mock()

    with (
      patch("robosystems.database.SessionFactory", return_value=mock_session),
      patch(
        "robosystems.models.core.connection.connection.Connection"
      ) as MockConnection,
    ):
      MockConnection.get_by_id.return_value = mock_conn
      _record_failed_sync_result(context, config, RuntimeError("token dead"))

    mock_conn.record_sync_result.assert_called_once()
    payload = mock_conn.record_sync_result.call_args.args[1]
    assert payload["status"] == "failed"
    assert payload["error"]["code"] == "RuntimeError"
    assert "token dead" in payload["error"]["message"]
    mock_conn.update_last_sync.assert_not_called()

  def test_loader_failure_stamps_failed_result_and_reraises(self, tmp_path):
    from robosystems.adapters.quickbooks.pipeline.load import qb_load

    config = _make_config()
    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)

    mock_loader = MagicMock()
    mock_loader.load.side_effect = RuntimeError("QB API down")

    with (
      patch(_PATCH_LOAD_WORK_DIR, return_value=work_dir),
      patch(_PATCH_OLTP_LOADER, return_value=mock_loader),
      patch(
        "robosystems.adapters.quickbooks.pipeline.load._record_failed_sync_result"
      ) as record_failed,
      patch(
        "robosystems.adapters.quickbooks.pipeline.load._update_last_sync"
      ) as update_sync,
      pytest.raises(RuntimeError),
    ):
      context = build_asset_context()
      qb_load(context, config)

    record_failed.assert_called_once()
    update_sync.assert_not_called()


@pytest.mark.unit
class TestAutoMapTrigger:
  def test_an_unattended_sync_never_enqueues_the_mapping_operator(self):
    from robosystems.adapters.quickbooks.pipeline.configs import QBSyncConfig
    from robosystems.adapters.quickbooks.pipeline.load import (
      _trigger_auto_map_if_needed,
    )

    config = QBSyncConfig(
      graph_id="kg_test", connection_id="conn_1", user_id="usr_1", unattended=True
    )
    with (
      patch("robosystems.db.extensions.extensions_session") as session,
      patch("robosystems.worker.client.enqueue_task") as enqueue,
    ):
      _trigger_auto_map_if_needed(build_asset_context(), config)
    session.assert_not_called()
    enqueue.assert_not_called()


@pytest.mark.unit
class TestApplyCdc:
  def test_no_plan_is_reported_as_unchecked(self, tmp_path):
    from robosystems.adapters.quickbooks.pipeline.load import _apply_cdc

    with patch(
      "robosystems.adapters.quickbooks.pipeline.load.get_pipeline_work_dir",
      return_value=tmp_path,
    ):
      summary = _apply_cdc(build_asset_context(), _make_config())
    assert summary == {"checked": False, "reason": "no_plan"}

  def test_deletions_are_applied_in_the_graphs_session(self, tmp_path):
    from robosystems.adapters.quickbooks.pipeline.cdc import (
      CdcApplyResult,
      CdcPlan,
      write_plan,
    )
    from robosystems.adapters.quickbooks.pipeline.load import _apply_cdc

    write_plan(
      tmp_path / "extract",
      CdcPlan(
        checked=True,
        watermark="2026-09-30T00:00:00+00:00",
        changed=3,
        deletions=[{"entity": "Invoice", "id": "42", "last_updated": None}],
        extra_windows=[["2025-01-15", "2025-01-15"]],
      ),
    )
    session = MagicMock()
    session_cm = MagicMock()
    session_cm.__enter__ = Mock(return_value=session)
    session_cm.__exit__ = Mock(return_value=False)
    with (
      patch(
        "robosystems.adapters.quickbooks.pipeline.load.get_pipeline_work_dir",
        return_value=tmp_path,
      ),
      patch("robosystems.db.extensions.extensions_session", return_value=session_cm),
      patch(
        "robosystems.adapters.quickbooks.pipeline.cdc.apply_deletions",
        return_value=CdcApplyResult(voided=1),
      ) as apply,
    ):
      summary = _apply_cdc(build_asset_context(), _make_config(graph_id="kg_x"))

    apply.assert_called_once()
    assert apply.call_args.args[0] is session
    assert summary["checked"] is True and summary["old_edit_windows"] == 1
    assert summary["deletions"] == {
      "found": 1,
      "voided": 1,
      "flagged": 0,
      "skipped": 0,
      "already_applied": 0,
      "unmatched": 0,
    }
    assert summary["observed_labels"] == {}

  def test_the_sync_summary_carries_what_cdc_did(self):
    from robosystems.adapters.quickbooks.pipeline.load import _sync_result_summary

    summary = _sync_result_summary(
      _make_config(), _make_load_result(), cdc={"checked": True, "reason": None}
    )
    assert summary["cdc"] == {"checked": True, "reason": None}


@pytest.mark.unit
class TestFiscalYearStart:
  def _company_info(self, tmp_path, month):
    import pandas as pd

    extract = tmp_path / "extract"
    extract.mkdir(parents=True)
    pd.DataFrame([{"Id": "1", "FiscalYearStartMonth": month}]).to_parquet(
      extract / "raw_company_info.parquet", index=False
    )
    return extract

  def test_the_month_name_quickbooks_stores_becomes_a_number(self, tmp_path):
    from robosystems.adapters.quickbooks.pipeline.load import fiscal_year_start_month

    assert fiscal_year_start_month(self._company_info(tmp_path, "July")) == 7
    assert fiscal_year_start_month(self._company_info(tmp_path / "b", "")) == 1
    assert fiscal_year_start_month(tmp_path / "missing") == 1

  def test_the_first_sync_initializes_the_calendar_on_the_companys_year(self, tmp_path):
    from robosystems.adapters.quickbooks.pipeline.load import (
      _bootstrap_fiscal_calendar_if_needed,
    )

    self._company_info(tmp_path, "April")
    session = MagicMock()
    session_cm = MagicMock()
    session_cm.__enter__ = Mock(return_value=session)
    session_cm.__exit__ = Mock(return_value=False)
    service = MagicMock()
    service.get.return_value = None
    service.ensure_fiscal_periods.return_value = 3
    with (
      patch("robosystems.db.extensions.extensions_session", return_value=session_cm),
      patch(
        "robosystems.operations.roboledger.fiscal_calendar.FiscalCalendarService",
        return_value=service,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.load.get_pipeline_work_dir",
        return_value=tmp_path,
      ),
    ):
      _bootstrap_fiscal_calendar_if_needed(build_asset_context(), _make_config())

    assert service.initialize.call_args.kwargs["fiscal_year_start_month"] == 4
