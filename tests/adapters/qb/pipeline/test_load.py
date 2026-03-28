"""Tests for QuickBooks load Dagster asset (OLTP version)."""

from unittest.mock import MagicMock, Mock, patch

import pytest
from dagster import build_asset_context

_PATCH_LOAD_WORK_DIR = (
  "robosystems.adapters.quickbooks.pipeline.load.get_pipeline_work_dir"
)
_PATCH_OLTP_LOADER = "robosystems.operations.extensions.loader.OLTPLoader"
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
  transactions=10,
  entries=10,
  line_items=25,
  dimensions=2,
):
  """Create a LoadResult for tests."""
  from robosystems.operations.extensions.loader import LoadResult

  return LoadResult(
    graph_id=graph_id,
    source=source,
    connection_id=connection_id,
    elements=elements,
    transactions=transactions,
    entries=entries,
    line_items=line_items,
    dimensions=dimensions,
  )


@pytest.mark.unit
class TestQbLoadAsset:
  """Tests for the qb_load Dagster asset."""

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
    )

  def test_load_returns_row_counts_in_metadata(self, tmp_path):
    """Test that metadata includes per-table row counts."""
    from robosystems.adapters.quickbooks.pipeline.load import qb_load

    config = _make_config()
    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)

    mock_loader = MagicMock()
    mock_loader.load.return_value = _make_load_result(
      elements=3, transactions=7, entries=7, line_items=15, dimensions=1
    )

    with (
      patch(_PATCH_LOAD_WORK_DIR, return_value=work_dir),
      patch(_PATCH_OLTP_LOADER, return_value=mock_loader),
      patch(
        "robosystems.adapters.quickbooks.pipeline.load._update_last_sync",
      ),
    ):
      context = build_asset_context()
      result = qb_load(context, config)

    assert result.metadata["elements"] == 3
    assert result.metadata["transactions"] == 7
    assert result.metadata["entries"] == 7
    assert result.metadata["line_items"] == 15
    assert result.metadata["dimensions"] == 1
    assert result.metadata["total_rows"] == 33

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
    ):
      context = build_asset_context()
      qb_load(context, config)

    mock_sync.assert_called_once()

  def test_load_reports_errors_in_metadata(self, tmp_path):
    """Test that FK resolution errors are counted in metadata."""
    from robosystems.adapters.quickbooks.pipeline.load import qb_load

    config = _make_config()
    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)

    load_result = _make_load_result(line_items=8)
    load_result.errors = ["unknown entry: X", "unknown account: Y"]

    mock_loader = MagicMock()
    mock_loader.load.return_value = load_result

    with (
      patch(_PATCH_LOAD_WORK_DIR, return_value=work_dir),
      patch(_PATCH_OLTP_LOADER, return_value=mock_loader),
      patch(
        "robosystems.adapters.quickbooks.pipeline.load._update_last_sync",
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
      patch("robosystems.models.iam.connection.Connection") as MockConnection,
    ):
      MockConnection.get_by_id.return_value = mock_conn
      _update_last_sync(context, config)

    MockConnection.get_by_id.assert_called_once_with("conn_sync", mock_session)
    mock_conn.update_last_sync.assert_called_once_with(mock_session)
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
