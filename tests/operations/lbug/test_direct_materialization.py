"""
Tests for direct graph materialization without Dagster orchestration.

These tests cover the fast path for materializing DuckDB staging tables
to the graph database, bypassing Dagster job overhead.
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from robosystems.operations.lbug.direct_materialization import (
  _restage_stale_files,
  materialize_graph_directly,
)


class TestMaterializeGraphDirectly:
  """Test materialize_graph_directly function."""

  @pytest.fixture
  def mock_db_session(self):
    """Create a mock database session."""
    return Mock()

  @pytest.mark.asyncio
  async def test_successful_materialization(self, mock_db_session):
    """Test successful graph materialization."""
    graph_id = "kg1234567890abcdef"

    with (
      patch("robosystems.graph_api.client.factory.GraphClientFactory") as mock_factory,
      patch(
        "robosystems.middleware.sse.operation_manager.get_operation_manager"
      ) as mock_get_manager,
      patch("robosystems.models.core.Graph") as mock_graph_class,
      patch("robosystems.models.core.GraphFile") as mock_file_class,
      patch("robosystems.models.core.GraphTable"),
      patch("robosystems.dagster.reporting.report_asset_materialization"),
    ):
      # Mock graph record
      mock_graph = Mock()
      mock_graph.graph_stale = True
      mock_graph.graph_stale_reason = "new_data"
      mock_graph.graph_metadata = {}
      mock_graph_class.get_by_id.return_value = mock_graph

      # Mock no stale files to recover
      mock_file_class.recover_stale_files.return_value = []

      # Mock tables with staged data
      mock_table = Mock()
      mock_table.table_name = "Entity"
      mock_table.table_type = "node"
      mock_db_session.query.return_value.join.return_value.filter.return_value.filter.return_value.distinct.return_value.all.return_value = [
        mock_table
      ]
      mock_db_session.query.return_value.join.return_value.filter.return_value.filter.return_value.distinct.return_value.count.return_value = 0

      # Mock graph client
      mock_client = AsyncMock()
      mock_client.materialize_table.return_value = {"rows_ingested": 100}
      mock_factory.create_client = AsyncMock(return_value=mock_client)

      # Mock operation manager
      mock_manager = Mock()
      mock_manager.emit_progress = AsyncMock()
      mock_manager.complete_operation = AsyncMock()
      mock_get_manager.return_value = mock_manager

      result = await materialize_graph_directly(
        db=mock_db_session,
        graph_id=graph_id,
        force=False,
        rebuild=False,
        ignore_errors=True,
        operation_id="op123",
      )

      assert result["status"] == "success"
      assert result["graph_id"] == graph_id
      assert result["total_rows"] == 100
      assert "Entity" in result["tables_materialized"]
      mock_graph.mark_fresh.assert_called_once()
      mock_client.close.assert_called_once()

  @pytest.mark.asyncio
  async def test_graph_not_found(self, mock_db_session):
    """Test materialization when graph is not found."""
    graph_id = "kg_nonexistent"

    with (
      patch(
        "robosystems.middleware.sse.operation_manager.get_operation_manager"
      ) as mock_get_manager,
      patch("robosystems.models.core.Graph") as mock_graph_class,
    ):
      mock_graph_class.get_by_id.return_value = None

      mock_manager = Mock()
      mock_manager.emit_progress = AsyncMock()
      mock_manager.fail_operation = AsyncMock()
      mock_get_manager.return_value = mock_manager

      result = await materialize_graph_directly(
        db=mock_db_session,
        graph_id=graph_id,
        operation_id="op123",
      )

      assert result["status"] == "error"
      assert "not found" in result["message"]
      mock_manager.fail_operation.assert_called_once()

  @pytest.mark.asyncio
  async def test_skip_fresh_graph(self, mock_db_session):
    """Test skipping materialization when graph is fresh."""
    graph_id = "kg1234567890abcdef"

    with (
      patch("robosystems.graph_api.client.factory.GraphClientFactory") as mock_factory,
      patch(
        "robosystems.middleware.sse.operation_manager.get_operation_manager"
      ) as mock_get_manager,
      patch("robosystems.models.core.Graph") as mock_graph_class,
      patch("robosystems.models.core.GraphFile") as mock_file_class,
      patch("robosystems.models.core.GraphTable"),
    ):
      mock_graph = Mock()
      mock_graph.graph_stale = False
      mock_graph.graph_stale_reason = None
      mock_graph_class.get_by_id.return_value = mock_graph

      mock_file_class.recover_stale_files.return_value = []

      # No staged data
      mock_db_session.query.return_value.join.return_value.filter.return_value.filter.return_value.distinct.return_value.count.return_value = 0

      mock_manager = Mock()
      mock_manager.emit_progress = AsyncMock()
      mock_manager.complete_operation = AsyncMock()
      mock_get_manager.return_value = mock_manager

      result = await materialize_graph_directly(
        db=mock_db_session,
        graph_id=graph_id,
        force=False,
        rebuild=False,
        operation_id="op123",
      )

      assert result["status"] == "skipped"
      assert "fresh" in result["message"]
      mock_factory.create_client.assert_not_called()

  @pytest.mark.asyncio
  async def test_force_materialization_on_fresh_graph(self, mock_db_session):
    """Test forcing materialization even when graph is fresh."""
    graph_id = "kg1234567890abcdef"

    with (
      patch("robosystems.graph_api.client.factory.GraphClientFactory") as mock_factory,
      patch(
        "robosystems.middleware.sse.operation_manager.get_operation_manager"
      ) as mock_get_manager,
      patch("robosystems.models.core.Graph") as mock_graph_class,
      patch("robosystems.models.core.GraphFile") as mock_file_class,
      patch("robosystems.models.core.GraphTable"),
      patch("robosystems.dagster.reporting.report_asset_materialization"),
    ):
      mock_graph = Mock()
      mock_graph.graph_stale = False
      mock_graph.graph_stale_reason = None
      mock_graph.graph_metadata = {}
      mock_graph_class.get_by_id.return_value = mock_graph

      mock_file_class.recover_stale_files.return_value = []

      # Mock tables with staged data
      mock_table = Mock()
      mock_table.table_name = "Entity"
      mock_table.table_type = "node"
      mock_db_session.query.return_value.join.return_value.filter.return_value.filter.return_value.distinct.return_value.all.return_value = [
        mock_table
      ]
      mock_db_session.query.return_value.join.return_value.filter.return_value.filter.return_value.distinct.return_value.count.return_value = 0

      mock_client = AsyncMock()
      mock_client.materialize_table.return_value = {"rows_ingested": 50}
      mock_factory.create_client = AsyncMock(return_value=mock_client)

      mock_manager = Mock()
      mock_manager.emit_progress = AsyncMock()
      mock_manager.complete_operation = AsyncMock()
      mock_get_manager.return_value = mock_manager

      result = await materialize_graph_directly(
        db=mock_db_session,
        graph_id=graph_id,
        force=True,
        rebuild=False,
      )

      assert result["status"] == "success"
      mock_client.materialize_table.assert_called_once()

  @pytest.mark.asyncio
  async def test_rebuild_deletes_and_recreates_database(self, mock_db_session):
    """Test rebuild deletes and recreates the graph database."""
    graph_id = "kg1234567890abcdef"

    with (
      patch("robosystems.graph_api.client.factory.GraphClientFactory") as mock_factory,
      patch(
        "robosystems.middleware.sse.operation_manager.get_operation_manager"
      ) as mock_get_manager,
      patch("robosystems.models.core.Graph") as mock_graph_class,
      patch("robosystems.models.core.GraphFile") as mock_file_class,
      patch("robosystems.models.core.GraphTable"),
      patch("robosystems.models.core.GraphSchema") as mock_schema_class,
      patch("robosystems.dagster.reporting.report_asset_materialization"),
    ):
      mock_graph = Mock()
      mock_graph.graph_stale = True
      mock_graph.graph_stale_reason = "rebuild_requested"
      mock_graph.graph_metadata = {}
      mock_graph_class.get_by_id.return_value = mock_graph

      mock_file_class.recover_stale_files.return_value = []

      # Mock schema
      mock_schema = Mock()
      mock_schema.schema_ddl = "CREATE NODE TABLE Entity ..."
      mock_schema_class.get_active_schema.return_value = mock_schema

      # Mock a table with staged data (so rebuild actually completes with materialization)
      mock_table = Mock()
      mock_table.table_name = "Entity"
      mock_table.table_type = "node"
      mock_db_session.query.return_value.join.return_value.filter.return_value.filter.return_value.distinct.return_value.all.return_value = [
        mock_table
      ]

      mock_client = AsyncMock()
      mock_client.materialize_table.return_value = {"rows_ingested": 100}
      mock_factory.create_client = AsyncMock(return_value=mock_client)

      mock_manager = Mock()
      mock_manager.emit_progress = AsyncMock()
      mock_manager.complete_operation = AsyncMock()
      mock_get_manager.return_value = mock_manager

      result = await materialize_graph_directly(
        db=mock_db_session,
        graph_id=graph_id,
        rebuild=True,
      )

      assert result["status"] == "success"
      assert result["rebuild"] is True
      mock_client.delete_database.assert_called_once_with(graph_id)
      mock_client.create_database.assert_called_once()
      mock_client.materialize_table.assert_called_once()

  @pytest.mark.asyncio
  async def test_table_ordering_nodes_before_relationships(self, mock_db_session):
    """Test that node tables are materialized before relationship tables."""
    graph_id = "kg1234567890abcdef"

    with (
      patch("robosystems.graph_api.client.factory.GraphClientFactory") as mock_factory,
      patch(
        "robosystems.middleware.sse.operation_manager.get_operation_manager"
      ) as mock_get_manager,
      patch("robosystems.models.core.Graph") as mock_graph_class,
      patch("robosystems.models.core.GraphFile") as mock_file_class,
      patch("robosystems.models.core.GraphTable"),
      patch("robosystems.dagster.reporting.report_asset_materialization"),
    ):
      mock_graph = Mock()
      mock_graph.graph_stale = True
      mock_graph.graph_stale_reason = "new_data"
      mock_graph.graph_metadata = {}
      mock_graph_class.get_by_id.return_value = mock_graph

      mock_file_class.recover_stale_files.return_value = []

      # Mock both node and relationship tables
      node_table = Mock()
      node_table.table_name = "Entity"
      node_table.table_type = "node"

      rel_table = Mock()
      rel_table.table_name = "EntityRelationship"
      rel_table.table_type = "relationship"

      mock_db_session.query.return_value.join.return_value.filter.return_value.filter.return_value.distinct.return_value.all.return_value = [
        rel_table,
        node_table,
      ]
      mock_db_session.query.return_value.join.return_value.filter.return_value.filter.return_value.distinct.return_value.count.return_value = 0

      mock_client = AsyncMock()
      mock_client.materialize_table.return_value = {"rows_ingested": 10}
      mock_factory.create_client = AsyncMock(return_value=mock_client)

      mock_manager = Mock()
      mock_manager.emit_progress = AsyncMock()
      mock_manager.complete_operation = AsyncMock()
      mock_get_manager.return_value = mock_manager

      result = await materialize_graph_directly(
        db=mock_db_session,
        graph_id=graph_id,
      )

      assert result["status"] == "success"

      # Verify order: nodes first, then relationships
      calls = mock_client.materialize_table.call_args_list
      assert len(calls) == 2
      assert calls[0].kwargs["table_name"] == "Entity"
      assert calls[1].kwargs["table_name"] == "EntityRelationship"

  @pytest.mark.asyncio
  async def test_error_handling_continues_on_ignore_errors(self, mock_db_session):
    """Test that errors are ignored when ignore_errors=True."""
    graph_id = "kg1234567890abcdef"

    with (
      patch("robosystems.graph_api.client.factory.GraphClientFactory") as mock_factory,
      patch(
        "robosystems.middleware.sse.operation_manager.get_operation_manager"
      ) as mock_get_manager,
      patch("robosystems.models.core.Graph") as mock_graph_class,
      patch("robosystems.models.core.GraphFile") as mock_file_class,
      patch("robosystems.models.core.GraphTable"),
      patch("robosystems.dagster.reporting.report_asset_materialization"),
    ):
      mock_graph = Mock()
      mock_graph.graph_stale = True
      mock_graph.graph_stale_reason = "new_data"
      mock_graph.graph_metadata = {}
      mock_graph_class.get_by_id.return_value = mock_graph

      mock_file_class.recover_stale_files.return_value = []

      # Two tables
      table1 = Mock()
      table1.table_name = "Entity1"
      table1.table_type = "node"

      table2 = Mock()
      table2.table_name = "Entity2"
      table2.table_type = "node"

      mock_db_session.query.return_value.join.return_value.filter.return_value.filter.return_value.distinct.return_value.all.return_value = [
        table1,
        table2,
      ]
      mock_db_session.query.return_value.join.return_value.filter.return_value.filter.return_value.distinct.return_value.count.return_value = 0

      # First table fails, second succeeds
      mock_client = AsyncMock()
      mock_client.materialize_table.side_effect = [
        Exception("Failed"),
        {"rows_ingested": 50},
      ]
      mock_factory.create_client = AsyncMock(return_value=mock_client)

      mock_manager = Mock()
      mock_manager.emit_progress = AsyncMock()
      mock_manager.complete_operation = AsyncMock()
      mock_get_manager.return_value = mock_manager

      result = await materialize_graph_directly(
        db=mock_db_session,
        graph_id=graph_id,
        ignore_errors=True,
      )

      assert result["status"] == "success"
      assert result["total_rows"] == 50
      assert "Entity2" in result["tables_materialized"]

  @pytest.mark.asyncio
  async def test_error_raised_when_ignore_errors_false(self, mock_db_session):
    """Test that errors are raised when ignore_errors=False."""
    graph_id = "kg1234567890abcdef"

    with (
      patch("robosystems.graph_api.client.factory.GraphClientFactory") as mock_factory,
      patch(
        "robosystems.middleware.sse.operation_manager.get_operation_manager"
      ) as mock_get_manager,
      patch("robosystems.models.core.Graph") as mock_graph_class,
      patch("robosystems.models.core.GraphFile") as mock_file_class,
      patch("robosystems.models.core.GraphTable"),
    ):
      mock_graph = Mock()
      mock_graph.graph_stale = True
      mock_graph.graph_stale_reason = "new_data"
      mock_graph.graph_metadata = {}
      mock_graph_class.get_by_id.return_value = mock_graph

      mock_file_class.recover_stale_files.return_value = []

      table1 = Mock()
      table1.table_name = "Entity1"
      table1.table_type = "node"

      mock_db_session.query.return_value.join.return_value.filter.return_value.filter.return_value.distinct.return_value.all.return_value = [
        table1
      ]
      mock_db_session.query.return_value.join.return_value.filter.return_value.filter.return_value.distinct.return_value.count.return_value = 0

      mock_client = AsyncMock()
      mock_client.materialize_table.side_effect = Exception("Materialization failed")
      mock_factory.create_client = AsyncMock(return_value=mock_client)

      mock_manager = Mock()
      mock_manager.emit_progress = AsyncMock()
      mock_manager.fail_operation = AsyncMock()
      mock_get_manager.return_value = mock_manager

      result = await materialize_graph_directly(
        db=mock_db_session,
        graph_id=graph_id,
        ignore_errors=False,
        operation_id="op123",
      )

      assert result["status"] == "error"
      assert "Materialization failed" in result["message"]


class TestRestageStaleFiles:
  """Test _restage_stale_files function."""

  @pytest.fixture
  def mock_db_session(self):
    """Create a mock database session."""
    return Mock()

  @pytest.mark.asyncio
  async def test_restage_stale_files_success(self, mock_db_session):
    """Test successful re-staging of stale files."""
    graph_id = "kg1234567890abcdef"
    file_ids = ["file1", "file2"]

    with (
      patch(
        "robosystems.operations.lbug.direct_staging.stage_file_directly"
      ) as mock_stage,
      patch("robosystems.models.core.GraphFile") as mock_file_class,
    ):
      mock_file1 = Mock()
      mock_file1.file_name = "data1.csv"
      mock_file1.table_id = "table1"
      mock_file1.s3_key = "path/to/data1.csv"
      mock_file1.file_size_bytes = 1000
      mock_file1.row_count = 100

      mock_file2 = Mock()
      mock_file2.file_name = "data2.csv"
      mock_file2.table_id = "table2"
      mock_file2.s3_key = "path/to/data2.csv"
      mock_file2.file_size_bytes = 2000
      mock_file2.row_count = 200

      mock_file_class.get_by_id.side_effect = [mock_file1, mock_file2]
      mock_stage.return_value = {"status": "success"}

      await _restage_stale_files(mock_db_session, graph_id, file_ids)

      assert mock_stage.call_count == 2

  @pytest.mark.asyncio
  async def test_restage_handles_missing_file(self, mock_db_session):
    """Test that missing files are skipped during re-staging."""
    graph_id = "kg1234567890abcdef"
    file_ids = ["file1", "missing_file"]

    with (
      patch(
        "robosystems.operations.lbug.direct_staging.stage_file_directly"
      ) as mock_stage,
      patch("robosystems.models.core.GraphFile") as mock_file_class,
    ):
      mock_file = Mock()
      mock_file.file_name = "data.csv"
      mock_file.table_id = "table1"
      mock_file.s3_key = "path/to/data.csv"
      mock_file.file_size_bytes = 1000
      mock_file.row_count = 100

      mock_file_class.get_by_id.side_effect = [mock_file, None]
      mock_stage.return_value = {"status": "success"}

      await _restage_stale_files(mock_db_session, graph_id, file_ids)

      # Only one file should be staged
      assert mock_stage.call_count == 1

  @pytest.mark.asyncio
  async def test_restage_handles_staging_error(self, mock_db_session):
    """Test that staging errors are logged but don't stop processing."""
    graph_id = "kg1234567890abcdef"
    file_ids = ["file1", "file2"]

    with (
      patch(
        "robosystems.operations.lbug.direct_staging.stage_file_directly"
      ) as mock_stage,
      patch("robosystems.models.core.GraphFile") as mock_file_class,
    ):
      mock_file = Mock()
      mock_file.file_name = "data.csv"
      mock_file.table_id = "table1"
      mock_file.s3_key = "path/to/data.csv"
      mock_file.file_size_bytes = 1000
      mock_file.row_count = 100

      mock_file_class.get_by_id.return_value = mock_file
      mock_stage.side_effect = [Exception("Staging failed"), {"status": "success"}]

      # Should not raise, should continue to next file
      await _restage_stale_files(mock_db_session, graph_id, file_ids)

      assert mock_stage.call_count == 2


class TestProgressTracking:
  """Test SSE progress tracking during materialization."""

  @pytest.fixture
  def mock_db_session(self):
    """Create a mock database session."""
    return Mock()

  @pytest.mark.asyncio
  async def test_progress_events_emitted(self, mock_db_session):
    """Test that progress events are emitted during materialization."""
    graph_id = "kg1234567890abcdef"
    operation_id = "op123"

    with (
      patch("robosystems.graph_api.client.factory.GraphClientFactory") as mock_factory,
      patch(
        "robosystems.middleware.sse.operation_manager.get_operation_manager"
      ) as mock_get_manager,
      patch("robosystems.models.core.Graph") as mock_graph_class,
      patch("robosystems.models.core.GraphFile") as mock_file_class,
      patch("robosystems.models.core.GraphTable"),
      patch("robosystems.dagster.reporting.report_asset_materialization"),
    ):
      mock_graph = Mock()
      mock_graph.graph_stale = True
      mock_graph.graph_stale_reason = "new_data"
      mock_graph.graph_metadata = {}
      mock_graph_class.get_by_id.return_value = mock_graph

      mock_file_class.recover_stale_files.return_value = []

      mock_table = Mock()
      mock_table.table_name = "Entity"
      mock_table.table_type = "node"
      mock_db_session.query.return_value.join.return_value.filter.return_value.filter.return_value.distinct.return_value.all.return_value = [
        mock_table
      ]
      mock_db_session.query.return_value.join.return_value.filter.return_value.filter.return_value.distinct.return_value.count.return_value = 0

      mock_client = AsyncMock()
      mock_client.materialize_table.return_value = {"rows_ingested": 100}
      mock_factory.create_client = AsyncMock(return_value=mock_client)

      mock_manager = Mock()
      mock_manager.emit_progress = AsyncMock()
      mock_manager.complete_operation = AsyncMock()
      mock_get_manager.return_value = mock_manager

      await materialize_graph_directly(
        db=mock_db_session,
        graph_id=graph_id,
        operation_id=operation_id,
      )

      # Verify progress events were emitted
      assert mock_manager.emit_progress.call_count >= 3
      mock_manager.complete_operation.assert_called_once()

  @pytest.mark.asyncio
  async def test_failure_operation_on_error(self, mock_db_session):
    """Test that fail_operation is called on error."""
    graph_id = "kg_nonexistent"
    operation_id = "op123"

    with (
      patch(
        "robosystems.middleware.sse.operation_manager.get_operation_manager"
      ) as mock_get_manager,
      patch("robosystems.models.core.Graph") as mock_graph_class,
    ):
      mock_graph_class.get_by_id.return_value = None

      mock_manager = Mock()
      mock_manager.emit_progress = AsyncMock()
      mock_manager.fail_operation = AsyncMock()
      mock_get_manager.return_value = mock_manager

      result = await materialize_graph_directly(
        db=mock_db_session,
        graph_id=graph_id,
        operation_id=operation_id,
      )

      assert result["status"] == "error"
      mock_manager.fail_operation.assert_called_once_with(
        operation_id, error=result["message"]
      )
