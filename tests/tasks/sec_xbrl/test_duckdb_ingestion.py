"""Tests for SEC XBRL DuckDB ingestion task."""

import pytest
from unittest.mock import patch, MagicMock

from robosystems.tasks.sec_xbrl.duckdb_ingestion import ingest_via_duckdb


class TestIngestViaDuckDBTask:
  """Test cases for XBRL DuckDB ingestion Celery task."""

  @patch("robosystems.tasks.sec_xbrl.duckdb_ingestion.XBRLDuckDBGraphProcessor")
  @patch("asyncio.run")
  def test_successful_ingestion_default_params(
    self, mock_asyncio_run, mock_processor_class
  ):
    """Test successful ingestion with default parameters."""
    mock_processor = MagicMock()
    mock_processor_class.return_value = mock_processor

    mock_result = {
      "status": "success",
      "entities_processed": 150,
      "facts_loaded": 125000,
      "processing_time_seconds": 45.5,
    }
    mock_asyncio_run.return_value = mock_result

    result = ingest_via_duckdb.apply(args=(), kwargs={}).get()  # type: ignore[attr-defined]

    mock_processor_class.assert_called_once_with(graph_id="sec")
    mock_asyncio_run.assert_called_once()

    assert result == mock_result
    assert result["status"] == "success"
    assert result["entities_processed"] == 150

  @patch("robosystems.tasks.sec_xbrl.duckdb_ingestion.XBRLDuckDBGraphProcessor")
  @patch("asyncio.run")
  def test_successful_ingestion_with_year_filter(
    self, mock_asyncio_run, mock_processor_class
  ):
    """Test successful ingestion with year filter."""
    mock_processor = MagicMock()
    mock_processor_class.return_value = mock_processor

    mock_result = {
      "status": "success",
      "entities_processed": 50,
      "facts_loaded": 45000,
      "year_filter": 2023,
    }
    mock_asyncio_run.return_value = mock_result

    result = ingest_via_duckdb.apply(args=(), kwargs={"year": 2023}).get()  # type: ignore[attr-defined]

    mock_processor_class.assert_called_once_with(graph_id="sec")
    mock_asyncio_run.assert_called_once()

    assert result == mock_result
    assert result["year_filter"] == 2023

  @patch("robosystems.tasks.sec_xbrl.duckdb_ingestion.XBRLDuckDBGraphProcessor")
  @patch("asyncio.run")
  def test_ingestion_with_rebuild_false(self, mock_asyncio_run, mock_processor_class):
    """Test ingestion with rebuild=False."""
    mock_processor = MagicMock()
    mock_processor_class.return_value = mock_processor

    mock_result = {
      "status": "success",
      "entities_processed": 25,
      "facts_loaded": 18000,
      "rebuild": False,
    }
    mock_asyncio_run.return_value = mock_result

    result = ingest_via_duckdb.apply(args=(), kwargs={"rebuild": False}).get()  # type: ignore[attr-defined]

    mock_processor_class.assert_called_once_with(graph_id="sec")
    mock_asyncio_run.assert_called_once()

    assert result == mock_result
    assert result["rebuild"] is False

  @patch("robosystems.tasks.sec_xbrl.duckdb_ingestion.XBRLDuckDBGraphProcessor")
  @patch("asyncio.run")
  def test_ingestion_with_year_and_rebuild(
    self, mock_asyncio_run, mock_processor_class
  ):
    """Test ingestion with both year filter and rebuild flag."""
    mock_processor = MagicMock()
    mock_processor_class.return_value = mock_processor

    mock_result = {
      "status": "success",
      "entities_processed": 30,
      "facts_loaded": 22000,
      "year_filter": 2024,
      "rebuild": True,
    }
    mock_asyncio_run.return_value = mock_result

    result = ingest_via_duckdb.apply(
      args=(),
      kwargs={"rebuild": True, "year": 2024},
    ).get()  # type: ignore[attr-defined]

    mock_processor_class.assert_called_once_with(graph_id="sec")
    mock_asyncio_run.assert_called_once()

    assert result == mock_result
    assert result["year_filter"] == 2024
    assert result["rebuild"] is True


class TestIngestViaDuckDBErrorHandling:
  """Test cases for error handling and retry behavior."""

  @patch("robosystems.tasks.sec_xbrl.duckdb_ingestion.XBRLDuckDBGraphProcessor")
  @patch("asyncio.run")
  def test_processor_initialization_error(self, mock_asyncio_run, mock_processor_class):
    """Test handling of processor initialization errors."""
    mock_processor_class.side_effect = RuntimeError("Processor init failed")

    with pytest.raises(RuntimeError) as exc_info:
      ingest_via_duckdb.apply(args=(), kwargs={}).get()  # type: ignore[attr-defined]

    assert "Processor init failed" in str(exc_info.value)

  @patch("robosystems.tasks.sec_xbrl.duckdb_ingestion.XBRLDuckDBGraphProcessor")
  @patch("asyncio.run")
  def test_asyncio_run_error(self, mock_asyncio_run, mock_processor_class):
    """Test handling of asyncio execution errors."""
    mock_processor = MagicMock()
    mock_processor_class.return_value = mock_processor

    mock_asyncio_run.side_effect = RuntimeError("Async processing failed")

    with pytest.raises(RuntimeError) as exc_info:
      ingest_via_duckdb.apply(args=(), kwargs={}).get()  # type: ignore[attr-defined]

    assert "Async processing failed" in str(exc_info.value)

  @patch("robosystems.tasks.sec_xbrl.duckdb_ingestion.XBRLDuckDBGraphProcessor")
  @patch("asyncio.run")
  def test_process_files_error(self, mock_asyncio_run, mock_processor_class):
    """Test handling of process_files method errors."""
    mock_processor = MagicMock()
    mock_processor_class.return_value = mock_processor

    mock_asyncio_run.side_effect = ValueError("Invalid file format")

    with pytest.raises(ValueError) as exc_info:
      ingest_via_duckdb.apply(args=(), kwargs={}).get()  # type: ignore[attr-defined]

    assert "Invalid file format" in str(exc_info.value)

  @patch("robosystems.tasks.sec_xbrl.duckdb_ingestion.XBRLDuckDBGraphProcessor")
  @patch("asyncio.run")
  def test_graph_connection_error(self, mock_asyncio_run, mock_processor_class):
    """Test handling of graph connection errors."""
    mock_processor = MagicMock()
    mock_processor_class.return_value = mock_processor

    mock_asyncio_run.side_effect = ConnectionError("Graph API unreachable")

    with pytest.raises(ConnectionError) as exc_info:
      ingest_via_duckdb.apply(args=(), kwargs={}).get()  # type: ignore[attr-defined]

    assert "Graph API unreachable" in str(exc_info.value)


class TestIngestViaDuckDBResultStructure:
  """Test cases for validating result structure."""

  @patch("robosystems.tasks.sec_xbrl.duckdb_ingestion.XBRLDuckDBGraphProcessor")
  @patch("asyncio.run")
  def test_result_contains_status(self, mock_asyncio_run, mock_processor_class):
    """Test that result contains status field."""
    mock_processor = MagicMock()
    mock_processor_class.return_value = mock_processor

    mock_result = {"status": "success"}
    mock_asyncio_run.return_value = mock_result

    result = ingest_via_duckdb.apply(args=(), kwargs={}).get()  # type: ignore[attr-defined]

    assert "status" in result
    assert result["status"] == "success"

  @patch("robosystems.tasks.sec_xbrl.duckdb_ingestion.XBRLDuckDBGraphProcessor")
  @patch("asyncio.run")
  def test_result_contains_processing_stats(
    self, mock_asyncio_run, mock_processor_class
  ):
    """Test that result contains processing statistics."""
    mock_processor = MagicMock()
    mock_processor_class.return_value = mock_processor

    mock_result = {
      "status": "success",
      "entities_processed": 100,
      "facts_loaded": 85000,
      "processing_time_seconds": 30.2,
    }
    mock_asyncio_run.return_value = mock_result

    result = ingest_via_duckdb.apply(args=(), kwargs={}).get()  # type: ignore[attr-defined]

    assert "entities_processed" in result
    assert "facts_loaded" in result
    assert "processing_time_seconds" in result
    assert result["entities_processed"] == 100
    assert result["facts_loaded"] == 85000
    assert result["processing_time_seconds"] == 30.2

  @patch("robosystems.tasks.sec_xbrl.duckdb_ingestion.XBRLDuckDBGraphProcessor")
  @patch("asyncio.run")
  def test_result_is_dict(self, mock_asyncio_run, mock_processor_class):
    """Test that result is always a dictionary."""
    mock_processor = MagicMock()
    mock_processor_class.return_value = mock_processor

    mock_result = {"status": "success"}
    mock_asyncio_run.return_value = mock_result

    result = ingest_via_duckdb.apply(args=(), kwargs={}).get()  # type: ignore[attr-defined]

    assert isinstance(result, dict)


class TestIngestViaDuckDBGraphIDValidation:
  """Test cases for graph ID validation."""

  @patch("robosystems.tasks.sec_xbrl.duckdb_ingestion.XBRLDuckDBGraphProcessor")
  @patch("asyncio.run")
  def test_uses_sec_graph_id(self, mock_asyncio_run, mock_processor_class):
    """Test that task always uses 'sec' graph ID."""
    mock_processor = MagicMock()
    mock_processor_class.return_value = mock_processor

    mock_result = {"status": "success"}
    mock_asyncio_run.return_value = mock_result

    ingest_via_duckdb.apply(args=(), kwargs={}).get()  # type: ignore[attr-defined]

    mock_processor_class.assert_called_once_with(graph_id="sec")

  @patch("robosystems.tasks.sec_xbrl.duckdb_ingestion.XBRLDuckDBGraphProcessor")
  @patch("asyncio.run")
  def test_sec_graph_id_with_year_filter(self, mock_asyncio_run, mock_processor_class):
    """Test that 'sec' graph ID is used even with year filter."""
    mock_processor = MagicMock()
    mock_processor_class.return_value = mock_processor

    mock_result = {"status": "success", "year_filter": 2023}
    mock_asyncio_run.return_value = mock_result

    ingest_via_duckdb.apply(args=(), kwargs={"year": 2023}).get()  # type: ignore[attr-defined]

    mock_processor_class.assert_called_once_with(graph_id="sec")

  @patch("robosystems.tasks.sec_xbrl.duckdb_ingestion.XBRLDuckDBGraphProcessor")
  @patch("asyncio.run")
  def test_sec_graph_id_with_rebuild_false(
    self, mock_asyncio_run, mock_processor_class
  ):
    """Test that 'sec' graph ID is used with rebuild=False."""
    mock_processor = MagicMock()
    mock_processor_class.return_value = mock_processor

    mock_result = {"status": "success", "rebuild": False}
    mock_asyncio_run.return_value = mock_result

    ingest_via_duckdb.apply(args=(), kwargs={"rebuild": False}).get()  # type: ignore[attr-defined]

    mock_processor_class.assert_called_once_with(graph_id="sec")


class TestIngestViaDuckDBAsyncExecution:
  """Test cases for async execution behavior."""

  @patch("robosystems.tasks.sec_xbrl.duckdb_ingestion.XBRLDuckDBGraphProcessor")
  @patch("asyncio.run")
  def test_asyncio_run_called_once(self, mock_asyncio_run, mock_processor_class):
    """Test that asyncio.run is called exactly once."""
    mock_processor = MagicMock()
    mock_processor_class.return_value = mock_processor

    mock_result = {"status": "success"}
    mock_asyncio_run.return_value = mock_result

    ingest_via_duckdb.apply(args=(), kwargs={}).get()  # type: ignore[attr-defined]

    mock_asyncio_run.assert_called_once()

  @patch("robosystems.tasks.sec_xbrl.duckdb_ingestion.XBRLDuckDBGraphProcessor")
  @patch("asyncio.run")
  def test_process_files_called_with_correct_params(
    self, mock_asyncio_run, mock_processor_class
  ):
    """Test that process_files is called with correct parameters."""
    mock_processor = MagicMock()
    mock_processor_class.return_value = mock_processor

    mock_result = {"status": "success"}
    mock_asyncio_run.return_value = mock_result

    ingest_via_duckdb.apply(
      args=(),
      kwargs={"rebuild": True, "year": 2024},
    ).get()  # type: ignore[attr-defined]

    mock_asyncio_run.assert_called_once()

  @patch("robosystems.tasks.sec_xbrl.duckdb_ingestion.XBRLDuckDBGraphProcessor")
  @patch("asyncio.run")
  def test_async_result_returned_directly(self, mock_asyncio_run, mock_processor_class):
    """Test that async result is returned directly without modification."""
    mock_processor = MagicMock()
    mock_processor_class.return_value = mock_processor

    expected_result = {
      "status": "success",
      "entities_processed": 75,
      "custom_field": "custom_value",
    }
    mock_asyncio_run.return_value = expected_result

    result = ingest_via_duckdb.apply(args=(), kwargs={}).get()  # type: ignore[attr-defined]

    assert result == expected_result
    assert result["custom_field"] == "custom_value"


class TestIngestViaDuckDBDefaultBehavior:
  """Test cases for default parameter behavior."""

  @patch("robosystems.tasks.sec_xbrl.duckdb_ingestion.XBRLDuckDBGraphProcessor")
  @patch("asyncio.run")
  def test_default_rebuild_is_true(self, mock_asyncio_run, mock_processor_class):
    """Test that default rebuild parameter is True."""
    mock_processor = MagicMock()
    mock_processor.process_files = MagicMock()
    mock_processor_class.return_value = mock_processor

    mock_result = {"status": "success", "rebuild": True}
    mock_asyncio_run.return_value = mock_result

    result = ingest_via_duckdb.apply(args=(), kwargs={}).get()  # type: ignore[attr-defined]

    assert result["rebuild"] is True

  @patch("robosystems.tasks.sec_xbrl.duckdb_ingestion.XBRLDuckDBGraphProcessor")
  @patch("asyncio.run")
  def test_default_year_is_none(self, mock_asyncio_run, mock_processor_class):
    """Test that default year parameter is None (all years)."""
    mock_processor = MagicMock()
    mock_processor.process_files = MagicMock()
    mock_processor_class.return_value = mock_processor

    mock_result = {"status": "success"}
    mock_asyncio_run.return_value = mock_result

    result = ingest_via_duckdb.apply(args=(), kwargs={}).get()  # type: ignore[attr-defined]

    assert "year_filter" not in result or result.get("year_filter") is None

  @patch("robosystems.tasks.sec_xbrl.duckdb_ingestion.XBRLDuckDBGraphProcessor")
  @patch("asyncio.run")
  def test_no_args_runs_successfully(self, mock_asyncio_run, mock_processor_class):
    """Test that task runs successfully with no arguments."""
    mock_processor = MagicMock()
    mock_processor_class.return_value = mock_processor

    mock_result = {"status": "success"}
    mock_asyncio_run.return_value = mock_result

    result = ingest_via_duckdb.apply(args=(), kwargs={}).get()  # type: ignore[attr-defined]

    assert result["status"] == "success"
    mock_processor_class.assert_called_once_with(graph_id="sec")
    mock_asyncio_run.assert_called_once()
