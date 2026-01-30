"""Tests for SEC staging models.

Tests the staging result models used for decoupled
DuckDB staging and LadybugDB materialization.
"""

from robosystems.adapters.sec.processors.ingestion import (
  MaterializeResult,
  StagingResult,
  TableInfo,
)


class TestTableInfo:
  """Tests for TableInfo dataclass."""

  def test_to_dict(self):
    """Test TableInfo serialization to dict."""
    info = TableInfo(
      name="Entity",
      row_count=1000,
      file_count=50,
      staged_at="2025-01-15T10:00:00Z",
    )
    result = info.to_dict()

    assert result["name"] == "Entity"
    assert result["row_count"] == 1000
    assert result["file_count"] == 50
    assert result["staged_at"] == "2025-01-15T10:00:00Z"
    assert result["skipped"] is False  # Default value

  def test_to_dict_skipped(self):
    """Test TableInfo serialization with skipped=True."""
    info = TableInfo(
      name="OptionalTable",
      row_count=0,
      file_count=0,
      staged_at="2025-01-15T10:00:00Z",
      skipped=True,
    )
    result = info.to_dict()

    assert result["name"] == "OptionalTable"
    assert result["row_count"] == 0
    assert result["skipped"] is True

  def test_from_dict(self):
    """Test TableInfo deserialization from dict."""
    data = {
      "name": "Fact",
      "row_count": 50000,
      "file_count": 200,
      "staged_at": "2025-01-15T11:00:00Z",
    }
    info = TableInfo.from_dict(data)

    assert info.name == "Fact"
    assert info.row_count == 50000
    assert info.file_count == 200
    assert info.staged_at == "2025-01-15T11:00:00Z"
    assert info.skipped is False  # Default when not in dict

  def test_from_dict_with_skipped(self):
    """Test TableInfo deserialization with skipped field."""
    data = {
      "name": "OptionalTable",
      "row_count": 0,
      "file_count": 0,
      "staged_at": "2025-01-15T11:00:00Z",
      "skipped": True,
    }
    info = TableInfo.from_dict(data)

    assert info.name == "OptionalTable"
    assert info.skipped is True

  def test_roundtrip(self):
    """Test TableInfo serialization roundtrip."""
    original = TableInfo(
      name="Element",
      row_count=5000,
      file_count=100,
      staged_at="2025-01-15T12:00:00Z",
    )
    restored = TableInfo.from_dict(original.to_dict())

    assert restored.name == original.name
    assert restored.row_count == original.row_count
    assert restored.file_count == original.file_count
    assert restored.staged_at == original.staged_at
    assert restored.skipped == original.skipped

  def test_roundtrip_skipped(self):
    """Test TableInfo serialization roundtrip with skipped=True."""
    original = TableInfo(
      name="OptionalTable",
      row_count=0,
      file_count=0,
      staged_at="2025-01-15T12:00:00Z",
      skipped=True,
    )
    restored = TableInfo.from_dict(original.to_dict())

    assert restored.skipped is True


class TestStagingResult:
  """Tests for StagingResult dataclass."""

  def test_success_result(self):
    """Test successful staging result."""
    result = StagingResult(
      status="success",
      table_names=["Entity", "Fact", "Element"],
      total_files=500,
      total_rows=100000,
      duration_seconds=120.5,
      duckdb_path="/data/staging/sec.duckdb",
    )

    assert result.status == "success"
    assert len(result.table_names) == 3
    assert result.total_files == 500
    assert result.total_rows == 100000
    assert result.error is None

  def test_error_result(self):
    """Test error staging result."""
    result = StagingResult(
      status="error",
      table_names=[],
      error="Graph client initialization failed",
      duration_seconds=0.5,
    )

    assert result.status == "error"
    assert result.error == "Graph client initialization failed"
    assert len(result.table_names) == 0

  def test_to_dict(self):
    """Test StagingResult serialization."""
    result = StagingResult(
      status="success",
      table_names=["Entity"],
      total_files=10,
      total_rows=1000,
      duration_seconds=5.0,
    )
    data = result.to_dict()

    assert data["status"] == "success"
    assert data["table_names"] == ["Entity"]
    assert data["total_files"] == 10
    assert data["total_rows"] == 1000


class TestMaterializeResult:
  """Tests for MaterializeResult dataclass."""

  def test_success_result(self):
    """Test successful materialization result."""
    result = MaterializeResult(
      status="success",
      total_rows_ingested=50000,
      total_time_ms=30000.0,
      tables=[
        {"table_name": "Entity", "rows_ingested": 1000, "status": "success"},
        {"table_name": "Fact", "rows_ingested": 49000, "status": "success"},
      ],
    )

    assert result.status == "success"
    assert result.total_rows_ingested == 50000
    assert len(result.tables) == 2

  def test_error_result(self):
    """Test error materialization result."""
    result = MaterializeResult(
      status="error",
      error="No tables found in schema",
    )

    assert result.status == "error"
    assert result.error == "No tables found in schema"

  def test_to_dict(self):
    """Test MaterializeResult serialization."""
    result = MaterializeResult(
      status="success",
      total_rows_ingested=1000,
      total_time_ms=5000.0,
    )
    data = result.to_dict()

    assert data["status"] == "success"
    assert data["total_rows_ingested"] == 1000
    assert data["total_time_ms"] == 5000.0
