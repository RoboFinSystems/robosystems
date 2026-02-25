"""Tests for SEC parquet consolidation.

Tests get_quarter_end_date, consolidate_parquet_tables_by_date,
consolidate_parquet_from_disk, merge_with_existing_s3, and atomic_s3_upload.
"""

import tempfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from robosystems.adapters.sec.processors.consolidation import (
  atomic_s3_upload,
  consolidate_parquet_from_disk,
  consolidate_parquet_tables_by_date,
  get_quarter_end_date,
  merge_with_existing_s3,
)


def _make_parquet_bytes(data: dict) -> bytes:
  """Helper to create parquet bytes from a dict of column data."""
  table = pa.table(data)
  buf = BytesIO()
  pq.write_table(table, buf)
  return buf.getvalue()


@dataclass
class FakeProcessedFilingResult:
  """Minimal stand-in for ProcessedFilingResult."""

  success: bool
  source_file_id: str
  partition_key: str
  tables: dict
  filing_date: str | None = None
  error: str | None = None
  skipped_reason: str | None = None


@pytest.mark.unit
class TestGetQuarterEndDate:
  """Tests for get_quarter_end_date."""

  def test_q1(self):
    assert get_quarter_end_date(2024, 1) == "2024-03-31"

  def test_q2(self):
    assert get_quarter_end_date(2024, 2) == "2024-06-30"

  def test_q3(self):
    assert get_quarter_end_date(2024, 3) == "2024-09-30"

  def test_q4(self):
    assert get_quarter_end_date(2024, 4) == "2024-12-31"

  def test_different_year(self):
    assert get_quarter_end_date(2009, 1) == "2009-03-31"
    assert get_quarter_end_date(2030, 4) == "2030-12-31"


@pytest.mark.unit
class TestConsolidateParquetTablesByDate:
  """Tests for consolidate_parquet_tables_by_date."""

  def test_empty_results(self):
    """Returns empty dict for empty results list."""
    result = consolidate_parquet_tables_by_date([])
    assert result == {}

  def test_skips_failed_results(self):
    """Skips results where success=False."""
    failed = FakeProcessedFilingResult(
      success=False,
      source_file_id="sf1",
      partition_key="2024_1045810_0001",
      tables={"nodes/Entity": _make_parquet_bytes({"identifier": ["a"]})},
    )
    result = consolidate_parquet_tables_by_date([failed])
    assert result == {}

  def test_single_result(self):
    """Single result produces one date entry."""
    parquet_data = _make_parquet_bytes(
      {"identifier": ["e1", "e2"], "name": ["Corp A", "Corp B"]}
    )
    filing = FakeProcessedFilingResult(
      success=True,
      source_file_id="sf1",
      partition_key="2024_1045810_0001",
      tables={"nodes/Entity": parquet_data},
      filing_date="2024-01-15",
    )
    result = consolidate_parquet_tables_by_date([filing])

    assert "2024-01-15" in result
    assert "nodes/Entity" in result["2024-01-15"]
    # Verify the consolidated bytes are valid parquet
    table = pq.read_table(BytesIO(result["2024-01-15"]["nodes/Entity"]))
    assert table.num_rows == 2

  def test_multiple_results_same_date(self):
    """Multiple filings on the same date are consolidated."""
    data1 = _make_parquet_bytes({"identifier": ["e1"], "name": ["Corp A"]})
    data2 = _make_parquet_bytes({"identifier": ["e2"], "name": ["Corp B"]})

    filings = [
      FakeProcessedFilingResult(
        success=True,
        source_file_id="sf1",
        partition_key="2024_111_0001",
        tables={"nodes/Entity": data1},
        filing_date="2024-01-15",
      ),
      FakeProcessedFilingResult(
        success=True,
        source_file_id="sf2",
        partition_key="2024_222_0002",
        tables={"nodes/Entity": data2},
        filing_date="2024-01-15",
      ),
    ]
    result = consolidate_parquet_tables_by_date(filings)

    table = pq.read_table(BytesIO(result["2024-01-15"]["nodes/Entity"]))
    assert table.num_rows == 2

  def test_multiple_dates(self):
    """Filings on different dates produce separate entries."""
    data1 = _make_parquet_bytes({"identifier": ["e1"]})
    data2 = _make_parquet_bytes({"identifier": ["e2"]})

    filings = [
      FakeProcessedFilingResult(
        success=True,
        source_file_id="sf1",
        partition_key="2024_111_0001",
        tables={"nodes/Entity": data1},
        filing_date="2024-01-15",
      ),
      FakeProcessedFilingResult(
        success=True,
        source_file_id="sf2",
        partition_key="2024_222_0002",
        tables={"nodes/Entity": data2},
        filing_date="2024-02-20",
      ),
    ]
    result = consolidate_parquet_tables_by_date(filings)

    assert "2024-01-15" in result
    assert "2024-02-20" in result

  def test_shared_node_tables_deduplicated(self):
    """Shared node tables (Element, Label, etc.) are deduplicated on identifier."""
    data1 = _make_parquet_bytes(
      {"identifier": ["elem1", "elem2"], "name": ["E1", "E2"]}
    )
    data2 = _make_parquet_bytes(
      {"identifier": ["elem2", "elem3"], "name": ["E2dup", "E3"]}
    )

    filings = [
      FakeProcessedFilingResult(
        success=True,
        source_file_id="sf1",
        partition_key="2024_111_0001",
        tables={"nodes/Element": data1},
        filing_date="2024-01-15",
      ),
      FakeProcessedFilingResult(
        success=True,
        source_file_id="sf2",
        partition_key="2024_222_0002",
        tables={"nodes/Element": data2},
        filing_date="2024-01-15",
      ),
    ]
    result = consolidate_parquet_tables_by_date(filings)

    table = pq.read_table(BytesIO(result["2024-01-15"]["nodes/Element"]))
    # elem2 appears twice but should be deduped to 3 unique
    assert table.num_rows == 3
    identifiers = table.column("identifier").to_pylist()
    assert "elem1" in identifiers
    assert "elem2" in identifiers
    assert "elem3" in identifiers

  def test_missing_filing_date_uses_unknown(self):
    """Results without filing_date are grouped under 'unknown'."""
    data = _make_parquet_bytes({"identifier": ["e1"]})
    filing = FakeProcessedFilingResult(
      success=True,
      source_file_id="sf1",
      partition_key="2024_111_0001",
      tables={"nodes/Entity": data},
      filing_date=None,
    )
    result = consolidate_parquet_tables_by_date([filing])
    assert "unknown" in result


def _can_write_parquet_to_disk():
  """Check if pyarrow can write parquet to disk (fails after DuckDB registers 'file' scheme)."""
  try:
    import tempfile as _tf

    with _tf.TemporaryDirectory() as d:
      pq.write_table(pa.table({"x": [1]}), Path(d) / "test.parquet")
    return True
  except Exception:
    return False


@pytest.mark.unit
@pytest.mark.skipif(
  not _can_write_parquet_to_disk(),
  reason="pyarrow filesystem conflicts with DuckDB in same process",
)
class TestConsolidateParquetFromDisk:
  """Tests for consolidate_parquet_from_disk."""

  def test_nonexistent_directory(self):
    """Returns empty bytes for nonexistent directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
      result = consolidate_parquet_from_disk(Path(tmpdir), "nodes/NonExistent")
      assert result == b""

  def test_empty_directory(self):
    """Returns empty bytes for directory with no parquet files."""
    with tempfile.TemporaryDirectory() as tmpdir:
      table_dir = Path(tmpdir) / "nodes" / "Entity"
      table_dir.mkdir(parents=True)
      result = consolidate_parquet_from_disk(Path(tmpdir), "nodes/Entity")
      assert result == b""

  @staticmethod
  def _write_parquet(table, path):
    """Write parquet using BytesIO to avoid DuckDB/pyarrow filesystem conflict."""
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    Path(path).write_bytes(sink.getvalue().to_pybytes())

  def test_single_file(self):
    """Consolidates single parquet file correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
      table_dir = Path(tmpdir) / "nodes" / "Entity"
      table_dir.mkdir(parents=True)

      table = pa.table({"identifier": ["e1", "e2"], "name": ["A", "B"]})
      self._write_parquet(table, table_dir / "part_001.parquet")

      result = consolidate_parquet_from_disk(Path(tmpdir), "nodes/Entity")
      assert len(result) > 0
      read_back = pq.read_table(BytesIO(result))
      assert read_back.num_rows == 2

  def test_multiple_files(self):
    """Consolidates multiple parquet files into one."""
    with tempfile.TemporaryDirectory() as tmpdir:
      table_dir = Path(tmpdir) / "nodes" / "Entity"
      table_dir.mkdir(parents=True)

      t1 = pa.table({"identifier": ["e1"], "name": ["A"]})
      t2 = pa.table({"identifier": ["e2"], "name": ["B"]})
      self._write_parquet(t1, table_dir / "part_001.parquet")
      self._write_parquet(t2, table_dir / "part_002.parquet")

      result = consolidate_parquet_from_disk(Path(tmpdir), "nodes/Entity")
      read_back = pq.read_table(BytesIO(result))
      assert read_back.num_rows == 2

  def test_shared_table_dedup(self):
    """Shared node tables are deduplicated on identifier."""
    with tempfile.TemporaryDirectory() as tmpdir:
      table_dir = Path(tmpdir) / "nodes" / "Element"
      table_dir.mkdir(parents=True)

      t1 = pa.table({"identifier": ["elem1", "elem2"]})
      t2 = pa.table({"identifier": ["elem2", "elem3"]})
      self._write_parquet(t1, table_dir / "part_001.parquet")
      self._write_parquet(t2, table_dir / "part_002.parquet")

      result = consolidate_parquet_from_disk(Path(tmpdir), "nodes/Element")
      read_back = pq.read_table(BytesIO(result))
      assert read_back.num_rows == 3

  def test_corrupted_file_skipped(self):
    """Corrupted parquet files are skipped gracefully."""
    with tempfile.TemporaryDirectory() as tmpdir:
      table_dir = Path(tmpdir) / "nodes" / "Entity"
      table_dir.mkdir(parents=True)

      t1 = pa.table({"identifier": ["e1"]})
      self._write_parquet(t1, table_dir / "good.parquet")
      # Write corrupt file
      (table_dir / "bad.parquet").write_bytes(b"not a parquet file")

      result = consolidate_parquet_from_disk(Path(tmpdir), "nodes/Entity")
      read_back = pq.read_table(BytesIO(result))
      assert read_back.num_rows == 1

  def test_all_corrupted_returns_empty(self):
    """Returns empty bytes when all files are corrupted."""
    with tempfile.TemporaryDirectory() as tmpdir:
      table_dir = Path(tmpdir) / "nodes" / "Entity"
      table_dir.mkdir(parents=True)
      (table_dir / "bad1.parquet").write_bytes(b"corrupt")
      (table_dir / "bad2.parquet").write_bytes(b"also corrupt")

      result = consolidate_parquet_from_disk(Path(tmpdir), "nodes/Entity")
      assert result == b""


@pytest.mark.unit
class TestMergeWithExistingS3:
  """Tests for merge_with_existing_s3."""

  def test_no_existing_file_returns_new_data(self):
    """Returns new data when S3 file doesn't exist (NoSuchKey)."""
    mock_s3 = MagicMock()
    mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
    mock_s3.get_object.side_effect = mock_s3.exceptions.NoSuchKey("Not found")

    new_data = _make_parquet_bytes({"identifier": ["e1"]})
    result = merge_with_existing_s3(
      mock_s3, "bucket", "key.parquet", new_data, "nodes/Entity"
    )
    assert result == new_data

  def test_s3_error_returns_new_data(self):
    """Returns new data when S3 read fails with generic error."""
    mock_s3 = MagicMock()
    mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
    mock_s3.get_object.side_effect = RuntimeError("Connection error")

    new_data = _make_parquet_bytes({"identifier": ["e1"]})
    result = merge_with_existing_s3(
      mock_s3, "bucket", "key.parquet", new_data, "nodes/Entity"
    )
    assert result == new_data

  def test_merge_non_shared_table(self):
    """Non-shared tables concatenate without dedup."""
    existing_data = _make_parquet_bytes({"identifier": ["f1"], "value": ["old"]})
    new_data = _make_parquet_bytes({"identifier": ["f1"], "value": ["new"]})

    mock_s3 = MagicMock()
    mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
    mock_s3.get_object.return_value = {
      "Body": MagicMock(read=MagicMock(return_value=existing_data))
    }

    result = merge_with_existing_s3(
      mock_s3, "bucket", "key.parquet", new_data, "nodes/Fact"
    )
    table = pq.read_table(BytesIO(result))
    # f1 appears twice (no dedup for non-shared tables)
    assert table.num_rows == 2

  def test_merge_shared_table_deduplicates(self):
    """Shared tables are deduplicated on identifier during merge."""
    existing_data = _make_parquet_bytes(
      {"identifier": ["elem1", "elem2"], "name": ["E1", "E2"]}
    )
    new_data = _make_parquet_bytes(
      {"identifier": ["elem2", "elem3"], "name": ["E2new", "E3"]}
    )

    mock_s3 = MagicMock()
    mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
    mock_s3.get_object.return_value = {
      "Body": MagicMock(read=MagicMock(return_value=existing_data))
    }

    result = merge_with_existing_s3(
      mock_s3, "bucket", "key.parquet", new_data, "nodes/Element"
    )
    table = pq.read_table(BytesIO(result))
    assert table.num_rows == 3

  def test_merge_corrupted_new_data_keeps_existing(self):
    """When new data can't be parsed, returns existing data."""
    existing_data = _make_parquet_bytes({"identifier": ["e1"]})

    mock_s3 = MagicMock()
    mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
    mock_s3.get_object.return_value = {
      "Body": MagicMock(read=MagicMock(return_value=existing_data))
    }

    result = merge_with_existing_s3(
      mock_s3, "bucket", "key.parquet", b"not parquet", "nodes/Entity"
    )
    assert result == existing_data


@pytest.mark.unit
class TestAtomicS3Upload:
  """Tests for atomic_s3_upload."""

  def test_successful_upload(self):
    """Upload follows put -> copy -> delete pattern."""
    mock_s3 = MagicMock()

    atomic_s3_upload(mock_s3, "bucket", "final/key.parquet", b"data")

    # Verify the three-step atomic pattern
    mock_s3.put_object.assert_called_once()
    put_call = mock_s3.put_object.call_args
    assert put_call.kwargs["Bucket"] == "bucket"
    assert ".tmp." in put_call.kwargs["Key"]
    assert put_call.kwargs["Body"] == b"data"

    mock_s3.copy_object.assert_called_once()
    copy_call = mock_s3.copy_object.call_args
    assert copy_call.kwargs["Key"] == "final/key.parquet"

    # Temp file is cleaned up
    mock_s3.delete_object.assert_called_once()

  def test_copy_failure_cleans_up_temp(self):
    """If copy fails, temp file is cleaned up and exception re-raised."""
    mock_s3 = MagicMock()
    mock_s3.copy_object.side_effect = RuntimeError("Copy failed")

    with pytest.raises(RuntimeError, match="Copy failed"):
      atomic_s3_upload(mock_s3, "bucket", "final/key.parquet", b"data")

    # Temp file cleanup should be attempted
    mock_s3.delete_object.assert_called_once()

  def test_put_failure_attempts_cleanup(self):
    """If put fails, temp cleanup is attempted and exception re-raised."""
    mock_s3 = MagicMock()
    mock_s3.put_object.side_effect = RuntimeError("Put failed")

    with pytest.raises(RuntimeError, match="Put failed"):
      atomic_s3_upload(mock_s3, "bucket", "final/key.parquet", b"data")

  def test_cleanup_failure_is_swallowed(self):
    """If temp cleanup also fails, the original exception is still raised."""
    mock_s3 = MagicMock()
    mock_s3.copy_object.side_effect = RuntimeError("Copy failed")
    mock_s3.delete_object.side_effect = RuntimeError("Cleanup also failed")

    with pytest.raises(RuntimeError, match="Copy failed"):
      atomic_s3_upload(mock_s3, "bucket", "final/key.parquet", b"data")
