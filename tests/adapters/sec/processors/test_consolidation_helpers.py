"""Tests for SEC consolidation and S3 helper functions.

Tests the pure Arrow dedup, S3 existence checks, and pattern generation
used by the part-file output pipeline.
"""

from unittest.mock import MagicMock

import pyarrow as pa

from robosystems.adapters.sec.processors.consolidation import _dedup_arrow_table
from robosystems.adapters.sec.processors.ingestion.models import (
  s3_get_table_patterns,
  s3_prefix_has_objects,
  s3_table_data_exists,
)


class TestDedupArrowTable:
  """Tests for _dedup_arrow_table (pure Arrow dedup)."""

  def test_dedup_removes_duplicates(self):
    """Keeps first occurrence, removes later duplicates."""
    table = pa.table(
      {
        "identifier": ["a", "b", "a", "c", "b"],
        "value": [1, 2, 3, 4, 5],
      }
    )
    result = _dedup_arrow_table(table, "identifier")
    assert result.num_rows == 3
    assert result.column("identifier").to_pylist() == ["a", "b", "c"]
    # First occurrence kept
    assert result.column("value").to_pylist() == [1, 2, 4]

  def test_dedup_no_duplicates_returns_same_table(self):
    """Returns the same table object when no duplicates exist."""
    table = pa.table(
      {
        "identifier": ["a", "b", "c"],
        "value": [1, 2, 3],
      }
    )
    result = _dedup_arrow_table(table, "identifier")
    # Should return the same object (no copy)
    assert result is table

  def test_dedup_empty_table(self):
    """Handles empty table."""
    table = pa.table(
      {
        "identifier": pa.array([], type=pa.string()),
        "value": pa.array([], type=pa.int64()),
      }
    )
    result = _dedup_arrow_table(table, "identifier")
    assert result.num_rows == 0

  def test_dedup_single_row(self):
    """Handles single-row table."""
    table = pa.table({"identifier": ["a"], "value": [1]})
    result = _dedup_arrow_table(table, "identifier")
    assert result is table

  def test_dedup_all_same(self):
    """All rows have the same identifier — keeps only first."""
    table = pa.table(
      {
        "identifier": ["x", "x", "x"],
        "value": [10, 20, 30],
      }
    )
    result = _dedup_arrow_table(table, "identifier")
    assert result.num_rows == 1
    assert result.column("value").to_pylist() == [10]

  def test_dedup_with_label_logging(self):
    """Label parameter doesn't break anything (just used for logging)."""
    table = pa.table(
      {
        "identifier": ["a", "a"],
        "value": [1, 2],
      }
    )
    result = _dedup_arrow_table(table, "identifier", label="nodes/Element")
    assert result.num_rows == 1


class TestS3PrefixHasObjects:
  """Tests for s3_prefix_has_objects."""

  def test_prefix_with_objects(self):
    """Returns True when objects exist under prefix."""
    s3_client = MagicMock()
    s3_client.list_objects.return_value = [
      "sec/processed/filed=2024-Q1/nodes/Element/part_abc.parquet"
    ]
    assert s3_prefix_has_objects(s3_client, "bucket", "sec/processed/") is True
    s3_client.list_objects.assert_called_once_with(
      "bucket", prefix="sec/processed/", max_keys=1
    )

  def test_prefix_without_objects(self):
    """Returns False when no objects exist under prefix."""
    s3_client = MagicMock()
    s3_client.list_objects.return_value = []
    assert (
      s3_prefix_has_objects(s3_client, "bucket", "sec/processed/nonexistent/") is False
    )


class TestS3TableDataExists:
  """Tests for s3_table_data_exists."""

  def test_old_format_exists(self):
    """Returns True when old format TABLE.parquet exists."""
    s3_client = MagicMock()
    s3_client.object_exists.return_value = True
    result = s3_table_data_exists(
      s3_client, "bucket", "sec/processed", "filed=2024-Q1", "nodes", "Element"
    )
    assert result is True
    s3_client.object_exists.assert_called_once_with(
      "bucket", "sec/processed/filed=2024-Q1/nodes/Element.parquet"
    )

  def test_new_format_exists(self):
    """Returns True when new format TABLE/*.parquet exists."""
    s3_client = MagicMock()
    s3_client.object_exists.return_value = False  # Old format doesn't exist
    s3_client.list_objects.return_value = ["part_abc.parquet"]  # But part files do
    result = s3_table_data_exists(
      s3_client, "bucket", "sec/processed", "filed=2024-Q1", "nodes", "Element"
    )
    assert result is True

  def test_neither_format_exists(self):
    """Returns False when no data exists in either format."""
    s3_client = MagicMock()
    s3_client.object_exists.return_value = False
    s3_client.list_objects.return_value = []
    result = s3_table_data_exists(
      s3_client, "bucket", "sec/processed", "filed=2024-Q1", "nodes", "Element"
    )
    assert result is False


class TestS3GetTablePatterns:
  """Tests for s3_get_table_patterns."""

  def test_old_format_only(self):
    """Returns only old format pattern when only TABLE.parquet exists."""
    s3_client = MagicMock()
    s3_client.object_exists.return_value = True
    s3_client.list_objects.return_value = []
    patterns = s3_get_table_patterns(
      s3_client, "bucket", "sec/processed", "filed=2024-Q1", "nodes", "Element"
    )
    assert patterns == ["s3://bucket/sec/processed/filed=2024-Q1/nodes/Element.parquet"]

  def test_new_format_only(self):
    """Returns only new format pattern when only part files exist."""
    s3_client = MagicMock()
    s3_client.object_exists.return_value = False
    s3_client.list_objects.return_value = ["part_abc.parquet"]
    patterns = s3_get_table_patterns(
      s3_client, "bucket", "sec/processed", "filed=2024-Q1", "nodes", "Element"
    )
    assert patterns == [
      "s3://bucket/sec/processed/filed=2024-Q1/nodes/Element/*.parquet"
    ]

  def test_both_formats(self):
    """Returns both patterns when data exists in both formats."""
    s3_client = MagicMock()
    s3_client.object_exists.return_value = True
    s3_client.list_objects.return_value = ["part_abc.parquet"]
    patterns = s3_get_table_patterns(
      s3_client, "bucket", "sec/processed", "filed=2024-Q1", "nodes", "Element"
    )
    assert len(patterns) == 2
    assert "s3://bucket/sec/processed/filed=2024-Q1/nodes/Element.parquet" in patterns
    assert "s3://bucket/sec/processed/filed=2024-Q1/nodes/Element/*.parquet" in patterns

  def test_no_data(self):
    """Returns empty list when no data exists."""
    s3_client = MagicMock()
    s3_client.object_exists.return_value = False
    s3_client.list_objects.return_value = []
    patterns = s3_get_table_patterns(
      s3_client, "bucket", "sec/processed", "filed=2024-Q1", "nodes", "Element"
    )
    assert patterns == []
