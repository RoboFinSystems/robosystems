"""Tests for S3 zip cache helpers (spot instance resilience).

Tests cache_exists, zip_and_upload/download_and_extract round-trip,
zip slip protection, and delete_cache_keys batching.
"""

import io
import zipfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from robosystems.adapters.sec.processors.cache import (
  cache_exists,
  delete_cache_keys,
  download_and_extract,
  zip_and_upload,
)


@pytest.mark.unit
class TestCacheExists:
  """Tests for cache_exists HEAD request check."""

  def test_returns_true_when_object_exists(self):
    s3 = MagicMock()
    s3.head_object.return_value = {}
    assert cache_exists(s3, "bucket", "sec/cache/2024-Q1/abc.zip") is True
    s3.head_object.assert_called_once_with(
      Bucket="bucket", Key="sec/cache/2024-Q1/abc.zip"
    )

  def test_returns_false_on_404(self):
    s3 = MagicMock()
    s3.head_object.side_effect = ClientError(
      {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
    )
    assert cache_exists(s3, "bucket", "sec/cache/2024-Q1/abc.zip") is False

  def test_raises_on_non_404_error(self):
    s3 = MagicMock()
    s3.head_object.side_effect = ClientError(
      {"Error": {"Code": "403", "Message": "Forbidden"}}, "HeadObject"
    )
    with pytest.raises(ClientError):
      cache_exists(s3, "bucket", "sec/cache/2024-Q1/abc.zip")


@pytest.mark.unit
class TestZipAndUpload:
  """Tests for zip_and_upload."""

  def test_creates_valid_zip_and_uploads(self):
    s3 = MagicMock()
    tables = {
      "nodes/Entity": b"entity-parquet-bytes",
      "relationships/FACT_HAS_ENTITY": b"rel-parquet-bytes",
    }

    zip_and_upload(s3, "bucket", "cache/key.zip", tables)

    s3.put_object.assert_called_once()
    call_kwargs = s3.put_object.call_args[1]
    assert call_kwargs["Bucket"] == "bucket"
    assert call_kwargs["Key"] == "cache/key.zip"

    # Verify the uploaded body is a valid zip with correct contents
    uploaded_bytes = call_kwargs["Body"]
    with zipfile.ZipFile(io.BytesIO(uploaded_bytes)) as zf:
      names = sorted(zf.namelist())
      assert names == [
        "nodes/Entity.parquet",
        "relationships/FACT_HAS_ENTITY.parquet",
      ]
      assert zf.read("nodes/Entity.parquet") == b"entity-parquet-bytes"
      assert zf.read("relationships/FACT_HAS_ENTITY.parquet") == b"rel-parquet-bytes"

  def test_empty_tables_creates_empty_zip(self):
    s3 = MagicMock()
    zip_and_upload(s3, "bucket", "cache/key.zip", {})

    uploaded_bytes = s3.put_object.call_args[1]["Body"]
    with zipfile.ZipFile(io.BytesIO(uploaded_bytes)) as zf:
      assert zf.namelist() == []


@pytest.mark.unit
class TestDownloadAndExtract:
  """Tests for download_and_extract."""

  def _make_zip_bytes(self, entries: dict[str, bytes]) -> bytes:
    """Helper to create zip bytes from a dict of name -> content."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
      for name, content in entries.items():
        zf.writestr(name, content)
    return buf.getvalue()

  def test_extracts_to_correct_structure(self, tmp_path: Path):
    zip_bytes = self._make_zip_bytes(
      {
        "nodes/Entity.parquet": b"entity-data",
        "nodes/Fact.parquet": b"fact-data",
        "relationships/FACT_HAS_ENTITY.parquet": b"rel-data",
      }
    )
    s3 = MagicMock()
    body_mock = MagicMock()
    body_mock.read.return_value = zip_bytes
    s3.get_object.return_value = {"Body": body_mock}

    download_and_extract(s3, "bucket", "cache/key.zip", tmp_path, "sf_123")

    # Verify files are extracted to work_dir/{entity_type}/{table_name}/{source_file_id}.parquet
    assert (
      tmp_path / "nodes" / "Entity" / "sf_123.parquet"
    ).read_bytes() == b"entity-data"
    assert (tmp_path / "nodes" / "Fact" / "sf_123.parquet").read_bytes() == b"fact-data"
    assert (
      tmp_path / "relationships" / "FACT_HAS_ENTITY" / "sf_123.parquet"
    ).read_bytes() == b"rel-data"

  def test_skips_non_parquet_entries(self, tmp_path: Path):
    zip_bytes = self._make_zip_bytes(
      {
        "nodes/Entity.parquet": b"entity-data",
        "README.md": b"some readme",
        "metadata.json": b"{}",
      }
    )
    s3 = MagicMock()
    body_mock = MagicMock()
    body_mock.read.return_value = zip_bytes
    s3.get_object.return_value = {"Body": body_mock}

    download_and_extract(s3, "bucket", "cache/key.zip", tmp_path, "sf_123")

    # Only parquet file extracted
    assert (tmp_path / "nodes" / "Entity" / "sf_123.parquet").exists()
    assert not (tmp_path / "README.md").exists()
    assert not (tmp_path / "metadata.json").exists()

  def test_zip_slip_traversal_blocked(self, tmp_path: Path):
    """Paths with ../ traversal are silently skipped."""
    zip_bytes = self._make_zip_bytes(
      {
        "../../etc/passwd.parquet": b"malicious-data",
        "nodes/Entity.parquet": b"good-data",
      }
    )
    s3 = MagicMock()
    body_mock = MagicMock()
    body_mock.read.return_value = zip_bytes
    s3.get_object.return_value = {"Body": body_mock}

    download_and_extract(s3, "bucket", "cache/key.zip", tmp_path, "sf_123")

    # Traversal entry skipped, legitimate entry extracted
    assert (
      tmp_path / "nodes" / "Entity" / "sf_123.parquet"
    ).read_bytes() == b"good-data"
    # Malicious path not written anywhere
    assert not (tmp_path / ".." / ".." / "etc").exists()

  def test_empty_zip_extracts_nothing(self, tmp_path: Path):
    zip_bytes = self._make_zip_bytes({})
    s3 = MagicMock()
    body_mock = MagicMock()
    body_mock.read.return_value = zip_bytes
    s3.get_object.return_value = {"Body": body_mock}

    download_and_extract(s3, "bucket", "cache/key.zip", tmp_path, "sf_123")

    # No files created
    assert list(tmp_path.iterdir()) == []


@pytest.mark.unit
class TestZipRoundTrip:
  """Tests for zip_and_upload -> download_and_extract round-trip."""

  def test_round_trip_preserves_data(self, tmp_path: Path):
    """Data survives zip -> upload -> download -> extract cycle."""
    tables = {
      "nodes/Entity": b"entity-parquet-bytes-12345",
      "nodes/Element": b"element-parquet-bytes-67890",
      "relationships/FACT_HAS_ENTITY": b"rel-bytes-abcde",
    }

    # Capture uploaded bytes
    s3_upload = MagicMock()
    zip_and_upload(s3_upload, "bucket", "cache/key.zip", tables)
    uploaded_bytes = s3_upload.put_object.call_args[1]["Body"]

    # Feed captured bytes into download
    s3_download = MagicMock()
    body_mock = MagicMock()
    body_mock.read.return_value = uploaded_bytes
    s3_download.get_object.return_value = {"Body": body_mock}

    download_and_extract(s3_download, "bucket", "cache/key.zip", tmp_path, "sf_abc")

    # Verify all tables round-tripped correctly
    assert (
      tmp_path / "nodes" / "Entity" / "sf_abc.parquet"
    ).read_bytes() == b"entity-parquet-bytes-12345"
    assert (
      tmp_path / "nodes" / "Element" / "sf_abc.parquet"
    ).read_bytes() == b"element-parquet-bytes-67890"
    assert (
      tmp_path / "relationships" / "FACT_HAS_ENTITY" / "sf_abc.parquet"
    ).read_bytes() == b"rel-bytes-abcde"


@pytest.mark.unit
class TestDeleteCacheKeys:
  """Tests for delete_cache_keys bulk deletion."""

  def test_returns_zero_for_empty_list(self):
    s3 = MagicMock()
    assert delete_cache_keys(s3, "bucket", []) == 0
    s3.delete_objects.assert_not_called()

  def test_deletes_single_batch(self):
    s3 = MagicMock()
    s3.delete_objects.return_value = {}  # No errors

    keys = [f"sec/cache/2024-Q1/sf_{i}.zip" for i in range(5)]
    result = delete_cache_keys(s3, "bucket", keys)

    assert result == 5
    s3.delete_objects.assert_called_once()
    call_kwargs = s3.delete_objects.call_args[1]
    assert call_kwargs["Bucket"] == "bucket"
    assert len(call_kwargs["Delete"]["Objects"]) == 5

  def test_batches_over_1000_keys(self):
    s3 = MagicMock()
    s3.delete_objects.return_value = {}

    keys = [f"sec/cache/2024-Q1/sf_{i}.zip" for i in range(2500)]
    result = delete_cache_keys(s3, "bucket", keys)

    assert result == 2500
    assert s3.delete_objects.call_count == 3  # 1000 + 1000 + 500
    # Verify batch sizes
    calls = s3.delete_objects.call_args_list
    assert len(calls[0][1]["Delete"]["Objects"]) == 1000
    assert len(calls[1][1]["Delete"]["Objects"]) == 1000
    assert len(calls[2][1]["Delete"]["Objects"]) == 500

  def test_accounts_for_partial_errors(self):
    s3 = MagicMock()
    s3.delete_objects.return_value = {
      "Errors": [
        {"Key": "sec/cache/2024-Q1/sf_2.zip", "Code": "InternalError"},
      ]
    }

    keys = [f"sec/cache/2024-Q1/sf_{i}.zip" for i in range(5)]
    result = delete_cache_keys(s3, "bucket", keys)

    # 5 attempted - 1 error = 4 deleted
    assert result == 4

  def test_exactly_1000_keys_single_batch(self):
    s3 = MagicMock()
    s3.delete_objects.return_value = {}

    keys = [f"sec/cache/2024-Q1/sf_{i}.zip" for i in range(1000)]
    result = delete_cache_keys(s3, "bucket", keys)

    assert result == 1000
    assert s3.delete_objects.call_count == 1
