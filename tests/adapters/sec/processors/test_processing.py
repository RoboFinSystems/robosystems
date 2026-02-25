"""Tests for SEC filing processing.

Tests process_single_filing_to_memory orchestration, partition key parsing,
XBRL file selection, form type filtering, and error handling.
"""

import os
import zipfile
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

from robosystems.adapters.sec.processors.processing import (
  ProcessedFilingResult,
  process_single_filing_to_memory,
)


@pytest.mark.unit
class TestProcessedFilingResultDataclass:
  """Tests for ProcessedFilingResult dataclass."""

  def test_minimal_creation(self):
    """Create with only required fields."""
    result = ProcessedFilingResult(
      success=True,
      source_file_id="sf1",
      partition_key="2024_1045810_0001",
      tables={},
    )
    assert result.success is True
    assert result.source_file_id == "sf1"
    assert result.filing_date is None
    assert result.error is None
    assert result.skipped_reason is None

  def test_full_creation(self):
    """Create with all fields populated."""
    result = ProcessedFilingResult(
      success=True,
      source_file_id="sf1",
      partition_key="2024_1045810_0001",
      tables={"nodes/Entity": b"parquet data"},
      filing_date="2024-01-15",
      error=None,
      skipped_reason=None,
    )
    assert result.tables == {"nodes/Entity": b"parquet data"}
    assert result.filing_date == "2024-01-15"

  def test_failed_result(self):
    """Create a failure result."""
    result = ProcessedFilingResult(
      success=False,
      source_file_id="sf1",
      partition_key="bad_key",
      tables={},
      error="Invalid partition key: bad_key",
    )
    assert result.success is False
    assert "Invalid" in result.error

  def test_skipped_result(self):
    """Create a skipped result (form type filtered)."""
    result = ProcessedFilingResult(
      success=True,
      source_file_id="sf1",
      partition_key="2024_111_0001",
      tables={},
      skipped_reason="form_type=8-K",
    )
    assert result.success is True
    assert result.skipped_reason == "form_type=8-K"
    assert result.tables == {}


@pytest.mark.unit
class TestPartitionKeyParsing:
  """Tests for partition key parsing in process_single_filing_to_memory."""

  def test_invalid_partition_key_too_few_parts(self):
    """Returns error for partition key with too few parts."""
    mock_s3 = MagicMock()
    mock_loader = MagicMock()

    result = process_single_filing_to_memory(
      storage_key="sec/raw/file.zip",
      partition_key="bad_key",
      source_file_id="sf1",
      s3_client=mock_s3,
      raw_bucket="bucket",
      metadata_loader=mock_loader,
    )

    assert result.success is False
    assert "Invalid partition key" in result.error

  def test_invalid_partition_key_single_part(self):
    """Returns error for single-part partition key."""
    mock_s3 = MagicMock()
    mock_loader = MagicMock()

    result = process_single_filing_to_memory(
      storage_key="sec/raw/file.zip",
      partition_key="singlepart",
      source_file_id="sf1",
      s3_client=mock_s3,
      raw_bucket="bucket",
      metadata_loader=mock_loader,
    )

    assert result.success is False
    assert "Invalid partition key" in result.error

  def test_valid_partition_key_format(self):
    """A valid partition key (year_cik_accession) is accepted past parsing.

    This tests that the key is parsed correctly by checking the download
    is attempted (which happens after successful parse).
    """
    mock_s3 = MagicMock()
    mock_s3.download_fileobj.side_effect = RuntimeError("Download error")
    mock_loader = MagicMock()

    result = process_single_filing_to_memory(
      storage_key="sec/raw/file.zip",
      partition_key="2024_1045810_0001045810-24-000001",
      source_file_id="sf1",
      s3_client=mock_s3,
      raw_bucket="bucket",
      metadata_loader=mock_loader,
    )

    # Should fail on download, not parsing
    assert result.success is False
    assert "Download failed" in result.error


@pytest.mark.unit
class TestS3DownloadHandling:
  """Tests for S3 download error handling."""

  def test_download_failure(self):
    """Returns error when S3 download fails."""
    mock_s3 = MagicMock()
    mock_s3.download_fileobj.side_effect = Exception("S3 connection timeout")
    mock_loader = MagicMock()

    result = process_single_filing_to_memory(
      storage_key="sec/raw/file.zip",
      partition_key="2024_1045810_0001045810-24-000001",
      source_file_id="sf1",
      s3_client=mock_s3,
      raw_bucket="bucket",
      metadata_loader=mock_loader,
    )

    assert result.success is False
    assert "Download failed" in result.error


@pytest.mark.unit
class TestXBRLFileSelection:
  """Tests for XBRL file selection from ZIP contents."""

  def _make_zip_bytes(self, filenames: list[str]) -> bytes:
    """Helper to create a ZIP in memory with given filenames."""
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
      for name in filenames:
        zf.writestr(name, f"<content of {name}>")
    buf.seek(0)
    return buf.read()

  @patch("robosystems.adapters.sec.processors.processing.XBRLGraphProcessor")
  def test_prefers_htm_over_xml(self, mock_processor_cls):
    """Prefers .htm files for inline XBRL over .xml."""
    mock_processor_instance = MagicMock()
    mock_processor_cls.return_value = mock_processor_instance

    zip_data = self._make_zip_bytes(
      [
        "filing.xml",
        "filing.htm",
        "schema.xsd",
      ]
    )

    mock_s3 = MagicMock()
    mock_s3.download_fileobj.side_effect = lambda bucket, key, buf: buf.write(zip_data)

    mock_loader = MagicMock()
    mock_loader.get_metadata.return_value = (
      {"cik": "1045810"},
      {"form": "10-K", "filingDate": "2024-03-01", "primaryDocument": None},
    )

    process_single_filing_to_memory(
      storage_key="sec/raw/file.zip",
      partition_key="2024_1045810_0001045810-24-000001",
      source_file_id="sf1",
      s3_client=mock_s3,
      raw_bucket="bucket",
      metadata_loader=mock_loader,
    )

    # Check the processor was called with the htm file
    call_kwargs = mock_processor_cls.call_args.kwargs
    assert "filing.htm" in call_kwargs["local_file_path"]

  @patch("robosystems.adapters.sec.processors.processing.XBRLGraphProcessor")
  def test_falls_back_to_xml_when_no_htm(self, mock_processor_cls):
    """Falls back to .xml when no .htm files exist."""
    mock_processor_instance = MagicMock()
    mock_processor_cls.return_value = mock_processor_instance

    zip_data = self._make_zip_bytes(
      [
        "filing.xml",
        "schema.xsd",
      ]
    )

    mock_s3 = MagicMock()
    mock_s3.download_fileobj.side_effect = lambda bucket, key, buf: buf.write(zip_data)

    mock_loader = MagicMock()
    mock_loader.get_metadata.return_value = (
      {"cik": "1045810"},
      {"form": "10-K", "filingDate": "2024-03-01", "primaryDocument": None},
    )

    process_single_filing_to_memory(
      storage_key="sec/raw/file.zip",
      partition_key="2024_1045810_0001045810-24-000001",
      source_file_id="sf1",
      s3_client=mock_s3,
      raw_bucket="bucket",
      metadata_loader=mock_loader,
    )

    call_kwargs = mock_processor_cls.call_args.kwargs
    assert "filing.xml" in call_kwargs["local_file_path"]

  def test_no_xbrl_files_returns_error(self):
    """Returns error when ZIP contains no XBRL files."""
    zip_data = self._make_zip_bytes(
      [
        "document.pdf",
        "image.png",
      ]
    )

    mock_s3 = MagicMock()
    mock_s3.download_fileobj.side_effect = lambda bucket, key, buf: buf.write(zip_data)
    mock_loader = MagicMock()

    result = process_single_filing_to_memory(
      storage_key="sec/raw/file.zip",
      partition_key="2024_1045810_0001045810-24-000001",
      source_file_id="sf1",
      s3_client=mock_s3,
      raw_bucket="bucket",
      metadata_loader=mock_loader,
    )

    assert result.success is False
    assert "No XBRL files found" in result.error

  def test_excludes_taxonomy_files(self):
    """Taxonomy suffixes (_def.xml, _lab.xml, etc.) are excluded."""
    zip_data = self._make_zip_bytes(
      [
        "filing_def.xml",
        "filing_lab.xml",
        "filing_pre.xml",
        "filing_cal.xml",
        "schema.xsd",
      ]
    )

    mock_s3 = MagicMock()
    mock_s3.download_fileobj.side_effect = lambda bucket, key, buf: buf.write(zip_data)
    mock_loader = MagicMock()

    result = process_single_filing_to_memory(
      storage_key="sec/raw/file.zip",
      partition_key="2024_1045810_0001045810-24-000001",
      source_file_id="sf1",
      s3_client=mock_s3,
      raw_bucket="bucket",
      metadata_loader=mock_loader,
    )

    assert result.success is False
    assert "No XBRL files found" in result.error


@pytest.mark.unit
class TestFormTypeFiltering:
  """Tests for form type filtering."""

  def _make_zip_bytes(self, filenames: list[str]) -> bytes:
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
      for name in filenames:
        zf.writestr(name, f"<content of {name}>")
    buf.seek(0)
    return buf.read()

  @patch("robosystems.adapters.sec.processors.processing.XBRLGraphProcessor")
  def test_form_type_not_in_allowed_list(self, mock_processor_cls):
    """Filing with non-allowed form type is skipped (not an error)."""
    zip_data = self._make_zip_bytes(["filing.htm"])

    mock_s3 = MagicMock()
    mock_s3.download_fileobj.side_effect = lambda bucket, key, buf: buf.write(zip_data)

    mock_loader = MagicMock()
    mock_loader.get_metadata.return_value = (
      {"cik": "1045810"},
      {"form": "8-K", "filingDate": "2024-03-01", "primaryDocument": "filing.htm"},
    )

    result = process_single_filing_to_memory(
      storage_key="sec/raw/file.zip",
      partition_key="2024_1045810_0001045810-24-000001",
      source_file_id="sf1",
      s3_client=mock_s3,
      raw_bucket="bucket",
      metadata_loader=mock_loader,
      allowed_form_types=["10-K", "10-Q"],
    )

    assert result.success is True
    assert result.tables == {}
    assert result.skipped_reason == "form_type=8-K"
    # XBRLGraphProcessor should NOT be instantiated
    mock_processor_cls.assert_not_called()

  @patch("robosystems.adapters.sec.processors.processing.XBRLGraphProcessor")
  def test_form_type_in_allowed_list(self, mock_processor_cls):
    """Filing with allowed form type proceeds to processing."""
    mock_processor_instance = MagicMock()
    mock_processor_cls.return_value = mock_processor_instance

    zip_data = self._make_zip_bytes(["filing.htm"])

    mock_s3 = MagicMock()
    mock_s3.download_fileobj.side_effect = lambda bucket, key, buf: buf.write(zip_data)

    mock_loader = MagicMock()
    mock_loader.get_metadata.return_value = (
      {"cik": "1045810"},
      {"form": "10-K", "filingDate": "2024-03-01", "primaryDocument": "filing.htm"},
    )

    process_single_filing_to_memory(
      storage_key="sec/raw/file.zip",
      partition_key="2024_1045810_0001045810-24-000001",
      source_file_id="sf1",
      s3_client=mock_s3,
      raw_bucket="bucket",
      metadata_loader=mock_loader,
      allowed_form_types=["10-K", "10-Q"],
    )

    # XBRLGraphProcessor should be instantiated (form type passed filter)
    mock_processor_cls.assert_called_once()

  @patch("robosystems.adapters.sec.processors.processing.XBRLGraphProcessor")
  def test_no_form_type_filter(self, mock_processor_cls):
    """When allowed_form_types is None, all form types are processed."""
    mock_processor_instance = MagicMock()
    mock_processor_cls.return_value = mock_processor_instance

    zip_data = self._make_zip_bytes(["filing.htm"])

    mock_s3 = MagicMock()
    mock_s3.download_fileobj.side_effect = lambda bucket, key, buf: buf.write(zip_data)

    mock_loader = MagicMock()
    mock_loader.get_metadata.return_value = (
      {"cik": "1045810"},
      {"form": "8-K", "filingDate": "2024-03-01", "primaryDocument": "filing.htm"},
    )

    process_single_filing_to_memory(
      storage_key="sec/raw/file.zip",
      partition_key="2024_1045810_0001045810-24-000001",
      source_file_id="sf1",
      s3_client=mock_s3,
      raw_bucket="bucket",
      metadata_loader=mock_loader,
      allowed_form_types=None,
    )

    # 8-K should still be processed when no filter
    mock_processor_cls.assert_called_once()


@pytest.mark.unit
class TestZipHandling:
  """Tests for ZIP extraction error handling."""

  def test_invalid_zip_returns_error(self):
    """Returns error for invalid ZIP data."""
    mock_s3 = MagicMock()
    mock_s3.download_fileobj.side_effect = lambda bucket, key, buf: buf.write(
      b"not a zip file"
    )
    mock_loader = MagicMock()

    result = process_single_filing_to_memory(
      storage_key="sec/raw/file.zip",
      partition_key="2024_1045810_0001045810-24-000001",
      source_file_id="sf1",
      s3_client=mock_s3,
      raw_bucket="bucket",
      metadata_loader=mock_loader,
    )

    assert result.success is False
    assert result.error is not None


@pytest.mark.unit
class TestProcessorOrchestration:
  """Tests for the full processing orchestration."""

  def _make_zip_bytes(self, filenames: list[str]) -> bytes:
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
      for name in filenames:
        zf.writestr(name, f"<content of {name}>")
    buf.seek(0)
    return buf.read()

  @patch("robosystems.adapters.sec.processors.processing.XBRLGraphProcessor")
  def test_processor_exception_returns_error(self, mock_processor_cls):
    """Returns error result when XBRLGraphProcessor.process() raises."""
    mock_processor_instance = MagicMock()
    mock_processor_instance.process.side_effect = RuntimeError("Processing failed")
    mock_processor_cls.return_value = mock_processor_instance

    zip_data = self._make_zip_bytes(["filing.htm"])

    mock_s3 = MagicMock()
    mock_s3.download_fileobj.side_effect = lambda bucket, key, buf: buf.write(zip_data)

    mock_loader = MagicMock()
    mock_loader.get_metadata.return_value = (
      {"cik": "1045810"},
      {"form": "10-K", "filingDate": "2024-03-01", "primaryDocument": "filing.htm"},
    )

    result = process_single_filing_to_memory(
      storage_key="sec/raw/file.zip",
      partition_key="2024_1045810_0001045810-24-000001",
      source_file_id="sf1",
      s3_client=mock_s3,
      raw_bucket="bucket",
      metadata_loader=mock_loader,
    )

    assert result.success is False
    assert "Processing failed" in result.error

  @patch("robosystems.adapters.sec.processors.processing.XBRLGraphProcessor")
  def test_successful_processing_collects_parquet(self, mock_processor_cls):
    """Successful processing collects parquet files from output directory."""
    zip_data = self._make_zip_bytes(["filing.htm"])

    mock_s3 = MagicMock()
    mock_s3.download_fileobj.side_effect = lambda bucket, key, buf: buf.write(zip_data)

    mock_loader = MagicMock()
    mock_loader.get_metadata.return_value = (
      {"cik": "1045810"},
      {"form": "10-K", "filingDate": "2024-03-01", "primaryDocument": "filing.htm"},
    )

    def fake_process():
      """Simulate XBRLGraphProcessor.process() by writing parquet files."""
      # Access the output_dir from the constructor call
      call_kwargs = mock_processor_cls.call_args.kwargs
      output_dir = call_kwargs["output_dir"]

      # Create mock parquet output
      nodes_dir = os.path.join(output_dir, "nodes")
      os.makedirs(nodes_dir, exist_ok=True)
      with open(os.path.join(nodes_dir, "Entity.parquet"), "wb") as f:
        f.write(b"entity parquet data")

      rels_dir = os.path.join(output_dir, "relationships")
      os.makedirs(rels_dir, exist_ok=True)
      with open(os.path.join(rels_dir, "ENTITY_HAS_REPORT.parquet"), "wb") as f:
        f.write(b"rel parquet data")

    mock_processor_instance = MagicMock()
    mock_processor_instance.process.side_effect = fake_process
    mock_processor_cls.return_value = mock_processor_instance

    result = process_single_filing_to_memory(
      storage_key="sec/raw/file.zip",
      partition_key="2024_1045810_0001045810-24-000001",
      source_file_id="sf1",
      s3_client=mock_s3,
      raw_bucket="bucket",
      metadata_loader=mock_loader,
    )

    assert result.success is True
    assert result.filing_date == "2024-03-01"
    assert "nodes/Entity" in result.tables
    assert "relationships/ENTITY_HAS_REPORT" in result.tables
    assert result.tables["nodes/Entity"] == b"entity parquet data"
