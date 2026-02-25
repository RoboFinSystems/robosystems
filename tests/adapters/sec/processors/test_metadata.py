"""Tests for SEC metadata loader.

Tests the SECMetadataLoader class for loading SEC filer and report
metadata from S3 snapshots with fallback to SEC API.
"""

import json
from unittest.mock import MagicMock, patch

import pytest


@pytest.mark.unit
class TestSECMetadataLoaderInit:
  """Tests for SECMetadataLoader initialization."""

  def test_init_creates_empty_cache(self):
    """Loader starts with empty cache."""
    from robosystems.adapters.sec.processors.metadata import SECMetadataLoader

    loader = SECMetadataLoader()
    assert loader._cache == {}

  def test_clear_cache_empties_populated_cache(self):
    """clear_cache removes all cached entries."""
    from robosystems.adapters.sec.processors.metadata import SECMetadataLoader

    loader = SECMetadataLoader()
    loader._cache["12345"] = {"name": "Test Corp"}
    loader.clear_cache()
    assert loader._cache == {}


@pytest.mark.unit
class TestLoadSubmissionsFromS3:
  """Tests for _load_submissions_from_s3."""

  def test_load_from_s3_success(self):
    """Successfully loads submissions from S3."""
    from robosystems.adapters.sec.processors.metadata import SECMetadataLoader

    loader = SECMetadataLoader()
    mock_s3 = MagicMock()
    submissions_data = {"name": "NVIDIA CORP", "cik": "1045810"}
    mock_s3.get_object.return_value = {
      "Body": MagicMock(
        read=MagicMock(return_value=json.dumps(submissions_data).encode("utf-8"))
      )
    }

    result = loader._load_submissions_from_s3(mock_s3, "test-bucket", "1045810")
    assert result == submissions_data

  def test_load_from_s3_no_such_key(self):
    """Returns None when S3 key doesn't exist."""
    from robosystems.adapters.sec.processors.metadata import SECMetadataLoader

    loader = SECMetadataLoader()
    mock_s3 = MagicMock()
    mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
    mock_s3.get_object.side_effect = mock_s3.exceptions.NoSuchKey("Not found")

    result = loader._load_submissions_from_s3(mock_s3, "test-bucket", "1045810")
    assert result is None

  def test_load_from_s3_generic_error(self):
    """Returns None on generic S3 error."""
    from robosystems.adapters.sec.processors.metadata import SECMetadataLoader

    loader = SECMetadataLoader()
    mock_s3 = MagicMock()
    mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
    mock_s3.get_object.side_effect = RuntimeError("Connection error")

    result = loader._load_submissions_from_s3(mock_s3, "test-bucket", "1045810")
    assert result is None


@pytest.mark.unit
class TestGetMetadataCacheHitMiss:
  """Tests for get_metadata cache behavior."""

  def test_cache_hit_skips_s3_and_api(self):
    """When CIK is cached, no S3 or API calls are made."""
    from robosystems.adapters.sec.processors.metadata import SECMetadataLoader

    loader = SECMetadataLoader()
    cached_submissions = {
      "name": "NVIDIA CORP",
      "tickers": ["NVDA"],
      "exchanges": ["Nasdaq"],
      "sic": "3674",
      "sicDescription": "Semiconductors",
      "stateOfIncorporation": "DE",
      "fiscalYearEnd": "0128",
      "ein": "943177549",
      "entityType": "operating",
      "category": "Large accelerated filer",
      "filings": {
        "accessionNumber": ["0001045810-24-000001"],
        "form": ["10-K"],
        "filingDate": ["2024-02-21"],
        "reportDate": ["2024-01-28"],
        "acceptanceDateTime": ["2024-02-21T16:06:00"],
        "primaryDocument": ["nvda-20240128.htm"],
        "periodOfReport": ["2024-01-28"],
        "isXBRL": [1],
        "isInlineXBRL": [1],
      },
    }
    loader._cache["1045810"] = cached_submissions

    mock_s3 = MagicMock()
    sec_filer, sec_report = loader.get_metadata(
      "1045810", "0001045810-24-000001", s3_client=mock_s3, bucket="bucket"
    )

    # S3 should never be called since cache hit
    mock_s3.get_object.assert_not_called()
    assert sec_filer["name"] == "NVIDIA CORP"
    assert sec_filer["ticker"] == "NVDA"

  def test_s3_snapshot_loaded_and_cached(self):
    """When not cached but S3 has snapshot, load from S3 and cache."""
    from robosystems.adapters.sec.processors.metadata import SECMetadataLoader

    loader = SECMetadataLoader()
    submissions_data = {
      "name": "Apple Inc",
      "tickers": ["AAPL"],
      "exchanges": ["Nasdaq"],
      "sic": "3571",
      "filings": {
        "accessionNumber": ["0000320193-24-000001"],
        "form": ["10-K"],
        "filingDate": ["2024-11-01"],
        "reportDate": ["2024-09-28"],
        "acceptanceDateTime": ["2024-11-01T06:01:00"],
        "primaryDocument": ["aapl-20240928.htm"],
        "periodOfReport": ["2024-09-28"],
        "isXBRL": [1],
        "isInlineXBRL": [1],
      },
    }

    mock_s3 = MagicMock()
    mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
    mock_s3.get_object.return_value = {
      "Body": MagicMock(
        read=MagicMock(return_value=json.dumps(submissions_data).encode("utf-8"))
      )
    }

    sec_filer, sec_report = loader.get_metadata(
      "320193", "0000320193-24-000001", s3_client=mock_s3, bucket="bucket"
    )

    assert sec_filer["name"] == "Apple Inc"
    assert sec_report["form"] == "10-K"
    assert "320193" in loader._cache

  @patch("robosystems.adapters.sec.SECClient")
  def test_api_fallback_when_no_s3(self, mock_sec_client_cls):
    """Falls back to SEC API when S3 snapshot doesn't exist."""
    from robosystems.adapters.sec.processors.metadata import SECMetadataLoader

    loader = SECMetadataLoader()
    mock_s3 = MagicMock()
    mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
    mock_s3.get_object.side_effect = mock_s3.exceptions.NoSuchKey("Not found")

    mock_client_instance = MagicMock()
    mock_client_instance.get_submissions.return_value = {
      "name": "Test Corp",
      "tickers": [],
      "filings": {"recent": {}},
    }
    mock_sec_client_cls.return_value = mock_client_instance

    sec_filer, sec_report = loader.get_metadata(
      "999999", "0000999999-24-000001", s3_client=mock_s3, bucket="bucket"
    )

    mock_sec_client_cls.assert_called_once_with(cik="999999")
    mock_client_instance.get_submissions.assert_called_once()
    assert sec_filer["name"] == "Test Corp"
    assert "999999" in loader._cache

  @patch("robosystems.adapters.sec.SECClient")
  def test_api_fallback_when_no_s3_client(self, mock_sec_client_cls):
    """Falls back to SEC API when no S3 client is provided."""
    from robosystems.adapters.sec.processors.metadata import SECMetadataLoader

    loader = SECMetadataLoader()
    mock_client_instance = MagicMock()
    mock_client_instance.get_submissions.return_value = {
      "name": "No S3 Corp",
      "tickers": [],
      "filings": {},
    }
    mock_sec_client_cls.return_value = mock_client_instance

    sec_filer, sec_report = loader.get_metadata("888888", "0000888888-24-000001")

    mock_sec_client_cls.assert_called_once_with(cik="888888")
    assert sec_filer["name"] == "No S3 Corp"


@pytest.mark.unit
class TestGetMetadataFilingLookup:
  """Tests for filing-specific metadata extraction from submissions."""

  def test_accession_found_in_new_format(self):
    """Extracts filing metadata from new format (filings directly)."""
    from robosystems.adapters.sec.processors.metadata import SECMetadataLoader

    loader = SECMetadataLoader()
    loader._cache["12345"] = {
      "name": "Test Corp",
      "tickers": ["TEST"],
      "exchanges": ["NYSE"],
      "sic": "1234",
      "filings": {
        "accessionNumber": ["0000012345-24-000001", "0000012345-24-000002"],
        "form": ["10-K", "10-Q"],
        "filingDate": ["2024-03-01", "2024-08-01"],
        "reportDate": ["2024-01-31", "2024-06-30"],
        "acceptanceDateTime": ["2024-03-01T10:00:00", "2024-08-01T09:00:00"],
        "primaryDocument": ["test-20240131.htm", "test-20240630.htm"],
        "periodOfReport": ["2024-01-31", "2024-06-30"],
        "isXBRL": [1, 1],
        "isInlineXBRL": [1, 1],
      },
    }

    sec_filer, sec_report = loader.get_metadata("12345", "0000012345-24-000002")

    assert sec_report["form"] == "10-Q"
    assert sec_report["filingDate"] == "2024-08-01"
    assert sec_report["reportDate"] == "2024-06-30"
    assert sec_report["isInlineXBRL"] is True

  def test_accession_found_in_legacy_format(self):
    """Extracts filing metadata from legacy format (filings.recent)."""
    from robosystems.adapters.sec.processors.metadata import SECMetadataLoader

    loader = SECMetadataLoader()
    loader._cache["12345"] = {
      "name": "Legacy Corp",
      "tickers": [],
      "filings": {
        "recent": {
          "accessionNumber": ["0000012345-24-000001"],
          "form": ["10-K"],
          "filingDate": ["2024-03-01"],
          "reportDate": ["2024-01-31"],
          "acceptanceDateTime": ["2024-03-01T10:00:00"],
          "primaryDocument": ["legacy.htm"],
          "periodOfReport": ["2024-01-31"],
          "isXBRL": [1],
          "isInlineXBRL": [0],
        }
      },
    }

    sec_filer, sec_report = loader.get_metadata("12345", "0000012345-24-000001")

    assert sec_report["form"] == "10-K"
    assert sec_report["isInlineXBRL"] is False

  def test_accession_not_found(self):
    """Returns minimal report when accession not found in filings."""
    from robosystems.adapters.sec.processors.metadata import SECMetadataLoader

    loader = SECMetadataLoader()
    loader._cache["12345"] = {
      "name": "Test Corp",
      "tickers": [],
      "filings": {
        "accessionNumber": ["0000012345-24-000099"],
        "form": ["10-K"],
      },
    }

    sec_filer, sec_report = loader.get_metadata("12345", "0000012345-24-000001")

    assert sec_report["accessionNumber"] == "0000012345-24-000001"
    assert "form" not in sec_report or sec_report.get("form") is None

  def test_empty_submissions_filings(self):
    """Handles submissions with empty filings."""
    from robosystems.adapters.sec.processors.metadata import SECMetadataLoader

    loader = SECMetadataLoader()
    loader._cache["12345"] = {
      "name": "Empty Corp",
      "tickers": [],
      "filings": {},
    }

    sec_filer, sec_report = loader.get_metadata("12345", "0000012345-24-000001")

    assert sec_filer["name"] == "Empty Corp"
    assert sec_report["accessionNumber"] == "0000012345-24-000001"


@pytest.mark.unit
class TestGetMetadataFilerFields:
  """Tests for sec_filer field extraction."""

  def test_all_filer_fields_populated(self):
    """All filer fields are correctly extracted from submissions."""
    from robosystems.adapters.sec.processors.metadata import SECMetadataLoader

    loader = SECMetadataLoader()
    loader._cache["12345"] = {
      "name": "Full Corp",
      "tickers": ["FULL", "FULL.A"],
      "exchanges": ["NYSE", "Nasdaq"],
      "sic": "3674",
      "sicDescription": "Semiconductors",
      "stateOfIncorporation": "DE",
      "fiscalYearEnd": "1231",
      "ein": "123456789",
      "entityType": "operating",
      "category": "Large accelerated filer",
      "website": "https://fullcorp.com",
      "phone": "555-1234",
      "filings": {},
    }

    sec_filer, _ = loader.get_metadata("12345", "0000012345-24-000001")

    assert sec_filer["cik"] == "12345"
    assert sec_filer["name"] == "Full Corp"
    assert sec_filer["entity_name"] == "Full Corp"
    assert sec_filer["ticker"] == "FULL"  # First ticker
    assert sec_filer["exchange"] == "NYSE"  # First exchange
    assert sec_filer["sic"] == "3674"
    assert sec_filer["sicDescription"] == "Semiconductors"
    assert sec_filer["stateOfIncorporation"] == "DE"
    assert sec_filer["fiscalYearEnd"] == "1231"
    assert sec_filer["ein"] == "123456789"
    assert sec_filer["entityType"] == "operating"
    assert sec_filer["category"] == "Large accelerated filer"
    assert sec_filer["website"] == "https://fullcorp.com"
    assert sec_filer["phone"] == "555-1234"

  def test_missing_optional_fields(self):
    """Missing optional fields default to None."""
    from robosystems.adapters.sec.processors.metadata import SECMetadataLoader

    loader = SECMetadataLoader()
    loader._cache["12345"] = {
      "name": "Minimal Corp",
      "filings": {},
    }

    sec_filer, _ = loader.get_metadata("12345", "0000012345-24-000001")

    assert sec_filer["name"] == "Minimal Corp"
    assert sec_filer["ticker"] is None
    assert sec_filer["exchange"] is None
    assert sec_filer["sic"] is None
    assert sec_filer["website"] is None

  def test_investor_website_fallback(self):
    """Uses investorWebsite when website is not provided."""
    from robosystems.adapters.sec.processors.metadata import SECMetadataLoader

    loader = SECMetadataLoader()
    loader._cache["12345"] = {
      "name": "Investor Corp",
      "investorWebsite": "https://ir.investorcorp.com",
      "filings": {},
    }

    sec_filer, _ = loader.get_metadata("12345", "0000012345-24-000001")

    assert sec_filer["website"] == "https://ir.investorcorp.com"
