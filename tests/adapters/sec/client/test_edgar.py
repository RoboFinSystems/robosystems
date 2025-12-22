"""Tests for SEC API client adapter."""

import os
from unittest.mock import Mock, patch
from zipfile import ZipFile

import numpy as np
import pandas as pd
import pytest
import requests
from requests.exceptions import HTTPError

# Set test environment variables before importing to ensure fast retries
os.environ["TESTING"] = "true"

from robosystems.adapters.sec import SECClient, enable_test_mode
from robosystems.adapters.sec.client.edgar import SEC_REQUEST_TIMEOUT

# Enable fast retries for all tests in this module
enable_test_mode()


class TestSECClient:
  """Test cases for SEC client functionality."""

  @pytest.fixture
  def client(self):
    """Create SEC client instance."""
    return SECClient()

  @pytest.fixture
  def client_with_cik(self):
    """Create SEC client with CIK."""
    return SECClient(cik="0000320193")  # Apple Inc CIK

  @patch("robosystems.adapters.sec.client.edgar.requests.get")
  def test_get_companies_success(self, mock_get, client):
    """Test successful companies data retrieval."""
    # Setup mock response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = b'{"cik":"12345","name":"Test Company"}'
    mock_response.json.return_value = {
      "0": {"cik_str": "0000320193", "ticker": "AAPL", "title": "Apple Inc."},
      "1": {
        "cik_str": "0000789019",
        "ticker": "MSFT",
        "title": "Microsoft Corporation",
      },
    }
    mock_get.return_value = mock_response

    # Execute
    result = client.get_companies()

    # Verify
    expected_url = "https://www.sec.gov/files/company_tickers.json"
    mock_get.assert_called_once_with(
      expected_url, headers=client._headers, timeout=SEC_REQUEST_TIMEOUT
    )
    assert result == mock_response.json.return_value

  @patch("robosystems.adapters.sec.client.edgar.requests.get")
  def test_get_companies_empty_response(self, mock_get, client):
    """Test handling of empty response (rate limiting)."""
    # Setup mock empty response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = b""
    mock_get.return_value = mock_response

    # Execute and expect HTTPError
    with pytest.raises(HTTPError, match="SEC returned empty response - rate limited"):
      client.get_companies()

  @patch("robosystems.adapters.sec.client.edgar.requests.get")
  def test_get_companies_invalid_json(self, mock_get, client):
    """Test handling of invalid JSON response."""
    # Setup mock invalid JSON response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = b"Invalid JSON content"
    mock_response.json.side_effect = ValueError("Invalid JSON")
    mock_get.return_value = mock_response

    # Execute and expect HTTPError
    with pytest.raises(HTTPError, match="SEC returned invalid JSON - rate limited"):
      client.get_companies()

  @patch("robosystems.adapters.sec.client.edgar.requests.get")
  def test_get_companies_network_error(self, mock_get, client):
    """Test handling of network errors."""
    # Setup mock to raise network error
    mock_get.side_effect = requests.RequestException("Network connection failed")

    # Execute and expect exception
    with pytest.raises(requests.RequestException):
      client.get_companies()

  @patch("robosystems.adapters.sec.client.edgar.SECClient.get_companies")
  def test_get_companies_df(self, mock_get_companies, client):
    """Test companies DataFrame conversion."""
    # Setup mock companies data
    mock_companies = {
      "0": {"cik_str": "0000320193", "ticker": "AAPL", "title": "Apple Inc."},
      "1": {
        "cik_str": "0000789019",
        "ticker": "MSFT",
        "title": "Microsoft Corporation",
      },
    }
    mock_get_companies.return_value = mock_companies

    # Execute
    result = client.get_companies_df()

    # Verify
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert result.iloc[0]["ticker"] == "AAPL"
    assert result.iloc[1]["ticker"] == "MSFT"
    mock_get_companies.assert_called_once()

  @patch("robosystems.adapters.sec.client.edgar.requests.get")
  def test_get_submissions_success(self, mock_get, client_with_cik):
    """Test successful submissions retrieval."""
    # Setup mock response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = b'{"filings": {"recent": []}}'
    mock_response.json.return_value = {
      "cik": "0000320193",
      "filings": {
        "recent": [
          {"accessionNumber": "0000320193-23-000001", "form": "10-K"},
          {"accessionNumber": "0000320193-23-000002", "form": "10-Q"},
        ]
      },
    }
    mock_get.return_value = mock_response

    # Execute
    result = client_with_cik.get_submissions()

    # Verify
    expected_url = "https://data.sec.gov/submissions/CIK0000320193.json"
    mock_get.assert_called_once_with(
      expected_url, headers=client_with_cik._headers, timeout=SEC_REQUEST_TIMEOUT
    )
    assert result == mock_response.json.return_value

  def test_get_submissions_no_cik(self, client):
    """Test get_submissions without CIK raises ValueError."""
    with pytest.raises(ValueError, match="CIK is required for get_submissions"):
      client.get_submissions()

  @patch("robosystems.adapters.sec.client.edgar.requests.get")
  def test_get_submissions_empty_response(self, mock_get, client_with_cik):
    """Test handling of empty response in get_submissions."""
    # Setup mock empty response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = b""
    mock_get.return_value = mock_response

    # Execute and expect HTTPError
    with pytest.raises(HTTPError, match="SEC returned empty response - rate limited"):
      client_with_cik.get_submissions()

  @patch("robosystems.adapters.sec.client.edgar.SECClient.get_submissions")
  def test_submissions_df_basic(self, mock_get_submissions, client_with_cik):
    """Test basic submissions DataFrame conversion."""
    # Setup mock submissions data
    mock_submissions = {
      "filings": {
        "recent": [
          {
            "accessionNumber": "0000320193-23-000001",
            "form": "10-K",
            "isXBRL": 1,
            "isInlineXBRL": 0,
          },
          {
            "accessionNumber": "0000320193-23-000002",
            "form": "10-Q",
            "isXBRL": 1,
            "isInlineXBRL": 0,
          },
        ]
      }
    }
    mock_get_submissions.return_value = mock_submissions

    # Execute
    result = client_with_cik.submissions_df()

    # Verify
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert result.iloc[0]["form"] == "10-K"
    assert result.iloc[1]["form"] == "10-Q"
    # Verify boolean columns are converted (numpy bool is expected in pandas)
    assert isinstance(result.iloc[0]["isXBRL"], (bool, np.bool_))
    assert isinstance(result.iloc[0]["isInlineXBRL"], (bool, np.bool_))
    # Also verify the dtype of the columns
    assert result["isXBRL"].dtype == bool
    assert result["isInlineXBRL"].dtype == bool

  @patch("robosystems.adapters.sec.client.edgar.SECClient.get_submissions")
  def test_submissions_df_with_files(self, mock_get_submissions, client_with_cik):
    """Test submissions DataFrame with additional files."""
    # Setup mock with additional files
    mock_submissions_1 = {
      "filings": {
        "recent": [
          {
            "accessionNumber": "0000320193-23-000001",
            "form": "10-K",
            "isXBRL": 1,
            "isInlineXBRL": 0,
          }
        ],
        "files": [{"name": "CIK0000320193-submissions-001.json"}],
      }
    }
    mock_submissions_2 = {
      "filings": {
        "recent": [
          {
            "accessionNumber": "0000320193-22-000001",
            "form": "10-K",
            "isXBRL": 1,
            "isInlineXBRL": 0,
          }
        ]
      }
    }

    mock_get_submissions.side_effect = [mock_submissions_1, mock_submissions_2]

    # Execute
    result = client_with_cik.submissions_df()

    # Verify
    assert len(result) == 2  # Should combine both files
    assert mock_get_submissions.call_count == 2

  def test_get_report_url_inline_xbrl(self, client_with_cik):
    """Test report URL generation for inline XBRL reports."""
    sec_report = {
      "accessionNumber": "0000320193-23-000001",
      "primaryDocument": "d1234567.htm",
      "isInlineXBRL": True,
    }

    # Execute
    result = client_with_cik.get_report_url(sec_report)

    # Verify
    expected_url = (
      "https://www.sec.gov/Archives/edgar/data/320193/000032019323000001/d1234567.htm"
    )
    assert result == expected_url

  def test_get_report_url_no_cik(self, client):
    """Test get_report_url without CIK returns None."""
    sec_report = {"accessionNumber": "0000320193-23-000001", "isInlineXBRL": True}

    result = client.get_report_url(sec_report)
    assert result is None

  @patch("robosystems.adapters.sec.client.edgar.SECClient.download_xbrlzip")
  @patch("robosystems.adapters.sec.client.edgar.SECClient.get_xbrlzip_url")
  def test_get_report_url_non_inline_xbrl(
    self, mock_get_zip_url, mock_download_zip, client_with_cik
  ):
    """Test report URL generation for non-inline XBRL reports."""
    # Setup mocks
    mock_zip_url = "https://www.sec.gov/test.zip"
    mock_get_zip_url.return_value = mock_zip_url

    mock_zip = Mock(spec=ZipFile)
    mock_zip.namelist.return_value = ["test.xsd", "test.xml"]
    mock_download_zip.return_value = mock_zip

    sec_report = {
      "accessionNumber": "0000320193-23-000001",
      "primaryDocument": "test.xml",
      "isInlineXBRL": False,
    }

    # Execute
    result = client_with_cik.get_report_url(sec_report)

    # Verify
    expected_url = (
      "https://www.sec.gov/Archives/edgar/data/320193/000032019323000001/test.xml"
    )
    assert result == expected_url
    mock_get_zip_url.assert_called_once_with(sec_report)
    mock_download_zip.assert_called_once_with(mock_zip_url)

  @patch("robosystems.adapters.sec.client.edgar.SECClient.download_xbrlzip")
  @patch("robosystems.adapters.sec.client.edgar.SECClient.get_xbrlzip_url")
  def test_get_report_url_non_inline_no_zip(
    self, mock_get_zip_url, mock_download_zip, client_with_cik
  ):
    """Test report URL generation when ZIP download fails."""
    # Setup mocks
    mock_zip_url = "https://www.sec.gov/test.zip"
    mock_get_zip_url.return_value = mock_zip_url
    mock_download_zip.return_value = None  # ZIP download fails

    sec_report = {
      "accessionNumber": "0000320193-23-000001",
      "primaryDocument": "test.xml",
      "isInlineXBRL": False,
    }

    # Execute - should return None when ZIP download fails
    result = client_with_cik.get_report_url(sec_report)
    assert result is None

  def test_get_xbrlzip_url(self, client_with_cik):
    """Test XBRL ZIP URL generation."""
    filing = {"accessionNumber": "0000320193-23-000001"}

    # Execute
    result = client_with_cik.get_xbrlzip_url(filing)

    # Verify
    expected_url = "https://www.sec.gov/Archives/edgar/data/320193/000032019323000001/0000320193-23-000001-xbrl.zip"
    assert result == expected_url

  def test_get_xbrlzip_url_no_cik(self, client):
    """Test get_xbrlzip_url without CIK raises ValueError."""
    filing = {"accessionNumber": "0000320193-23-000001"}

    with pytest.raises(ValueError, match="CIK is required for get_xbrlzip_url"):
      client.get_xbrlzip_url(filing)

  @patch("robosystems.adapters.sec.client.edgar.requests.get")
  def test_download_xbrlzip_success(self, mock_get, client):
    """Test successful XBRL ZIP download."""
    # Create a proper ZIP file content in memory
    from io import BytesIO

    zip_buffer = BytesIO()
    with ZipFile(zip_buffer, "w") as zip_file:
      zip_file.writestr("test.xml", "<xml>test content</xml>")
    zip_content = zip_buffer.getvalue()

    # Setup mock response with valid ZIP content
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = zip_content
    mock_get.return_value = mock_response

    zip_url = "https://www.sec.gov/test.zip"

    # Execute
    result = client.download_xbrlzip(zip_url)

    # Verify
    assert isinstance(result, ZipFile)
    mock_get.assert_called_once_with(
      zip_url, headers=client._headers, timeout=SEC_REQUEST_TIMEOUT
    )

  @patch("robosystems.adapters.sec.client.edgar.requests.get")
  def test_download_xbrlzip_empty_response(self, mock_get, client):
    """Test handling of empty ZIP response."""
    # Setup mock empty response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = b""
    mock_get.return_value = mock_response

    zip_url = "https://www.sec.gov/test.zip"

    # Execute and expect HTTPError
    with pytest.raises(HTTPError, match="SEC returned empty file - rate limited"):
      client.download_xbrlzip(zip_url)

  @patch("robosystems.adapters.sec.client.edgar.requests.get")
  def test_download_xbrlzip_bad_zip(self, mock_get, client):
    """Test handling of corrupted ZIP files."""
    # Setup mock response with invalid ZIP content
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = b"Invalid ZIP content"
    mock_get.return_value = mock_response

    zip_url = "https://www.sec.gov/test.zip"

    # Execute - should return None for bad ZIP
    result = client.download_xbrlzip(zip_url)
    assert result is None

  @patch("robosystems.adapters.sec.client.edgar.requests.get")
  def test_get_largest_xml_file_success(self, mock_get, client):
    """Test successful largest XML file detection."""
    # Mock HTML response with file table
    html_content = """
        <html><body>
        <table>
        <tr><th>Filename</th><th>Size</th></tr>
        <tr><td><a href="/test1.xml">test1.xml</a></td><td>1000</td></tr>
        <tr><td><a href="/test2.xml">test2.xml</a></td><td>2000</td></tr>
        <tr><td><a href="/other.txt">other.txt</a></td><td>500</td></tr>
        </table>
        </body></html>
        """

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = html_content
    mock_get.return_value = mock_response

    filing_url = "https://www.sec.gov/test-filing/"

    # Execute
    result = client.get_largest_xml_file(filing_url)

    # Verify - should return the largest XML file (test2.xml)
    expected_url = "https://www.sec.gov/test2.xml"
    assert result == expected_url

  @patch("robosystems.adapters.sec.client.edgar.requests.get")
  def test_get_largest_xml_file_no_table(self, mock_get, client):
    """Test handling when no file table is found."""
    # Mock HTML response without table
    html_content = "<html><body><p>No table here</p></body></html>"

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = html_content
    mock_get.return_value = mock_response

    filing_url = "https://www.sec.gov/test-filing/"

    # Execute - should return None when no table found
    result = client.get_largest_xml_file(filing_url)
    assert result is None

  @patch("robosystems.adapters.sec.client.edgar.requests.get")
  def test_get_largest_xml_file_no_xml_files(self, mock_get, client):
    """Test handling when no XML files are found."""
    # Mock HTML response with table but no XML files
    html_content = """
        <html><body>
        <table>
        <tr><th>Filename</th><th>Size</th></tr>
        <tr><td><a href="/test1.txt">test1.txt</a></td><td>1000</td></tr>
        <tr><td><a href="/test2.pdf">test2.pdf</a></td><td>2000</td></tr>
        </table>
        </body></html>
        """

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = html_content
    mock_get.return_value = mock_response

    filing_url = "https://www.sec.gov/test-filing/"

    # Execute - should return None when no XML files found
    result = client.get_largest_xml_file(filing_url)
    assert result is None

  @patch("robosystems.adapters.sec.client.edgar.requests.get")
  def test_get_largest_xml_file_request_error(self, mock_get, client):
    """Test handling of request errors in get_largest_xml_file."""
    # Setup mock to raise network error
    mock_get.side_effect = requests.RequestException("Network error")

    filing_url = "https://www.sec.gov/test-filing/"

    # Execute and expect exception
    with pytest.raises(requests.RequestException):
      client.get_largest_xml_file(filing_url)

  def test_initialization(self):
    """Test SEC client initialization."""
    # Test without CIK
    client1 = SECClient()
    assert client1.cik is None
    assert client1._headers is not None

    # Test with CIK
    client2 = SECClient(cik="1234567890")
    assert client2.cik == "1234567890"

    # Test with integer CIK (should convert to string)
    client3 = SECClient(cik=1234567890)
    assert client3.cik == "1234567890"
