"""Tests for OpenFIGI API client adapter."""

import os
import pytest
from unittest.mock import Mock, patch
from requests.exceptions import RequestException

# Set test environment variables before importing to ensure fast retries
os.environ["OPENFIGI_RETRY_MIN_WAIT"] = "1"
os.environ["OPENFIGI_RETRY_MAX_WAIT"] = "5"
os.environ["TESTING"] = "true"

from robosystems.adapters.openfigi import OpenFIGIClient


class TestOpenFIGIClient:
  """Test cases for OpenFIGI client functionality."""

  @pytest.fixture
  def client(self):
    """Create OpenFIGI client instance."""
    return OpenFIGIClient()

  @patch("robosystems.adapters.openfigi.env")
  @patch("robosystems.adapters.openfigi.requests.post")
  def test_get_figi_success(self, mock_post, mock_env, client):
    """Test successful FIGI retrieval."""
    # Setup mock env to have no API key
    mock_env.OPENFIGI_API_KEY = None
    mock_env.OPENFIGI_RETRY_MIN_WAIT = 1
    mock_env.OPENFIGI_RETRY_MAX_WAIT = 10

    # Setup mock response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
      "data": [
        {
          "figi": "BBG000BLNNH6",
          "name": "Apple Inc",
          "ticker": "AAPL",
          "exchCode": "US",
          "compositeFIGI": "BBG000B9XRY4",
          "uniqueID": "EQ0010169500001000",
          "securityType": "Common Stock",
          "marketSector": "Equity",
          "shareClassFIGI": "BBG001S5N8V8",
          "uniqueIDFutOpt": None,
          "securityType2": "Common Stock",
          "securityDescription": "AAPL",
        }
      ]
    }
    mock_post.return_value = mock_response

    # Test payload
    payload = [{"idType": "ID_ISIN", "idValue": "US0378331005"}]

    # Execute
    result = client.get_figi(payload)

    # Verify
    assert result == mock_response.json.return_value
    mock_post.assert_called_once_with(
      url="https://api.openfigi.com/v3/mapping",
      headers={"Content-Type": "application/json"},
      json=payload,
    )

  @patch("robosystems.adapters.openfigi.env")
  @patch("robosystems.adapters.openfigi.requests.post")
  def test_get_figi_with_api_key(self, mock_post, mock_env, client):
    """Test FIGI retrieval with API key configured."""
    # Setup mock env with API key
    mock_env.OPENFIGI_API_KEY = "test-api-key"
    mock_env.OPENFIGI_RETRY_MIN_WAIT = 1
    mock_env.OPENFIGI_RETRY_MAX_WAIT = 10

    # Setup mock response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": []}
    mock_post.return_value = mock_response

    payload = [{"idType": "TICKER", "idValue": "AAPL"}]

    # Execute
    client.get_figi(payload)

    # Verify API key is included in headers
    call_args = mock_post.call_args
    headers = call_args[1]["headers"]
    assert headers["X-OPENFIGI-APIKEY"] == "test-api-key"

  @patch("robosystems.adapters.openfigi.env")
  @patch("robosystems.adapters.openfigi.requests.post")
  def test_get_figi_rate_limit_error(self, mock_post, mock_env, client):
    """Test handling of rate limit errors."""
    # Setup mock env
    mock_env.OPENFIGI_API_KEY = None
    mock_env.OPENFIGI_RETRY_MIN_WAIT = 1
    mock_env.OPENFIGI_RETRY_MAX_WAIT = 10

    # Setup mock response for rate limit
    mock_response = Mock()
    mock_response.status_code = 429
    mock_response.text = "Rate limit exceeded"
    mock_post.return_value = mock_response

    payload = [{"idType": "TICKER", "idValue": "TEST"}]

    # Execute and expect RequestException with retry message
    with pytest.raises(RequestException, match="OpenFIGI retry required"):
      client.get_figi(payload)

  @patch("robosystems.adapters.openfigi.env")
  @patch("robosystems.adapters.openfigi.requests.post")
  def test_get_figi_server_error(self, mock_post, mock_env, client):
    """Test handling of server errors."""
    # Setup mock env
    mock_env.OPENFIGI_API_KEY = None
    mock_env.OPENFIGI_RETRY_MIN_WAIT = 1
    mock_env.OPENFIGI_RETRY_MAX_WAIT = 10

    # Setup mock response for server error
    mock_response = Mock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"
    mock_post.return_value = mock_response

    payload = [{"idType": "TICKER", "idValue": "TEST"}]

    # Execute and expect RequestException with retry message
    with pytest.raises(RequestException, match="OpenFIGI retry required"):
      client.get_figi(payload)

  @patch("robosystems.adapters.openfigi.env")
  @patch("robosystems.adapters.openfigi.requests.post")
  def test_get_figi_network_error(self, mock_post, mock_env, client):
    """Test handling of network/request errors."""
    # Setup mock env
    mock_env.OPENFIGI_API_KEY = None
    mock_env.OPENFIGI_RETRY_MIN_WAIT = 1
    mock_env.OPENFIGI_RETRY_MAX_WAIT = 10

    # Setup mock to raise network error
    mock_post.side_effect = RequestException("Network connection failed")

    payload = [{"idType": "TICKER", "idValue": "TEST"}]

    # Execute and expect RequestException with retry message
    with pytest.raises(RequestException, match="OpenFIGI retry required"):
      client.get_figi(payload)

  @patch("robosystems.adapters.openfigi.env")
  @patch("robosystems.adapters.openfigi.requests.post")
  def test_get_figi_retry_on_failure(self, mock_post, mock_env, client):
    """Test that retry mechanism works on failures."""
    # Setup mock env
    mock_env.OPENFIGI_API_KEY = None
    mock_env.OPENFIGI_RETRY_MIN_WAIT = 1
    mock_env.OPENFIGI_RETRY_MAX_WAIT = 10

    # Setup mock to fail twice then succeed
    mock_response_fail = Mock()
    mock_response_fail.status_code = 500
    mock_response_fail.text = "Server Error"

    mock_response_success = Mock()
    mock_response_success.status_code = 200
    mock_response_success.json.return_value = {"data": []}

    mock_post.side_effect = [RequestException("Network error"), mock_response_success]

    payload = [{"idType": "TICKER", "idValue": "TEST"}]

    # Execute - should succeed on retry
    result = client.get_figi(payload)

    # Verify it was called twice (initial + 1 retry)
    assert mock_post.call_count == 2
    assert result == {"data": []}

  def test_get_figi_empty_payload(self, client):
    """Test FIGI retrieval with empty payload."""
    # This should work but return empty results
    payload = []

    # Execute - should not raise exception for empty payload
    # (requests.post will handle empty JSON)
    with patch("robosystems.adapters.openfigi.requests.post") as mock_post:
      mock_response = Mock()
      mock_response.status_code = 200
      mock_response.json.return_value = {"data": []}
      mock_post.return_value = mock_response

      result = client.get_figi(payload)

      assert result == {"data": []}
      mock_post.assert_called_once()

  @patch("robosystems.adapters.openfigi.env")
  @patch("robosystems.adapters.openfigi.requests.post")
  def test_get_figi_malformed_response(self, mock_post, mock_env, client):
    """Test handling of malformed JSON response."""
    # Setup mock env
    mock_env.OPENFIGI_API_KEY = None
    mock_env.OPENFIGI_RETRY_MIN_WAIT = 1
    mock_env.OPENFIGI_RETRY_MAX_WAIT = 10

    # Setup mock response with invalid JSON
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.side_effect = ValueError("Invalid JSON")
    mock_post.return_value = mock_response

    payload = [{"idType": "TICKER", "idValue": "TEST"}]

    # Execute - should raise RequestException due to retry logic
    with pytest.raises(RequestException, match="OpenFIGI retry required"):
      client.get_figi(payload)

  @patch("robosystems.adapters.openfigi.env")
  @patch("robosystems.adapters.openfigi.requests.post")
  def test_get_figi_response_structure(self, mock_post, mock_env, client):
    """Test expected response structure from OpenFIGI API."""
    # Setup mock env
    mock_env.OPENFIGI_API_KEY = None
    mock_env.OPENFIGI_RETRY_MIN_WAIT = 1
    mock_env.OPENFIGI_RETRY_MAX_WAIT = 10

    # Setup comprehensive mock response matching OpenFIGI structure
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
      "data": [
        {
          "figi": "BBG000B9XRY4",
          "name": "Apple Inc.",
          "ticker": "AAPL",
          "exchCode": "US",
          "compositeFIGI": "BBG000B9XRY4",
          "uniqueID": "EQ0010169500001000",
          "securityType": "Common Stock",
          "marketSector": "Equity",
          "shareClassFIGI": "BBG001S5N8V8",
          "uniqueIDFutOpt": None,
          "securityType2": "Common Stock",
          "securityDescription": "AAPL",
        }
      ],
      "error": None,
    }
    mock_post.return_value = mock_response

    payload = [{"idType": "TICKER", "idValue": "AAPL"}]

    # Execute
    result = client.get_figi(payload)

    # Verify structure
    assert "data" in result
    assert isinstance(result["data"], list)
    assert len(result["data"]) == 1

    # Verify first item structure
    item = result["data"][0]
    required_fields = ["figi", "name", "ticker", "exchCode", "compositeFIGI"]
    for field in required_fields:
      assert field in item
      assert item[field] is not None
