"""Tests for SEC EFTS (Electronic Filing Text Search) client."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from robosystems.adapters.sec.client.efts import (
  EFTS_MAX_PAGE_SIZE,
  EFTS_MAX_RESULTS,
  EFTSClient,
  EFTSHit,
)


class TestEFTSHit:
  """Tests for EFTSHit dataclass and parsing."""

  def test_from_hit_basic(self):
    """Test basic EFTS hit parsing."""
    hit = {
      "_id": "0000320193-24-000001:aapl-20240101.htm",
      "_source": {
        "ciks": [320193],
        "form": "10-K",
        "file_num": "001-36743",
        "file_date": "2024-01-15",
        "display_names": ["aapl-20240101.htm"],
        "file_url": "/Archives/edgar/data/320193/000032019324000001/aapl-20240101.htm",
      },
    }

    result = EFTSHit.from_hit(hit)

    assert result.cik == "0000320193"
    assert result.accession_number == "0000320193-24-000001"
    assert result.form_type == "10-K"
    assert result.file_number == "001-36743"
    assert result.filing_date == "2024-01-15"
    assert result.primary_document == "aapl-20240101.htm"
    assert (
      result.file_url
      == "/Archives/edgar/data/320193/000032019324000001/aapl-20240101.htm"
    )

  def test_from_hit_cik_zero_padding(self):
    """Test that CIKs are zero-padded to 10 digits."""
    hit = {
      "_id": "0001234567-24-000001:doc.htm",
      "_source": {
        "ciks": [1234567],  # 7 digits
        "form": "10-Q",
      },
    }

    result = EFTSHit.from_hit(hit)
    assert result.cik == "0001234567"  # Should be zero-padded

  def test_from_hit_empty_ciks(self):
    """Test handling of empty CIKs list."""
    hit = {
      "_id": "0000000000-24-000001:doc.htm",
      "_source": {
        "ciks": [],
        "form": "10-K",
      },
    }

    result = EFTSHit.from_hit(hit)
    assert result.cik == ""

  def test_from_hit_no_colon_in_id(self):
    """Test handling of _id without colon."""
    hit = {
      "_id": "0000320193-24-000001",
      "_source": {
        "ciks": [320193],
        "form": "10-K",
      },
    }

    result = EFTSHit.from_hit(hit)
    assert result.accession_number == "0000320193-24-000001"

  def test_from_hit_missing_optional_fields(self):
    """Test handling of missing optional fields."""
    hit = {
      "_id": "0000320193-24-000001:doc.htm",
      "_source": {
        "ciks": [320193],
        "form": "10-K",
        # No file_num, file_date, display_names, file_url
      },
    }

    result = EFTSHit.from_hit(hit)
    assert result.file_number is None
    assert result.filing_date is None
    assert result.primary_document is None
    assert result.file_url is None

  def test_from_hit_empty_display_names(self):
    """Test handling of empty display_names list."""
    hit = {
      "_id": "0000320193-24-000001:doc.htm",
      "_source": {
        "ciks": [320193],
        "form": "10-K",
        "display_names": [],
      },
    }

    result = EFTSHit.from_hit(hit)
    assert result.primary_document is None


class TestEFTSClientBuildParams:
  """Tests for EFTSClient._build_params method."""

  def test_build_params_form_types(self):
    """Test form types parameter building."""
    client = EFTSClient()
    params = client._build_params(form_types=["10-K", "10-Q"])

    # Should include forms and exclude amendments
    assert "forms" in params
    forms = params["forms"].split(",")
    assert "10-K" in forms
    assert "10-Q" in forms
    assert "-10-K/A" in forms
    assert "-10-Q/A" in forms

  def test_build_params_include_amendments(self):
    """Test including amendments."""
    client = EFTSClient()
    params = client._build_params(form_types=["10-K"], include_amendments=True)

    forms = params["forms"].split(",")
    assert "10-K" in forms
    assert "-10-K/A" not in forms  # Should NOT exclude amendments

  def test_build_params_date_range(self):
    """Test date range parameters."""
    client = EFTSClient()
    params = client._build_params(start_date="2024-01-01", end_date="2024-12-31")

    assert params["startdt"] == "2024-01-01"
    assert params["enddt"] == "2024-12-31"

  def test_build_params_default_dates(self):
    """Test default date values."""
    client = EFTSClient()
    params = client._build_params()

    assert params["startdt"] == "2001-01-01"
    assert params["enddt"] == datetime.now().strftime("%Y-%m-%d")

  def test_build_params_ciks_zero_padded(self):
    """Test CIKs are zero-padded."""
    client = EFTSClient()
    params = client._build_params(ciks=["320193", "789019"])

    ciks = params["ciks"].split(",")
    assert "0000320193" in ciks
    assert "0000789019" in ciks

  def test_build_params_text_query(self):
    """Test text query parameter."""
    client = EFTSClient()
    params = client._build_params(text_query='"material weakness"')

    assert params["q"] == '"material weakness"'


class TestEFTSClientQuery:
  """Tests for EFTSClient.query method."""

  @pytest.fixture
  def mock_session(self):
    """Create mock aiohttp session."""
    session = AsyncMock(spec=aiohttp.ClientSession)
    return session

  @pytest.mark.asyncio
  async def test_query_no_results(self, mock_session):
    """Test query with no results."""
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.read = AsyncMock(return_value=b"{}")
    mock_response.json = AsyncMock(
      return_value={"hits": {"total": {"value": 0}, "hits": []}}
    )
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock()

    mock_session.get = MagicMock(return_value=mock_response)

    client = EFTSClient()
    client._session = mock_session

    results = await client.query(form_types=["10-K"], start_date="2024-01-01")

    assert results == []

  @pytest.mark.asyncio
  async def test_query_single_page(self, mock_session):
    """Test query that fits in single page."""
    hits_data = [
      {
        "_id": "0000320193-24-000001:doc.htm",
        "_source": {"ciks": [320193], "form": "10-K"},
      },
      {
        "_id": "0000789019-24-000001:doc.htm",
        "_source": {"ciks": [789019], "form": "10-K"},
      },
    ]

    # First call returns count, second returns data
    call_count = 0

    async def mock_json():
      nonlocal call_count
      call_count += 1
      if call_count == 1:
        # Initial page (size=1) to get count
        return {"hits": {"total": {"value": 2}, "hits": hits_data[:1]}}
      else:
        # Full page
        return {"hits": {"total": {"value": 2}, "hits": hits_data}}

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.read = AsyncMock(return_value=b"{}")
    mock_response.json = mock_json
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock()

    mock_session.get = MagicMock(return_value=mock_response)

    client = EFTSClient()
    client._session = mock_session

    results = await client.query(form_types=["10-K"])

    assert len(results) == 2
    assert results[0].cik == "0000320193"
    assert results[1].cik == "0000789019"

  @pytest.mark.asyncio
  async def test_query_with_max_results(self, mock_session):
    """Test query with max_results limit."""
    # Return more hits than max_results
    hits_data = [
      {
        "_id": f"000032019{i}-24-000001:doc.htm",
        "_source": {"ciks": [320190 + i], "form": "10-K"},
      }
      for i in range(10)
    ]

    call_count = 0

    async def mock_json():
      nonlocal call_count
      call_count += 1
      if call_count == 1:
        return {"hits": {"total": {"value": 10}, "hits": hits_data[:1]}}
      else:
        # Return only up to max_results
        return {"hits": {"total": {"value": 10}, "hits": hits_data[:5]}}

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.read = AsyncMock(return_value=b"{}")
    mock_response.json = mock_json
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock()

    mock_session.get = MagicMock(return_value=mock_response)

    client = EFTSClient()
    client._session = mock_session

    results = await client.query(form_types=["10-K"], max_results=5)

    assert len(results) == 5

  @pytest.mark.asyncio
  async def test_client_not_initialized_error(self):
    """Test error when client not initialized."""
    client = EFTSClient()

    with pytest.raises(RuntimeError, match="Client not initialized"):
      await client._fetch_page({}, offset=0)

  @pytest.mark.asyncio
  async def test_context_manager(self):
    """Test async context manager initializes session."""
    with patch("aiohttp.ClientSession") as mock_cls:
      mock_session = AsyncMock()
      mock_session.close = AsyncMock()
      mock_cls.return_value = mock_session

      async with EFTSClient() as client:
        assert client._session is mock_session

      mock_session.close.assert_called_once()


class TestEFTSClientQueryByYear:
  """Tests for EFTSClient.query_by_year convenience method."""

  @pytest.mark.asyncio
  async def test_query_by_year_date_range(self):
    """Test that query_by_year sets correct date range."""
    client = EFTSClient()

    # Mock the query method
    client.query = AsyncMock(return_value=[])

    await client.query_by_year(2024)

    client.query.assert_called_once_with(
      form_types=["10-K", "10-Q"],
      start_date="2024-01-01",
      end_date="2024-12-31",
      ciks=None,
    )

  @pytest.mark.asyncio
  async def test_query_by_year_with_ciks(self):
    """Test query_by_year with CIK filter."""
    client = EFTSClient()
    client.query = AsyncMock(return_value=[])

    await client.query_by_year(2024, ciks=["320193"])

    client.query.assert_called_once_with(
      form_types=["10-K", "10-Q"],
      start_date="2024-01-01",
      end_date="2024-12-31",
      ciks=["320193"],
    )

  @pytest.mark.asyncio
  async def test_query_by_year_custom_forms(self):
    """Test query_by_year with custom form types."""
    client = EFTSClient()
    client.query = AsyncMock(return_value=[])

    await client.query_by_year(2024, form_types=["8-K"])

    client.query.assert_called_once_with(
      form_types=["8-K"],
      start_date="2024-01-01",
      end_date="2024-12-31",
      ciks=None,
    )


class TestEFTSConstants:
  """Tests for EFTS module constants."""

  def test_max_page_size(self):
    """Verify EFTS max page size constant."""
    assert EFTS_MAX_PAGE_SIZE == 100

  def test_max_results(self):
    """Verify EFTS max results constant."""
    assert EFTS_MAX_RESULTS == 10000
