"""Tests for SEC async filing downloader."""

from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from robosystems.adapters.sec.client.downloader import (
  DownloadStats,
  SECDownloader,
)
from robosystems.adapters.sec.client.efts import EFTSHit


class TestDownloadStats:
  """Tests for DownloadStats dataclass."""

  def test_default_values(self):
    """Test default stats are zero."""
    stats = DownloadStats()

    assert stats.filings_found == 0
    assert stats.downloaded == 0
    assert stats.skipped == 0
    assert stats.failed == 0
    assert stats.bytes_downloaded == 0

  def test_custom_values(self):
    """Test stats with custom values."""
    stats = DownloadStats(
      filings_found=100,
      downloaded=80,
      skipped=15,
      failed=5,
      bytes_downloaded=1024000,
    )

    assert stats.filings_found == 100
    assert stats.downloaded == 80
    assert stats.skipped == 15
    assert stats.failed == 5
    assert stats.bytes_downloaded == 1024000


class TestSECDownloaderInit:
  """Tests for SECDownloader initialization."""

  def test_default_values(self):
    """Test default configuration."""
    downloader = SECDownloader()

    assert downloader.requests_per_second == 5.0
    assert downloader.max_concurrent == 10
    assert downloader.skip_existing is True

  def test_custom_values(self):
    """Test custom configuration."""
    downloader = SECDownloader(
      requests_per_second=3.0,
      max_concurrent=5,
      skip_existing=False,
    )

    assert downloader.requests_per_second == 3.0
    assert downloader.max_concurrent == 5
    assert downloader.skip_existing is False

  def test_post_init_creates_limiter(self):
    """Test that __post_init__ creates rate limiter."""
    downloader = SECDownloader(requests_per_second=8.0)

    assert downloader._limiter is not None
    assert downloader._limiter.rate == 8.0

  def test_post_init_creates_semaphore(self):
    """Test that __post_init__ creates semaphore."""
    downloader = SECDownloader(max_concurrent=20)

    assert downloader._semaphore is not None
    assert downloader._semaphore._value == 20


class TestSECDownloaderS3Keys:
  """Tests for S3 key construction."""

  def test_get_s3_key(self):
    """Test S3 key construction for filing."""
    downloader = SECDownloader()
    hit = EFTSHit(
      cik="0000320193",
      accession_number="0000320193-24-000001",
      form_type="10-K",
      file_number=None,
      filing_date=None,
      primary_document=None,
      file_url=None,
    )

    key = downloader._get_s3_key(hit, year=2024)

    # Should use get_raw_key format: {prefix}/sec/year=2024/{cik}/{accession}.zip
    assert "sec" in key
    assert "year=2024" in key
    assert "0000320193" in key
    assert "0000320193-24-000001.zip" in key
    # Should NOT have double slashes
    assert "//" not in key

  def test_get_xbrl_zip_url(self):
    """Test XBRL ZIP URL construction."""
    downloader = SECDownloader()
    hit = EFTSHit(
      cik="0000320193",
      accession_number="0000320193-24-000001",
      form_type="10-K",
      file_number=None,
      filing_date=None,
      primary_document=None,
      file_url=None,
    )

    url = downloader._get_xbrl_zip_url(hit)

    # CIK should have leading zeros stripped
    assert "320193" in url
    # Accession with no dashes
    assert "000032019324000001" in url
    # Filename with dashes
    assert "0000320193-24-000001-xbrl.zip" in url
    # Full URL format
    expected = "https://www.sec.gov/Archives/edgar/data/320193/000032019324000001/0000320193-24-000001-xbrl.zip"
    assert url == expected


class TestSECDownloaderContextManager:
  """Tests for async context manager."""

  @pytest.mark.asyncio
  async def test_context_manager_creates_session(self):
    """Test that context manager creates aiohttp session."""
    with patch("aiohttp.ClientSession") as mock_cls:
      mock_session = AsyncMock()
      mock_session.close = AsyncMock()
      mock_cls.return_value = mock_session

      async with SECDownloader() as downloader:
        assert downloader._session is mock_session

      mock_session.close.assert_called_once()

  @pytest.mark.asyncio
  async def test_context_manager_resets_stats(self):
    """Test that context manager resets stats."""
    async with SECDownloader() as downloader:
      # Manually set some stats
      downloader._stats.downloaded = 10

    # Enter again - stats should be fresh
    async with SECDownloader() as downloader:
      assert downloader._stats.downloaded == 0


class TestSECDownloaderDownloadFiling:
  """Tests for single filing download."""

  @pytest.fixture
  def mock_hit(self):
    """Create mock EFTS hit."""
    return EFTSHit(
      cik="0000320193",
      accession_number="0000320193-24-000001",
      form_type="10-K",
      file_number=None,
      filing_date=None,
      primary_document=None,
      file_url=None,
    )

  @pytest.mark.asyncio
  async def test_download_not_initialized_error(self, mock_hit):
    """Test error when downloader not initialized."""
    downloader = SECDownloader()

    with pytest.raises(RuntimeError, match="Downloader not initialized"):
      await downloader._download_filing(mock_hit, 2024, "test-bucket")

  @pytest.mark.asyncio
  async def test_download_skips_existing(self, mock_hit):
    """Test that existing files are skipped."""
    downloader = SECDownloader(skip_existing=True)

    # Mock S3 client to return file exists
    mock_s3 = MagicMock()
    mock_s3.s3_client.head_object = MagicMock()  # No exception = exists
    downloader._s3 = mock_s3

    # Mock session
    mock_session = AsyncMock()
    downloader._session = mock_session
    downloader._stats = DownloadStats()

    result = await downloader._download_filing(mock_hit, 2024, "test-bucket")

    assert result is True
    assert downloader._stats.skipped == 1
    # Should not have made any HTTP requests
    mock_session.get.assert_not_called()

  @pytest.mark.asyncio
  async def test_download_404_skipped(self, mock_hit):
    """Test that 404 responses are skipped (no XBRL ZIP available)."""
    downloader = SECDownloader(skip_existing=False)

    # Mock S3 client to return file doesn't exist
    mock_s3 = MagicMock()
    mock_s3.s3_client.head_object.side_effect = Exception("Not found")
    downloader._s3 = mock_s3

    # Mock 404 response
    mock_response = AsyncMock()
    mock_response.status = 404
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock()

    mock_session = AsyncMock()
    mock_session.get = MagicMock(return_value=mock_response)
    downloader._session = mock_session
    downloader._stats = DownloadStats()

    result = await downloader._download_filing(mock_hit, 2024, "test-bucket")

    assert result is True
    assert downloader._stats.skipped == 1

  @pytest.mark.asyncio
  async def test_download_success(self, mock_hit):
    """Test successful download and S3 upload."""
    downloader = SECDownloader(skip_existing=False)

    # Mock S3 client
    mock_s3 = MagicMock()
    mock_s3.s3_client.head_object.side_effect = Exception("Not found")
    mock_s3.s3_client.put_object = MagicMock()
    downloader._s3 = mock_s3

    # Mock successful response
    content = b"ZIP file content"
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.read = AsyncMock(return_value=content)
    mock_response.raise_for_status = MagicMock()
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock()

    mock_session = AsyncMock()
    mock_session.get = MagicMock(return_value=mock_response)
    downloader._session = mock_session
    downloader._stats = DownloadStats()

    result = await downloader._download_filing(mock_hit, 2024, "test-bucket")

    assert result is True
    assert downloader._stats.downloaded == 1
    assert downloader._stats.bytes_downloaded == len(content)
    mock_s3.s3_client.put_object.assert_called_once()

  @pytest.mark.asyncio
  async def test_download_empty_response_fails(self, mock_hit):
    """Test that empty responses are recorded as failed."""
    downloader = SECDownloader(skip_existing=False)

    # Mock S3 client
    mock_s3 = MagicMock()
    mock_s3.s3_client.head_object.side_effect = Exception("Not found")
    downloader._s3 = mock_s3

    # Mock empty response
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.read = AsyncMock(return_value=b"")
    mock_response.raise_for_status = MagicMock()
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock()

    mock_session = AsyncMock()
    mock_session.get = MagicMock(return_value=mock_response)
    downloader._session = mock_session
    downloader._stats = DownloadStats()

    result = await downloader._download_filing(mock_hit, 2024, "test-bucket")

    assert result is False
    assert downloader._stats.failed == 1

  @pytest.mark.asyncio
  async def test_download_client_error_fails(self, mock_hit):
    """Test that client errors are recorded as failed."""
    downloader = SECDownloader(skip_existing=False)

    # Mock S3 client
    mock_s3 = MagicMock()
    mock_s3.s3_client.head_object.side_effect = Exception("Not found")
    downloader._s3 = mock_s3

    # Mock client error
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.__aenter__ = AsyncMock(
      side_effect=aiohttp.ClientError("Network error")
    )
    mock_response.__aexit__ = AsyncMock()

    mock_session = AsyncMock()
    mock_session.get = MagicMock(return_value=mock_response)
    downloader._session = mock_session
    downloader._stats = DownloadStats()

    result = await downloader._download_filing(mock_hit, 2024, "test-bucket")

    assert result is False
    assert downloader._stats.failed == 1


class TestSECDownloaderDownloadFilings:
  """Tests for batch filing download."""

  @pytest.fixture
  def mock_hits(self):
    """Create list of mock EFTS hits."""
    return [
      EFTSHit(
        cik=f"000032019{i}",
        accession_number=f"000032019{i}-24-000001",
        form_type="10-K",
        file_number=None,
        filing_date=None,
        primary_document=None,
        file_url=None,
      )
      for i in range(3)
    ]

  @pytest.mark.asyncio
  async def test_download_filings_sets_count(self, mock_hits):
    """Test that download_filings sets filings_found count."""
    downloader = SECDownloader()

    # Mock _download_filing to succeed
    downloader._download_filing = AsyncMock(return_value=True)
    downloader._session = AsyncMock()
    downloader._stats = DownloadStats()

    with patch.object(downloader, "_get_s3_client"):
      stats = await downloader.download_filings(mock_hits, 2024, "test-bucket")

    assert stats.filings_found == 3

  @pytest.mark.asyncio
  async def test_download_filings_default_bucket(self, mock_hits):
    """Test that default bucket is used when not specified."""
    downloader = SECDownloader()
    downloader._download_filing = AsyncMock(return_value=True)
    downloader._session = AsyncMock()
    downloader._stats = DownloadStats()

    with patch("robosystems.adapters.sec.client.downloader.env") as mock_env:
      mock_env.SHARED_RAW_BUCKET = "default-bucket"

      with patch.object(downloader, "_get_s3_client"):
        await downloader.download_filings(mock_hits, 2024)

      # Verify _download_filing was called with the default bucket
      calls = downloader._download_filing.call_args_list
      for call in calls:
        assert call[0][2] == "default-bucket"
