"""
Async SEC filing downloader.

Downloads SEC filings discovered via EFTS to S3 with:
- Parallel async downloads
- Precise rate limiting
- Skip existing files
- Progress tracking
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import aiohttp

from robosystems.config import env
from robosystems.logger import logger

from ..config import SEC_CONFIG
from .rate_limiter import AsyncRateLimiter, RateMonitor

if TYPE_CHECKING:
  from xbrlkit.edgar import EftsHit

  from robosystems.operations.aws.s3 import S3Client
SEC_BASE_URL = SEC_CONFIG["base_url"]
SEC_HEADERS = SEC_CONFIG["headers"]


@dataclass
class DownloadStats:
  """Statistics from a download run."""

  filings_found: int = 0
  downloaded: int = 0
  skipped: int = 0
  failed: int = 0
  bytes_downloaded: int = 0


@dataclass
class SECDownloader:
  """
  Async SEC filing downloader with EFTS discovery.

  Example:
      downloader = SECDownloader()
      stats = await downloader.download_year(2024)
      print(f"Downloaded {stats.downloaded} filings")
  """

  requests_per_second: float = 5.0
  max_concurrent: int = 10
  skip_existing: bool = True

  # Internal state
  _limiter: AsyncRateLimiter = field(init=False)
  _monitor: RateMonitor = field(init=False)
  _semaphore: asyncio.Semaphore = field(init=False)
  _s3: S3Client | None = field(default=None, init=False)
  _session: aiohttp.ClientSession | None = field(default=None, init=False)
  _stats: DownloadStats = field(default_factory=DownloadStats, init=False)

  def __post_init__(self):
    self._limiter = AsyncRateLimiter(rate=self.requests_per_second)
    self._monitor = RateMonitor()
    self._semaphore = asyncio.Semaphore(self.max_concurrent)

  def _get_s3_client(self) -> S3Client:
    """Lazily initialize S3 client to avoid circular imports."""
    if self._s3 is None:
      from robosystems.operations.aws.s3 import S3Client

      self._s3 = S3Client()
    return self._s3

  async def __aenter__(self):
    self._session = aiohttp.ClientSession(headers=SEC_HEADERS)
    self._stats = DownloadStats()
    return self

  async def __aexit__(self, *args):
    if self._session:
      await self._session.close()
      self._session = None

  def _get_xbrl_zip_url(self, hit: EftsHit) -> str:
    """Construct XBRL ZIP URL from EFTS hit."""
    cik_no_leading_zeros = str(int(hit.cik))
    accno_no_dash = hit.accession.replace("-", "")
    filename = f"{hit.accession}-xbrl.zip"
    return f"{SEC_BASE_URL}/Archives/edgar/data/{cik_no_leading_zeros}/{accno_no_dash}/{filename}"

  def _get_s3_key(self, hit: EftsHit, year: int) -> str:
    """Construct S3 key for filing."""
    from robosystems.config.storage.shared import DataSourceType, get_raw_key

    # Format: sec/year=2024/CIK/accession.zip
    return get_raw_key(
      DataSourceType.SEC, f"year={year}", hit.cik, f"{hit.accession}.zip"
    )

  async def _file_exists(self, bucket: str, key: str) -> bool:
    """Check if file already exists in S3."""
    try:
      self._get_s3_client().s3_client.head_object(Bucket=bucket, Key=key)
      return True
    except Exception:
      return False

  async def _download_filing(
    self,
    hit: EftsHit,
    year: int,
    bucket: str,
    retry_count: int = 0,
  ) -> bool:
    """Download a single filing to S3."""
    MAX_RETRIES = 3
    MAX_RETRY_AFTER = 300  # Cap at 5 minutes to prevent DoS

    if not self._session:
      raise RuntimeError(
        "Downloader not initialized. Use 'async with SECDownloader():'"
      )

    s3_key = self._get_s3_key(hit, year)

    # Skip if exists
    if self.skip_existing and await self._file_exists(bucket, s3_key):
      self._stats.skipped += 1
      return True

    url = self._get_xbrl_zip_url(hit)

    async with self._semaphore:
      async with self._limiter:
        try:
          async with self._session.get(url) as response:
            if response.status == 404:
              # No XBRL ZIP available for this filing
              logger.debug(f"No XBRL ZIP for {hit.accession}")
              self._stats.skipped += 1
              return True

            if response.status == 429:
              if retry_count >= MAX_RETRIES:
                logger.error(f"Max retries exceeded for {hit.accession}")
                self._stats.failed += 1
                return False
              retry_after = min(
                int(response.headers.get("Retry-After", 60)), MAX_RETRY_AFTER
              )
              logger.warning(
                f"Rate limited, waiting {retry_after}s "
                f"(retry {retry_count + 1}/{MAX_RETRIES})"
              )
              await asyncio.sleep(retry_after)
              return await self._download_filing(hit, year, bucket, retry_count + 1)

            response.raise_for_status()
            content = await response.read()

            if not content or len(content) == 0:
              logger.warning(f"Empty response for {hit.accession}")
              self._stats.failed += 1
              return False

            await self._monitor.record(len(content))

        except aiohttp.ClientError as e:
          logger.error(f"Download failed for {hit.accession}: {e}")
          self._stats.failed += 1
          return False

      # Upload to S3 (inside semaphore to limit concurrent uploads)
      try:
        self._get_s3_client().s3_client.put_object(
          Bucket=bucket,
          Key=s3_key,
          Body=content,
          ContentType="application/zip",
        )
        self._stats.downloaded += 1
        self._stats.bytes_downloaded += len(content)
        return True
      except Exception as e:
        logger.error(f"S3 upload failed for {hit.accession}: {e}")
        self._stats.failed += 1
        return False

  async def download_filings(
    self,
    hits: list[EftsHit],
    year: int,
    bucket: str | None = None,
  ) -> DownloadStats:
    """Download EFTS hits to S3 in parallel, under the shared rate limiter.

    `year` partitions the S3 path; `bucket` defaults to SHARED_RAW_BUCKET.
    """
    bucket = bucket or env.SHARED_RAW_BUCKET
    self._stats.filings_found = len(hits)

    logger.info(f"Downloading {len(hits)} filings to s3://{bucket}/...")

    # Create download tasks
    tasks = [self._download_filing(hit, year, bucket) for hit in hits]

    # Execute with progress logging
    completed = 0
    for coro in asyncio.as_completed(tasks):
      await coro
      completed += 1
      if completed % 100 == 0:
        stats = self._monitor.get_stats()
        logger.info(
          f"Progress: {completed}/{len(hits)} "
          f"({stats.requests_per_second} req/s, {stats.mb_per_second} MB/s)"
        )

    logger.info(
      f"Download complete: {self._stats.downloaded} new, "
      f"{self._stats.skipped} skipped, {self._stats.failed} failed"
    )

    return self._stats

  async def download_year(
    self,
    year: int,
    form_types: list[str] | None = None,
    ciks: list[str] | None = None,
    bucket: str | None = None,
  ) -> DownloadStats:
    """
    Download all filings for a year using EFTS discovery.

    `form_types` defaults to 10-K, 10-Q, 20-F, 40-F, DEF 14A, S-1; `ciks`
    narrows to specific filers.

    Example:
        async with SECDownloader() as downloader:
            stats = await downloader.download_year(2024)
    """
    from xbrlkit.edgar import EftsClient

    from ..config import xbrlkit_config

    form_types = form_types or ["10-K", "10-Q", "20-F", "40-F", "DEF 14A", "S-1"]

    # Discover filings via EFTS (xbrlkit's synchronous client, in a thread)
    efts = EftsClient(xbrlkit_config(), per_sec=self.requests_per_second)
    hits = await asyncio.to_thread(
      efts.query_by_year, year, forms=form_types, ciks=ciks
    )

    if not hits:
      logger.warning(f"No filings found for {year}")
      return DownloadStats()

    # Download them
    return await self.download_filings(hits, year, bucket)


async def download_sec_filings(
  year: int,
  form_types: list[str] | None = None,
  ciks: list[str] | None = None,
  requests_per_second: float = 5.0,
  skip_existing: bool = True,
) -> DownloadStats:
  """
  Convenience function to download SEC filings for a year.

  Example:
      stats = await download_sec_filings(2024)
      print(f"Downloaded {stats.downloaded} filings")
  """
  async with SECDownloader(
    requests_per_second=requests_per_second,
    skip_existing=skip_existing,
  ) as downloader:
    return await downloader.download_year(year, form_types=form_types, ciks=ciks)


def download_sec_filings_sync(
  year: int,
  form_types: list[str] | None = None,
  ciks: list[str] | None = None,
  requests_per_second: float = 5.0,
  skip_existing: bool = True,
) -> DownloadStats:
  """
  Synchronous wrapper for download_sec_filings.

  Example:
      from robosystems.adapters.sec.client.downloader import download_sec_filings_sync

      stats = download_sec_filings_sync(2024)
  """
  return asyncio.run(
    download_sec_filings(
      year=year,
      form_types=form_types,
      ciks=ciks,
      requests_per_second=requests_per_second,
      skip_existing=skip_existing,
    )
  )
