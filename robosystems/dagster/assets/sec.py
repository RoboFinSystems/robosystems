"""SEC XBRL pipeline Dagster assets.

Pipeline stages (run independently via separate jobs):

1. DOWNLOAD (sec_download job):
   - sec_raw_filings - Discover via EFTS, download XBRL ZIPs (year-partitioned)

2. PROCESS (sec_process job, sensor-triggered):
   - sec_process_filing - Process single filing to parquet (dynamic partitions)

3. MATERIALIZE (two-stage pipeline):
   - sec_stage job: sec_duckdb_staged - Stage processed files to persistent DuckDB
   - sec_materialize job: sec_graph_materialized - Materialize from DuckDB to LadybugDB

   If LadybugDB fails, re-run sec_materialize - DuckDB staging is preserved.

The pipeline leverages existing adapters:
- robosystems.adapters.sec.client.EFTSClient - EFTS discovery API
- robosystems.adapters.sec.SECClient - EDGAR API client (submissions)
- robosystems.adapters.sec.XBRLGraphProcessor - XBRL processing
- robosystems.adapters.sec.XBRLDuckDBGraphProcessor - DuckDB staging/materialization

Architecture Notes:
- EFTS-based O(1) discovery replaces per-company iteration
- Year partitioning for downloads
- Dynamic partitioning for processing (one partition per filing, parallel)
- Sensor discovers unprocessed filings and triggers processing
- Graph materialization always rebuilds from all processed data
"""

from datetime import UTC, datetime

from dagster import (
  AssetExecutionContext,
  BackfillPolicy,
  Config,
  DynamicPartitionsDefinition,
  MaterializeResult,
  RetryPolicy,
  StaticPartitionsDefinition,
  asset,
)

from robosystems.config import env
from robosystems.config.storage.shared import (
  DataSourceType,
  get_processed_key,
  get_raw_key,
)
from robosystems.dagster.resources import S3Resource

# In-memory cache for SEC submissions during a single run
_sec_submissions_cache: dict[str, dict] = {}


def _store_entity_submissions_snapshot(
  s3_client, bucket: str, cik: str, submissions_data: dict
) -> str | None:
  """Store entity submissions snapshot to S3.

  Submissions are stored at the CIK level (not year-partitioned) since they
  contain cumulative data spanning all years.

  Args:
      s3_client: boto3 S3 client
      bucket: S3 bucket name
      cik: Company CIK
      submissions_data: Complete submissions data from SEC API

  Returns:
      S3 key where data was stored, or None on failure
  """
  import json
  from datetime import datetime

  try:
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    submissions_json = json.dumps(submissions_data, default=str)

    # Store as latest (primary location for quick retrieval)
    latest_s3_key = f"submissions/{cik}/latest.json"
    s3_client.put_object(
      Bucket=bucket,
      Key=latest_s3_key,
      Body=submissions_json.encode("utf-8"),
      ContentType="application/json",
    )

    # Store versioned copy for history/audit
    version_s3_key = f"submissions/{cik}/versions/v{timestamp}.json"
    s3_client.put_object(
      Bucket=bucket,
      Key=version_s3_key,
      Body=submissions_json.encode("utf-8"),
      ContentType="application/json",
    )

    return latest_s3_key

  except Exception as e:
    # Don't fail the pipeline if snapshot storage fails
    import logging

    logging.getLogger(__name__).warning(
      f"Failed to store submissions snapshot for {cik}: {e}"
    )
    return None


def _load_entity_submissions_snapshot(s3_client, bucket: str, cik: str) -> dict | None:
  """Load entity submissions snapshot from S3.

  Args:
      s3_client: boto3 S3 client
      bucket: S3 bucket name
      cik: Company CIK

  Returns:
      Submissions data dict, or None if not found
  """
  import json

  try:
    s3_key = get_raw_key(DataSourceType.SEC, "submissions", f"{cik}.json")
    response = s3_client.get_object(Bucket=bucket, Key=s3_key)
    return json.loads(response["Body"].read().decode("utf-8"))
  except Exception:
    return None


def _get_sec_metadata(
  cik: str, accession: str, s3_client=None, bucket: str | None = None
) -> tuple[dict, dict]:
  """Fetch SEC filer and report metadata for a given CIK and accession number.

  Attempts to load from S3 snapshot first (stored during download phase),
  falling back to SEC API only if no snapshot exists.

  Args:
      cik: Company CIK
      accession: Accession number (with dashes)
      s3_client: Optional boto3 S3 client for loading snapshots
      bucket: Optional S3 bucket name for snapshots

  Returns:
      Tuple of (sec_filer dict, sec_report dict) with full metadata.
  """
  from robosystems.adapters.sec import SECClient

  submissions = None

  # Check in-memory cache first
  if cik in _sec_submissions_cache:
    submissions = _sec_submissions_cache[cik]

  # Try loading from S3 snapshot
  if submissions is None and s3_client is not None and bucket is not None:
    submissions = _load_entity_submissions_snapshot(s3_client, bucket, cik)
    if submissions:
      _sec_submissions_cache[cik] = submissions

  # Fallback to SEC API if no snapshot
  if submissions is None:
    import logging

    logging.getLogger(__name__).warning(
      f"No S3 snapshot for CIK {cik}, falling back to SEC API"
    )
    client = SECClient(cik=cik)
    submissions = client.get_submissions()
    _sec_submissions_cache[cik] = submissions

  # Build sec_filer from company-level data
  sec_filer = {
    "cik": cik,
    "name": submissions.get("name"),
    "entity_name": submissions.get("name"),  # Alternative key used by processor
    "ticker": submissions.get("tickers", [None])[0]
    if submissions.get("tickers")
    else None,
    "exchange": submissions.get("exchanges", [None])[0]
    if submissions.get("exchanges")
    else None,
    "sic": submissions.get("sic"),
    "sicDescription": submissions.get("sicDescription"),
    "stateOfIncorporation": submissions.get("stateOfIncorporation"),
    "fiscalYearEnd": submissions.get("fiscalYearEnd"),
    "ein": submissions.get("ein"),
    "entityType": submissions.get("entityType"),
    "category": submissions.get("category"),
    "website": submissions.get("website") or submissions.get("investorWebsite"),
    "phone": submissions.get("phone"),
  }

  # Find the specific filing in filings
  # Supports both new complete format (filings directly) and legacy format (filings.recent)
  sec_report: dict = {"accessionNumber": accession}
  filings_data = submissions.get("filings", {})

  # New complete format: filings are directly in submissions["filings"]
  # Legacy format: filings are in submissions["filings"]["recent"]
  if "accessionNumber" in filings_data:
    # New complete format - filings directly at this level
    filings = filings_data
  else:
    # Legacy format - filings nested under "recent"
    filings = filings_data.get("recent", {})

  def safe_get(field: str, idx: int, default=None):
    """Safely get value from filings list with bounds checking."""
    lst = filings.get(field, [])
    return lst[idx] if idx < len(lst) else default

  if filings and "accessionNumber" in filings:
    accession_numbers = filings["accessionNumber"]
    for i, acc_num in enumerate(accession_numbers):
      if acc_num == accession:
        # Found the filing - extract all metadata
        sec_report = {
          "accessionNumber": accession,
          "form": safe_get("form", i),
          "filingDate": safe_get("filingDate", i),
          "reportDate": safe_get("reportDate", i),
          "acceptanceDateTime": safe_get("acceptanceDateTime", i),
          "primaryDocument": safe_get("primaryDocument", i),
          "periodOfReport": safe_get("periodOfReport", i),
          "isXBRL": bool(safe_get("isXBRL", i, False)),
          "isInlineXBRL": bool(safe_get("isInlineXBRL", i, False)),
        }
        break

  return sec_filer, sec_report


# Start year for SEC data loading (XBRL filings began 2009)
SEC_START_YEAR = 2009

# Quarter partitions for SEC data (SEC_START_YEAR-Q1 through current year Q4)
# EFTS has a 10k result limit per query; quarterly partitions typically return 5-7k filings
# Dynamically includes current year so no manual updates needed on Jan 1
_current_year = datetime.now(UTC).year
SEC_QUARTERS = [
  f"{year}-Q{q}"
  for year in range(SEC_START_YEAR, _current_year + 1)
  for q in range(1, 5)
]
sec_quarter_partitions = StaticPartitionsDefinition(SEC_QUARTERS)

# Dynamic partitions for individual filing processing
# Partition key format: {year}_{cik}_{accession}
# Note: S3 storage uses year-based paths regardless of quarterly download partitions
sec_filing_partitions = DynamicPartitionsDefinition(name="sec_filings")


# ============================================================================
# Configuration Classes
# ============================================================================


class SECDownloadConfig(Config):
  """Configuration for SEC raw filings download.

  Production Scaling Notes:
  - Each year partition runs independently (can parallelize years)
  - Submissions fetching: ~8 req/sec, 5 concurrent (configurable)
  - Filing downloads: ~5 req/sec, 10 concurrent (configurable)
  - For full year (~5000 companies, ~10000 filings): ~45 min total
  - Use max_filings for testing, dry_run for discovery only
  """

  skip_existing: bool = True  # Skip already downloaded filings
  form_types: list[str] = ["10-K", "10-Q"]  # Form types to download
  tickers: list[str] = []  # Optional ticker filter (empty = all companies)
  ciks: list[str] = []  # Optional CIK filter
  max_filings: int = 0  # Max filings to download (0 = unlimited)
  dry_run: bool = False  # If True, discover only - don't download

  # Concurrency controls for production
  submissions_rate: float = 8.0  # Submissions requests per second
  submissions_concurrency: int = 5  # Max concurrent submission fetches
  download_rate: float = 5.0  # Download requests per second
  download_concurrency: int = 10  # Max concurrent downloads


class SECSingleFilingConfig(Config):
  """Configuration for single filing processing."""

  # No config needed - partition key contains all info
  pass


class SECStageConfig(Config):
  """Configuration for DuckDB staging.

  Supports both full staging (mode="full") and incremental staging (mode="incremental").

  Full mode: Creates tables from scratch, optionally rebuilding graph.
  Incremental mode: Appends new filings to existing tables (requires initial full staging).
  """

  graph_id: str = "sec"  # Target graph ID
  mode: str = "full"  # "full" or "incremental"
  rebuild_graph: bool = (
    True  # Whether to rebuild LadybugDB before staging (full mode only)
  )
  year: int | None = None  # Optional year filter (full mode only)
  reset_staging: bool = False  # Delete DuckDB staging too (full mode only)
  filing_date: str | None = None  # YYYY-MM-DD date filter (incremental mode)


class SECMaterializeConfig(Config):
  """Configuration for graph materialization (Stage 2).

  Use this config with sec_graph_materialized asset to materialize
  from DuckDB staging to LadybugDB.

  Options:
    graph_id: Target graph ID (default: "sec")
    rebuild_graph: If True, delete and recreate the LadybugDB database
                   with the roboledger SEC schema before materializing.
                   DuckDB staging is preserved for retry. (default: False)
  """

  graph_id: str = "sec"  # Target graph ID
  rebuild_graph: bool = False  # Rebuild LadybugDB before materialization


# ============================================================================
# Year-Partitioned Assets (download phase)
# ============================================================================


@asset(
  group_name="sec_pipeline",
  description="Download SEC XBRL filings for a specific quarter using EFTS discovery",
  kinds={"download"},
  partitions_def=sec_quarter_partitions,
  metadata={
    "pipeline": "sec",
    "stage": "extraction",
  },
  # Run all partitions sequentially in a single run to prevent SEC rate limiting
  backfill_policy=BackfillPolicy.single_run(),
)
def sec_raw_filings(
  context: AssetExecutionContext,
  config: SECDownloadConfig,
  s3: S3Resource,
) -> MaterializeResult:
  """Download SEC XBRL filings for a specific quarter using EFTS discovery.

  Uses SEC EFTS API to discover all filings matching criteria in a single query,
  then downloads them with async rate-limited parallelism.

  EFTS has a 10k result limit per query. Quarterly partitions typically return
  5-7k filings, safely under the limit.

  Uses BackfillPolicy.single_run() to run all partitions sequentially in a single
  run, preventing SEC rate limiting during backfills.

  Returns:
      MaterializeResult with download statistics
  """
  import asyncio

  # Parse partition key: "2024-Q1" -> year=2024, quarter=1
  partition_key = context.partition_key
  year, quarter_str = partition_key.split("-Q")
  year = int(year)
  quarter = int(quarter_str)
  context.log.info(f"Downloading SEC filings for {year}-Q{quarter} via EFTS")

  bucket = env.SHARED_RAW_BUCKET

  async def run_efts_download():
    # Import here to avoid circular imports at module load time
    import aiohttp

    from robosystems.adapters.sec.client.efts import EFTSClient, EFTSHit
    from robosystems.adapters.sec.client.rate_limiter import (
      AsyncRateLimiter,
      RateMonitor,
    )
    from robosystems.config import ExternalServicesConfig

    SEC_CONFIG = ExternalServicesConfig.SEC_CONFIG
    SEC_BASE_URL = SEC_CONFIG["base_url"]
    SEC_HEADERS = SEC_CONFIG["headers"]

    # Step 1: Discover filings via EFTS
    context.log.info("Phase 1: Discovering filings via EFTS...")

    async with EFTSClient(requests_per_second=5.0) as efts:
      # Build CIK filter if specified
      cik_filter = None
      if config.ciks:
        cik_filter = config.ciks
      elif config.tickers:
        # Resolve tickers to CIKs using company list
        from robosystems.adapters.sec import SECClient

        sec_client = SECClient()
        companies_raw = sec_client.get_companies()
        cik_filter = []
        for _, company in companies_raw.items():
          ticker = company.get("ticker", "")
          if ticker in config.tickers:
            cik = str(company.get("cik_str", company.get("cik", "")))
            cik_filter.append(cik)

      hits = await efts.query_by_quarter(
        year=year,
        quarter=quarter,
        form_types=config.form_types,
        ciks=cik_filter,
      )

    context.log.info(f"EFTS discovered {len(hits)} filings for {year}-Q{quarter}")

    if not hits:
      return {
        "filings_found": 0,
        "submissions_fetched": 0,
        "downloaded": 0,
        "skipped": 0,
        "no_xbrl": 0,
        "failed": 0,
        "dry_run": config.dry_run,
      }

    # Step 1.5: Fetch submissions data for unique CIKs (parallel with rate limiting)
    # This provides company metadata (name, SIC, fiscal year end, etc.)
    unique_ciks = list({hit.cik for hit in hits})
    context.log.info(
      f"Phase 1.5: Fetching submissions for {len(unique_ciks)} unique companies..."
    )

    # Filter to only CIKs that need fetching
    ciks_to_fetch = []
    submissions_skipped = 0
    for cik in unique_ciks:
      submissions_key = get_raw_key(DataSourceType.SEC, "submissions", f"{cik}.json")
      if config.skip_existing:
        try:
          s3.client.head_object(Bucket=bucket, Key=submissions_key)
          submissions_skipped += 1
          continue
        except Exception:
          pass
      ciks_to_fetch.append(cik)

    context.log.info(
      f"Submissions: {submissions_skipped} cached, {len(ciks_to_fetch)} to fetch"
    )

    submissions_fetched = submissions_skipped
    submissions_failed = 0

    if ciks_to_fetch:
      import json
      from datetime import UTC, datetime

      from robosystems.adapters.sec import SECClient

      # Rate limiter and semaphore for parallel fetching (configurable)
      submissions_limiter = AsyncRateLimiter(rate=config.submissions_rate)
      submissions_semaphore = asyncio.Semaphore(config.submissions_concurrency)

      def build_complete_submissions_sync(cik: str) -> dict:
        """Build complete master submissions file (all pagination files)."""
        client = SECClient(cik=cik)
        return client.get_complete_submissions()

      def incremental_update_submissions(
        existing: dict, cik: str, new_recent: dict
      ) -> dict:
        """Incrementally update existing submissions with new filings from recent page."""
        # Get new filings from recent page
        new_accessions = set(
          new_recent.get("filings", {}).get("recent", {}).get("accessionNumber", [])
        )
        existing_accessions = set(
          existing.get("filings", {}).get("accessionNumber", [])
        )

        # Find truly new accession numbers
        new_only = new_accessions - existing_accessions
        if not new_only:
          return existing  # No new filings

        # Find indices of new filings in the recent data
        recent_data = new_recent.get("filings", {}).get("recent", {})
        recent_accessions = recent_data.get("accessionNumber", [])

        # Prepend new filings to existing (new filings go at the front)
        for field in existing["filings"]:
          if field in recent_data:
            new_values = [
              recent_data[field][i]
              for i, acc in enumerate(recent_accessions)
              if acc in new_only
            ]
            existing["filings"][field] = new_values + existing["filings"][field]

        # Update metadata
        existing["_metadata"] = existing.get("_metadata", {})
        existing["_metadata"]["totalFilings"] = len(
          existing["filings"].get("accessionNumber", [])
        )
        existing["_metadata"]["lastUpdated"] = datetime.now(UTC).isoformat()

        return existing

      async def fetch_submission(cik: str) -> bool:
        nonlocal submissions_fetched, submissions_failed
        submissions_key = get_raw_key(DataSourceType.SEC, "submissions", f"{cik}.json")

        async with submissions_semaphore:
          async with submissions_limiter:
            try:
              # Check if master file already exists
              existing_data = None
              try:
                response = s3.client.get_object(Bucket=bucket, Key=submissions_key)
                existing_data = json.loads(response["Body"].read().decode("utf-8"))
              except Exception:
                pass  # File doesn't exist

              if existing_data is None:
                # No existing file - build complete master (sync, fetches all pages)
                context.log.info(
                  f"Building complete submissions master for CIK {cik}..."
                )
                # Run sync function in thread pool to not block event loop
                loop = asyncio.get_event_loop()
                submissions_data = await loop.run_in_executor(
                  None, build_complete_submissions_sync, cik
                )
              else:
                # Existing file - do incremental update from recent page only
                url = f"https://data.sec.gov/submissions/CIK{cik}.json"
                async with aiohttp.ClientSession(headers=SEC_HEADERS) as session:
                  async with session.get(url) as response:
                    if response.status == 429:
                      retry_after = int(response.headers.get("Retry-After", 60))
                      context.log.warning(
                        f"Rate limited on submissions, waiting {retry_after}s"
                      )
                      await asyncio.sleep(retry_after)
                      return await fetch_submission(cik)

                    response.raise_for_status()
                    new_recent = await response.json()

                submissions_data = incremental_update_submissions(
                  existing_data, cik, new_recent
                )

              # Store to S3
              s3.client.put_object(
                Bucket=bucket,
                Key=submissions_key,
                Body=json.dumps(submissions_data),
                ContentType="application/json",
              )
              submissions_fetched += 1
              return True

            except Exception as e:
              context.log.debug(f"Failed to fetch submissions for CIK {cik}: {e}")
              submissions_failed += 1
              return False

      # Run all fetches in parallel
      tasks = [fetch_submission(cik) for cik in ciks_to_fetch]
      completed = 0
      for coro in asyncio.as_completed(tasks):
        await coro
        completed += 1
        if completed % 50 == 0:
          context.log.info(f"Submissions progress: {completed}/{len(ciks_to_fetch)}")

    context.log.info(
      f"Submissions complete: {submissions_fetched} fetched, {submissions_failed} failed"
    )

    # Apply max_filings limit if specified
    if config.max_filings > 0 and len(hits) > config.max_filings:
      context.log.info(
        f"Limiting to {config.max_filings} filings (of {len(hits)} discovered)"
      )
      hits = hits[: config.max_filings]

    # Dry run mode - just report what would be downloaded
    if config.dry_run:
      context.log.info(f"[DRY RUN] Would download {len(hits)} filings:")
      for hit in hits[:10]:
        context.log.info(f"  - {hit.cik}/{hit.accession_number} ({hit.form_type})")
      if len(hits) > 10:
        context.log.info(f"  ... and {len(hits) - 10} more")
      return {
        "filings_found": len(hits),
        "submissions_fetched": submissions_fetched,
        "downloaded": 0,
        "skipped": 0,
        "no_xbrl": 0,
        "failed": 0,
        "dry_run": True,
      }

    # Step 2: Download filings with async rate limiting
    context.log.info(f"Phase 2: Downloading {len(hits)} filings...")

    limiter = AsyncRateLimiter(rate=config.download_rate)
    monitor = RateMonitor()
    semaphore = asyncio.Semaphore(config.download_concurrency)

    downloaded = 0
    skipped = 0  # Already exists in S3
    no_xbrl = 0  # Filing exists but no XBRL ZIP available
    no_xbrl_filings: list[str] = []  # Track which filings lack XBRL
    failed = 0

    async def download_filing(hit: EFTSHit) -> bool:
      nonlocal downloaded, skipped, no_xbrl, failed

      # Construct S3 key
      s3_key = get_raw_key(
        DataSourceType.SEC,
        f"year={year}",
        hit.cik,
        f"{hit.accession_number}.zip",
      )

      # Skip if exists
      if config.skip_existing:
        try:
          s3.client.head_object(Bucket=bucket, Key=s3_key)
          skipped += 1
          return True
        except Exception:
          pass  # File doesn't exist, continue to download

      # Construct XBRL ZIP URL
      cik_no_zeros = str(int(hit.cik))
      accno_no_dash = hit.accession_number.replace("-", "")
      url = f"{SEC_BASE_URL}/Archives/edgar/data/{cik_no_zeros}/{accno_no_dash}/{hit.accession_number}-xbrl.zip"

      async with semaphore:
        async with limiter:
          try:
            async with aiohttp.ClientSession(headers=SEC_HEADERS) as session:
              async with session.get(url) as response:
                if response.status == 404:
                  # No XBRL ZIP for this filing
                  no_xbrl += 1
                  no_xbrl_filings.append(f"{hit.cik}/{hit.accession_number}")
                  return True

                if response.status == 429:
                  retry_after = int(response.headers.get("Retry-After", 60))
                  context.log.warning(f"Rate limited, waiting {retry_after}s")
                  await asyncio.sleep(retry_after)
                  return await download_filing(hit)

                response.raise_for_status()
                content = await response.read()

                if not content:
                  failed += 1
                  return False

                await monitor.record(len(content))

          except Exception as e:
            context.log.debug(f"Download failed for {hit.accession_number}: {e}")
            failed += 1
            return False

      # Upload to S3
      try:
        s3.client.put_object(
          Bucket=bucket,
          Key=s3_key,
          Body=content,
          ContentType="application/zip",
        )
        downloaded += 1
        return True
      except Exception as e:
        context.log.warning(f"S3 upload failed for {hit.accession_number}: {e}")
        failed += 1
        return False

    # Execute downloads with progress logging
    tasks = [download_filing(hit) for hit in hits]
    completed = 0

    for coro in asyncio.as_completed(tasks):
      await coro
      completed += 1
      if completed % 100 == 0:
        stats = monitor.get_stats()
        context.log.info(
          f"Progress: {completed}/{len(hits)} "
          f"({stats.requests_per_second} req/s, {stats.mb_per_second} MB/s) "
          f"[{downloaded} new, {skipped} cached, {no_xbrl} no XBRL, {failed} failed]"
        )

    # Log filings without XBRL (limit to first 20 to avoid log spam)
    if no_xbrl_filings:
      sample = no_xbrl_filings[:20]
      context.log.info(
        f"Filings without XBRL ZIP ({no_xbrl} total): {sample}"
        + (f" ... and {no_xbrl - 20} more" if no_xbrl > 20 else "")
      )

    return {
      "filings_found": len(hits),
      "submissions_fetched": submissions_fetched,
      "downloaded": downloaded,
      "skipped": skipped,
      "no_xbrl": no_xbrl,
      "failed": failed,
      "dry_run": False,
    }

  # Run async code in sync Dagster context
  result = asyncio.run(run_efts_download())

  if result.get("dry_run"):
    context.log.info(
      f"[DRY RUN] Discovery complete for {year}-Q{quarter}: {result['filings_found']} filings found"
    )
  else:
    context.log.info(
      f"Download complete for {year}-Q{quarter}: "
      f"{result['downloaded']} downloaded, {result['skipped']} cached, "
      f"{result.get('no_xbrl', 0)} no XBRL, {result['failed']} failed"
    )

  return MaterializeResult(
    metadata={
      "year": year,
      "quarter": quarter,
      "filings_found": result["filings_found"],
      "submissions_fetched": result.get("submissions_fetched", 0),
      "filings_downloaded": result["downloaded"],
      "filings_cached": result["skipped"],
      "filings_no_xbrl": result.get("no_xbrl", 0),
      "errors": result["failed"],
      "dry_run": result.get("dry_run", False),
    }
  )


# ============================================================================
# Dynamic Partition Assets (per-filing processing)
# ============================================================================


@asset(
  group_name="sec_pipeline",
  description="Process a single SEC filing to parquet format",
  kinds={"transform"},
  partitions_def=sec_filing_partitions,
  # No deps - sensor handles discovery and triggers runs directly
  metadata={
    "pipeline": "sec",
    "stage": "processing",
  },
  # Retry once on failure (handles transient OOM on large filings)
  retry_policy=RetryPolicy(max_retries=1),
  # No concurrency limit - scales with infrastructure
  # Each filing processes independently
)
def sec_process_filing(
  context: AssetExecutionContext,
  config: SECSingleFilingConfig,
  s3: S3Resource,
) -> MaterializeResult:
  """Process a single SEC filing to parquet format.

  Takes partition key in format {year}_{cik}_{accession} and processes
  the corresponding raw ZIP file to parquet output.

  Returns:
      MaterializeResult with processing statistics
  """
  from robosystems.adapters.sec import XBRLGraphProcessor

  # Parse partition key: {year}_{cik}_{accession}
  partition_key = context.partition_key
  parts = partition_key.split("_", 2)  # Split into 3 parts max
  if len(parts) != 3:
    context.log.error(f"Invalid partition key format: {partition_key}")
    return MaterializeResult(
      metadata={"status": "error", "reason": f"Invalid partition key: {partition_key}"}
    )

  year, cik, accession = parts
  context.log.info(f"Processing filing: year={year}, cik={cik}, accession={accession}")

  raw_bucket = env.SHARED_RAW_BUCKET
  processed_bucket = env.SHARED_PROCESSED_BUCKET

  # Download raw ZIP from shared bucket
  raw_key = get_raw_key(DataSourceType.SEC, f"year={year}", cik, f"{accession}.zip")

  import os
  import tempfile
  import zipfile
  from io import BytesIO

  try:
    buffer = BytesIO()
    s3.client.download_fileobj(raw_bucket, raw_key, buffer)
    buffer.seek(0)
  except Exception as e:
    context.log.error(f"Failed to download {raw_key}: {e}")
    return MaterializeResult(
      metadata={"status": "error", "reason": f"Download failed: {e}"}
    )

  # Extract and process
  try:
    with tempfile.TemporaryDirectory() as tmpdir:
      with zipfile.ZipFile(buffer, "r") as zf:
        zf.extractall(tmpdir)

      # Find main XBRL instance file
      exclude_suffixes = ("_def.xml", "_lab.xml", "_pre.xml", "_cal.xml", ".xsd")
      all_files = os.listdir(tmpdir)
      xbrl_files = [
        f
        for f in all_files
        if f.endswith((".xml", ".htm", ".html"))
        and not any(f.endswith(suffix) for suffix in exclude_suffixes)
      ]

      # Prefer .htm files for inline XBRL
      htm_files = [f for f in xbrl_files if f.endswith((".htm", ".html"))]
      if htm_files:
        xbrl_files = sorted(
          htm_files,
          key=lambda f: os.path.getsize(os.path.join(tmpdir, f)),
          reverse=True,
        )

      if not xbrl_files:
        context.log.warning(f"No XBRL instance files found in {raw_key}")
        return MaterializeResult(
          metadata={"status": "error", "reason": "No XBRL files found"}
        )

      # Build report URL
      from robosystems.adapters.sec import SEC_BASE_URL

      report_url = f"{SEC_BASE_URL}/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/{xbrl_files[0]}"

      # Schema config
      schema_config = {
        "name": "SEC Database Schema",
        "description": "Complete financial reporting schema with XBRL taxonomy support",
        "base_schema": "base",
        "extensions": ["roboledger"],
      }

      # Fetch full SEC metadata from S3 snapshot (stored during download)
      sec_filer, sec_report = _get_sec_metadata(
        cik, accession, s3_client=s3.client, bucket=raw_bucket
      )
      # Ensure primaryDocument is set from local files if not in API response
      if not sec_report.get("primaryDocument"):
        sec_report["primaryDocument"] = xbrl_files[0]

      # Process with XBRLGraphProcessor
      processor = XBRLGraphProcessor(
        report_uri=report_url,
        entityId=cik,
        sec_filer=sec_filer,
        sec_report=sec_report,
        output_dir=tmpdir,
        local_file_path=os.path.join(tmpdir, xbrl_files[0]),
        schema_config=schema_config,
      )

      processor.process()

      # Upload parquet files to S3
      # Use filing date for partition (filed=YYYY-MM-DD) instead of year
      # This enables date-range filtering for incremental loads
      filing_date = sec_report.get("filingDate", "unknown")
      if filing_date == "unknown":
        context.log.warning(
          f"No filingDate found for {partition_key}, using 'unknown' partition"
        )

      files_uploaded = 0
      for entity_type in ["nodes", "relationships"]:
        entity_dir = os.path.join(tmpdir, entity_type)
        if os.path.exists(entity_dir):
          for parquet_file in os.listdir(entity_dir):
            if parquet_file.endswith(".parquet"):
              local_path = os.path.join(entity_dir, parquet_file)
              table_name = parquet_file.replace(".parquet", "")
              s3_key = get_processed_key(
                DataSourceType.SEC,
                "processed",
                f"filed={filing_date}",
                entity_type,
                table_name,
                f"{cik}_{accession}.parquet",
              )

              with open(local_path, "rb") as f:
                s3.client.upload_fileobj(f, processed_bucket, s3_key)
              files_uploaded += 1

      context.log.info(f"Processed {partition_key}: {files_uploaded} files uploaded")

      return MaterializeResult(
        metadata={
          "partition_key": partition_key,
          "year": year,
          "cik": cik,
          "accession": accession,
          "files_uploaded": files_uploaded,
          "status": "success",
        }
      )

  except Exception as e:
    context.log.error(f"Processing failed for {partition_key}: {e}")
    return MaterializeResult(
      metadata={
        "partition_key": partition_key,
        "status": "error",
        "reason": str(e),
      }
    )


# ============================================================================
# Graph Materialization Assets (Two-Stage Pipeline)
# ============================================================================
# Two-stage pipeline enables retry of materialization without re-staging:
# 1. sec_duckdb_staged: Stage processed files to persistent DuckDB
# 2. sec_graph_materialized: Materialize from DuckDB to LadybugDB (retry-safe)
#
# Key benefit: If LadybugDB materialization fails, retry without re-staging.


@asset(
  group_name="sec_pipeline",
  description="Stage SEC processed files to persistent DuckDB (full or incremental)",
  kinds={"duckdb"},
  metadata={
    "pipeline": "sec",
    "stage": "staging",
    "decoupled": True,
  },
)
def sec_duckdb_staged(
  context: AssetExecutionContext,
  config: SECStageConfig,
) -> MaterializeResult:
  """Stage SEC processed files to persistent DuckDB.

  Supports two modes:
  - full: Creates tables from scratch, discovers all processed files
  - incremental: Appends new filings to existing tables (INSERT INTO)

  Full mode (default):
  - Persists DuckDB to disk (survives job restarts)
  - Writes manifest for tracking staged tables
  - Can optionally rebuild graph and reset staging

  Incremental mode:
  - Requires initial full staging to exist
  - Uses filing_date filter for targeted updates
  - Records progress to _sec_staging_progress table

  Run with:
    just dagster-materialize sec_duckdb_staged  # full mode
    # incremental via Dagster UI with config: {"mode": "incremental", "filing_date": "2026-01-15"}

  Returns:
      MaterializeResult with staging statistics
  """
  import asyncio

  from robosystems.adapters.sec import XBRLDuckDBGraphProcessor
  from robosystems.operations.graph.shared_repository_service import (
    ensure_shared_repository_exists,
  )

  is_incremental = config.mode == "incremental"
  mode_str = "incremental" if is_incremental else "full"

  context.log.info(
    f"Staging SEC data to DuckDB for graph: {config.graph_id} (mode={mode_str})"
  )

  if is_incremental:
    context.log.info(f"Filing date filter: {config.filing_date or 'none'}")

  processor = XBRLDuckDBGraphProcessor(graph_id=config.graph_id)

  async def run_staging():
    if not is_incremental:
      # Full mode: ensure repository exists
      context.log.info("Ensuring SEC repository metadata exists...")
      repo_result = await ensure_shared_repository_exists(
        repository_name=config.graph_id,
        created_by="system",
        instance_id="local-dev" if env.ENVIRONMENT == "dev" else "ladybug-shared-prod",
      )
      context.log.info(f"SEC repository status: {repo_result.get('status', 'unknown')}")

    # Run staging with appropriate parameters
    result = await processor.stage_to_duckdb(
      rebuild=False if is_incremental else config.rebuild_graph,
      year=None if is_incremental else config.year,
      reset_staging=False if is_incremental else config.reset_staging,
      filing_date=config.filing_date if is_incremental else None,
      incremental=is_incremental,
    )
    return result

  result = asyncio.run(run_staging())

  if result.status == "error":
    context.log.error(f"Staging failed: {result.error}")
    return MaterializeResult(
      metadata={
        "graph_id": config.graph_id,
        "mode": mode_str,
        "status": "error",
        "error": result.error,
        "duration_seconds": result.duration_seconds,
      }
    )

  if result.status == "already_staged":
    context.log.info(
      f"Filing date {config.filing_date} already staged - skipped to prevent duplicates"
    )
    return MaterializeResult(
      metadata={
        "graph_id": config.graph_id,
        "mode": mode_str,
        "status": "already_staged",
        "filing_date": config.filing_date,
        "message": "Date already staged, skipped to prevent duplicates",
        "duration_seconds": result.duration_seconds,
      }
    )

  context.log.info(
    f"Staging complete ({mode_str}): {len(result.table_names)} tables, "
    f"{result.total_files} files, {result.duration_seconds:.2f}s"
  )

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "mode": mode_str,
      "status": result.status,
      "tables_staged": len(result.table_names),
      "table_names": result.table_names,
      "total_files": result.total_files,
      "total_rows": result.total_rows,
      "duckdb_path": result.duckdb_path,
      "duration_seconds": result.duration_seconds,
    }
  )


@asset(
  group_name="sec_pipeline",
  description="Materialize LadybugDB graph from staged DuckDB (Stage 2)",
  kinds={"ladybug"},
  deps=["sec_duckdb_staged"],  # Explicit dependency on staging
  metadata={
    "pipeline": "sec",
    "stage": "materialization",
  },
)
def sec_graph_materialized(
  context: AssetExecutionContext,
  config: SECMaterializeConfig,
) -> MaterializeResult:
  """Materialize LadybugDB graph from DuckDB staging.

  This is Stage 2 of the pipeline. It reads from the persistent
  DuckDB staging tables and materializes to LadybugDB.

  Precondition: sec_duckdb_staged must have completed successfully,
  creating a valid staging manifest.

  Key features:
  - Reads from persisted DuckDB (no S3 access needed)
  - Can be retried independently if materialization fails
  - Uses manifest to verify staging completeness

  Run with: just dagster-materialize sec_graph_materialized

  Returns:
      MaterializeResult with materialization statistics
  """
  import asyncio

  from robosystems.adapters.sec import XBRLDuckDBGraphProcessor

  context.log.info(f"Materializing graph from DuckDB staging: {config.graph_id}")
  if config.rebuild_graph:
    context.log.info("Rebuild requested - will delete and recreate LadybugDB database")

  processor = XBRLDuckDBGraphProcessor(graph_id=config.graph_id)

  async def run_materialization():
    result = await processor.materialize_from_duckdb(rebuild=config.rebuild_graph)
    return result

  result = asyncio.run(run_materialization())

  if result.status == "error":
    context.log.error(f"Materialization failed: {result.error}")
    return MaterializeResult(
      metadata={
        "graph_id": config.graph_id,
        "status": "error",
        "error": result.error,
      }
    )

  context.log.info(
    f"Materialization complete: {result.total_rows_ingested} rows, "
    f"{result.total_time_ms:.2f}ms"
  )

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "rebuild_graph": config.rebuild_graph,
      "status": result.status,
      "rows_ingested": result.total_rows_ingested,
      "execution_time_ms": result.total_time_ms,
      "tables": result.tables,
    }
  )
