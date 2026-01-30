"""SEC XBRL pipeline Dagster assets.

Pipeline stages (run independently via separate jobs):

1. DOWNLOAD (sec_download job):
   - sec_raw_filings - Discover via EFTS, download XBRL ZIPs (quarterly partitions)
   - Creates SourceFile records in PostgreSQL for processing tracking

2. PROCESS (sec_process job, quarterly partitions):
   - sec_processed_filings - Process entire quarter's filings as batch
   - Outputs consolidated parquet files (one per table per quarter)
   - Individual failures tracked in SourceFile; job continues processing
   - Parallel across quarters via DAGSTER_MAX_CONCURRENT_RUNS

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
- Quarterly partitioning for downloads AND processing
- SourceFile table tracks processing state (pending/processing/success/error)
- Consolidated parquet output for efficient DuckDB staging
- Graph materialization always rebuilds from all processed data
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from dagster import (
  AssetExecutionContext,
  BackfillPolicy,
  Config,
  MaterializeResult,
  StaticPartitionsDefinition,
  asset,
)

from robosystems.config import env
from robosystems.config.constants import SEC_PROCESS_BATCH_LIMIT
from robosystems.config.storage.shared import (
  DataSourceType,
  get_processed_key,
  get_raw_key,
)
from robosystems.dagster.resources import DatabaseResource, S3Resource
from robosystems.models.iam import Graph, SourceFile

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


class SECProcessConfig(Config):
  """Configuration for batch filing processing by quarter.

  Each Dagster run processes up to batch_limit filings and then exits.
  The sensor will trigger another run if pending files remain, enabling
  natural memory release between batches and better crash resilience.

  Individual filing failures are tracked in SourceFile records,
  but the job continues processing remaining filings in the batch.

  Memory Management:
  - Each job processes at most batch_limit filings (hardcoded at 10,000)
  - Job exits gracefully after batch, releasing all memory
  - Sensor re-triggers if more pending files exist
  - This prevents memory accumulation across thousands of filings

  Note: batch_limit is fixed at 10,000 - the maximum EFTS returns per
  quarterly partition. The heavy job profile (16 vCPU, 64 GB) handles this.
  """

  # Max filings to process per job run before exiting gracefully.
  # Fixed at 10,000 - the max EFTS can return per quarterly partition.
  # The heavy job profile (16 vCPU, 64 GB) is sized to handle full quarters.
  batch_limit: int = SEC_PROCESS_BATCH_LIMIT

  # Continue processing even if some filings fail
  # If False, job fails on first error (for debugging)
  continue_on_error: bool = True

  # Partition output by actual filing date (for incremental daily processing)
  # False (default): All filings output to quarter-end date (2024-03-31 for Q1)
  #   - Best for backfills: large consolidated files, efficient DuckDB ingest
  # True: Each filing outputs to its actual filing date (2024-01-15, 2024-01-16, etc.)
  #   - Best for incremental: new filings stage to their specific dates
  use_filing_date_partition: bool = False


class SECStageConfig(Config):
  """Configuration for DuckDB staging (full rebuild).

  Creates DuckDB tables from scratch using all S3 parquet files.

  Note: This step only stages data to DuckDB. LadybugDB rebuild is handled
  by the materialize step (sec_graph_materialized) via SECMaterializeConfig.rebuild_graph.
  """

  graph_id: str = "sec"  # Target graph ID
  year: int | None = None  # Optional year filter
  reset_staging: bool = False  # Clear all DuckDB tables first (use True if schema changed or DuckDB corrupted)
  skip_taxonomy_relationships: bool = False  # Skip taxonomy structure tables (Association, Structure, TAXONOMY_HAS_*, etc.) to reduce storage


class SECMaterializeConfig(Config):
  """Configuration for graph materialization (Stage 2).

  Use this config with sec_graph_materialized asset to materialize
  from DuckDB staging to LadybugDB.

  Options:
    graph_id: Target graph ID (default: "sec")
    rebuild_graph: If True (default), delete and recreate the LadybugDB database
                   with the roboledger SEC schema before materializing.
                   DuckDB staging is preserved. Set to False only for retry scenarios
                   where you want to resume without losing existing graph data.
    skip_taxonomy_relationships: If True, skip materializing taxonomy structure
                                 tables (Association, Structure, TAXONOMY_HAS_*, etc.)
  """

  graph_id: str = "sec"  # Target graph ID
  rebuild_graph: bool = (
    True  # Rebuild LadybugDB before materialization (avoids duplicates)
  )
  skip_taxonomy_relationships: bool = (
    False  # Skip taxonomy structure tables to reduce storage
  )


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
  db: DatabaseResource,
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

  # Get all partition keys (handles both single partition and backfill ranges)
  partition_keys = context.partition_keys

  # Multi-partition backfills are not supported - process one quarter at a time
  if len(partition_keys) > 1:
    context.log.warning(
      f"Multi-partition backfill not supported. Selected {len(partition_keys)} partitions: {partition_keys}. "
      "Please run each quarter as a separate materialization."
    )
    raise ValueError(
      f"Multi-partition backfill not supported. Please select one quarter at a time. "
      f"Selected: {partition_keys}"
    )

  partition_key = partition_keys[0]

  # Parse partition key: "2024-Q1" -> year=2024, quarter=1
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

    # Phase 1: Discover filings via EFTS
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

    # Phase 2: Fetch submissions data for unique CIKs (parallel with rate limiting)
    # This provides company metadata (name, SIC, fiscal year end, etc.)
    unique_ciks = list({hit.cik for hit in hits})
    context.log.info(
      f"Phase 2: Fetching submissions for {len(unique_ciks)} unique companies..."
    )

    # Always refresh submissions for CIKs with discovered filings.
    # The skip_existing flag controls ZIP downloads, not submissions metadata.
    # New filings discovered via EFTS may not be in stale submissions snapshots,
    # so we always do an incremental update (or full build if no existing file).
    ciks_to_fetch = unique_ciks

    context.log.info(f"Submissions: {len(ciks_to_fetch)} to refresh")

    submissions_fetched = 0
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

    # Phase 3: Download filings with async rate limiting
    context.log.info(f"Phase 3: Downloading {len(hits)} filings...")

    limiter = AsyncRateLimiter(rate=config.download_rate)
    monitor = RateMonitor()
    semaphore = asyncio.Semaphore(config.download_concurrency)

    downloaded = 0
    skipped = 0  # Already exists in S3
    no_xbrl = 0  # Filing exists but no XBRL ZIP available
    no_xbrl_filings: list[str] = []  # Track which filings lack XBRL
    failed = 0

    # Track files for SourceFile creation
    source_file_records: list[dict] = []

    async def download_filing(hit: EFTSHit) -> bool:
      nonlocal downloaded, skipped, no_xbrl, failed, source_file_records

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
          head_response = s3.client.head_object(Bucket=bucket, Key=s3_key)
          skipped += 1
          # Track for SourceFile creation (existing file)
          # Include quarter in partition_key for accurate batch processing
          partition_key = f"{year}-Q{quarter}_{hit.cik}_{hit.accession_number}"
          source_file_records.append(
            {
              "storage_key": s3_key,
              "source_id": hit.accession_number,
              "partition_key": partition_key,
              "file_size_bytes": head_response.get("ContentLength"),
            }
          )
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
        # Track for SourceFile creation (newly downloaded)
        # Include quarter in partition_key for accurate batch processing
        partition_key = f"{year}-Q{quarter}_{hit.cik}_{hit.accession_number}"
        source_file_records.append(
          {
            "storage_key": s3_key,
            "source_id": hit.accession_number,
            "partition_key": partition_key,
            "file_size_bytes": len(content),
          }
        )
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
      "source_file_records": source_file_records,
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

  # Create SourceFile records for downloaded/cached files
  source_file_records = result.get("source_file_records", [])
  source_files_created = 0
  source_files_existed = 0
  if source_file_records and not result.get("dry_run"):
    context.log.info(f"Creating {len(source_file_records)} SourceFile records...")
    with db.get_session() as session:
      # Ensure SEC graph exists (SourceFile has FK to graphs table)
      Graph.find_or_create_repository(
        graph_id="sec",
        graph_name="SEC EDGAR Filings",
        repository_type="sec",
        session=session,
        base_schema="sec",
        data_source_type="sec_edgar",
        data_source_url="https://www.sec.gov/cgi-bin/browse-edgar",
        sync_frequency="daily",
      )
      for record in source_file_records:
        _, created = SourceFile.get_or_create(
          graph_id="sec",
          storage_key=record["storage_key"],
          file_type="xbrl_filing",
          session=session,
          file_size_bytes=record.get("file_size_bytes"),
          source_id=record.get("source_id"),
          partition_key=record.get("partition_key"),
          commit=False,
        )
        if created:
          source_files_created += 1
        else:
          source_files_existed += 1
      session.commit()
    context.log.info(
      f"SourceFile records: {source_files_created} created, "
      f"{source_files_existed} already existed"
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
      "source_files_created": source_files_created,
      "source_files_existed": source_files_existed,
    }
  )


# ============================================================================
# Batch Processing Asset (SourceFile-driven)
# ============================================================================


@dataclass
class ProcessedFilingResult:
  """Result from processing a single filing."""

  success: bool
  source_file_id: str
  partition_key: str
  tables: dict[str, bytes]  # table_name -> parquet bytes
  filing_date: str | None = None  # YYYY-MM-DD from SEC metadata
  error: str | None = None


def _process_single_filing_to_memory(
  storage_key: str,
  partition_key: str,
  source_file_id: str,
  s3_client,
  raw_bucket: str,
) -> ProcessedFilingResult:
  """Process a single filing and return parquet data in memory.

  This function processes a filing but does NOT write to S3 or update SourceFile status.
  The caller is responsible for:
  - Consolidating results from multiple filings
  - Writing consolidated parquet files
  - Updating SourceFile status

  Args:
      storage_key: S3 key of the raw file
      partition_key: Partition key (year_cik_accession)
      source_file_id: SourceFile ID for tracking
      s3_client: boto3 S3 client
      raw_bucket: S3 bucket for raw files

  Returns:
      ProcessedFilingResult with parquet data or error
  """
  import gc
  import os
  import tempfile
  import zipfile
  from io import BytesIO

  from robosystems.adapters.sec import SEC_BASE_URL, XBRLGraphProcessor

  # Parse partition key to get year, cik, accession
  parts = partition_key.split("_", 2)
  if len(parts) != 3:
    return ProcessedFilingResult(
      success=False,
      source_file_id=source_file_id,
      partition_key=partition_key,
      tables={},
      error=f"Invalid partition key: {partition_key}",
    )

  year, cik, accession = parts

  # Download raw ZIP
  try:
    buffer = BytesIO()
    s3_client.download_fileobj(raw_bucket, storage_key, buffer)
    buffer.seek(0)
  except Exception as e:
    return ProcessedFilingResult(
      success=False,
      source_file_id=source_file_id,
      partition_key=partition_key,
      tables={},
      error=f"Download failed: {e}",
    )

  # Extract and process
  processor = None
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
        return ProcessedFilingResult(
          success=False,
          source_file_id=source_file_id,
          partition_key=partition_key,
          tables={},
          error="No XBRL files found",
        )

      # Build report URL
      cik_int = int(cik)
      accno_clean = accession.replace("-", "")
      report_url = (
        f"{SEC_BASE_URL}/Archives/edgar/data/{cik_int}/{accno_clean}/{xbrl_files[0]}"
      )

      # Schema config
      schema_config = {
        "name": "SEC Database Schema",
        "description": "Complete financial reporting schema with XBRL taxonomy support",
        "base_schema": "base",
        "extensions": ["roboledger"],
      }

      # Fetch full SEC metadata from S3 snapshot
      sec_filer, sec_report = _get_sec_metadata(
        cik, accession, s3_client=s3_client, bucket=raw_bucket
      )
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

      # Extract filing date from SEC metadata for output partitioning
      filing_date = sec_report.get("filingDate")

      # Collect parquet files as bytes (don't write to S3 yet)
      tables: dict[str, bytes] = {}

      for entity_type in ["nodes", "relationships"]:
        entity_dir = os.path.join(tmpdir, entity_type)
        if os.path.exists(entity_dir):
          for parquet_file in os.listdir(entity_dir):
            if parquet_file.endswith(".parquet"):
              local_path = os.path.join(entity_dir, parquet_file)
              table_name = parquet_file.replace(".parquet", "")
              # Key format: "nodes/TableName" or "relationships/RelName"
              key = f"{entity_type}/{table_name}"
              with open(local_path, "rb") as f:
                tables[key] = f.read()

      return ProcessedFilingResult(
        success=True,
        source_file_id=source_file_id,
        partition_key=partition_key,
        tables=tables,
        filing_date=filing_date,
        error=None,
      )

  except Exception as e:
    return ProcessedFilingResult(
      success=False,
      source_file_id=source_file_id,
      partition_key=partition_key,
      tables={},
      filing_date=None,
      error=str(e),
    )
  finally:
    # Always close the buffer to release memory
    buffer.close()
    # Clean up processor if it was created (releases DataFrames, etc.)
    if processor is not None:
      del processor
    gc.collect()


def _consolidate_parquet_tables_by_date(
  results: list[ProcessedFilingResult],
) -> dict[str, dict[str, bytes]]:
  """Consolidate parquet tables from multiple filing results, grouped by filing date.

  Takes a list of ProcessedFilingResult objects and merges all tables
  of the same type into single consolidated parquet files, organized by filing date.

  Note: This function is used by the local sec_pipeline.py script for development/testing.
  The Dagster asset uses disk-buffered processing instead for memory efficiency at scale.

  Args:
      results: List of successful ProcessedFilingResult objects

  Returns:
      Dict mapping filing_date -> table_key -> consolidated parquet bytes
      Example: {"2024-01-15": {"nodes/Entity": bytes, "nodes/Fact": bytes, ...}}
  """
  from io import BytesIO

  import pyarrow as pa
  import pyarrow.parquet as pq

  # Group results by filing date, then by table type
  # Structure: {filing_date: {table_key: [pa.Table, ...]}}
  tables_by_date_and_key: dict[str, dict[str, list[pa.Table]]] = {}

  for result in results:
    if not result.success:
      continue

    # Use filing_date from SEC metadata, fallback to "unknown" if missing
    filing_date = result.filing_date or "unknown"

    if filing_date not in tables_by_date_and_key:
      tables_by_date_and_key[filing_date] = {}

    for key, parquet_bytes in result.tables.items():
      if key not in tables_by_date_and_key[filing_date]:
        tables_by_date_and_key[filing_date][key] = []
      # Read parquet bytes into PyArrow table
      reader = pq.ParquetFile(BytesIO(parquet_bytes))
      table = reader.read()
      tables_by_date_and_key[filing_date][key].append(table)

  # Shared node tables that should be deduplicated across filings
  # These have deterministic IDs (UUID5) so same content = same identifier
  SHARED_NODE_TABLES = frozenset(
    {
      "nodes/Element",
      "nodes/Label",
      "nodes/Reference",
      "nodes/Unit",
      "nodes/Period",
    }
  )

  # Consolidate each table type for each filing date
  consolidated: dict[str, dict[str, bytes]] = {}

  for filing_date, tables_by_key in tables_by_date_and_key.items():
    consolidated[filing_date] = {}
    for key, tables in tables_by_key.items():
      if not tables:
        continue
      # Concatenate all tables of this type for this date
      combined = pa.concat_tables(tables, promote_options="permissive")

      # Deduplicate shared node tables on identifier column
      # This reduces duplicates that DuckDB would otherwise have to handle
      if key in SHARED_NODE_TABLES and "identifier" in combined.column_names:
        df = combined.to_pandas()
        df = df.drop_duplicates(subset=["identifier"], keep="first")
        combined = pa.Table.from_pandas(df, preserve_index=False)

      # Write to bytes
      buffer = BytesIO()
      pq.write_table(combined, buffer)
      consolidated[filing_date][key] = buffer.getvalue()

  return consolidated


def _get_quarter_end_date(year: int, quarter: int) -> str:
  """Get the last day of a quarter as YYYY-MM-DD string.

  Used for backfill partitioning - all filings in a quarter get the same
  partition date (end of quarter) for simpler S3 organization.

  Args:
      year: Calendar year
      quarter: Quarter number (1-4)

  Returns:
      Date string like "2024-03-31" for Q1 2024
  """
  quarter_end_days = {
    1: f"{year}-03-31",
    2: f"{year}-06-30",
    3: f"{year}-09-30",
    4: f"{year}-12-31",
  }
  return quarter_end_days[quarter]


def _consolidate_parquet_from_disk(
  work_dir: Path,
  table_key: str,
) -> bytes:
  """Consolidate all parquet files for a table from disk into single bytes.

  For shared node tables (Element, Label, Reference, Unit, Period), this also
  deduplicates on the identifier column to reduce memory pressure during
  DuckDB staging. These tables have deterministic UUIDs, so duplicates across
  filings are guaranteed to have the same identifier.

  Args:
      work_dir: Directory containing parquet files
      table_key: Table key like "nodes/Entity"

  Returns:
      Consolidated parquet bytes
  """
  from io import BytesIO

  import pyarrow as pa
  import pyarrow.parquet as pq

  # Shared node tables that should be deduplicated across filings
  # These have deterministic IDs (UUID5) so same content = same identifier
  SHARED_NODE_TABLES = frozenset(
    {
      "nodes/Element",
      "nodes/Label",
      "nodes/Reference",
      "nodes/Unit",
      "nodes/Period",
    }
  )

  table_dir = work_dir / table_key
  if not table_dir.exists():
    return b""

  parquet_files = list(table_dir.glob("*.parquet"))
  if not parquet_files:
    return b""

  # Read all parquet files into PyArrow tables
  tables = []
  for pq_file in parquet_files:
    try:
      table = pq.read_table(pq_file)
      tables.append(table)
    except Exception:
      # Skip corrupted files
      continue

  if not tables:
    return b""

  # Concatenate all tables
  combined = pa.concat_tables(tables, promote_options="permissive")

  # Deduplicate shared node tables on identifier column
  # This reduces duplicates that DuckDB would otherwise have to handle via
  # memory-intensive ROW_NUMBER() window functions during staging
  if table_key in SHARED_NODE_TABLES and "identifier" in combined.column_names:
    original_rows = combined.num_rows
    # Convert to pandas for deduplication, then back to Arrow
    df = combined.to_pandas()
    df = df.drop_duplicates(subset=["identifier"], keep="first")
    combined = pa.Table.from_pandas(df, preserve_index=False)
    deduped_rows = combined.num_rows
    if original_rows != deduped_rows:
      # Log would go here in production, but we're in a static function
      # The reduction will be visible in the final row counts
      pass

  # Write to bytes
  buffer = BytesIO()
  pq.write_table(combined, buffer)
  return buffer.getvalue()


@asset(
  group_name="sec_pipeline",
  description="Process batch of SEC filings with disk-buffered consolidation",
  kinds={"transform"},
  partitions_def=sec_quarter_partitions,
  metadata={
    "pipeline": "sec",
    "stage": "processing",
  },
  # Run all partitions sequentially in a single run to prevent memory exhaustion
  backfill_policy=BackfillPolicy.single_run(),
)
def sec_processed_filings(
  context: AssetExecutionContext,
  config: SECProcessConfig,
  s3: S3Resource,
  db: DatabaseResource,
) -> MaterializeResult:
  """Process a batch of SEC filings (up to batch_limit per run).

  This asset processes up to batch_limit pending SourceFiles (default 500),
  then exits gracefully. The sensor will trigger another run if pending
  files remain, enabling natural memory release between batches.

  Batch Processing Model:
  - Each job processes at most batch_limit filings (default 500)
  - Job exits after batch, container terminates, memory released
  - Sensor detects remaining pending files, triggers next batch
  - Continues until all filings are processed

  Memory Management:
  - Bounded to batch_limit filings per container lifecycle
  - Each filing's parquet written to local disk, not accumulated in memory
  - Container exit between batches releases all memory naturally
  - Much safer than processing thousands of filings in one container

  Output Structure (controlled by use_filing_date_partition config):
  - Backfill mode (default, use_filing_date_partition=False):
    s3://bucket/sec/processed/filed=2024-03-31/nodes/Entity/part-001.parquet
    All filings in quarter → quarter-end date → large consolidated files
  - Daily mode (use_filing_date_partition=True):
    s3://bucket/sec/processed/filed=2024-01-15/nodes/Entity/part-001.parquet
    Each filing → its actual filing date → consolidated per-date files

  Returns:
      MaterializeResult with processing statistics
  """
  import shutil
  import tempfile
  import time as time_module

  from sqlalchemy import and_

  raw_bucket = env.SHARED_RAW_BUCKET
  processed_bucket = env.SHARED_PROCESSED_BUCKET

  # Parse partition key: "2024-Q1" -> year=2024, quarter=1
  partition_key = context.partition_key
  year, quarter_str = partition_key.split("-Q")
  year = int(year)
  quarter = int(quarter_str)

  # Determine partition mode
  use_daily = config.use_filing_date_partition
  partition_mode = "daily" if use_daily else "quarterly"

  # For quarterly mode, use end-of-quarter date as partition
  # For daily mode, we'll use each filing's actual filing date
  default_partition_date = _get_quarter_end_date(year, quarter)

  context.log.info(
    f"Processing SEC filings for {year}-Q{quarter} "
    f"(partition_mode={partition_mode}, default={default_partition_date})"
  )

  # Query pending SourceFiles for this quarter
  # Partition keys are stored as "YYYY-QN_cik_accession" format
  quarter_prefix = f"{year}-Q{quarter}_"

  # Reset any stale "processing" files back to "pending" before starting.
  # This handles recovery from crashed runs. Safe because the sensor ensures
  # only one worker runs per partition at a time.
  with db.get_session() as session:
    stale_processing = (
      session.query(SourceFile)
      .filter(
        and_(
          SourceFile.graph_id == "sec",
          SourceFile.status == "processing",
          SourceFile.partition_key.like(f"{quarter_prefix}%"),
        )
      )
      .all()
    )
    if stale_processing:
      context.log.info(
        f"Resetting {len(stale_processing)} stale 'processing' files to 'pending'"
      )
      for sf in stale_processing:
        sf.status = "pending"
      session.commit()

  with db.get_session() as session:
    # Query pending files, ordered by discovery time, limited to batch_limit.
    # Sensor will re-trigger if more pending files exist after this batch.
    pending_files = (
      session.query(SourceFile)
      .filter(
        and_(
          SourceFile.graph_id == "sec",
          SourceFile.status == "pending",
          SourceFile.partition_key.like(f"{quarter_prefix}%"),
        )
      )
      .order_by(SourceFile.discovered_at.asc())
      .limit(config.batch_limit)
      .all()
    )

    # Extract data while session is open (avoid DetachedInstanceError)
    files_to_process = [
      {
        "id": sf.id,
        "storage_key": sf.storage_key,
        "partition_key": sf.partition_key or sf.storage_key,
      }
      for sf in pending_files
    ]

  if not files_to_process:
    context.log.info(f"No pending files for {year}-Q{quarter}")
    return MaterializeResult(
      metadata={
        "year": year,
        "quarter": quarter,
        "partition_mode": partition_mode,
        "default_partition_date": default_partition_date,
        "status": "no_pending_files",
        "filings_processed": 0,
        "filings_succeeded": 0,
        "filings_failed": 0,
      }
    )

  context.log.info(
    f"Processing batch of {len(files_to_process)} filings for {year}-Q{quarter} "
    f"(batch_limit={config.batch_limit}, partition_mode={partition_mode})"
  )

  # Create work directory for disk-buffered processing
  work_dir = Path(tempfile.mkdtemp(prefix=f"sec_processing_{year}Q{quarter}_"))
  context.log.info(f"Work directory: {work_dir}")

  # Track processing state
  succeeded = 0
  failed = 0
  failed_ids: list[str] = []
  # Track pending filings with their filing dates for daily mode
  pending_flush: list[dict] = []  # [{...file_info, "filing_date": "2024-01-15"}, ...]
  total_flushed = 0
  files_uploaded = 0
  flush_number = 0  # Counter for part file naming
  output_dates: set[str] = set()  # Track which dates received output

  # Use run_id prefix for unique part filenames to avoid collisions from crashed runs.
  # Format: part-{run_id[:8]}-{flush_number:03d}.parquet
  run_id_prefix = context.run_id[:8] if context.run_id else "local"

  def flush_to_s3() -> int:
    """Consolidate disk buffer and upload to S3, mark filings as success."""
    nonlocal files_uploaded, total_flushed, flush_number

    if not pending_flush:
      return 0

    flush_number += 1

    if use_daily:
      # Daily mode: group by filing_date and upload to respective partitions
      # Disk structure: work_dir/{filing_date}/nodes/Entity/...
      date_dirs = [
        d for d in work_dir.iterdir() if d.is_dir() and d.name.startswith("20")
      ]
      context.log.info(
        f"Flush #{flush_number}: {len(pending_flush)} filings to {len(date_dirs)} dates..."
      )

      for date_dir in date_dirs:
        partition_date = date_dir.name
        output_dates.add(partition_date)

        # Find table keys for this date
        table_keys = set()
        for pq_file in date_dir.rglob("*.parquet"):
          rel_path = pq_file.relative_to(date_dir)
          if len(rel_path.parts) >= 2:
            table_key = f"{rel_path.parts[0]}/{rel_path.parts[1]}"
            table_keys.add(table_key)

        for table_key in sorted(table_keys):
          parquet_bytes = _consolidate_parquet_from_disk(date_dir, table_key)
          if not parquet_bytes:
            continue

          entity_type, table_name = table_key.split("/", 1)
          part_filename = f"part-{run_id_prefix}-{flush_number:03d}.parquet"
          s3_key = get_processed_key(
            DataSourceType.SEC,
            "processed",
            f"filed={partition_date}",
            entity_type,
            table_name,
            part_filename,
          )

          s3.client.put_object(
            Bucket=processed_bucket,
            Key=s3_key,
            Body=parquet_bytes,
            ContentType="application/octet-stream",
          )
          files_uploaded += 1
          context.log.debug(f"Uploaded: {s3_key} ({len(parquet_bytes):,} bytes)")
    else:
      # Quarterly mode: all files go to default_partition_date
      # Disk structure: work_dir/nodes/Entity/...
      output_dates.add(default_partition_date)
      context.log.info(
        f"Flush #{flush_number}: {len(pending_flush)} filings to S3 "
        f"(partition: filed={default_partition_date})..."
      )

      # Find all table directories in work_dir
      table_keys = set()
      for subdir in work_dir.rglob("*.parquet"):
        rel_path = subdir.relative_to(work_dir)
        if len(rel_path.parts) >= 2:
          table_key = f"{rel_path.parts[0]}/{rel_path.parts[1]}"
          table_keys.add(table_key)

      for table_key in sorted(table_keys):
        parquet_bytes = _consolidate_parquet_from_disk(work_dir, table_key)
        if not parquet_bytes:
          continue

        entity_type, table_name = table_key.split("/", 1)
        part_filename = f"part-{run_id_prefix}-{flush_number:03d}.parquet"
        s3_key = get_processed_key(
          DataSourceType.SEC,
          "processed",
          f"filed={default_partition_date}",
          entity_type,
          table_name,
          part_filename,
        )

        s3.client.put_object(
          Bucket=processed_bucket,
          Key=s3_key,
          Body=parquet_bytes,
          ContentType="application/octet-stream",
        )
        files_uploaded += 1
        context.log.debug(f"Uploaded: {s3_key} ({len(parquet_bytes):,} bytes)")

    # Mark all pending filings as success (data is now safely in S3)
    with db.get_session() as session:
      for file_info in pending_flush:
        sf = SourceFile.get_by_storage_key(file_info["storage_key"], session)
        if sf:
          sf.mark_success(session)

    flushed_count = len(pending_flush)
    total_flushed += flushed_count
    context.log.info(
      f"Flushed {flushed_count} filings, {files_uploaded} total parquet files uploaded"
    )

    # Clear disk buffer and pending list
    for item in work_dir.iterdir():
      if item.is_dir():
        shutil.rmtree(item)
      else:
        item.unlink()
    pending_flush.clear()

    return flushed_count

  # Process each filing
  try:
    for i, file_info in enumerate(files_to_process):
      source_file_id = file_info["id"]
      storage_key = file_info["storage_key"]
      file_partition_key = file_info["partition_key"]

      # Log filing start
      context.log.info(
        f"[{i + 1}/{len(files_to_process)}] Processing: {file_partition_key}"
      )
      filing_start = time_module.time()

      # Mark as processing
      with db.get_session() as session:
        sf = SourceFile.get_by_storage_key(storage_key, session)
        if sf:
          sf.mark_processing(session)

      # Process filing
      result = _process_single_filing_to_memory(
        storage_key=storage_key,
        partition_key=file_partition_key,
        source_file_id=source_file_id,
        s3_client=s3.client,
        raw_bucket=raw_bucket,
      )

      filing_duration = time_module.time() - filing_start

      if result.success:
        # Determine the base directory for this filing's output
        # Daily mode: work_dir/{filing_date}/nodes/Entity/...
        # Quarterly mode: work_dir/nodes/Entity/...
        filing_date = result.filing_date or default_partition_date
        if use_daily:
          base_dir = work_dir / filing_date
        else:
          base_dir = work_dir

        # Write parquet files to disk (not memory accumulation)
        for table_key, parquet_bytes in result.tables.items():
          table_dir = base_dir / table_key
          table_dir.mkdir(parents=True, exist_ok=True)
          parquet_path = table_dir / f"{source_file_id}.parquet"
          parquet_path.write_bytes(parquet_bytes)

        succeeded += 1
        # Include filing_date for tracking
        pending_flush.append({**file_info, "filing_date": filing_date})

        # Log success with table counts
        table_summary = ", ".join(
          f"{k.split('/')[-1]}:{len(v) // 1024}KB"
          for k, v in sorted(result.tables.items())[:5]
        )
        if len(result.tables) > 5:
          table_summary += f", +{len(result.tables) - 5} more"
        context.log.info(
          f"[{i + 1}/{len(files_to_process)}] Written to disk: {file_partition_key} "
          f"({filing_duration:.1f}s, {len(result.tables)} tables: {table_summary})"
        )
      else:
        # Mark failed immediately
        with db.get_session() as session:
          sf = SourceFile.get_by_storage_key(storage_key, session)
          if sf:
            sf.mark_error(session, result.error or "Unknown error")
        failed += 1
        failed_ids.append(source_file_id)
        context.log.warning(
          f"[{i + 1}/{len(files_to_process)}] Failed: {file_partition_key} - {result.error}"
        )

        if not config.continue_on_error:
          context.log.error(f"Stopping on error: {result.error}")
          break

      # Batch progress summary every 100 filings
      if (i + 1) % 100 == 0:
        context.log.info(
          f"Progress: {i + 1}/{len(files_to_process)} processed, "
          f"{succeeded} succeeded, {failed} failed"
        )

    # Flush all processed filings to S3 at end of batch
    if pending_flush:
      context.log.info(f"Flushing {len(pending_flush)} filings to S3...")
      flush_to_s3()

  finally:
    # Cleanup work directory
    if work_dir.exists():
      shutil.rmtree(work_dir)
      context.log.debug(f"Cleaned up work directory: {work_dir}")

  dates_summary = sorted(output_dates)
  context.log.info(
    f"Complete: {succeeded}/{len(files_to_process)} filings succeeded, "
    f"{files_uploaded} parquet files uploaded to {len(dates_summary)} partition(s)"
  )

  # Check if more pending files exist (sensor will trigger another run)
  with db.get_session() as session:
    remaining_count = (
      session.query(SourceFile)
      .filter(
        and_(
          SourceFile.graph_id == "sec",
          SourceFile.status == "pending",
          SourceFile.partition_key.like(f"{quarter_prefix}%"),
        )
      )
      .count()
    )

  if remaining_count > 0:
    context.log.info(
      f"Batch complete. {remaining_count} pending files remain - "
      "sensor will trigger next batch."
    )

  return MaterializeResult(
    metadata={
      "year": year,
      "quarter": quarter,
      "partition_mode": partition_mode,
      "output_dates": dates_summary,
      "status": "success" if failed == 0 else "partial",
      "filings_processed": len(files_to_process),
      "filings_succeeded": succeeded,
      "filings_failed": failed,
      "filings_flushed": total_flushed,
      "failed_source_file_ids": failed_ids[:20],  # Limit to first 20
      "parquet_files_uploaded": files_uploaded,
      "batch_limit": config.batch_limit,
      "remaining_pending": remaining_count,
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
  """Stage SEC processed files to persistent DuckDB (full rebuild).

  Creates DuckDB tables from scratch using all S3 parquet files.
  Persists to disk so materialization can run independently.

  Options:
  - reset_staging: Delete existing DuckDB file before staging (fresh start)
  - year: Optional year filter for partial staging

  Run with:
    just dagster-materialize sec_duckdb_staged

  Returns:
      MaterializeResult with staging statistics
  """
  import asyncio

  from robosystems.adapters.sec import XBRLDuckDBGraphProcessor
  from robosystems.operations.graph.shared_repository_service import (
    ensure_shared_repository_exists,
  )

  context.log.info(f"Staging SEC data to DuckDB for graph: {config.graph_id}")
  if config.year:
    context.log.info(f"Year filter: {config.year}")
  if config.reset_staging:
    context.log.info("Reset staging enabled - will delete DuckDB file first")

  # Boost DuckDB memory before staging (only applies to ladybug-shared tier)
  try:
    from robosystems.graph_api.client.factory import boost_graph_memory

    boost_result = asyncio.run(boost_graph_memory(config.graph_id, target="duckdb"))
    context.log.info(f"Memory boost: {boost_result.get('message', 'done')}")
  except Exception as boost_err:
    context.log.warning(f"Could not boost memory (non-fatal): {boost_err}")

  processor = XBRLDuckDBGraphProcessor(graph_id=config.graph_id)

  # Progress callback for Dagster logging (visible in Dagster UI)
  def dagster_progress(msg: str) -> None:
    context.log.info(msg)

  async def run_staging():
    # Ensure repository exists
    context.log.info("Ensuring SEC repository metadata exists...")
    repo_result = await ensure_shared_repository_exists(
      repository_name=config.graph_id,
      created_by="system",
      instance_id="local-dev" if env.ENVIRONMENT == "dev" else "ladybug-shared-prod",
    )
    context.log.info(f"SEC repository status: {repo_result.get('status', 'unknown')}")

    # Run full staging from all S3 parquet files
    result = await processor.stage_to_duckdb(
      year=config.year,
      reset_staging=config.reset_staging,
      skip_taxonomy_relationships=config.skip_taxonomy_relationships,
      progress_callback=dagster_progress,
    )
    return result

  result = asyncio.run(run_staging())

  if result.status == "error":
    context.log.error(f"Staging failed: {result.error}")
    return MaterializeResult(
      metadata={
        "graph_id": config.graph_id,
        "status": "error",
        "error": result.error,
        "duration_seconds": result.duration_seconds,
      }
    )

  context.log.info(
    f"Staging complete: {len(result.table_names)} tables, "
    f"{result.total_files} files, {result.duration_seconds:.2f}s"
  )

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
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

  # Boost LadybugDB memory before materialization (only applies to ladybug-shared tier)
  try:
    from robosystems.graph_api.client.factory import boost_graph_memory

    boost_result = asyncio.run(boost_graph_memory(config.graph_id, target="ladybug"))
    context.log.info(f"Memory boost: {boost_result.get('message', 'done')}")
  except Exception as boost_err:
    context.log.warning(f"Could not boost memory (non-fatal): {boost_err}")

  processor = XBRLDuckDBGraphProcessor(graph_id=config.graph_id)

  # Progress callback for Dagster logging (visible in Dagster UI)
  def dagster_progress(msg: str) -> None:
    context.log.info(msg)

  async def run_materialization():
    result = await processor.materialize_from_duckdb(
      rebuild=config.rebuild_graph,
      skip_taxonomy_relationships=config.skip_taxonomy_relationships,
      progress_callback=dagster_progress,
    )
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

  # Restore memory to defaults after materialization
  # This releases the temporarily boosted DuckDB and LadybugDB memory
  try:
    from robosystems.graph_api.client.factory import restore_graph_memory

    restore_result = asyncio.run(restore_graph_memory(config.graph_id))
    context.log.info(f"Memory restored: {restore_result.get('message', 'done')}")
  except Exception as restore_err:
    # Don't fail the job if restore fails - materialization succeeded
    context.log.warning(f"Could not restore memory (non-fatal): {restore_err}")

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
