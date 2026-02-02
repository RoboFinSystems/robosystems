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
from pydantic import Field

from robosystems.config import env
from robosystems.config.constants import SEC_PROCESS_BATCH_LIMIT
from robosystems.config.storage.shared import (
  DataSourceType,
  get_processed_key,
  get_raw_key,
)
from robosystems.dagster.resources import DatabaseResource, S3Resource
from robosystems.logger import get_logger
from robosystems.models.iam import Graph, SourceFile

logger = get_logger(__name__)

# In-memory cache for SEC submissions during a single run
_sec_submissions_cache: dict[str, dict] = {}


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
    logger.warning("No S3 snapshot for CIK %s, falling back to SEC API", cik)
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

# Form type batches for EFTS queries to avoid 10k result limit
# Q2 (proxy season) can exceed 10k when all forms are included
# Batch 1: Core financial statements
# Batch 2: Supplementary filings (proxies, registrations)
SEC_FORM_TYPE_BATCHES = [
  ["10-K", "10-Q", "20-F", "40-F"],  # Core financials (~7k in Q2)
  ["DEF 14A", "S-1"],  # Supplementary (~3k in Q2)
]

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
  skip_submissions: bool = False  # Skip fetching/updating submissions.json files
  form_types: list[str] = [
    "10-K",
    "10-Q",
    "20-F",
    "40-F",
    "DEF 14A",
    "S-1",
  ]  # Form types to download
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
  - Each job processes at most batch_limit filings (default 2,000)
  - Job exits gracefully after batch, releasing all memory
  - Sensor re-triggers if more pending files exist
  - Processing is disk-buffered (not memory-intensive)

  Note: Smaller batches leverage the merge strategy for crash resilience.
  Each batch flushes to S3 (merging with existing data), so a crash loses
  at most one batch instead of an entire quarter.
  """

  # Max filings to process per job run before exiting gracefully.
  # Smaller batches provide crash resilience via incremental S3 merging.
  batch_limit: int = SEC_PROCESS_BATCH_LIMIT

  # Continue processing even if some filings fail
  # If False, job fails on first error (for debugging)
  continue_on_error: bool = True


class SECStageConfig(Config):
  """Configuration for DuckDB staging (full rebuild).

  Creates DuckDB tables from scratch using all S3 parquet files.

  Note: This step only stages data to DuckDB. LadybugDB rebuild is handled
  by the materialize step (sec_graph_materialized) via SECMaterializeConfig.rebuild_graph.

  Common scenarios:
    - Normal re-run: Use defaults (reset_staging=False). Tables are overwritten.
    - Enable skip_taxonomy_relationships: Set reset_staging=True AND skip_taxonomy_relationships=True.
      Without reset_staging, old taxonomy tables would remain in DuckDB.
    - Fresh start after corruption: Set reset_staging=True to delete all staging.
  """

  graph_id: str = "sec"  # Target graph ID
  year: int | None = None  # Optional year filter
  reset_staging: bool = False  # Delete entire DuckDB staging database first (required when changing skip_taxonomy_relationships)
  skip_taxonomy_relationships: bool = False  # Skip taxonomy structure tables (Association, Structure, ~600M rows) to reduce storage


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


class SECIncrementalStageConfig(Config):
  """Configuration for incremental SEC staging.

  Stages current quarter's files incrementally using INSERT INTO with
  deduplication. Safe to run daily - only net new rows are added.

  Precondition: Initial full staging must have been done (tables exist).
  """

  graph_id: str = "sec"  # Target graph ID
  year: int | None = None  # Year to stage (default: current year)
  quarter: int | None = Field(
    default=None, ge=1, le=4
  )  # Quarter 1-4 (default: current)
  skip_taxonomy_relationships: bool = False  # Skip taxonomy structure tables


class SECDirectCopyConfig(Config):
  """Configuration for direct S3 → LadybugDB copy (bypasses DuckDB staging).

  This approach:
  1. Reads parquet files directly from S3 using LadybugDB's httpfs extension
  2. Uses spill_to_disk=true for memory-efficient loading of large tables
  3. Handles duplicates via ignore_errors=true (constraint violations skipped)

  Benefits over DuckDB staging:
  - No memory pressure from DuckDB merge/dedupe operations
  - Proven to work at scale (200M+ rows)
  - Simpler pipeline with fewer moving parts

  Trade-offs:
  - Relies on LadybugDB constraints for deduplication (not pre-deduped)
  - May load some duplicate rows that get rejected at insert time
  """

  graph_id: str = "sec"  # Target graph ID
  rebuild_graph: bool = True  # Rebuild LadybugDB before copy (avoids duplicates)
  skip_taxonomy_relationships: bool = False  # Skip taxonomy structure tables
  skip_tables: list[
    str
  ] = []  # Tables to skip (e.g., ["Entity"] for type mismatch issues)
  year: int | None = None  # Optional year filter (None = all years)


class SECIncrementalCopyConfig(Config):
  """Configuration for incremental S3 → LadybugDB copy (bypasses DuckDB staging).

  This is the preferred approach for daily incremental updates:
  1. Copies directly from S3 parquet to LadybugDB
  2. Uses ignore_errors=true to skip duplicates (constraint violations)
  3. Only scans current quarter + previous quarter during 5-day overlap

  Benefits over DuckDB incremental staging:
  - No need to diff what's new in DuckDB vs LadybugDB
  - Simpler and faster for daily updates
  - LadybugDB handles deduplication via constraints

  When to use this vs sec_duckdb_incremental_staged:
  - Use this for daily incremental updates (simpler, faster)
  - Use DuckDB staging for backfills or when you need DuckDB queries
  """

  graph_id: str = "sec"  # Target graph ID
  year: int | None = None  # Year to copy (default: current year)
  quarter: int | None = Field(
    default=None, ge=1, le=4
  )  # Quarter 1-4 (default: current)
  skip_taxonomy_relationships: bool = False  # Skip taxonomy structure tables


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

      # Split form types into batches to avoid EFTS 10k limit (especially Q2 proxy season)
      # Each batch is queried separately and results are combined
      requested_forms = set(config.form_types)
      form_batches = []
      for batch in SEC_FORM_TYPE_BATCHES:
        batch_forms = [f for f in batch if f in requested_forms]
        if batch_forms:
          form_batches.append(batch_forms)

      # Also include any forms not in predefined batches (custom forms)
      known_forms = {f for batch in SEC_FORM_TYPE_BATCHES for f in batch}
      custom_forms = [f for f in config.form_types if f not in known_forms]
      if custom_forms:
        form_batches.append(custom_forms)

      # Query each batch and combine results
      hits = []
      for batch_idx, batch_forms in enumerate(form_batches):
        context.log.info(
          f"EFTS batch {batch_idx + 1}/{len(form_batches)}: {batch_forms}"
        )
        batch_hits = await efts.query_by_quarter(
          year=year,
          quarter=quarter,
          form_types=batch_forms,
          ciks=cik_filter,
        )
        context.log.info(f"  Batch {batch_idx + 1} found {len(batch_hits)} filings")
        hits.extend(batch_hits)

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
    # Can be skipped with skip_submissions=True to avoid rate limiting issues
    submissions_fetched = 0
    submissions_failed = 0

    if config.skip_submissions:
      context.log.info("Phase 2: Skipping submissions fetch (skip_submissions=True)")
    else:
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
          submissions_key = get_raw_key(
            DataSourceType.SEC, "submissions", f"{cik}.json"
          )

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
          _ = await coro
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


def _merge_with_existing_s3(
  s3_client,
  bucket: str,
  s3_key: str,
  new_data: bytes,
  table_key: str,
) -> bytes:
  """Download existing S3 parquet, merge with new data, return merged bytes.

  For shared tables (Element, Label, etc.), deduplicates on identifier column.
  For per-filing tables, simply concatenates (no duplicates possible).

  Args:
      s3_client: boto3 S3 client
      bucket: S3 bucket name
      s3_key: S3 key for the existing file (may not exist)
      new_data: New parquet bytes to merge
      table_key: Table key like "nodes/Entity" for dedup decisions

  Returns:
      Merged parquet bytes (or new_data if no existing file)
  """
  from io import BytesIO

  import pyarrow as pa
  import pyarrow.parquet as pq

  # Shared node tables that need deduplication on identifier
  SHARED_NODE_TABLES = frozenset(
    {
      "nodes/Element",
      "nodes/Label",
      "nodes/Reference",
      "nodes/Unit",
      "nodes/Period",
    }
  )

  # Try to download existing file
  existing_data: bytes | None = None
  try:
    response = s3_client.get_object(Bucket=bucket, Key=s3_key)
    existing_data = response["Body"].read()
  except s3_client.exceptions.NoSuchKey:
    # No existing file - return new data as-is
    return new_data
  except Exception as e:
    # Other errors - return new data as-is (will overwrite)
    logger.warning("Failed to read existing S3 file %s, will overwrite: %s", s3_key, e)
    return new_data

  # Read both tables
  try:
    existing_table = pq.read_table(BytesIO(existing_data))
    new_table = pq.read_table(BytesIO(new_data))
  except Exception as e:
    # If we can't parse new data but have valid existing data, keep existing
    logger.warning(
      "Failed to parse parquet for merge at %s, keeping existing: %s", s3_key, e
    )
    return existing_data

  # Concatenate tables
  combined = pa.concat_tables([existing_table, new_table], promote_options="permissive")

  # Deduplicate shared tables on identifier
  if table_key in SHARED_NODE_TABLES and "identifier" in combined.column_names:
    df = combined.to_pandas()
    df = df.drop_duplicates(subset=["identifier"], keep="first")
    combined = pa.Table.from_pandas(df, preserve_index=False)

  # Write merged result
  buffer = BytesIO()
  pq.write_table(combined, buffer)
  return buffer.getvalue()


def _atomic_s3_upload(
  s3_client,
  bucket: str,
  final_key: str,
  data: bytes,
) -> None:
  """Upload data to S3 atomically using temp file + copy pattern.

  Uploads to a temp key first, then copies to final location and deletes temp.
  This ensures the final key either has complete data or doesn't exist.

  Args:
      s3_client: boto3 S3 client
      bucket: S3 bucket name
      final_key: Final S3 key for the file
      data: Parquet bytes to upload
  """
  import uuid

  # Generate unique temp key
  temp_key = f"{final_key}.tmp.{uuid.uuid4().hex[:8]}"

  try:
    # Upload to temp location
    s3_client.put_object(
      Bucket=bucket,
      Key=temp_key,
      Body=data,
      ContentType="application/octet-stream",
    )

    # Copy to final location (atomic operation)
    s3_client.copy_object(
      Bucket=bucket,
      CopySource={"Bucket": bucket, "Key": temp_key},
      Key=final_key,
    )

    # Delete temp file
    s3_client.delete_object(Bucket=bucket, Key=temp_key)

  except Exception:
    # Try to clean up temp file on any error
    try:
      s3_client.delete_object(Bucket=bucket, Key=temp_key)
    except Exception as cleanup_exc:
      logger.warning(
        "Failed to delete temporary S3 object %s during cleanup: %s",
        temp_key,
        cleanup_exc,
      )
    raise


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

  Output Structure (quarterly partitions with append):
    s3://bucket/sec/processed/filed=2024-Q1/nodes/Entity.parquet
    - Aligns Dagster partition (quarterly) with S3 partition (quarterly)
    - Single file per table per quarter, merged on each run
    - Shared tables (Element, Label, etc.) deduplicated on identifier
    - Simplifies staging - single glob per quarter

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

  # Use quarterly partitions (e.g., "2024-Q1") - aligns Dagster partition with S3 partition
  partition_date = partition_key  # e.g., "2024-Q1"

  context.log.info(
    f"Processing SEC filings for {partition_key} (quarterly partition: filed={partition_date})"
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
        "partition_date": partition_date,
        "status": "no_pending_files",
        "filings_processed": 0,
        "filings_succeeded": 0,
        "filings_failed": 0,
      }
    )

  context.log.info(
    f"Processing batch of {len(files_to_process)} filings for {partition_key} "
    f"(batch_limit={config.batch_limit})"
  )

  # Create work directory for disk-buffered processing
  work_dir = Path(tempfile.mkdtemp(prefix=f"sec_processing_{year}Q{quarter}_"))
  context.log.info(f"Work directory: {work_dir}")

  # Track processing state
  succeeded = 0
  failed = 0
  failed_ids: list[str] = []
  pending_flush: list[dict] = []  # [{...file_info}, ...]
  total_flushed = 0
  tables_uploaded = 0  # Track number of table files uploaded

  def flush_to_s3() -> int:
    """Consolidate disk buffer, merge with existing S3 data, upload, mark success.

    Uses quarterly partitions with append-based merging:
    - Downloads existing TABLE.parquet from S3 (if exists)
    - Merges new data with existing data
    - Deduplicates shared tables (Element, Label, etc.) on identifier
    - Uploads merged result atomically
    """
    nonlocal tables_uploaded, total_flushed

    if not pending_flush:
      return 0

    context.log.info(
      f"Flushing {len(pending_flush)} filings to S3 (partition: filed={partition_date})..."
    )

    # Find all table directories in work_dir
    # Disk structure: work_dir/nodes/Entity/...
    table_keys = set()
    for subdir in work_dir.rglob("*.parquet"):
      rel_path = subdir.relative_to(work_dir)
      if len(rel_path.parts) >= 2:
        table_key = f"{rel_path.parts[0]}/{rel_path.parts[1]}"
        table_keys.add(table_key)

    for table_key in sorted(table_keys):
      # Consolidate this batch's data from disk
      new_parquet_bytes = _consolidate_parquet_from_disk(work_dir, table_key)
      if not new_parquet_bytes:
        continue

      entity_type, table_name = table_key.split("/", 1)
      # Single file per table per quarter: TABLE.parquet (not part files)
      s3_key = get_processed_key(
        DataSourceType.SEC,
        "processed",
        f"filed={partition_date}",
        entity_type,
        f"{table_name}.parquet",
      )

      # Merge with existing S3 data (append-based accumulation)
      merged_bytes = _merge_with_existing_s3(
        s3_client=s3.client,
        bucket=processed_bucket,
        s3_key=s3_key,
        new_data=new_parquet_bytes,
        table_key=table_key,
      )

      # Upload atomically (temp file + copy pattern)
      _atomic_s3_upload(
        s3_client=s3.client,
        bucket=processed_bucket,
        final_key=s3_key,
        data=merged_bytes,
      )
      tables_uploaded += 1
      context.log.debug(f"Uploaded: {s3_key} ({len(merged_bytes):,} bytes)")

    # Mark all pending filings as success (data is now safely in S3)
    with db.get_session() as session:
      for file_info in pending_flush:
        sf = SourceFile.get_by_storage_key(file_info["storage_key"], session)
        if sf:
          sf.mark_success(session)

    flushed_count = len(pending_flush)
    total_flushed += flushed_count
    context.log.info(
      f"Flushed {flushed_count} filings, {tables_uploaded} total table files uploaded"
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
        # Write parquet files to disk (not memory accumulation)
        # Disk structure: work_dir/nodes/Entity/...
        for table_key, parquet_bytes in result.tables.items():
          table_dir = work_dir / table_key
          table_dir.mkdir(parents=True, exist_ok=True)
          parquet_path = table_dir / f"{source_file_id}.parquet"
          parquet_path.write_bytes(parquet_bytes)

        succeeded += 1
        pending_flush.append(file_info)

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

  context.log.info(
    f"Complete: {succeeded}/{len(files_to_process)} filings succeeded, "
    f"{tables_uploaded} table files uploaded to partition filed={partition_date}"
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
      "partition_date": partition_date,
      "status": "success" if failed == 0 else "partial",
      "filings_processed": len(files_to_process),
      "filings_succeeded": succeeded,
      "filings_failed": failed,
      "filings_flushed": total_flushed,
      "failed_source_file_ids": failed_ids[:20],  # Limit to first 20
      "tables_uploaded": tables_uploaded,
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
    uv run dagster asset materialize -m robosystems.dagster --select sec_duckdb_staged

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
        "duration_ms": result.duration_ms,
      }
    )

  context.log.info(
    f"Staging complete: {len(result.table_names)} tables, "
    f"{result.total_files} files, {result.duration_ms / 1000:.2f}s"
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
      "duration_ms": result.duration_ms,
    }
  )


@asset(
  group_name="sec_pipeline",
  description="Incrementally stage current quarter's filings to DuckDB",
  kinds={"duckdb"},
  metadata={
    "pipeline": "sec",
    "stage": "incremental_staging",
    "decoupled": True,
  },
)
def sec_duckdb_incremental_staged(
  context: AssetExecutionContext,
  config: SECIncrementalStageConfig,
) -> MaterializeResult:
  """INSERT current quarter's files into existing DuckDB tables.

  Points at entire quarter's parquet files and uses INSERT INTO with
  UNION ALL + ROW_NUMBER deduplication. Safe to run daily - only net
  new rows are added, duplicates are automatically filtered out.

  Precondition: Initial full staging must have been done (tables exist).

  Run with:
    uv run dagster asset materialize -m robosystems.dagster --select sec_duckdb_incremental_staged
  """
  import asyncio

  from robosystems.adapters.sec import XBRLDuckDBGraphProcessor

  processor = XBRLDuckDBGraphProcessor(graph_id=config.graph_id)

  async def run_incremental():
    return await processor.stage_incremental_to_duckdb(
      year=config.year,
      quarter=config.quarter,
      skip_taxonomy_relationships=config.skip_taxonomy_relationships,
      progress_callback=context.log.info,
    )

  result = asyncio.run(run_incremental())

  if result.status == "error":
    context.log.error(f"Incremental staging failed: {result.error}")
    return MaterializeResult(
      metadata={
        "graph_id": config.graph_id,
        "status": "error",
        "error": result.error or "Unknown error",
        "duration_ms": result.duration_ms,
      }
    )

  context.log.info(
    f"Incremental staging complete: {len(result.table_names)} tables, "
    f"{result.total_rows} rows, {result.duration_ms / 1000:.2f}s"
  )

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "status": result.status,
      "year": config.year,
      "quarter": config.quarter,
      "tables_staged": len(result.table_names),
      "total_rows": result.total_rows,  # Net new rows
      "duration_ms": result.duration_ms,
    }
  )


@asset(
  group_name="sec_pipeline",
  description="Incremental S3 → LadybugDB copy (bypasses DuckDB, uses ignore_errors)",
  kinds={"ladybug"},
  metadata={
    "pipeline": "sec",
    "stage": "incremental_copy",
    "decoupled": True,
  },
)
def sec_graph_incremental_copy(
  context: AssetExecutionContext,
  config: SECIncrementalCopyConfig,
) -> MaterializeResult:
  """Copy current quarter's files directly to LadybugDB (bypasses DuckDB staging).

  This is the preferred approach for daily incremental updates:
  1. Copies directly from S3 parquet to LadybugDB
  2. Uses ignore_errors=true to skip duplicates (constraint violations)
  3. Only scans current quarter + previous quarter during 5-day overlap

  Benefits over DuckDB incremental staging:
  - No need to diff what's new in DuckDB vs LadybugDB
  - Simpler and faster for daily updates
  - LadybugDB handles deduplication via constraints

  Precondition: LadybugDB database must already exist with SEC schema.

  Run with:
    uv run dagster asset materialize -m robosystems.dagster --select sec_graph_incremental_copy
  """
  import asyncio

  from robosystems.adapters.sec import XBRLDuckDBGraphProcessor

  processor = XBRLDuckDBGraphProcessor(graph_id=config.graph_id)

  async def run_incremental_copy():
    return await processor.copy_incremental_to_ladybug(
      year=config.year,
      quarter=config.quarter,
      skip_taxonomy_relationships=config.skip_taxonomy_relationships,
      progress_callback=context.log.info,
    )

  result = asyncio.run(run_incremental_copy())

  if result.status == "error":
    context.log.error(f"Incremental copy failed: {result.error}")
    return MaterializeResult(
      metadata={
        "graph_id": config.graph_id,
        "status": "error",
        "error": result.error or "Unknown error",
        "duration_ms": result.duration_ms,
      }
    )

  context.log.info(
    f"Incremental copy complete: {len(result.table_names)} tables, "
    f"{result.total_rows} records, {result.duration_ms / 1000:.2f}s"
  )

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "status": result.status,
      "year": config.year,
      "quarter": config.quarter,
      "tables_copied": len(result.table_names),
      "total_records": result.total_rows,
      "duration_ms": result.duration_ms,
    }
  )


class SECEntityUpdateConfig(Config):
  """Configuration for incremental Entity update asset."""

  graph_id: str = "sec"
  year: int | None = Field(default=None, description="Year (default: current year)")
  quarter: int | None = Field(
    default=None, ge=1, le=4, description="Quarter 1-4 (default: current)"
  )


@asset(
  group_name="sec_pipeline",
  description="Update existing Entity nodes with latest data (handles mutable Entity attributes)",
  kinds={"ladybug"},
  deps=["sec_graph_incremental_copy"],  # Run after incremental copy
  metadata={
    "pipeline": "sec",
    "stage": "entity_update",
    "decoupled": True,
  },
)
def sec_entity_incremental_update(
  context: AssetExecutionContext,
  config: SECEntityUpdateConfig,
) -> MaterializeResult:
  """Update existing Entity nodes with latest attribute values.

  This solves the Entity mutability problem: unlike other XBRL nodes (facts,
  periods, etc.) which are immutable, Entity attributes can change over time:
  - Company name changes
  - Ticker/exchange changes (listing updates)
  - Filer category changes (large accelerated filer, etc.)
  - Fiscal year end changes
  - Contact info updates (phone, website)

  The incremental COPY operation only INSERTs new records - it cannot update
  existing ones. This asset uses Cypher MERGE to update existing Entity nodes.

  Process:
  1. Read latest Entity parquet from S3 (current quarter)
  2. Query existing Entity nodes from LadybugDB
  3. Compare and identify entities with actual changes
  4. Execute MERGE queries in batches to update changed entities

  Note: Only entities with actual changes are updated (typically 50-200 per
  quarter). MERGE is 40x slower than COPY, but this is acceptable for the
  small number of updates.

  Run with:
    uv run dagster asset materialize -m robosystems.dagster --select sec_entity_incremental_update
  """
  import asyncio

  from robosystems.adapters.sec import XBRLDuckDBGraphProcessor

  context.log.info(
    f"Starting Entity update for graph {config.graph_id} "
    f"(Q{config.quarter or 'current'} {config.year or 'current'})"
  )

  processor = XBRLDuckDBGraphProcessor(graph_id=config.graph_id)

  async def run_entity_update():
    return await processor.update_entities_from_s3(
      year=config.year,
      quarter=config.quarter,
      progress_callback=context.log.info,
    )

  result = asyncio.run(run_entity_update())

  if result.status == "error":
    context.log.error(f"Entity update failed: {result.error}")
    return MaterializeResult(
      metadata={
        "graph_id": config.graph_id,
        "status": "error",
        "error": result.error or "Unknown error",
        "duration_ms": result.duration_ms,
      }
    )

  context.log.info(
    f"Entity update complete: {result.entities_updated} updated, "
    f"{result.entities_unchanged} unchanged, {result.entities_failed} failed "
    f"({result.duration_ms / 1000:.2f}s)"
  )

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "status": result.status,
      "year": config.year,
      "quarter": config.quarter,
      "entities_checked": result.entities_checked,
      "entities_updated": result.entities_updated,
      "entities_unchanged": result.entities_unchanged,
      "entities_failed": result.entities_failed,
      "duration_ms": result.duration_ms,
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
    "decoupled": True,
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

  Run with:
    uv run dagster asset materialize -m robosystems.dagster --select sec_graph_materialized

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
    f"{result.duration_ms / 1000:.2f}s"
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
      "total_rows_ingested": result.total_rows_ingested,
      "duration_ms": result.duration_ms,
      "tables": result.tables,
    }
  )


# ============================================================================
# Direct S3 → LadybugDB Copy Asset (Alternative to DuckDB Staging)
# ============================================================================
# This asset bypasses DuckDB staging entirely and copies directly from S3.
# Uses LadybugDB's native parquet reading with spill_to_disk for memory efficiency.
# Duplicates are handled via ignore_errors=true (constraint violations skipped).


@asset(
  group_name="sec_pipeline",
  description="Direct S3 → LadybugDB copy (bypasses DuckDB staging)",
  kinds={"ladybug"},
  metadata={
    "pipeline": "sec",
    "stage": "direct_copy",
    "decoupled": True,
  },
)
def sec_graph_direct_copy(
  context: AssetExecutionContext,
  config: SECDirectCopyConfig,
) -> MaterializeResult:
  """Copy SEC data directly from S3 to LadybugDB (bypasses DuckDB staging).

  This asset provides an alternative materialization path that:
  1. Reads parquet files directly from S3 using LadybugDB's httpfs extension
  2. Uses spill_to_disk=true for memory-efficient loading of large tables
  3. Handles duplicates via ignore_errors=true (constraint violations skipped)
  4. Uses quarter-by-quarter batching for large tables (100M+ rows)

  When to use this vs sec_graph_materialized:
  - Use this for faster loading (single pass, no DuckDB intermediate)
  - Use sec_graph_materialized when you need DuckDB's query capabilities

  The flow is:
  1. (Optional) Rebuild LadybugDB database with SEC schema
  2. Discover filing dates and group by quarter (for batching)
  3. Get table names from schema
  4. For each table:
     - Large tables (QUARTER_CHUNKABLE_TABLES): Load quarter by quarter
     - Small tables: Single COPY with glob pattern
  5. LadybugDB loads directly from S3 with spill_to_disk=true

  Run with:
    uv run dagster asset materialize -m robosystems.dagster --select sec_graph_direct_copy

  Returns:
      MaterializeResult with copy statistics
  """
  import asyncio
  import time

  from robosystems.adapters.sec.processors.ingestion import (
    QUARTER_CHUNKABLE_TABLES,
    TAXONOMY_STRUCTURE_TABLES,
    XBRLDuckDBGraphProcessor,
  )
  from robosystems.graph_api.client.factory import (
    boost_graph_memory,
    get_graph_client,
    restore_graph_memory,
  )
  from robosystems.operations.graph.shared_repository_service import (
    ensure_shared_repository_exists,
  )
  from robosystems.schemas.extensions.roboledger import RoboLedgerContext

  # Timeout for quarter-by-quarter copies (same as materialization batches)
  QUARTER_COPY_TIMEOUT = 1800  # 30 minutes per quarter
  # Timeout for small tables (single copy)
  SINGLE_COPY_TIMEOUT = 3600  # 60 minutes

  context.log.info(f"Direct S3 → LadybugDB copy for graph: {config.graph_id}")
  if config.year:
    context.log.info(f"Year filter: {config.year}")
  if config.rebuild_graph:
    context.log.info("Rebuild enabled - will delete and recreate database")

  start_time = time.time()

  # Boost LadybugDB memory before copy
  try:
    boost_result = asyncio.run(boost_graph_memory(config.graph_id, target="ladybug"))
    context.log.info(f"Memory boost: {boost_result.get('message', 'done')}")
  except Exception as boost_err:
    context.log.warning(f"Could not boost memory (non-fatal): {boost_err}")

  async def run_direct_copy():
    # Create processor for helper methods
    processor = XBRLDuckDBGraphProcessor(graph_id=config.graph_id)

    # Ensure repository exists
    context.log.info("Ensuring SEC repository metadata exists...")
    repo_result = await ensure_shared_repository_exists(
      repository_name=config.graph_id,
      created_by="system",
      instance_id="local-dev" if env.ENVIRONMENT == "dev" else "ladybug-shared-prod",
    )
    context.log.info(f"SEC repository status: {repo_result.get('status', 'unknown')}")

    # Get graph client (matches working pattern from materialize_from_duckdb)
    client = await get_graph_client(graph_id=config.graph_id, operation_type="write")

    # Rebuild LadybugDB if requested (use processor's method for consistency)
    if config.rebuild_graph:
      context.log.info("Rebuilding LadybugDB database with SEC schema...")
      await processor._rebuild_ladybug_database(client, reset_staging=False)
      context.log.info("LadybugDB database rebuilt with SEC schema")

    # Discover quarterly partitions for batched loading of large tables
    context.log.info("Discovering quarterly partitions for batching...")
    quarterly_partitions = await processor._discover_filed_partitions(year=config.year)
    context.log.info(f"Found {len(quarterly_partitions)} quarterly partitions")

    # Get table names from schema
    context.log.info("Getting table names from schema...")
    tables_by_type = RoboLedgerContext.get_all_table_names_for_context(
      RoboLedgerContext.SEC_REPOSITORY
    )

    # Query database for existing tables to avoid "table does not exist" errors
    context.log.info("Querying database for existing tables...")
    try:
      existing_tables_result = await client.query(
        cypher="CALL show_tables() RETURN *",
        graph_id=config.graph_id,
      )
      existing_table_names = {
        row.get("name") for row in existing_tables_result.get("data", [])
      }
      context.log.info(f"Database has {len(existing_table_names)} tables")

      # Filter to only tables that exist in the database
      original_count = len(tables_by_type)
      tables_by_type = {
        name: entity_type
        for name, entity_type in tables_by_type.items()
        if name in existing_table_names
      }
      filtered_count = original_count - len(tables_by_type)
      if filtered_count > 0:
        context.log.info(f"Filtered out {filtered_count} tables not in database schema")
    except Exception as e:
      context.log.warning(f"Could not query existing tables: {e}")

    # Filter taxonomy tables if requested
    if config.skip_taxonomy_relationships:
      original_count = len(tables_by_type)
      tables_by_type = {
        name: entity_type
        for name, entity_type in tables_by_type.items()
        if name not in TAXONOMY_STRUCTURE_TABLES
      }
      skipped_count = original_count - len(tables_by_type)
      context.log.info(
        f"Skipping {skipped_count} taxonomy tables (skip_taxonomy_relationships=True)"
      )

    # Filter explicitly skipped tables (e.g., Entity with type mismatch issues)
    if config.skip_tables:
      original_count = len(tables_by_type)
      tables_by_type = {
        name: entity_type
        for name, entity_type in tables_by_type.items()
        if name not in config.skip_tables
      }
      skipped_count = original_count - len(tables_by_type)
      context.log.info(
        f"Skipping {skipped_count} tables from skip_tables config: {config.skip_tables}"
      )

    context.log.info(f"Schema defines {len(tables_by_type)} tables to copy")

    # Sort tables: nodes before relationships (relationships have uppercase names)
    node_tables = [
      (name, etype) for name, etype in tables_by_type.items() if not name.isupper()
    ]
    rel_tables = [
      (name, etype) for name, etype in tables_by_type.items() if name.isupper()
    ]
    ordered_tables = node_tables + rel_tables

    context.log.info(
      f"  Node tables ({len(node_tables)}): {[t[0] for t in node_tables]}"
    )
    context.log.info(
      f"  Relationship tables ({len(rel_tables)}): {[t[0] for t in rel_tables]}"
    )

    # Build S3 patterns and copy each table
    bucket = env.SHARED_PROCESSED_BUCKET
    source_prefix = "sec/processed"

    tables_copied = []
    total_rows = 0
    failed_tables = []

    # Use the client we already have from above
    for i, (table_name, entity_type) in enumerate(ordered_tables, 1):
      is_large = table_name in QUARTER_CHUNKABLE_TABLES
      use_batching = is_large and len(quarterly_partitions) > 0

      if use_batching:
        # Quarter-by-quarter loading for large tables
        context.log.info(
          f"[{i}/{len(ordered_tables)}] Copying {table_name} by quarter "
          f"({len(quarterly_partitions)} quarters)..."
        )

        table_rows = 0
        table_failed = False

        for q_idx, quarter_key in enumerate(quarterly_partitions, 1):
          # Single file per table per quarter: TABLE.parquet
          s3_pattern = (
            f"s3://{bucket}/{source_prefix}/filed={quarter_key}/"
            f"{entity_type}/{table_name}.parquet"
          )

          context.log.info(
            f"  [{quarter_key}] Quarter {q_idx}/{len(quarterly_partitions)}..."
          )

          try:
            copy_result = await client.copy_from_s3(
              graph_id=config.graph_id,
              table_name=table_name,
              s3_pattern=s3_pattern,  # Single file for this quarter
              ignore_errors=True,
              timeout=QUARTER_COPY_TIMEOUT,
              wait_for_completion=True,
            )

            if copy_result.get("status") == "completed":
              records = copy_result.get("records_loaded", 0)
              duration = copy_result.get("duration_seconds", 0)
              table_rows += records
              if records > 0:
                context.log.info(
                  f"  [{quarter_key}] {records:,} records in {duration:.1f}s"
                )
              else:
                context.log.info(f"  [{quarter_key}] done in {duration:.1f}s")
            elif "No files found" in copy_result.get("error", ""):
              context.log.info(f"  [{quarter_key}] No files (skipped)")
            else:
              error = copy_result.get("error", "Unknown error")
              context.log.error(f"  [{quarter_key}] FAILED: {error}")
              table_failed = True
              failed_tables.append(
                {
                  "table": f"{table_name}/{quarter_key}",
                  "error": error,
                }
              )

          except Exception as e:
            if "No files found" in str(e):
              context.log.info(f"  [{quarter_key}] No files (skipped)")
            else:
              context.log.error(f"  [{quarter_key}] FAILED: {e}")
              table_failed = True
              failed_tables.append(
                {
                  "table": f"{table_name}/{quarter_key}",
                  "error": str(e),
                }
              )

        total_rows += table_rows
        if not table_failed:
          tables_copied.append(table_name)
        if table_rows > 0:
          context.log.info(
            f"[{i}/{len(ordered_tables)}] {table_name}: {table_rows:,} total records"
          )
        else:
          context.log.info(f"[{i}/{len(ordered_tables)}] {table_name}: done")

      else:
        # Single COPY for small tables - use quarterly pattern
        # Glob across all quarterly partitions: filed=*-Q*/entity/TABLE.parquet
        s3_pattern = (
          f"s3://{bucket}/{source_prefix}/filed=*-Q*/{entity_type}/{table_name}.parquet"
        )

        context.log.info(
          f"[{i}/{len(ordered_tables)}] Copying {table_name} "
          f"({len(quarterly_partitions)} quarters)..."
        )

        try:
          copy_result = await client.copy_from_s3(
            graph_id=config.graph_id,
            table_name=table_name,
            s3_pattern=s3_pattern,  # Glob pattern for all quarters
            ignore_errors=True,
            timeout=SINGLE_COPY_TIMEOUT,
            wait_for_completion=True,
          )

          if copy_result.get("status") == "completed":
            records = copy_result.get("records_loaded", 0)
            duration = copy_result.get("duration_seconds", 0)
            total_rows += records
            tables_copied.append(table_name)
            if records > 0:
              context.log.info(
                f"  [OK] {table_name}: {records:,} records in {duration:.1f}s"
              )
            else:
              context.log.info(f"  [OK] {table_name}: done in {duration:.1f}s")
          elif "No files found" in copy_result.get("error", ""):
            context.log.info(f"  [OK] {table_name}: No files (skipped)")
            tables_copied.append(table_name)  # Not a failure
          else:
            error = copy_result.get("error", "Unknown error")
            context.log.error(f"  [FAILED] {table_name}: {error}")
            failed_tables.append({"table": table_name, "error": error})

        except Exception as e:
          if "No files found" in str(e):
            context.log.info(f"  [OK] {table_name}: No files (skipped)")
            tables_copied.append(table_name)
          else:
            context.log.error(f"  [FAILED] {table_name}: {e}")
            failed_tables.append({"table": table_name, "error": str(e)})

    return {
      "status": "success" if not failed_tables else "partial",
      "tables_copied": tables_copied,
      "total_rows": total_rows,
      "failed_tables": failed_tables,
      "quarters_used": len(quarterly_partitions),
    }

  result = asyncio.run(run_direct_copy())

  duration_ms = (time.time() - start_time) * 1000

  # Restore memory to defaults
  try:
    restore_result = asyncio.run(restore_graph_memory(config.graph_id))
    context.log.info(f"Memory restored: {restore_result.get('message', 'done')}")
  except Exception as restore_err:
    context.log.warning(f"Could not restore memory (non-fatal): {restore_err}")

  context.log.info(
    f"Direct copy complete: {len(result['tables_copied'])} tables, "
    f"{result['total_rows']:,} rows, {duration_ms / 1000:.1f}s"
  )

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "rebuild_graph": config.rebuild_graph,
      "status": result["status"],
      "total_rows": result["total_rows"],
      "tables_copied": result["tables_copied"],
      "failed_tables": result["failed_tables"],
      "quarters_used": result["quarters_used"],
      "duration_ms": duration_ms,
    }
  )
