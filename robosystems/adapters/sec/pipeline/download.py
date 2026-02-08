"""SEC Download Asset.

This module contains the sec_raw_filings asset for downloading SEC XBRL filings
using EFTS discovery.
"""

from dagster import (
  AssetExecutionContext,
  BackfillPolicy,
  MaterializeResult,
  asset,
)

from robosystems.config import env
from robosystems.config.storage.shared import DataSourceType, get_raw_key
from robosystems.dagster.resources import DatabaseResource, S3Resource
from robosystems.models.iam import Graph, SourceFile

from .configs import (
  SEC_FORM_TYPE_BATCHES,
  SECDownloadConfig,
  sec_quarter_partitions,
)


@asset(
  group_name="sec_pipeline",
  description="Download SEC XBRL filings from EFTS to S3",
  kinds={"download"},
  partitions_def=sec_quarter_partitions,
  metadata={
    "pipeline": "sec",
    "graph_id": "sec",
    "stage": "download",
    "mode": "full",
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
    from robosystems.adapters.sec.config import SEC_CONFIG

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
      _ = await coro
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
