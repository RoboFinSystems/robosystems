"""sec_raw_filings: EFTS discovery and download of SEC XBRL ZIPs to S3."""

from dagster import (
  AssetExecutionContext,
  BackfillPolicy,
  MaterializeResult,
  asset,
)

from robosystems.config import env
from robosystems.config.storage.shared import DataSourceType, get_raw_key
from robosystems.dagster.resources import DatabaseResource, S3Resource
from robosystems.models.core import Graph, SourceFile

from .configs import (
  SEC_FORM_TYPE_BATCHES,
  SECDownloadConfig,
  sec_quarter_partitions,
)

_MAX_429_RETRIES = 3
_MAX_RETRY_AFTER = 300


async def _get_with_429_retry(session, url: str, log) -> tuple[int, bytes]:
  """GET ``url``, waiting out 429s in place so the caller keeps its slot.

  Returns ``(status, body)``; the body is empty for a 404 or an exhausted 429.
  """
  import asyncio

  for attempt in range(_MAX_429_RETRIES + 1):
    async with session.get(url) as response:
      if response.status == 404:
        return 404, b""
      if response.status != 429:
        response.raise_for_status()
        return response.status, await response.read()
      if attempt == _MAX_429_RETRIES:
        log.warning(f"Rate limited, giving up after {_MAX_429_RETRIES} retries")
        return 429, b""
      retry_after = min(int(response.headers.get("Retry-After", 60)), _MAX_RETRY_AFTER)
    log.warning(
      f"Rate limited, waiting {retry_after}s (retry {attempt + 1}/{_MAX_429_RETRIES})"
    )
    await asyncio.sleep(retry_after)
  raise AssertionError("unreachable")


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
  backfill_policy=BackfillPolicy.single_run(),
)
def sec_raw_filings(
  context: AssetExecutionContext,
  config: SECDownloadConfig,
  s3: S3Resource,
  db: DatabaseResource,
) -> MaterializeResult:
  """Download one quarter's XBRL ZIPs and register them as SourceFiles.

  Refuses a multi-quarter selection: run each quarter separately.
  """
  import asyncio

  partition_keys = context.partition_keys
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

  year, quarter_str = partition_key.split("-Q")
  year = int(year)
  quarter = int(quarter_str)
  context.log.info(f"Downloading SEC filings for {year}-Q{quarter} via EFTS")

  bucket = env.SHARED_RAW_BUCKET

  async def run_efts_download():
    import aiohttp
    from xbrlkit.edgar import EftsClient, EftsHit

    from robosystems.adapters.sec.client.edgar import edgar_client
    from robosystems.adapters.sec.client.rate_limiter import (
      AsyncRateLimiter,
      RateMonitor,
    )
    from robosystems.adapters.sec.config import SEC_CONFIG, xbrlkit_config

    SEC_BASE_URL = SEC_CONFIG["base_url"]
    SEC_HEADERS = SEC_CONFIG["headers"]

    context.log.info("Phase 1: Discovering filings via EFTS...")

    # EFTS discovery and the submissions header are xbrlkit's synchronous
    # clients (their own spacing and Retry-After handling); they run in a
    # thread so the event loop that drives the parallel downloads stays free.
    efts = EftsClient(xbrlkit_config(), per_sec=5.0)
    cik_filter = None
    if config.ciks:
      cik_filter = config.ciks
    elif config.tickers:
      companies_raw = await asyncio.to_thread(edgar_client().company_tickers)
      cik_filter = []
      for _, company in companies_raw.items():
        ticker = company.get("ticker", "")
        if ticker in config.tickers:
          cik = str(company.get("cik_str", company.get("cik", "")))
          cik_filter.append(cik)

    # One EFTS query per form batch to stay under its 10k cap.
    requested_forms = set(config.form_types)
    form_batches = []
    for batch in SEC_FORM_TYPE_BATCHES:
      batch_forms = [f for f in batch if f in requested_forms]
      if batch_forms:
        form_batches.append(batch_forms)

    known_forms = {f for batch in SEC_FORM_TYPE_BATCHES for f in batch}
    custom_forms = [f for f in config.form_types if f not in known_forms]
    if custom_forms:
      form_batches.append(custom_forms)

    hits = []
    for batch_idx, batch_forms in enumerate(form_batches):
      context.log.info(f"EFTS batch {batch_idx + 1}/{len(form_batches)}: {batch_forms}")
      batch_hits = await asyncio.to_thread(
        efts.query_by_quarter,
        year,
        quarter,
        forms=batch_forms,
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

    # Submissions supply company metadata (name, SIC, fiscal year end).
    submissions_fetched = 0
    submissions_failed = 0

    if config.skip_submissions:
      context.log.info("Phase 2: Skipping submissions fetch (skip_submissions=True)")
    else:
      unique_ciks = list({hit.cik for hit in hits})
      context.log.info(
        f"Phase 2: Fetching submissions for {len(unique_ciks)} unique companies..."
      )

      # Always refreshed (skip_existing governs ZIPs only): a stored snapshot
      # may predate the filings EFTS just found.
      ciks_to_fetch = unique_ciks

      context.log.info(f"Submissions: {len(ciks_to_fetch)} to refresh")

      if ciks_to_fetch:
        import json
        from datetime import UTC, datetime

        submissions_limiter = AsyncRateLimiter(rate=config.submissions_rate)
        submissions_semaphore = asyncio.Semaphore(config.submissions_concurrency)

        def build_complete_submissions_sync(cik: str) -> dict:
          """Every page of the filer's submissions, as one master file. A
          missing page fails the CIK: a short master is never repaired."""
          from robosystems.adapters.sec.client.edgar import (
            complete_submissions_strict,
          )

          return complete_submissions_strict(cik)

        def incremental_update_submissions(
          existing: dict, cik: str, new_recent: dict
        ) -> dict:
          """Prepend the recent page's unseen filings to the stored master."""
          new_accessions = set(
            new_recent.get("filings", {}).get("recent", {}).get("accessionNumber", [])
          )
          existing_accessions = set(
            existing.get("filings", {}).get("accessionNumber", [])
          )

          new_only = new_accessions - existing_accessions
          if not new_only:
            return existing

          recent_data = new_recent.get("filings", {}).get("recent", {})
          recent_accessions = recent_data.get("accessionNumber", [])

          for field in existing["filings"]:
            if field in recent_data:
              new_values = [
                recent_data[field][i]
                for i, acc in enumerate(recent_accessions)
                if acc in new_only
              ]
              existing["filings"][field] = new_values + existing["filings"][field]

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
                existing_data = None
                try:
                  response = s3.client.get_object(Bucket=bucket, Key=submissions_key)
                  existing_data = json.loads(response["Body"].read().decode("utf-8"))
                except Exception:
                  pass

                if existing_data is None:
                  context.log.info(
                    f"Building complete submissions master for CIK {cik}..."
                  )
                  loop = asyncio.get_event_loop()
                  submissions_data = await loop.run_in_executor(
                    None, build_complete_submissions_sync, cik
                  )
                else:
                  new_recent = await asyncio.to_thread(edgar_client().submissions, cik)

                  submissions_data = incremental_update_submissions(
                    existing_data, cik, new_recent
                  )

                s3.client.put_object(
                  Bucket=bucket,
                  Key=submissions_key,
                  Body=json.dumps(submissions_data),
                  ContentType="application/json",
                )
                submissions_fetched += 1
                return True

              except Exception as e:
                context.log.warning(f"Failed to fetch submissions for CIK {cik}: {e}")
                submissions_failed += 1
                return False

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

    if config.max_filings > 0 and len(hits) > config.max_filings:
      context.log.info(
        f"Limiting to {config.max_filings} filings (of {len(hits)} discovered)"
      )
      hits = hits[: config.max_filings]

    if config.dry_run:
      context.log.info(f"[DRY RUN] Would download {len(hits)} filings:")
      for hit in hits[:10]:
        context.log.info(f"  - {hit.cik}/{hit.accession} ({hit.form})")
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

    context.log.info(f"Phase 3: Downloading {len(hits)} filings...")

    limiter = AsyncRateLimiter(rate=config.download_rate)
    monitor = RateMonitor()
    semaphore = asyncio.Semaphore(config.download_concurrency)

    downloaded = 0
    skipped = 0  # already in S3
    no_xbrl = 0  # filing has no XBRL ZIP
    no_xbrl_filings: list[str] = []
    failed = 0

    source_file_records: list[dict] = []

    async def download_filing(hit: EftsHit) -> bool:
      nonlocal downloaded, skipped, no_xbrl, failed, source_file_records

      s3_key = get_raw_key(
        DataSourceType.SEC,
        f"year={year}",
        hit.cik,
        f"{hit.accession}.zip",
      )

      if config.skip_existing:
        try:
          head_response = s3.client.head_object(Bucket=bucket, Key=s3_key)
          skipped += 1
          # The quarter prefix is what the processing sensor groups by.
          partition_key = f"{year}-Q{quarter}_{hit.cik}_{hit.accession}"
          source_file_records.append(
            {
              "storage_key": s3_key,
              "source_id": hit.accession,
              "partition_key": partition_key,
              "file_size_bytes": head_response.get("ContentLength"),
            }
          )
          return True
        except Exception:
          pass

      cik_no_zeros = str(int(hit.cik))
      accno_no_dash = hit.accession.replace("-", "")
      url = f"{SEC_BASE_URL}/Archives/edgar/data/{cik_no_zeros}/{accno_no_dash}/{hit.accession}-xbrl.zip"

      async with semaphore:
        async with limiter:
          try:
            async with aiohttp.ClientSession(headers=SEC_HEADERS) as session:
              status, content = await _get_with_429_retry(session, url, context.log)

            if status == 404:
              no_xbrl += 1
              no_xbrl_filings.append(f"{hit.cik}/{hit.accession}")
              return True

            if not content:
              failed += 1
              return False

            await monitor.record(len(content))

          except Exception as e:
            context.log.debug(f"Download failed for {hit.accession}: {e}")
            failed += 1
            return False

      try:
        s3.client.put_object(
          Bucket=bucket,
          Key=s3_key,
          Body=content,
          ContentType="application/zip",
        )
        downloaded += 1
        partition_key = f"{year}-Q{quarter}_{hit.cik}_{hit.accession}"
        source_file_records.append(
          {
            "storage_key": s3_key,
            "source_id": hit.accession,
            "partition_key": partition_key,
            "file_size_bytes": len(content),
          }
        )
        return True
      except Exception as e:
        context.log.warning(f"S3 upload failed for {hit.accession}: {e}")
        failed += 1
        return False

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

  source_file_records = result.get("source_file_records", [])
  source_files_created = 0
  source_files_existed = 0
  if source_file_records and not result.get("dry_run"):
    context.log.info(f"Creating {len(source_file_records)} SourceFile records...")
    with db.get_session() as session:
      # SourceFile has an FK to graphs.
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
