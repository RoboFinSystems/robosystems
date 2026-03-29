"""SEC Processing Asset.

This module contains the sec_processed_filings asset for processing
SEC XBRL filings into consolidated parquet files.
"""

import gc
import signal
import uuid
from pathlib import Path

from dagster import (
  AssetExecutionContext,
  BackfillPolicy,
  MaterializeResult,
  asset,
)
from sqlalchemy import and_

from robosystems.adapters.sec.processors import (
  SECMetadataLoader,
  atomic_s3_upload,
  cache_exists,
  consolidate_parquet_from_disk,
  delete_cache_keys,
  download_and_extract,
  process_single_filing_to_memory,
  zip_and_upload,
)
from robosystems.config import env
from robosystems.config.storage.shared import (
  DataSourceType,
  get_cache_key,
  get_processed_key,
)
from robosystems.dagster.resources import DatabaseResource, S3Resource
from robosystems.models.core import SourceFile

from .configs import SECProcessConfig, sec_quarter_partitions


@asset(
  group_name="sec_pipeline",
  description="Process SEC filings into parquet files",
  kinds={"transform"},
  partitions_def=sec_quarter_partitions,
  metadata={
    "pipeline": "sec",
    "graph_id": "sec",
    "stage": "process",
    "mode": "full",
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
  """Process one batch of SEC filings, flush to S3, then exit.

  Processes up to batch_size pending SourceFiles (default 250), writes
  one part file per table to S3, marks them success, and exits. The sensor
  re-triggers if more pending files remain, enabling natural memory release
  between runs.

  Spot Resilience:
  - Each filing's results are cached to S3 as a zip immediately after processing
  - On restart, cached results are restored from S3 (skips reprocessing)
  - SIGTERM handler stops the loop; best-effort flush follows (if the 2-minute
    spot window allows, filings are consolidated and marked success — if not,
    the cache covers them on the next run)

  Memory Management:
  - One batch per run (default 250 filings), then container exits
  - Small batch size keeps Arrow concat under ~325 MB peak (Label at ~1.3 MB/file)
  - One part file per table per batch (no chunking needed)
  - Shared tables (Element, Label, etc.) deduped within batch via pure Arrow
  - DuckDB handles final cross-batch dedup during staging
  - del + gc.collect() after each table upload to force memory release

  Output Structure (part files):
    s3://bucket/sec/processed/filed=2024-Q1/nodes/Entity/part_a1b2c3d4e5f6.parquet
    - One part file per table per batch, multiple batches per quarter
    - UUID naming prevents collisions across runs
    - DuckDB reads both old format (TABLE.parquet) and new (TABLE/*.parquet)

  Returns:
      MaterializeResult with processing statistics
  """
  import shutil
  import tempfile
  import time as time_module

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
    # Query pending files, ordered by discovery time, limited to batch_size.
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
      .limit(config.batch_size)
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

  if config.form_types:
    context.log.info(f"Form type filter active: {config.form_types}")

  context.log.info(
    f"Processing batch of {len(files_to_process)} filings for {partition_key} "
    f"(batch_size={config.batch_size})"
  )

  # Create work directory for disk-buffered processing
  work_dir = Path(tempfile.mkdtemp(prefix=f"sec_processing_{year}Q{quarter}_"))
  context.log.info(f"Work directory: {work_dir}")

  # Create metadata loader for fetching SEC metadata
  metadata_loader = SECMetadataLoader()

  # Create shared enricher — reuses fastembed model + taxonomies across all filings
  # instead of loading ~130MB model per filing
  from robosystems.adapters.sec.config import XBRL_SEMANTIC_ENRICHMENT

  shared_enricher = None
  if XBRL_SEMANTIC_ENRICHMENT:
    from robosystems.adapters.sec.enrichment import SemanticEnricher

    shared_enricher = SemanticEnricher()
    context.log.info("Created shared SemanticEnricher for batch processing")

  # SIGTERM handler for spot instance resilience.
  # With the S3 cache, every completed filing is already safe — SIGTERM just
  # stops the loop so the current filing can finish and be cached before exit.
  shutting_down = False

  def handle_sigterm(signum, frame):
    nonlocal shutting_down
    shutting_down = True
    context.log.warning(
      "SIGTERM received — finishing current filing then exiting. "
      "All completed filings are safe in S3 cache."
    )

  original_sigterm = signal.getsignal(signal.SIGTERM)
  signal.signal(signal.SIGTERM, handle_sigterm)

  # Track processing state
  succeeded = 0
  failed = 0
  skipped = 0
  cache_hits = 0
  failed_ids: list[str] = []
  pending_flush: list[dict] = []  # [{...file_info}, ...]
  flushed_cache_keys: list[str] = []  # Cache keys to delete after successful flush
  total_flushed = 0
  tables_uploaded = 0  # Track number of table files uploaded

  def flush_to_s3() -> int:
    """Consolidate disk buffer into one part file per table on S3, mark success.

    For each table: concat all parquets in the batch (Arrow) -> upload as
    a single part file. Shared tables deduped within the batch via pure Arrow.

    Crash resilience: If the job crashes after S3 upload but before mark_success,
    orphan part files remain on S3 and filings stay "pending". On re-run, new
    part files are written alongside orphans (UUIDs prevent overwrites). This
    creates duplicate rows across part files, but DuckDB handles dedup during
    staging via GROUP BY + FIRST() with spill-to-disk.
    """
    nonlocal tables_uploaded, total_flushed

    if not pending_flush:
      return 0

    # Find all table directories in work_dir
    # Disk structure: work_dir/nodes/Entity/...
    table_keys = set()
    for subdir in work_dir.rglob("*.parquet"):
      rel_path = subdir.relative_to(work_dir)
      if len(rel_path.parts) >= 2:
        table_key = f"{rel_path.parts[0]}/{rel_path.parts[1]}"
        table_keys.add(table_key)

    for table_key in sorted(table_keys):
      consolidated = consolidate_parquet_from_disk(work_dir, table_key)
      if not consolidated:
        continue

      entity_type, table_name = table_key.split("/", 1)
      part_id = uuid.uuid4().hex[:12]
      s3_key = get_processed_key(
        DataSourceType.SEC,
        "processed",
        f"filed={partition_date}",
        entity_type,
        table_name,
        f"part_{part_id}.parquet",
      )

      atomic_s3_upload(
        s3_client=s3.client,
        bucket=processed_bucket,
        final_key=s3_key,
        data=consolidated,
      )
      tables_uploaded += 1
      context.log.info(f"Uploaded: {s3_key} ({len(consolidated):,} bytes)")

      del consolidated
      gc.collect()

    # Mark all pending filings as success (data is now safely in S3)
    with db.get_session() as session:
      for file_info in pending_flush:
        sf = SourceFile.get_by_storage_key(file_info["storage_key"], session)
        if sf:
          sf.mark_success(session)

    flushed_count = len(pending_flush)
    total_flushed += flushed_count
    context.log.info(
      f"Flushed {flushed_count} filings, {tables_uploaded} table files uploaded"
    )

    # Clean up S3 cache entries for flushed filings
    if config.enable_cache and flushed_cache_keys:
      try:
        deleted = delete_cache_keys(s3.client, processed_bucket, flushed_cache_keys)
        context.log.info(f"Cleaned up {deleted} cache entries from S3")
      except Exception as e:
        context.log.warning(f"Cache cleanup failed (non-fatal): {e}")
      flushed_cache_keys.clear()

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
      # Check for SIGTERM before starting a new filing
      if shutting_down:
        context.log.warning(
          f"Shutting down after SIGTERM — {i}/{len(files_to_process)} filings processed, "
          f"{len(pending_flush)} pending flush (cached on S3)"
        )
        break

      source_file_id = file_info["id"]
      storage_key = file_info["storage_key"]
      file_partition_key = file_info["partition_key"]

      # Check S3 cache for previously processed results (spot resilience)
      if config.enable_cache:
        cache_key = get_cache_key(DataSourceType.SEC, partition_date, source_file_id)
        try:
          if cache_exists(s3.client, processed_bucket, cache_key):
            filing_start = time_module.time()
            download_and_extract(
              s3.client, processed_bucket, cache_key, work_dir, source_file_id
            )
            filing_duration = time_module.time() - filing_start

            # Mark as processing then track for flush
            with db.get_session() as session:
              sf = SourceFile.get_by_storage_key(storage_key, session)
              if sf:
                sf.mark_processing(session)

            succeeded += 1
            cache_hits += 1
            pending_flush.append(file_info)
            flushed_cache_keys.append(cache_key)
            context.log.info(
              f"[{i + 1}/{len(files_to_process)}] Cache hit: {file_partition_key} "
              f"({filing_duration:.1f}s)"
            )
            continue
        except Exception as e:
          context.log.warning(
            f"[{i + 1}/{len(files_to_process)}] Cache read failed for "
            f"{file_partition_key}, reprocessing: {e}"
          )

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
      result = process_single_filing_to_memory(
        storage_key=storage_key,
        partition_key=file_partition_key,
        source_file_id=source_file_id,
        s3_client=s3.client,
        raw_bucket=raw_bucket,
        metadata_loader=metadata_loader,
        allowed_form_types=config.form_types,
        enricher=shared_enricher,
      )

      filing_duration = time_module.time() - filing_start

      # Handle skipped filings (form type filter)
      if result.skipped_reason:
        with db.get_session() as session:
          sf = SourceFile.get_by_storage_key(storage_key, session)
          if sf:
            sf.status = "skipped"
            sf.error_message = result.skipped_reason
            session.commit()
        skipped += 1
        context.log.debug(
          f"[{i + 1}/{len(files_to_process)}] Skipped: {file_partition_key} "
          f"({result.skipped_reason})"
        )
        continue

      if result.success:
        # Write parquet files to disk (not memory accumulation)
        # Disk structure: work_dir/nodes/Entity/...
        for table_key, parquet_bytes in result.tables.items():
          table_dir = work_dir / table_key
          table_dir.mkdir(parents=True, exist_ok=True)
          parquet_path = table_dir / f"{source_file_id}.parquet"
          parquet_path.write_bytes(parquet_bytes)

        # Cache to S3 for spot resilience (single zip, atomic PUT)
        if config.enable_cache:
          cache_key = get_cache_key(DataSourceType.SEC, partition_date, source_file_id)
          try:
            zip_and_upload(s3.client, processed_bucket, cache_key, result.tables)
            flushed_cache_keys.append(cache_key)
          except Exception as e:
            context.log.warning(
              f"[{i + 1}/{len(files_to_process)}] Cache write failed for "
              f"{file_partition_key} (non-fatal): {e}"
            )

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
          f"{succeeded} succeeded, {failed} failed, {skipped} skipped, "
          f"{cache_hits} cache hits"
        )

    # Release heavy objects before flush to reclaim memory.
    # SemanticEnricher holds fastembed model (~130 MB) + taxonomy data.
    # These are no longer needed — all filings have been processed.
    if shared_enricher is not None:
      del shared_enricher
      context.log.info("Released SemanticEnricher before flush")
    del metadata_loader
    gc.collect()

    # Flush all processed filings to S3
    if pending_flush:
      context.log.info(f"Flushing {len(pending_flush)} filings to S3...")
      flush_to_s3()

  finally:
    # Restore original SIGTERM handler
    signal.signal(signal.SIGTERM, original_sigterm)

    # Cleanup work directory
    if work_dir.exists():
      shutil.rmtree(work_dir)
      context.log.debug(f"Cleaned up work directory: {work_dir}")

  context.log.info(
    f"Complete: {succeeded}/{len(files_to_process)} filings succeeded, "
    f"{skipped} skipped, {failed} failed, {cache_hits} cache hits, "
    f"{tables_uploaded} table files uploaded to partition filed={partition_date}"
    + (" (terminated by SIGTERM)" if shutting_down else "")
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
      "filings_skipped": skipped,
      "filings_flushed": total_flushed,
      "failed_source_file_ids": failed_ids[:20],  # Limit to first 20
      "cache_hits": cache_hits,
      "sigterm_received": shutting_down,
      "tables_uploaded": tables_uploaded,
      "batch_size": config.batch_size,
      "form_types_filter": config.form_types,
      "remaining_pending": remaining_count,
    }
  )
