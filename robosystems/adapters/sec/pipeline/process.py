"""sec_processed_filings: one batch of SEC XBRL filings to parquet part files."""

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
  backfill_policy=BackfillPolicy.single_run(),
)
def sec_processed_filings(
  context: AssetExecutionContext,
  config: SECProcessConfig,
  s3: S3Resource,
  db: DatabaseResource,
) -> MaterializeResult:
  """Process up to batch_size pending filings, write one part file per table,
  mark them success, and exit; the sensor re-triggers while files remain.

  Each filing's output is cached to S3 as it completes, so a Spot restart
  restores instead of reprocessing. SIGTERM stops the loop and a best-effort
  flush follows; whatever it misses, the cache covers next run.
  """
  import shutil
  import tempfile
  import time as time_module

  raw_bucket = env.SHARED_RAW_BUCKET
  processed_bucket = env.SHARED_PROCESSED_BUCKET

  partition_key = context.partition_key
  year, quarter_str = partition_key.split("-Q")
  year = int(year)
  quarter = int(quarter_str)

  partition_date = partition_key  # also the S3 filed= partition

  context.log.info(
    f"Processing SEC filings for {partition_key} (quarterly partition: filed={partition_date})"
  )

  # SourceFile.partition_key is "YYYY-QN_cik_accession".
  quarter_prefix = f"{year}-Q{quarter}_"

  # Recover files a crashed run left "processing". Safe only because the
  # sensor runs one worker per quarter.
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

  work_dir = Path(tempfile.mkdtemp(prefix=f"sec_processing_{year}Q{quarter}_"))
  context.log.info(f"Work directory: {work_dir}")

  metadata_loader = SECMetadataLoader()

  # One enricher for the batch, not a ~130 MB model load per filing.
  from robosystems.adapters.sec.config import XBRL_SEMANTIC_ENRICHMENT

  shared_enricher = None
  if XBRL_SEMANTIC_ENRICHMENT:
    from robosystems.adapters.sec.enrichment import SemanticEnricher

    shared_enricher = SemanticEnricher()
    context.log.info("Created shared SemanticEnricher for batch processing")

  # Completed filings are already cached, so SIGTERM only stops the loop after
  # the current filing.
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

  succeeded = 0
  failed = 0
  skipped = 0
  cache_hits = 0
  failed_ids: list[str] = []
  pending_flush: list[dict] = []
  flushed_cache_keys: list[str] = []  # deleted once the flush succeeds
  total_flushed = 0
  tables_uploaded = 0

  def flush_to_s3() -> int:
    """Upload one part file per table from the disk buffer, then mark success.

    A crash between upload and mark_success leaves orphan part files and the
    filings pending; the re-run writes new UUID-named parts beside them, and
    staging dedups the duplicate rows.
    """
    nonlocal tables_uploaded, total_flushed

    if not pending_flush:
      return 0

    # work_dir/<nodes|relationships>/<Table>/<source_file_id>.parquet
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

    if config.enable_cache and flushed_cache_keys:
      try:
        deleted = delete_cache_keys(s3.client, processed_bucket, flushed_cache_keys)
        context.log.info(f"Cleaned up {deleted} cache entries from S3")
      except Exception as e:
        context.log.warning(f"Cache cleanup failed (non-fatal): {e}")
      flushed_cache_keys.clear()

    for item in work_dir.iterdir():
      if item.is_dir():
        shutil.rmtree(item)
      else:
        item.unlink()
    pending_flush.clear()

    return flushed_count

  try:
    for i, file_info in enumerate(files_to_process):
      if shutting_down:
        context.log.warning(
          f"Shutting down after SIGTERM — {i}/{len(files_to_process)} filings processed, "
          f"{len(pending_flush)} pending flush (cached on S3)"
        )
        break

      source_file_id = file_info["id"]
      storage_key = file_info["storage_key"]
      file_partition_key = file_info["partition_key"]

      if config.enable_cache:
        cache_key = get_cache_key(DataSourceType.SEC, partition_date, source_file_id)
        try:
          if cache_exists(s3.client, processed_bucket, cache_key):
            filing_start = time_module.time()
            download_and_extract(
              s3.client, processed_bucket, cache_key, work_dir, source_file_id
            )
            filing_duration = time_module.time() - filing_start

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

      context.log.info(
        f"[{i + 1}/{len(files_to_process)}] Processing: {file_partition_key}"
      )
      filing_start = time_module.time()

      with db.get_session() as session:
        sf = SourceFile.get_by_storage_key(storage_key, session)
        if sf:
          sf.mark_processing(session)

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
        # Buffered on disk, not in memory.
        for table_key, parquet_bytes in result.tables.items():
          table_dir = work_dir / table_key
          table_dir.mkdir(parents=True, exist_ok=True)
          parquet_path = table_dir / f"{source_file_id}.parquet"
          parquet_path.write_bytes(parquet_bytes)

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

      if (i + 1) % 100 == 0:
        context.log.info(
          f"Progress: {i + 1}/{len(files_to_process)} processed, "
          f"{succeeded} succeeded, {failed} failed, {skipped} skipped, "
          f"{cache_hits} cache hits"
        )

    # Free the enricher's model and taxonomies before the flush's Arrow concat.
    if shared_enricher is not None:
      del shared_enricher
      context.log.info("Released SemanticEnricher before flush")
    del metadata_loader
    gc.collect()

    if pending_flush:
      context.log.info(f"Flushing {len(pending_flush)} filings to S3...")
      flush_to_s3()

  finally:
    signal.signal(signal.SIGTERM, original_sigterm)

    if work_dir.exists():
      shutil.rmtree(work_dir)
      context.log.debug(f"Cleaned up work directory: {work_dir}")

  context.log.info(
    f"Complete: {succeeded}/{len(files_to_process)} filings succeeded, "
    f"{skipped} skipped, {failed} failed, {cache_hits} cache hits, "
    f"{tables_uploaded} table files uploaded to partition filed={partition_date}"
    + (" (terminated by SIGTERM)" if shutting_down else "")
  )

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
      "failed_source_file_ids": failed_ids[:20],
      "cache_hits": cache_hits,
      "sigterm_received": shutting_down,
      "tables_uploaded": tables_uploaded,
      "batch_size": config.batch_size,
      "form_types_filter": config.form_types,
      "remaining_pending": remaining_count,
    }
  )
