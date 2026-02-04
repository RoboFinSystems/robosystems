"""SEC Processing Asset.

This module contains the sec_processed_filings asset for processing
SEC XBRL filings into consolidated parquet files.
"""

from pathlib import Path

from dagster import (
  AssetExecutionContext,
  BackfillPolicy,
  MaterializeResult,
  asset,
)
from sqlalchemy import and_

from robosystems.adapters.sec import SECMetadataLoader
from robosystems.adapters.sec.processors import (
  atomic_s3_upload,
  consolidate_parquet_from_disk,
  merge_with_existing_s3,
  process_single_filing_to_memory,
)
from robosystems.config import env
from robosystems.config.storage.shared import DataSourceType, get_processed_key
from robosystems.dagster.resources import DatabaseResource, S3Resource
from robosystems.models.iam import SourceFile

from .configs import SECProcessConfig, sec_quarter_partitions


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

  # Create metadata loader for fetching SEC metadata
  metadata_loader = SECMetadataLoader()

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
      new_parquet_bytes = consolidate_parquet_from_disk(work_dir, table_key)
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
      merged_bytes = merge_with_existing_s3(
        s3_client=s3.client,
        bucket=processed_bucket,
        s3_key=s3_key,
        new_data=new_parquet_bytes,
        table_key=table_key,
      )

      # Upload atomically (temp file + copy pattern)
      atomic_s3_upload(
        s3_client=s3.client,
        bucket=processed_bucket,
        final_key=s3_key,
        data=merged_bytes,
      )
      tables_uploaded += 1
      context.log.info(f"Uploaded: {s3_key} ({len(merged_bytes):,} bytes)")

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
      result = process_single_filing_to_memory(
        storage_key=storage_key,
        partition_key=file_partition_key,
        source_file_id=source_file_id,
        s3_client=s3.client,
        raw_bucket=raw_bucket,
        metadata_loader=metadata_loader,
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
