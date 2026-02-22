"""
SEC Parquet Consolidation.

This module contains functions for consolidating parquet files from multiple
SEC filings into single files for efficient DuckDB staging.
"""

from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from robosystems.logger import get_logger

from .constants import QUARTER_END_DAYS, SHARED_NODE_TABLES

if TYPE_CHECKING:
  from .processing import ProcessedFilingResult

logger = get_logger(__name__)


def get_quarter_end_date(year: int, quarter: int) -> str:
  """Get the last day of a quarter as YYYY-MM-DD string.

  Used for backfill partitioning - all filings in a quarter get the same
  partition date (end of quarter) for simpler S3 organization.

  Args:
      year: Calendar year
      quarter: Quarter number (1-4)

  Returns:
      Date string like "2024-03-31" for Q1 2024
  """
  return f"{year}{QUARTER_END_DAYS[quarter]}"


def consolidate_parquet_tables_by_date(
  results: list["ProcessedFilingResult"],
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

  # Consolidate each table type for each filing date
  consolidated: dict[str, dict[str, bytes]] = {}

  for filing_date, tables_by_key in tables_by_date_and_key.items():
    consolidated[filing_date] = {}
    for key, tables in tables_by_key.items():
      if not tables:
        continue
      # Concatenate all tables of this type for this date
      combined = pa.concat_tables(tables, promote_options="permissive")
      del tables

      # Write to bytes
      buffer = BytesIO()
      pq.write_table(combined, buffer)
      consolidated[filing_date][key] = buffer.getvalue()

  return consolidated


def consolidate_parquet_from_disk(
  work_dir: Path,
  table_key: str,
) -> bytes:
  """Consolidate all parquet files for a table from disk into single bytes.

  Pure Arrow concat — no dedup. Deduplication is handled by DuckDB during
  staging via GROUP BY + FIRST() with spill-to-disk.

  Args:
      work_dir: Directory containing parquet files
      table_key: Table key like "nodes/Entity"

  Returns:
      Consolidated parquet bytes
  """
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
  del tables

  # Write to bytes
  buffer = BytesIO()
  pq.write_table(combined, buffer)
  return buffer.getvalue()


def merge_with_existing_s3(
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
  # Try to download existing file
  existing_data: bytes | None = None
  try:
    response = s3_client.get_object(Bucket=bucket, Key=s3_key)
    existing_data = response["Body"].read()
    logger.info(
      "Downloaded existing S3 file for merge: %s (%s bytes)",
      s3_key,
      f"{len(existing_data):,}",
    )
  except s3_client.exceptions.NoSuchKey:
    # No existing file - return new data as-is
    logger.info("No existing S3 file at %s, creating new file", s3_key)
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
  pre_dedup_rows = combined.num_rows

  # Deduplicate shared tables on identifier
  if table_key in SHARED_NODE_TABLES and "identifier" in combined.column_names:
    df = combined.to_pandas()
    df = df.drop_duplicates(subset=["identifier"], keep="first")
    combined = pa.Table.from_pandas(df, preserve_index=False)

  # Write merged result
  buffer = BytesIO()
  pq.write_table(combined, buffer)
  merged_bytes = buffer.getvalue()

  logger.info(
    "Merged %s: %s existing + %s new = %s rows (%s after dedup), %s bytes",
    table_key,
    f"{existing_table.num_rows:,}",
    f"{new_table.num_rows:,}",
    f"{pre_dedup_rows:,}",
    f"{combined.num_rows:,}",
    f"{len(merged_bytes):,}",
  )
  return merged_bytes


def atomic_s3_upload(
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
