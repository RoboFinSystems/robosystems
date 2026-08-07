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


def _dedup_arrow_table(
  table: pa.Table, column: str, label: str | None = None
) -> pa.Table:
  """Deduplicate an Arrow table on a column, keeping first occurrence.

  Uses pure Arrow operations — no Pandas round-trip. Iterates the dedup
  column to build a set of seen values, then filters via pa.Table.take().
  For 250-filing batches, shared tables are typically 30-60K rows, so the
  Python set approach is fast and memory-efficient.

  `label` (usually the table_key) only appears in debug logging.
  """
  original_rows = table.num_rows
  identifiers = table.column(column)

  seen: set[str] = set()
  keep_indices: list[int] = []
  for i, val in enumerate(identifiers.to_pylist()):
    if val not in seen:
      seen.add(val)
      keep_indices.append(i)

  if len(keep_indices) == original_rows:
    return table

  result = table.take(keep_indices)
  if label:
    logger.debug(
      "Deduplicated %s: %d -> %d rows",
      label,
      original_rows,
      result.num_rows,
    )
  return result


def get_quarter_end_date(year: int, quarter: int) -> str:
  """Get the last day of a quarter as a YYYY-MM-DD string ("2024-03-31").

  Backfill partitioning gives every filing in a quarter the same partition
  date, so S3 lays out one prefix per quarter.
  """
  return f"{year}{QUARTER_END_DAYS[quarter]}"


def consolidate_parquet_tables_by_date(
  results: list["ProcessedFilingResult"],
) -> dict[str, dict[str, bytes]]:
  """Consolidate parquet tables from multiple filing results, grouped by filing date.

  Merges all tables of the same type into a single parquet blob per filing
  date, returning `{filing_date: {table_key: parquet_bytes}}` — e.g.
  `{"2024-01-15": {"nodes/Entity": b"...", "nodes/Fact": b"..."}}`.

  Used by `robosystems/scripts/sec_pipeline.py`. The Dagster asset uses
  disk-buffered processing instead, to bound memory at corpus scale.
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

      # Deduplicate shared node tables on identifier column (pure Arrow, no Pandas)
      # Reduces part file size so DuckDB has less work during staging
      if key in SHARED_NODE_TABLES and "identifier" in combined.column_names:
        combined = _dedup_arrow_table(combined, "identifier")

      # Write to bytes
      buffer = BytesIO()
      pq.write_table(combined, buffer)
      consolidated[filing_date][key] = buffer.getvalue()

  return consolidated


def consolidate_parquet_from_disk(
  work_dir: Path,
  table_key: str,
) -> bytes | None:
  """Consolidate all parquet files for a table from disk into a single bytes object.

  Reads all parquet files for a table, concatenates them via Arrow, and returns
  consolidated parquet bytes. With batch sizes of 250 filings, peak Arrow memory
  stays well under limits (~325 MB for Label at ~1.3 MB/file).

  For shared node tables (Element, Label, Reference, Unit, Period), deduplicates
  on the identifier column using pure Arrow. Cross-batch deduplication is handled
  by DuckDB during staging via GROUP BY + FIRST().

  `table_key` is a path fragment like "nodes/Entity". Returns None when the
  table has no data.
  """
  table_dir = work_dir / table_key
  if not table_dir.exists():
    return None

  parquet_files = sorted(table_dir.glob("*.parquet"))
  if not parquet_files:
    return None

  tables = []
  for pq_file in parquet_files:
    try:
      table = pq.read_table(pq_file)
      tables.append(table)
    except Exception as e:
      logger.warning("Skipping corrupted parquet file %s: %s", pq_file, e)
      continue

  if not tables:
    return None

  combined = pa.concat_tables(tables, promote_options="permissive")
  del tables

  if table_key in SHARED_NODE_TABLES and "identifier" in combined.column_names:
    combined = _dedup_arrow_table(combined, "identifier", table_key)

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

  `s3_key` may not exist yet; in that case `new_data` is returned unchanged.
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

  # Deduplicate shared tables on identifier (pure Arrow, no Pandas round-trip)
  if table_key in SHARED_NODE_TABLES and "identifier" in combined.column_names:
    combined = _dedup_arrow_table(combined, "identifier", table_key)

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
  """
  import uuid

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
