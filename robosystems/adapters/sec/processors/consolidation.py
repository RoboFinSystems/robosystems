"""Consolidate per-filing parquet into one file per table for DuckDB staging."""

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
  """Deduplicate on ``column``, keeping the first occurrence; ``label`` is for logging."""
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
  """Merge filing results into ``{filing_date: {table_key: parquet_bytes}}``.

  In memory; the Dagster asset uses ``consolidate_parquet_from_disk`` to bound memory.
  """
  tables_by_date_and_key: dict[str, dict[str, list[pa.Table]]] = {}

  for result in results:
    if not result.success:
      continue

    filing_date = result.filing_date or "unknown"

    if filing_date not in tables_by_date_and_key:
      tables_by_date_and_key[filing_date] = {}

    for key, parquet_bytes in result.tables.items():
      if key not in tables_by_date_and_key[filing_date]:
        tables_by_date_and_key[filing_date][key] = []
      reader = pq.ParquetFile(BytesIO(parquet_bytes))
      table = reader.read()
      tables_by_date_and_key[filing_date][key].append(table)

  consolidated: dict[str, dict[str, bytes]] = {}

  for filing_date, tables_by_key in tables_by_date_and_key.items():
    consolidated[filing_date] = {}
    for key, tables in tables_by_key.items():
      if not tables:
        continue
      combined = pa.concat_tables(tables, promote_options="permissive")
      del tables

      if key in SHARED_NODE_TABLES and "identifier" in combined.column_names:
        combined = _dedup_arrow_table(combined, "identifier")

      buffer = BytesIO()
      pq.write_table(combined, buffer)
      consolidated[filing_date][key] = buffer.getvalue()

  return consolidated


def consolidate_parquet_from_disk(
  work_dir: Path,
  table_key: str,
) -> bytes | None:
  """Concatenate a table's parquet files under ``work_dir/table_key`` into one blob.

  Shared node tables are deduplicated within the batch; cross-batch dedup
  happens in DuckDB staging. None when the table has no data.
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
      # Handle, not a path — see adapters/sec/knowledge/__init__.py.
      with open(pq_file, "rb") as f:
        table = pq.read_table(f)
      tables.append(table)
    except Exception as e:
      logger.warning("Skipping unreadable parquet file %s: %s", pq_file, e)
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
  """Merge ``new_data`` into the parquet at ``s3_key`` (if any), deduplicating shared tables."""
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
    logger.info("No existing S3 file at %s, creating new file", s3_key)
    return new_data
  except Exception as e:
    # Unreadable existing file: the new data overwrites it.
    logger.warning("Failed to read existing S3 file %s, will overwrite: %s", s3_key, e)
    return new_data

  try:
    existing_table = pq.read_table(BytesIO(existing_data))
    new_table = pq.read_table(BytesIO(new_data))
  except Exception as e:
    logger.warning(
      "Failed to parse parquet for merge at %s, keeping existing: %s", s3_key, e
    )
    return existing_data

  combined = pa.concat_tables([existing_table, new_table], promote_options="permissive")
  pre_dedup_rows = combined.num_rows

  if table_key in SHARED_NODE_TABLES and "identifier" in combined.column_names:
    combined = _dedup_arrow_table(combined, "identifier", table_key)

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
  """Upload to a temp key then copy over ``final_key``, so readers never see a partial object."""
  import uuid

  temp_key = f"{final_key}.tmp.{uuid.uuid4().hex[:8]}"

  try:
    s3_client.put_object(
      Bucket=bucket,
      Key=temp_key,
      Body=data,
      ContentType="application/octet-stream",
    )

    s3_client.copy_object(
      Bucket=bucket,
      CopySource={"Bucket": bucket, "Key": temp_key},
      Key=final_key,
    )

    s3_client.delete_object(Bucket=bucket, Key=temp_key)

  except Exception:
    try:
      s3_client.delete_object(Bucket=bucket, Key=temp_key)
    except Exception as cleanup_exc:
      logger.warning(
        "Failed to delete temporary S3 object %s during cleanup: %s",
        temp_key,
        cleanup_exc,
      )
    raise
