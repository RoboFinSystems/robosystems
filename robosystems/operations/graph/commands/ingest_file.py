"""Mark an uploaded file as present and stage it into DuckDB.

Backs the ``ingest-file`` content operation. Small files stage inline; larger
ones are dispatched and the result carries an ``operation_id`` to follow.
"""

from __future__ import annotations

import io
import re
import uuid
from typing import Any

from fastapi import BackgroundTasks, HTTPException, status
from sqlalchemy.orm import Session

from robosystems.config import env
from robosystems.config.constants import (
  FALLBACK_BYTES_PER_ROW_CSV,
  FALLBACK_BYTES_PER_ROW_JSON,
  FALLBACK_BYTES_PER_ROW_PARQUET,
  MAX_FILE_SIZE_MB,
  MAX_ROWS_PER_FILE,
  SMALL_FILE_STAGING_THRESHOLD_MB,
)
from robosystems.config.graph_tier import GraphTierConfig
from robosystems.logger import logger
from robosystems.middleware.operations import run_off_loop
from robosystems.models.api.graphs.tables import FileUploadStatus
from robosystems.models.core import Graph, GraphFile, GraphTable, User
from robosystems.operations.aws.s3 import S3Client

__all__ = ["ingest_file_cmd"]

_FALLBACK_BYTES_PER_ROW = {
  "parquet": FALLBACK_BYTES_PER_ROW_PARQUET,
  "csv": FALLBACK_BYTES_PER_ROW_CSV,
  "json": FALLBACK_BYTES_PER_ROW_JSON,
}

# Parquet locates its metadata from the end of the file: a 4-byte footer length
# followed by the "PAR1" magic. pyarrow reads the last 64 KiB first, same as
# here, and only goes back for more when the footer is bigger.
_PARQUET_TRAILER_BYTES = 8
_PARQUET_TAIL_READ_BYTES = 64 * 1024
# A footer is Thrift metadata (schema + row-group index); a legitimate file under
# the 100 MB cap is nowhere near this. Anything larger is refused as malformed
# rather than decoded into memory.
_PARQUET_MAX_FOOTER_BYTES = 16 * 1024 * 1024
_STREAM_BUFFER_BYTES = 1024 * 1024
_JSON_WHITESPACE = re.compile(r"[ \t\n\r]*")


def _skip_json_whitespace(text: str, index: int) -> int:
  match = _JSON_WHITESPACE.match(text, index)
  return match.end() if match else index


class _PayloadTooLarge(Exception):
  """The object yielded more bytes than the size gate admitted."""


class _BoundedS3Body(io.RawIOBase):
  """Raw stream over an S3 body that stops once ``limit`` bytes have been read.

  The size gate ran against HEAD; the presigned PUT stays valid for an hour, so
  the object can be replaced by a larger one between HEAD and GET. Bounding the
  read here — rather than trusting ContentLength — is what keeps a swap from
  turning the count into an unbounded read.
  """

  def __init__(self, body: Any, limit: int) -> None:
    super().__init__()
    self._body = body
    self._limit = limit
    self._consumed = 0

  def readable(self) -> bool:
    return True

  def readinto(self, buffer: Any) -> int:
    chunk = self._body.read(len(buffer))
    self._consumed += len(chunk)
    if self._consumed > self._limit:
      raise _PayloadTooLarge(f"object exceeded {self._limit:,} bytes while being read")
    buffer[: len(chunk)] = chunk
    return len(chunk)


def _object_size(s3: Any, bucket: str, key: str) -> int:
  return int(s3.head_object(Bucket=bucket, Key=key)["ContentLength"])


def _read_range(s3: Any, bucket: str, key: str, start: int, end: int) -> bytes:
  response = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes={start}-{end}")
  return response["Body"].read()


def _parquet_row_count(
  s3: Any, bucket: str, key: str, file_size: int, byte_limit: int
) -> int:
  """Row count from the parquet footer alone — no row group is ever decoded.

  Reading the whole object and decoding it (``pq.read_table``) is what made a
  100 MB snappy file a multi-GB allocation; the footer carries ``num_rows``
  and is all a count needs. pyarrow parses metadata relative to the end of the
  buffer, so a buffer holding only footer + trailer reads like the full file.
  """
  import pyarrow.parquet as pq
  from botocore.exceptions import FlexibleChecksumError

  try:
    tail_len = min(file_size, _PARQUET_TAIL_READ_BYTES)
    tail = _read_range(s3, bucket, key, file_size - tail_len, file_size - 1)
  except FlexibleChecksumError:
    # LocalStack (and some S3-compatible stores) answer a ranged GET with the
    # whole object's checksum, which botocore then fails to verify — same
    # quirk `dagster/resources/storage.py` sidesteps. Real S3 omits the header
    # for a range. Fall back to one bounded full read; the parse below is
    # still footer-only, so this costs transfer, not memory.
    tail = io.BufferedReader(
      _BoundedS3Body(s3.get_object(Bucket=bucket, Key=key)["Body"], byte_limit),
      _STREAM_BUFFER_BYTES,
    ).read()
    file_size = len(tail)
  if len(tail) < _PARQUET_TRAILER_BYTES or tail[-4:] != b"PAR1":
    raise ValueError("missing parquet trailer")
  footer_len = int.from_bytes(tail[-8:-4], "little")
  footer_span = footer_len + _PARQUET_TRAILER_BYTES
  if footer_span > file_size:
    raise ValueError("parquet footer length exceeds the object")
  if footer_len > _PARQUET_MAX_FOOTER_BYTES:
    raise ValueError(f"parquet footer of {footer_len:,} bytes refused")
  if footer_span > len(tail):
    tail = _read_range(s3, bucket, key, file_size - footer_span, file_size - 1)
  return pq.read_metadata(io.BytesIO(tail[-footer_span:])).num_rows


def _csv_row_count(body: Any, byte_limit: int, row_cap: int) -> int:
  """Data rows in a CSV, streamed row by row; stops once past ``row_cap``.

  ``csv.reader`` over a text stream handles quoted newlines without holding
  more than one record, so memory is bounded by the longest record rather than
  the file. Counting stops one past the cap because the exact figure of a file
  that is being refused anyway is not worth the rest of the scan.
  """
  import csv

  text = io.TextIOWrapper(
    io.BufferedReader(_BoundedS3Body(body, byte_limit), _STREAM_BUFFER_BYTES),
    encoding="utf-8",
    newline="",
  )
  records = 0
  for _ in csv.reader(text):
    records += 1
    if records - 1 > row_cap:
      break
  return max(records - 1, 0)


def _json_row_count(body: Any, byte_limit: int, row_cap: int) -> int:
  """Elements of a top-level JSON array; anything else stages as one row.

  Walks the array with ``raw_decode`` one element at a time instead of
  ``json.loads`` on the whole payload: the text is held once (bounded by the
  size gate) and elements are decoded and dropped in turn, so a 100 MB array
  no longer expands into gigabytes of Python objects just to be ``len()``'d.
  """
  import json

  text = (
    io.BufferedReader(_BoundedS3Body(body, byte_limit), _STREAM_BUFFER_BYTES)
    .read()
    .decode("utf-8")
  )
  index = _skip_json_whitespace(text, 0)
  if index >= len(text) or text[index] != "[":
    return 1

  decoder = json.JSONDecoder()
  count = 0
  index += 1
  while True:
    index = _skip_json_whitespace(text, index)
    if index >= len(text):
      raise ValueError("unterminated JSON array")
    if text[index] == "]":
      return count
    _, index = decoder.raw_decode(text, index)
    count += 1
    if count > row_cap:
      return count
    index = _skip_json_whitespace(text, index)
    if index < len(text) and text[index] == ",":
      index += 1


def _measure_row_count(
  s3: Any,
  bucket: str,
  key: str,
  file_format: str,
  file_size: int,
  row_cap: int,
) -> tuple[int | None, bool]:
  """``(row_count, exact)`` for the object; sync, meant for a worker thread.

  A format that cannot be counted (malformed, unreadable) falls back to the
  bytes-per-row estimate — same degradation as before — and reports
  ``exact=False``. Unknown formats yield ``None``. A body that outgrows the
  size gate mid-read escapes as ``_PayloadTooLarge`` for the caller.
  """
  if file_format not in _FALLBACK_BYTES_PER_ROW:
    return None, False
  byte_limit = MAX_FILE_SIZE_MB * 1024 * 1024
  try:
    if file_format == "parquet":
      return _parquet_row_count(s3, bucket, key, file_size, byte_limit), True
    # Closed on every exit: an early stop at the row cap would otherwise
    # leave the HTTP connection to S3 open until garbage collection, and a
    # burst of cap-rejected uploads would drain the client's connection pool.
    body = s3.get_object(Bucket=bucket, Key=key)["Body"]
    try:
      if file_format == "csv":
        return _csv_row_count(body, byte_limit, row_cap), True
      return _json_row_count(body, byte_limit, row_cap), True
    finally:
      body.close()
  except _PayloadTooLarge:
    raise
  except Exception as e:
    logger.warning(
      f"Could not calculate row count for {key} ({file_format}): {e}. "
      f"Row count will be estimated."
    )
    return file_size // _FALLBACK_BYTES_PER_ROW[file_format], False


async def ingest_file_cmd(
  graph_id: str,
  file_id: str,
  ingest_to_graph: bool,
  current_user: User,
  db: Session,
  background_tasks: BackgroundTasks,
) -> dict:
  """Mark the file uploaded and trigger DuckDB staging (direct or Dagster)."""
  graph_file = GraphFile.get_by_id(file_id, db)
  if not graph_file or graph_file.graph_id != graph_id:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND, detail=f"File {file_id} not found"
    )

  # boto3 clients are safe to share across threads; the S3 calls below run in
  # a worker thread via ``run_off_loop`` so a 100 MB read never stalls the
  # single-worker API loop for every other tenant.
  s3_client = S3Client()
  bucket = env.USER_DATA_BUCKET

  try:
    actual_file_size = await run_off_loop(
      _object_size, s3_client.s3_client, bucket, graph_file.s3_key
    )
  except Exception as e:
    logger.error(f"Failed to get file size from S3: {e}")
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"File not found in S3: {graph_file.s3_key}",
    )

  if actual_file_size <= 0:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="File is empty",
    )

  max_file_size_bytes = MAX_FILE_SIZE_MB * 1024 * 1024
  if actual_file_size > max_file_size_bytes:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"File size {actual_file_size / (1024 * 1024):.2f} MB exceeds maximum of {MAX_FILE_SIZE_MB} MB",
    )

  graph = Graph.get_by_id(graph_id, db)
  if graph is None:
    # A graph the registry cannot resolve is refused, not exempted — skipping
    # the cap on a lookup miss is the fail-open shape this subsystem
    # eliminated everywhere else.
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"Graph {graph_id} not found",
    )

  # Gate on the measured instance footprint, not the logical staging sum:
  # `GraphTable.total_size_bytes` counts staging rows only and runs far under
  # the bytes on disk, so it would admit an upload to an instance already at
  # its cap. Instance scope for the same reason as materialization — a subgraph
  # shares its parent's box.
  from robosystems.middleware.graph.ingestion_limits import IngestionLimitChecker

  scope_graph_id = str(graph.parent_graph_id) if graph.parent_graph_id else graph_id
  graph_tier = str(graph.graph_tier) or "ladybug-standard"
  storage_check = await IngestionLimitChecker.check_instance_storage(
    db, scope_graph_id, graph_tier
  )

  if storage_check.get("retryable"):
    raise HTTPException(
      status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
      detail="Storage usage could not be verified; retry shortly",
      headers={"Retry-After": "30"},
    )

  storage_limit_bytes = storage_check["limit_gb"] * 1024**3
  # Headroom is judged on the enforced figure, which excludes blue-green
  # `-wip`/`-prev` build artifacts — same basis as the cap check itself, so an
  # in-flight rebuild doesn't reject uploads the durable footprint can absorb.
  current_storage_bytes = (storage_check["enforced_storage_gb"] or 0) * 1024**3
  if (
    not storage_check["allowed"]
    or current_storage_bytes + actual_file_size > storage_limit_bytes
  ):
    raise HTTPException(
      status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
      detail=f"Storage limit exceeded. Current: {current_storage_bytes / (1024**3):.2f} GB, "
      f"Limit: {storage_check['limit_gb']} GB, "
      f"Attempted upload: {actual_file_size / (1024**3):.2f} GB",
    )

  # The per-file row cap is the tier's per-table materialization cap, bounded
  # by the platform ceiling: a single file above `max_single_table_rows` can
  # never materialize on this tier, so it is refused here instead of after it
  # has been stored and staged. Fallback tracks ladybug-standard, as in
  # IngestionLimitChecker — a fallback wider than the box defeats the guard.
  tier_row_cap = GraphTierConfig.get_graph_limits(graph_tier).get(
    "max_single_table_rows", 2_500_000
  )
  row_cap = min(MAX_ROWS_PER_FILE, int(tier_row_cap))
  file_format = str(graph_file.file_format)

  try:
    actual_row_count, exact_count = await run_off_loop(
      _measure_row_count,
      s3_client.s3_client,
      bucket,
      graph_file.s3_key,
      file_format,
      actual_file_size,
      row_cap,
    )
  except _PayloadTooLarge:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"File exceeds maximum of {MAX_FILE_SIZE_MB} MB",
    )

  if actual_row_count is not None and actual_row_count > row_cap:
    raise HTTPException(
      status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
      detail=(
        f"File has {'at least ' if exact_count else 'an estimated '}"
        f"{actual_row_count:,} rows, exceeding the per-file limit of "
        f"{row_cap:,} rows for tier {graph_tier}"
      ),
    )
  logger.info(
    f"{'Calculated' if exact_count else 'Estimated'} row count for "
    f"{graph_file.file_name} ({file_format}): {actual_row_count}"
  )

  graph_file.file_size_bytes = actual_file_size
  graph_file.row_count = actual_row_count
  graph_file.upload_status = FileUploadStatus.UPLOADED.value
  db.commit()
  db.refresh(graph_file)

  table = (
    db.query(GraphTable)
    .filter(GraphTable.id == graph_file.table_id)
    .with_for_update()
    .first()
  )
  if table:
    all_files = GraphFile.get_all_for_table(table.id, db)
    uploaded_files = [
      f for f in all_files if f.upload_status == FileUploadStatus.UPLOADED.value
    ]

    new_file_count = len(uploaded_files)

    table.update_stats(
      session=db,
      file_count=new_file_count,
      total_size_bytes=sum(f.file_size_bytes for f in uploaded_files),
      row_count=sum(f.row_count for f in uploaded_files if f.row_count is not None),
    )

    if new_file_count > 0:
      # Size-based routing: small files use direct staging, large files use Dagster
      small_file_threshold_bytes = SMALL_FILE_STAGING_THRESHOLD_MB * 1024 * 1024

      if actual_file_size < small_file_threshold_bytes:
        # Fast path: Direct staging for small files
        from robosystems.operations.graph.engine.direct_staging import (
          stage_file_directly,
        )

        logger.info(
          f"Small file detected ({actual_file_size / (1024 * 1024):.2f} MB < {SMALL_FILE_STAGING_THRESHOLD_MB} MB). "
          f"Using direct staging for file {file_id}"
        )

        try:
          staging_result = await stage_file_directly(
            db=db,
            file_id=file_id,
            graph_id=graph_id,
            table_id=str(table.id),
            s3_key=graph_file.s3_key,
            file_size_bytes=actual_file_size,
            row_count=actual_row_count,
          )

          if staging_result.get("status") == "success":
            graph_file.duckdb_status = "staged"
            db.commit()
            db.refresh(graph_file)

            logger.info(
              f"Direct staging completed for file {file_id} in {staging_result.get('duration_ms', 0):.2f}ms"
            )

            # If ingest_to_graph requested, trigger Dagster job for that (still async)
            if ingest_to_graph:
              from robosystems.middleware.sse import (
                build_graph_job_config,
                run_and_monitor_dagster_job,
              )
              from robosystems.middleware.sse.event_storage import get_event_storage

              operation_id = str(uuid.uuid4())
              event_storage = get_event_storage()
              await event_storage.create_operation(
                operation_type="graph_ingestion",
                user_id=str(current_user.id),
                graph_id=graph_id,
                operation_id=operation_id,
              )

              run_config = build_graph_job_config(
                "materialize_file_job",
                file_id=file_id,
                graph_id=graph_id,
                table_name=table.table_name,
              )

              background_tasks.add_task(
                run_and_monitor_dagster_job,
                job_name="materialize_file_job",
                operation_id=operation_id,
                run_config=run_config,
              )

              graph_file.operation_id = operation_id
              db.commit()
              db.refresh(graph_file)

              logger.info(
                f"Direct staging done, graph ingestion job started for file {file_id}. "
                f"Monitor at /v1/operations/{operation_id}/stream"
              )
          else:
            logger.warning(
              f"Direct staging failed for file {file_id}: {staging_result.get('message')}. "
              f"File will be staged on next upload or query attempt."
            )

        except Exception as e:
          logger.warning(
            f"Direct staging error for file {file_id}: {e}. "
            f"File will be staged on next upload or query attempt."
          )

      else:
        # Standard path: Dagster job for large files
        from robosystems.middleware.sse import (
          build_graph_job_config,
          run_and_monitor_dagster_job,
        )
        from robosystems.middleware.sse.event_storage import get_event_storage

        operation_id = str(uuid.uuid4())

        logger.info(
          f"Large file detected ({actual_file_size / (1024 * 1024):.2f} MB >= {SMALL_FILE_STAGING_THRESHOLD_MB} MB). "
          f"Using Dagster job for file {file_id}"
        )

        try:
          # Register operation with SSE
          event_storage = get_event_storage()
          await event_storage.create_operation(
            operation_type="duckdb_staging",
            user_id=str(current_user.id),
            graph_id=graph_id,
            operation_id=operation_id,
          )

          # Build Dagster job config
          run_config = build_graph_job_config(
            "stage_file_job",
            file_id=file_id,
            graph_id=graph_id,
            table_id=str(table.id),
            ingest_to_graph=ingest_to_graph,
          )

          # Run Dagster job with SSE monitoring in background
          background_tasks.add_task(
            run_and_monitor_dagster_job,
            job_name="stage_file_job",
            operation_id=operation_id,
            run_config=run_config,
          )

          graph_file.operation_id = operation_id

          db.commit()
          db.refresh(graph_file)

          if ingest_to_graph:
            logger.info(
              f"v2 Incremental Ingestion: Dagster staging job started for file {file_id} "
              f"with auto-ingest to graph enabled. Monitor at /v1/operations/{operation_id}/stream"
            )
          else:
            logger.info(
              f"v2 Incremental Ingestion: Dagster staging job started for file {file_id}. "
              f"Monitor at /v1/operations/{operation_id}/stream"
            )

        except Exception as e:
          logger.warning(
            f"Failed to start Dagster staging job for file {file_id}: {e}. "
            f"File will be staged on next upload or query attempt."
          )

  logger.info(
    f"File {file_id} marked as uploaded: {graph_file.file_size_bytes or 0:,} bytes, {graph_file.row_count or 0:,} rows"
  )

  response = {
    "status": "success",
    "file_id": file_id,
    "upload_status": "uploaded",
    "file_size_bytes": graph_file.file_size_bytes,
    "row_count": graph_file.row_count,
    "message": "File validated and ready for ingestion",
  }

  # Check if file was staged directly (small file fast path)
  if graph_file.duckdb_status == "staged":
    response["duckdb_status"] = "staged"
    response["staged"] = True

    if graph_file.operation_id:
      # Operation_id means graph ingestion is in progress
      response["operation_id"] = graph_file.operation_id
      response["monitor_url"] = f"/v1/operations/{graph_file.operation_id}/stream"
      response["message"] = (
        f"File staged to DuckDB. Graph ingestion in progress. "
        f"Monitor at {response['monitor_url']}"
      )
      response["ingest_to_graph"] = True
    else:
      response["message"] = "File validated and staged to DuckDB (fast path)"
      response["ingest_to_graph"] = False
  elif graph_file.operation_id:
    # Large file: Dagster job handling staging (and possibly ingestion)
    response["operation_id"] = graph_file.operation_id
    response["monitor_url"] = f"/v1/operations/{graph_file.operation_id}/stream"
    response["staged"] = False

    if ingest_to_graph:
      response["message"] = (
        f"File validated. DuckDB staging in progress, then auto-ingesting to graph. "
        f"Monitor at {response['monitor_url']}"
      )
      response["ingest_to_graph"] = True
    else:
      response["message"] = (
        f"File validated. DuckDB staging in progress. Monitor at {response['monitor_url']}"
      )
      response["ingest_to_graph"] = False

  return response
