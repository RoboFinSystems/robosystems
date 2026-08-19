"""Tests for the ingest-file row count and row cap.

Counting must never decode the whole object: parquet reads its footer only,
CSV streams, JSON walks the array element by element. The count runs in a
worker thread (``run_off_loop``) so a 100 MB read cannot stall the API loop,
and a file over the per-file row cap is refused before it is stored.
"""

import io
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi import HTTPException

from robosystems.config.constants import MAX_FILE_SIZE_MB, MAX_ROWS_PER_FILE
from robosystems.middleware.graph.ingestion_limits import IngestionLimitChecker
from robosystems.operations.graph.commands import ingest_file as module
from robosystems.operations.graph.commands.ingest_file import (
  _csv_row_count,
  _json_row_count,
  _measure_row_count,
  _parquet_row_count,
  _PayloadTooLarge,
  ingest_file_cmd,
)

MB = 1024**2
BYTE_LIMIT = MAX_FILE_SIZE_MB * MB


def _parquet_bytes(rows: int, columns: int = 2, row_group_size: int = 10_000) -> bytes:
  table = pa.table({f"c{i}": list(range(rows)) for i in range(columns)})
  buf = io.BytesIO()
  pq.write_table(table, buf, row_group_size=row_group_size)
  return buf.getvalue()


class _RangeS3:
  """Just enough of a boto3 S3 client to serve GetObject with a Range header."""

  def __init__(self, data: bytes) -> None:
    self.data = data
    self.ranges: list[tuple[int, int]] = []
    self.full_reads = 0

  def get_object(self, Bucket: str, Key: str, Range: str | None = None) -> dict:
    if Range is None:
      self.full_reads += 1
      return {"Body": io.BytesIO(self.data)}
    start, end = (int(part) for part in Range.removeprefix("bytes=").split("-"))
    self.ranges.append((start, end))
    return {"Body": io.BytesIO(self.data[start : end + 1])}


# ── Parquet: footer only ────────────────────────────────────────────────


def test_parquet_count_comes_from_footer_not_read_table():
  data = _parquet_bytes(rows=25_000)
  s3 = _RangeS3(data)

  with patch.object(pq, "read_table") as read_table:
    count = _parquet_row_count(s3, "bucket", "k.parquet", len(data), BYTE_LIMIT)

  assert count == 25_000
  read_table.assert_not_called()
  # One tail read covering the footer; never the whole object.
  assert s3.full_reads == 0
  assert len(s3.ranges) == 1
  start, end = s3.ranges[0]
  assert end == len(data) - 1
  assert end - start + 1 <= 64 * 1024 < len(data)


def test_parquet_footer_larger_than_tail_read_fetches_exact_footer_range():
  # Many columns and tiny row groups inflate the footer past the 64 KiB tail.
  data = _parquet_bytes(rows=20_000, columns=60, row_group_size=200)
  footer_len = int.from_bytes(data[-8:-4], "little")
  assert footer_len > 64 * 1024
  s3 = _RangeS3(data)

  with patch.object(pq, "read_table") as read_table:
    count = _parquet_row_count(s3, "bucket", "k.parquet", len(data), BYTE_LIMIT)

  assert count == 20_000
  read_table.assert_not_called()
  assert s3.full_reads == 0
  assert s3.ranges[-1] == (len(data) - footer_len - 8, len(data) - 1)


def test_parquet_ranged_checksum_quirk_falls_back_to_bounded_full_read():
  """S3-compatible stores that fail botocore's range checksum still count exactly."""
  from botocore.exceptions import FlexibleChecksumError

  data = _parquet_bytes(rows=1_234)
  s3 = _RangeS3(data)

  def ranged_get(Bucket, Key, Range=None):
    if Range is not None:
      raise FlexibleChecksumError(error_msg="mismatch")
    return {"Body": io.BytesIO(data)}

  s3.get_object = ranged_get  # type: ignore[method-assign]
  with patch.object(pq, "read_table") as read_table:
    count = _parquet_row_count(s3, "bucket", "k.parquet", len(data), BYTE_LIMIT)

  assert count == 1_234
  read_table.assert_not_called()


def test_parquet_without_trailer_is_not_counted():
  s3 = _RangeS3(b"definitely not parquet" * 100)
  with pytest.raises(ValueError):
    _parquet_row_count(s3, "bucket", "k.parquet", len(s3.data), BYTE_LIMIT)


def test_parquet_hostile_footer_length_is_refused_before_decoding():
  # Trailer claims a footer larger than the object itself.
  data = b"x" * 100 + (10**6).to_bytes(4, "little") + b"PAR1"
  s3 = _RangeS3(data)
  with pytest.raises(ValueError):
    _parquet_row_count(s3, "bucket", "k.parquet", len(data), BYTE_LIMIT)


# ── CSV / JSON: streamed, bounded, capped ───────────────────────────────


def test_csv_count_handles_quoted_newlines_and_excludes_header():
  body = io.BytesIO(b'a,b\n1,"multi\nline"\n2,z\n')
  assert _csv_row_count(body, BYTE_LIMIT, MAX_ROWS_PER_FILE) == 2


def test_csv_count_stops_one_past_the_cap():
  body = io.BytesIO(b"h\n" + b"r\n" * 1_000)
  assert _csv_row_count(body, BYTE_LIMIT, row_cap=10) == 11


def test_json_count_walks_array_without_loading_it_whole():
  payload = json.dumps([{"a": i, "s": 'q"]x'} for i in range(500)]).encode()
  with patch("json.loads") as loads:
    count = _json_row_count(io.BytesIO(payload), BYTE_LIMIT, MAX_ROWS_PER_FILE)
  assert count == 500
  loads.assert_not_called()


def test_json_object_counts_as_one_row_and_cap_short_circuits():
  assert _json_row_count(io.BytesIO(b'{"a": 1}'), BYTE_LIMIT, 10) == 1
  payload = json.dumps(list(range(1_000))).encode()
  assert _json_row_count(io.BytesIO(payload), BYTE_LIMIT, row_cap=3) == 4


def test_body_that_outgrows_the_size_gate_is_cut_off():
  """The object was swapped for a bigger one between HEAD and GET."""
  body = io.BytesIO(b"h\n" + b"row\n" * 10_000)
  with pytest.raises(_PayloadTooLarge):
    _csv_row_count(body, byte_limit=1_000, row_cap=MAX_ROWS_PER_FILE)


def test_measure_falls_back_to_estimate_when_count_fails():
  s3 = MagicMock()
  s3.get_object.side_effect = RuntimeError("boom")
  count, exact = _measure_row_count(s3, "b", "k.csv", "csv", 2_000, MAX_ROWS_PER_FILE)
  assert exact is False
  assert count == 2_000 // module.FALLBACK_BYTES_PER_ROW_CSV


def test_measure_lets_payload_overflow_escape_the_estimate_fallback():
  s3 = MagicMock()
  s3.get_object.return_value = {"Body": io.BytesIO(b"h\n" + b"row\n" * 10_000)}
  with (
    patch.object(module, "MAX_FILE_SIZE_MB", 0),
    pytest.raises(_PayloadTooLarge),
  ):
    _measure_row_count(s3, "b", "k.csv", "csv", 40_000, MAX_ROWS_PER_FILE)


# ── The command: off-loop and capped ────────────────────────────────────


def _storage_ok() -> dict:
  return {
    "allowed": True,
    "retryable": False,
    "errors": [],
    "total_storage_gb": 1.0,
    "enforced_storage_gb": 1.0,
    "limit_gb": 20.0,
    "usage_percentage": None,
    "status": "healthy",
    "databases": [],
    "items": [],
  }


def _cmd_mocks(file_format: str, file_size: int):
  graph_file = MagicMock()
  graph_file.graph_id = "kg123"
  graph_file.s3_key = f"uploads/kg123/file.{file_format}"
  graph_file.file_format = file_format
  graph_file.file_name = f"file.{file_format}"

  graph = MagicMock()
  graph.parent_graph_id = None
  graph.graph_tier = "ladybug-standard"

  s3 = MagicMock()
  s3.s3_client.head_object.return_value = {"ContentLength": file_size}

  return (
    s3,
    patch("robosystems.models.core.GraphFile.get_by_id", return_value=graph_file),
    patch(
      "robosystems.operations.graph.commands.ingest_file.S3Client", return_value=s3
    ),
    patch("robosystems.models.core.Graph.get_by_id", return_value=graph),
    patch.object(
      IngestionLimitChecker,
      "check_instance_storage",
      new_callable=AsyncMock,
      return_value=_storage_ok(),
    ),
  )


async def _run() -> dict:
  return await ingest_file_cmd(
    graph_id="kg123",
    file_id="file_1",
    ingest_to_graph=False,
    current_user=MagicMock(),
    db=MagicMock(),
    background_tasks=MagicMock(),
  )


@pytest.mark.asyncio
async def test_blocking_s3_work_runs_through_run_off_loop():
  s3, files, s3_patch, graph, storage = _cmd_mocks("csv", 10 * MB)
  s3.s3_client.get_object.return_value = {"Body": io.BytesIO(b"h\n1\n2\n")}

  async def passthrough(func, *args):
    return func(*args)

  with (
    files,
    s3_patch,
    graph,
    storage,
    patch.object(
      module, "run_off_loop", new=AsyncMock(side_effect=passthrough)
    ) as off_loop,
  ):
    await _run()

  # HEAD and the read/count each go through the worker-thread seam; nothing
  # touches S3 directly on the loop.
  assert off_loop.await_count == 2
  assert off_loop.await_args_list[0].args[0] is module._object_size
  assert off_loop.await_args_list[1].args[0] is module._measure_row_count
  s3.s3_client.head_object.assert_called_once()
  s3.s3_client.get_object.assert_called_once()


@pytest.mark.asyncio
async def test_file_over_row_cap_is_refused():
  s3, files, s3_patch, graph, storage = _cmd_mocks("csv", 10 * MB)
  s3.s3_client.get_object.return_value = {"Body": io.BytesIO(b"h\n" + b"r\n" * 50)}

  with (
    files,
    s3_patch,
    graph,
    storage,
    patch.object(module, "MAX_ROWS_PER_FILE", 10),
  ):
    with pytest.raises(HTTPException) as exc:
      await _run()

  assert exc.value.status_code == 413
  assert "per-file limit of 10 rows" in str(exc.value.detail)


@pytest.mark.asyncio
async def test_row_cap_is_the_tier_table_cap_when_tighter():
  """A single file above the tier's `max_single_table_rows` can never materialize."""
  s3, files, s3_patch, graph, storage = _cmd_mocks("csv", 10 * MB)
  s3.s3_client.get_object.return_value = {"Body": io.BytesIO(b"h\n" + b"r\n" * 50)}

  with (
    files,
    s3_patch,
    graph,
    storage,
    patch.object(
      module.GraphTierConfig,
      "get_graph_limits",
      return_value={"max_single_table_rows": 20},
    ),
  ):
    with pytest.raises(HTTPException) as exc:
      await _run()

  assert exc.value.status_code == 413
  assert "per-file limit of 20 rows for tier ladybug-standard" in str(exc.value.detail)


@pytest.mark.asyncio
async def test_object_grown_past_size_gate_is_rejected_not_estimated():
  """HEAD passed the size gate, but the body read past it — refuse, don't guess."""
  s3, files, s3_patch, graph, storage = _cmd_mocks("csv", 100)

  with (
    files,
    s3_patch,
    graph,
    storage,
    patch.object(module, "_measure_row_count", side_effect=_PayloadTooLarge("grew")),
  ):
    with pytest.raises(HTTPException) as exc:
      await _run()

  assert exc.value.status_code == 400
  assert str(MAX_FILE_SIZE_MB) in str(exc.value.detail)


# ── Column names: identifiers in the staging layer ──────────────────────


def _parquet_with_columns(names: list[str]) -> bytes:
  table = pa.table({name: [1, 2, 3] for name in names})
  buf = io.BytesIO()
  pq.write_table(table, buf)
  return buf.getvalue()


def test_parquet_column_names_outside_the_identifier_class_are_refused():
  from robosystems.operations.graph.commands.ingest_file import _InvalidColumnName

  hostile = 'identifier") AS "benign", (SELECT 42) AS "injected" --'
  data = _parquet_with_columns(["identifier", hostile])
  with pytest.raises(_InvalidColumnName) as exc:
    _parquet_row_count(_RangeS3(data), "bucket", "k.parquet", len(data), BYTE_LIMIT)
  assert exc.value.name == hostile


def test_parquet_list_and_struct_columns_are_judged_by_their_top_level_name():
  """Leaf paths like ``embedding.list.element`` must not trip the check."""
  table = pa.table(
    {"identifier": ["a"], "embedding": [[1.0, 2.0]], "nested": [{"x": 1}]}
  )
  buf = io.BytesIO()
  pq.write_table(table, buf)
  data = buf.getvalue()
  assert (
    _parquet_row_count(_RangeS3(data), "bucket", "k.parquet", len(data), BYTE_LIMIT)
    == 1
  )


def test_invalid_column_name_is_not_swallowed_into_an_estimate():
  """Unlike a malformed file, a hostile column name must surface, not degrade
  to the bytes-per-row fallback and proceed to staging."""
  from robosystems.operations.graph.commands.ingest_file import _InvalidColumnName

  data = _parquet_with_columns(["identifier", "a b"])
  with pytest.raises(_InvalidColumnName):
    _measure_row_count(
      _RangeS3(data), "bucket", "k.parquet", "parquet", len(data), MAX_ROWS_PER_FILE
    )


@pytest.mark.asyncio
async def test_file_with_invalid_column_name_gets_a_400_naming_it():
  data = _parquet_with_columns(["identifier", "has-hyphen"])
  s3, files, s3_patch, graph, storage = _cmd_mocks("parquet", len(data))
  s3.s3_client.get_object.side_effect = _RangeS3(data).get_object

  with files, s3_patch, graph, storage:
    with pytest.raises(HTTPException) as exc:
      await _run()

  assert exc.value.status_code == 400
  assert "'has-hyphen'" in str(exc.value.detail)
  assert "file.parquet" in str(exc.value.detail)
