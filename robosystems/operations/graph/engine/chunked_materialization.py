"""Chunked materialization for user-graph staging tables.

Splits large DuckDB-to-LadybugDB COPY operations into hash-based batches
to avoid OOM on large tables. Uses the same batch_num/num_batches pattern
as the SEC adapter (see adapters/sec/processors/ingestion/materialization.py).
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

from robosystems.config.graph_tier import GraphTierConfig
from robosystems.logger import logger

if TYPE_CHECKING:
  from robosystems.graph_api.client.client import GraphClient

# Per-chunk timeout (seconds) — matches materialize_table default
CHUNK_TIMEOUT = 600.0

# Fallback chunk size when tier config is unavailable. Must never exceed the
# smallest tier's chunk_size_rows (ladybug-standard: 250k on m7g.medium) — a
# larger fallback applies a bigger tier's chunk to a smaller box, the OOM the
# guardrail exists to prevent. Pinned by test_graph_tier_config.
DEFAULT_CHUNK_SIZE_ROWS = 250_000


async def materialize_table_chunked(
  client: GraphClient,
  graph_id: str,
  table_name: str,
  tier: str,
  materialize_embeddings: bool = False,
  file_ids: list[str] | None = None,
) -> dict[str, Any]:
  """Materialize a staging table, chunking large tables into hash-based batches.

  For tables smaller than the tier's chunk_size_rows, delegates directly to
  client.materialize_table (single pass). For larger tables, iterates through
  hash-based batches using batch_num/num_batches parameters.

  Args:
      client: Graph API client instance.
      graph_id: Graph database identifier.
      table_name: DuckDB staging table to materialize.
      tier: Graph tier name (e.g. "ladybug-standard").
      materialize_embeddings: Include embedding columns and build HNSW vector indexes.
      file_ids: Optional file ID filter (passed through; chunking still applies).

  Returns:
      Dict with rows_ingested, chunked (bool), and batches (int) keys.
  """
  chunk_size = _get_chunk_size(tier)

  # Query row count to decide whether to chunk
  row_count = await _get_row_count(client, graph_id, table_name)

  if row_count is not None and row_count > chunk_size:
    return await _materialize_batched(
      client=client,
      graph_id=graph_id,
      table_name=table_name,
      row_count=row_count,
      chunk_size=chunk_size,
      materialize_embeddings=materialize_embeddings,
      file_ids=file_ids,
    )

  # Single-pass materialization
  result = await client.materialize_table(
    graph_id=graph_id,
    table_name=table_name,
    materialize_embeddings=materialize_embeddings,
    file_ids=file_ids,
    timeout=CHUNK_TIMEOUT,
  )
  return {
    "rows_ingested": result.get("rows_ingested", 0),
    "chunked": False,
    "batches": 1,
  }


def _get_chunk_size(tier: str) -> int:
  """Resolve chunk_size_rows from tier config, with fallback."""
  try:
    limits = GraphTierConfig.get_graph_limits(tier)
    return int(limits.get("chunk_size_rows", DEFAULT_CHUNK_SIZE_ROWS))
  except Exception:
    logger.warning(
      f"Could not load chunk_size_rows for tier {tier}, "
      f"using default {DEFAULT_CHUNK_SIZE_ROWS:,}"
    )
    return DEFAULT_CHUNK_SIZE_ROWS


async def _get_row_count(
  client: GraphClient,
  graph_id: str,
  table_name: str,
) -> int | None:
  """Query DuckDB for the row count of a staging table.

  Returns None on failure so callers can fall back to single-pass.
  """
  try:
    response = await client.query_table(
      graph_id=graph_id,
      sql=f"SELECT COUNT(*) FROM {table_name}",
    )
    if response.get("rows") and response["rows"][0]:
      return int(response["rows"][0][0])
  except Exception as exc:
    logger.warning(f"Could not get row count for {table_name}: {exc}")
  return None


async def _materialize_batched(
  client: GraphClient,
  graph_id: str,
  table_name: str,
  row_count: int,
  chunk_size: int,
  materialize_embeddings: bool,
  file_ids: list[str] | None,
) -> dict[str, Any]:
  """Run hash-based batched materialization for a large table."""
  num_batches = math.ceil(row_count / chunk_size)

  logger.info(
    f"Chunked materialization for {table_name}: "
    f"{row_count:,} rows in {num_batches} batches (chunk_size={chunk_size:,})"
  )

  total_rows = 0

  for batch_num in range(num_batches):
    try:
      response = await client.materialize_table(
        graph_id=graph_id,
        table_name=table_name,
        materialize_embeddings=materialize_embeddings,
        file_ids=file_ids,
        batch_num=batch_num,
        num_batches=num_batches,
        timeout=CHUNK_TIMEOUT,
      )
      batch_rows = response.get("rows_ingested", 0)
      total_rows += batch_rows

      logger.info(
        f"  [{table_name}] Batch {batch_num + 1}/{num_batches}: {batch_rows:,} rows"
      )

    except Exception as exc:
      # Fail fast: a failed batch means missing data, not something to skip.
      logger.error(
        f"  [{table_name}] Batch {batch_num + 1}/{num_batches} failed: {exc}"
      )
      raise

  logger.info(
    f"Chunked materialization complete for {table_name}: "
    f"{total_rows:,} rows across {num_batches} batches"
  )

  # Rebuild HNSW vector index after all batches complete.
  # Per-batch index creation is skipped (would only cover partial data),
  # so we rebuild once over the full table.
  if materialize_embeddings and total_rows > 0:
    try:
      logger.info(
        f"Rebuilding HNSW vector index for {table_name} after batched materialization"
      )
      await client.build_vector_index(
        graph_id=graph_id,
        table_name=table_name,
        backend="hnsw",
      )
      logger.info(f"HNSW vector index rebuilt for {table_name}")
    except Exception as exc:
      logger.warning(
        f"HNSW vector index rebuild failed for {table_name} (non-fatal): {exc}"
      )

  return {
    "rows_ingested": total_rows,
    "chunked": True,
    "batches": num_batches,
  }
