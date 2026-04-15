"""
Instance Busy Counter

Atomic DynamoDB counter on the instance-registry table to report when an
EC2 graph instance is running a destructive operation (materialization,
bulk staging, etc.). GHA pre-refresh workflows poll this counter before
cycling the container or replacing the instance, so long-running work is
not interrupted mid-flight.

The counter is per-instance and tracks concurrent destructive operations
via atomic DynamoDB ADD, which handles overlapping operations correctly.
A timestamp on every write drives stale detection on the reader side
(GHA) in case a crash leaves the counter incremented without a matching
decrement.

DynamoDB write failures are logged but never raised — a broken counter
must not block the actual work.
"""

import asyncio
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime

from botocore.exceptions import ClientError

from robosystems.config import env
from robosystems.logger import logger

from .allocation_manager import get_dynamodb_resource

# Known op_kind labels for observability. Not enforced at runtime — this
# is a convention for the writer side. GHA reads the kind only for logging.
OP_KIND_MATERIALIZATION = "materialization"
OP_KIND_DAGSTER_MATERIALIZATION = "dagster_materialization"
OP_KIND_SEC_STAGING = "sec_staging"
OP_KIND_EXTENSIONS_MATERIALIZE = "extensions_materialize"
OP_KIND_BULK_TABLE_CREATE = "bulk_table_create"


def _iso_now() -> str:
  return datetime.now(UTC).isoformat()


def _update_counter(instance_id: str, delta: int, op_kind: str) -> None:
  """Apply an atomic counter update to the instance-registry entry.

  Always safe — log-and-continue on any DynamoDB failure, never raises.
  Missing `active_destructive_ops` attribute is treated as 0 by DynamoDB
  ADD, so no schema migration or initialization is required.
  """
  if not instance_id:
    logger.warning("instance_busy: skipping counter update — instance_id is empty")
    return

  try:
    resource = get_dynamodb_resource()
    table = resource.Table(env.INSTANCE_REGISTRY_TABLE)
    table.update_item(
      Key={"instance_id": instance_id},
      UpdateExpression=(
        "ADD active_destructive_ops :delta "
        "SET last_destructive_op_at = :ts, "
        "    last_destructive_op_kind = :kind"
      ),
      ExpressionAttributeValues={
        ":delta": delta,
        ":ts": _iso_now(),
        ":kind": op_kind,
      },
    )
    logger.info(
      f"instance_busy: {op_kind} counter {'+' if delta > 0 else ''}{delta} "
      f"on instance {instance_id}"
    )
  except ClientError as e:
    logger.warning(
      f"instance_busy: DynamoDB update failed for instance {instance_id} "
      f"({op_kind}, delta={delta}): {e}"
    )
  except Exception as e:
    logger.warning(
      f"instance_busy: unexpected error updating counter for instance "
      f"{instance_id} ({op_kind}, delta={delta}): {e}"
    )


async def _update_counter_async(instance_id: str, delta: int, op_kind: str) -> None:
  """Run the blocking DynamoDB update in a worker thread."""
  try:
    await asyncio.to_thread(_update_counter, instance_id, delta, op_kind)
  except Exception as e:
    # Defense in depth — _update_counter swallows its own errors, so this
    # should only fire on event-loop / thread-pool failure.
    logger.warning(
      f"instance_busy: async counter dispatch failed for {instance_id} "
      f"({op_kind}, delta={delta}): {e}"
    )


async def resolve_instance_id_for_graph(graph_id: str) -> str:
  """Look up the EC2 instance ID hosting the given graph.

  Thin convenience helper for callers that have a ``graph_id`` but no
  ``GraphClient`` handy (e.g., Dagster assets that run on a worker and
  would like to publish a busy counter against the target instance).
  Creates a short-lived client solely to read its ``_instance_id``
  routing metadata, then closes it.

  Returns an empty string on any failure (graph not found, routing
  error, etc.) — callers pass the result to :func:`begin_destructive_op`
  which already treats empty strings as a no-op.

  This helper is intentionally tolerant: publishing the busy counter is
  a soft coordination signal, and a lookup failure must never block the
  destructive operation itself.
  """
  try:
    # Lazy import to avoid circular dependency with the graph_api client
    # factory at module load time.
    from robosystems.graph_api.client.factory import GraphClientFactory

    client = await GraphClientFactory.create_client(
      graph_id=graph_id, operation_type="write"
    )
    try:
      return client._instance_id or ""
    finally:
      await client.close()
  except Exception as e:
    logger.warning(
      f"instance_busy: could not resolve instance_id for graph {graph_id}: {e}"
    )
    return ""


async def begin_destructive_op(instance_id: str, op_kind: str) -> None:
  """Imperative +1 to the busy counter. Pair with :func:`end_destructive_op`.

  Prefer the :func:`instance_busy` context manager when possible — this
  imperative variant exists for call sites where inserting ``async with``
  would force a large re-indentation (e.g., deeply nested try/finally
  blocks). Callers MUST guarantee a matching ``end_destructive_op`` in a
  ``finally`` that runs regardless of success or failure.

  Log-and-continue on DynamoDB failure.
  """
  await _update_counter_async(instance_id, delta=1, op_kind=op_kind)


async def end_destructive_op(instance_id: str, op_kind: str) -> None:
  """Imperative -1 to the busy counter. Pair with :func:`begin_destructive_op`."""
  await _update_counter_async(instance_id, delta=-1, op_kind=op_kind)


@asynccontextmanager
async def instance_busy(
  instance_id: str,
  op_kind: str,
) -> AsyncIterator[None]:
  """Mark an EC2 graph instance as busy for the duration of the block.

  Atomically increments `active_destructive_ops` on the instance's
  instance-registry DynamoDB entry on entry and decrements on exit
  (including on exception). GHA refresh workflows read this attribute
  before cycling the container or replacing the instance.

  The counter is a soft coordination signal, not a correctness boundary:
    - DynamoDB write failures are logged but do not fail the caller.
    - GHA readers apply stale-detection via `last_destructive_op_at`.
    - Concurrent operations on the same instance are handled by atomic
      ADD on the DynamoDB side.

  Args:
    instance_id: EC2 instance ID of the graph host (e.g. `i-0abc...`).
      In orchestration-layer callers (API worker, Dagster), read this
      from `GraphClient._instance_id` after
      `GraphClientFactory.create_client(...)`.
      In graph_api handlers running on the instance itself, use
      `env.INSTANCE_ID`.
    op_kind: Short label for observability (see OP_KIND_* constants).

  Example:
    client = await GraphClientFactory.create_client(graph_id=graph_id, ...)
    async with instance_busy(client._instance_id, OP_KIND_MATERIALIZATION):
      await do_long_destructive_work(client)
  """
  await _update_counter_async(instance_id, delta=1, op_kind=op_kind)
  try:
    yield
  finally:
    await _update_counter_async(instance_id, delta=-1, op_kind=op_kind)


@contextmanager
def instance_busy_sync(
  instance_id: str,
  op_kind: str,
) -> Iterator[None]:
  """Synchronous variant of :func:`instance_busy` for sync call sites.

  Use this from Dagster ops or any other sync context where awaiting the
  async variant is not convenient. Same semantics: increment on entry,
  decrement on exit (including exceptions), log-and-continue on DDB
  failure.
  """
  _update_counter(instance_id, delta=1, op_kind=op_kind)
  try:
    yield
  finally:
    _update_counter(instance_id, delta=-1, op_kind=op_kind)
