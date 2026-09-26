"""Per-instance busy counter for destructive operations.

An atomic ADD on the instance-registry row that GHA refresh workflows poll
before cycling a container or instance. Every write stamps a timestamp so the
reader can detect a counter left incremented by a crash. It is a soft signal:
write failures are logged, never raised.
"""

import asyncio
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime

from botocore.exceptions import ClientError

from robosystems.config import env
from robosystems.logger import logger

from .allocation_manager import get_dynamodb_resource
from .write_pause import assert_graph_writes_allowed

# Conventional op_kind labels; GHA reads the kind only for logging.
OP_KIND_MATERIALIZATION = "materialization"
OP_KIND_DAGSTER_MATERIALIZATION = "dagster_materialization"
OP_KIND_SEC_STAGING = "sec_staging"
OP_KIND_EXTENSIONS_MATERIALIZE = "extensions_materialize"
OP_KIND_BULK_TABLE_CREATE = "bulk_table_create"
OP_KIND_BULK_TABLE_INSERT = "bulk_table_insert"


def _iso_now() -> str:
  return datetime.now(UTC).isoformat()


def _update_counter(instance_id: str, delta: int, op_kind: str) -> None:
  """Never raises. ADD treats a missing attribute as 0."""
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
    # Only thread-pool failures reach here; _update_counter swallows its own.
    logger.warning(
      f"instance_busy: async counter dispatch failed for {instance_id} "
      f"({op_kind}, delta={delta}): {e}"
    )


async def resolve_instance_id_for_graph(graph_id: str) -> str:
  """The EC2 instance hosting a graph, or "" on any failure.

  "" makes the counter a no-op; a lookup failure must never block the work.
  """
  try:
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
  """+1 to the busy counter. Prefer :func:`instance_busy`; otherwise callers
  must call :func:`end_destructive_op` in a ``finally``.

  This is where a write is admitted, so it is where a maintenance pause
  refuses one (GraphWritesPausedError, before counting). The per-call
  :func:`instance_busy` in the Graph API does not check: work admitted before
  a pause must finish, so the drain can wait for it.
  """
  await asyncio.to_thread(assert_graph_writes_allowed)
  await _update_counter_async(instance_id, delta=1, op_kind=op_kind)


async def end_destructive_op(instance_id: str, op_kind: str) -> None:
  """Imperative -1 to the busy counter. Pair with :func:`begin_destructive_op`."""
  await _update_counter_async(instance_id, delta=-1, op_kind=op_kind)


@asynccontextmanager
async def instance_busy(
  instance_id: str,
  op_kind: str,
) -> AsyncIterator[None]:
  """Mark an instance busy for the duration of the block, exceptions included."""
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
  """Synchronous :func:`instance_busy`."""
  _update_counter(instance_id, delta=1, op_kind=op_kind)
  try:
    yield
  finally:
    _update_counter(instance_id, delta=-1, op_kind=op_kind)
