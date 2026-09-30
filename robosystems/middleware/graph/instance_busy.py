"""Per-instance busy leases for destructive operations.

Each operation adds a lease token (``<started_at>|<op_kind>|<id>``) to a string
set on the instance-registry row and deletes it when done; refresh workflows
treat an instance with a live lease as busy. A lease older than
``STALE_WINDOW_SECONDS`` is read as a crashed holder, per lease, so one leaked by
a killed process expires on its own clock however busy the instance stays. It is
a soft signal: write failures are logged, never raised.
"""

import asyncio
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime

from botocore.exceptions import ClientError

from robosystems.config import env
from robosystems.logger import logger

from .allocation_manager import get_dynamodb_resource
from .write_pause import assert_graph_writes_allowed

# Conventional op_kind labels; readers use the kind only for logging.
OP_KIND_MATERIALIZATION = "materialization"
OP_KIND_DAGSTER_MATERIALIZATION = "dagster_materialization"
OP_KIND_SEC_STAGING = "sec_staging"
OP_KIND_EXTENSIONS_MATERIALIZE = "extensions_materialize"
OP_KIND_BULK_TABLE_CREATE = "bulk_table_create"
OP_KIND_BULK_TABLE_INSERT = "bulk_table_insert"

LEASE_ATTRIBUTE = "active_leases"

# A lease older than this is a crashed holder (6h covers full SEC backfills).
# Mirrors STALE_WINDOW_SECONDS in the three readers: graph_container_refresh.py,
# refresh-graph-container.sh and wait-graph-writers-idle.sh.
STALE_WINDOW_SECONDS = 21600


def _new_lease(op_kind: str) -> str:
  started_at = datetime.now(UTC).isoformat(timespec="seconds")
  return f"{started_at}|{op_kind}|{uuid.uuid4().hex[:12]}"


def _is_stale(lease: str, now: float) -> bool:
  try:
    started_at = datetime.fromisoformat(lease.split("|", 1)[0]).timestamp()
  except ValueError:
    return True
  return now - started_at > STALE_WINDOW_SECONDS


def _acquire(instance_id: str, op_kind: str) -> str:
  """Add a lease and return it, or "" when none was written. Never raises.

  Stale leases seen in the updated set are pruned, so a leaked one does not
  outlive its expiry on the row either.
  """
  if not instance_id:
    logger.warning("instance_busy: skipping lease — instance_id is empty")
    return ""

  lease = _new_lease(op_kind)
  try:
    table = get_dynamodb_resource().Table(env.INSTANCE_REGISTRY_TABLE)
    response = table.update_item(
      Key={"instance_id": instance_id},
      UpdateExpression=f"ADD {LEASE_ATTRIBUTE} :lease",
      ExpressionAttributeValues={":lease": {lease}},
      ReturnValues="UPDATED_NEW",
    )
    logger.info(f"instance_busy: {op_kind} lease acquired on instance {instance_id}")
  except Exception as e:
    logger.warning(
      f"instance_busy: could not acquire {op_kind} lease on instance {instance_id}: {e}"
    )
    return ""

  held = response.get("Attributes", {}).get(LEASE_ATTRIBUTE) or set()
  now = datetime.now(UTC).timestamp()
  stale = {held_lease for held_lease in held if _is_stale(held_lease, now)}
  if stale:
    logger.warning(
      f"instance_busy: pruning {len(stale)} stale lease(s) on instance "
      f"{instance_id}: {sorted(stale)}"
    )
    _delete(instance_id, stale)
  return lease


def _release(instance_id: str, lease: str) -> None:
  """Delete a lease. Never raises; a lease that was never written is a no-op."""
  if not instance_id or not lease:
    return
  if _delete(instance_id, {lease}):
    logger.info(f"instance_busy: lease released on instance {instance_id}")


def _delete(instance_id: str, leases: set[str]) -> bool:
  try:
    get_dynamodb_resource().Table(env.INSTANCE_REGISTRY_TABLE).update_item(
      Key={"instance_id": instance_id},
      UpdateExpression=f"DELETE {LEASE_ATTRIBUTE} :leases",
      ExpressionAttributeValues={":leases": leases},
    )
    return True
  except ClientError as e:
    logger.warning(
      f"instance_busy: DynamoDB lease delete failed for instance {instance_id}: {e}"
    )
  except Exception as e:
    logger.warning(
      f"instance_busy: unexpected error deleting lease on instance {instance_id}: {e}"
    )
  return False


async def _acquire_async(instance_id: str, op_kind: str) -> str:
  """Run the blocking acquire in a worker thread."""
  try:
    return await asyncio.to_thread(_acquire, instance_id, op_kind)
  except Exception as e:
    # Only thread-pool failures reach here; _acquire swallows its own.
    logger.warning(f"instance_busy: async lease acquire failed for {instance_id}: {e}")
    return ""


async def _release_async(instance_id: str, lease: str) -> None:
  """Run the blocking release in a worker thread."""
  try:
    await asyncio.to_thread(_release, instance_id, lease)
  except Exception as e:
    logger.warning(f"instance_busy: async lease release failed for {instance_id}: {e}")


async def resolve_instance_id_for_graph(graph_id: str) -> str:
  """The EC2 instance hosting a graph, or "" on any failure.

  "" makes the lease a no-op; a lookup failure must never block the work.
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


async def begin_destructive_op(instance_id: str, op_kind: str) -> str:
  """Take a lease and return it for :func:`end_destructive_op`. Prefer
  :func:`instance_busy`; otherwise callers must end it in a ``finally``.

  This is where a write is admitted, so it is where a maintenance pause
  refuses one (GraphWritesPausedError, before any lease). The per-call
  :func:`instance_busy` in the Graph API does not check: work admitted before
  a pause must finish, so the drain can wait for it.
  """
  await asyncio.to_thread(assert_graph_writes_allowed)
  return await _acquire_async(instance_id, op_kind)


async def end_destructive_op(instance_id: str, lease: str) -> None:
  """Release the lease :func:`begin_destructive_op` returned ("" is a no-op)."""
  await _release_async(instance_id, lease)


@asynccontextmanager
async def instance_busy(
  instance_id: str,
  op_kind: str,
) -> AsyncIterator[None]:
  """Mark an instance busy for the duration of the block, exceptions included."""
  lease = await _acquire_async(instance_id, op_kind)
  try:
    yield
  finally:
    await _release_async(instance_id, lease)


@contextmanager
def instance_busy_sync(
  instance_id: str,
  op_kind: str,
) -> Iterator[None]:
  """Synchronous :func:`instance_busy`."""
  lease = _acquire(instance_id, op_kind)
  try:
    yield
  finally:
    _release(instance_id, lease)
