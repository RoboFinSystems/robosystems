"""The per-connection sync lock is released by whoever finishes the work.

A provider that starts a Dagster run hands the lock to that run, which releases
it on completion. A provider that returns synchronously has already finished —
so if the dispatcher does not release it there, the lock sits for its full
30-minute TTL and every later sync on that connection answers 409 with nothing
actually in progress.

The asymmetry is easy to reintroduce because the failure is invisible on the
happy path: the sync call succeeds, and only the *next* one fails.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

MODULE = "robosystems.operations.connection_service"
# dispatch_connection_sync imports these inside the function, so they are
# patched at their source rather than as attributes of the module under test.
LOCKS = "robosystems.middleware.auth.distributed_lock"
VALKEY = "robosystems.config.valkey_registry"
REGISTRY = "robosystems.operations.providers.registry"


@pytest.fixture
def _lock():
  """A lock that always acquires, so the release path is what is observed."""
  acquired = MagicMock(acquired=True, lock_id="lock-abc", error_message=None)
  with (
    patch(f"{VALKEY}.create_redis_client"),
    patch(f"{LOCKS}.DistributedLock") as lock_cls,
    patch(f"{LOCKS}.release_lock_by_id") as release,
  ):
    lock_cls.return_value.acquire.return_value = acquired
    yield release


async def _dispatch(provider: str, sync_result: str):
  from robosystems.operations import connection_service

  connection = {
    "connection_id": "conn123",
    "provider": provider,
    "user_id": "usr1",
    "metadata": {},
  }

  async def _get_connection(*_args, **_kwargs):
    return connection

  async def _sync(*_args, **_kwargs):
    return sync_result

  with (
    patch.object(
      connection_service.ConnectionService, "get_connection", _get_connection
    ),
    patch(f"{REGISTRY}.provider_registry") as registry,
  ):
    registry.get_provider.return_value = {}
    registry.starts_async_run.side_effect = lambda p: p == "quickbooks"
    registry.sync_connection.side_effect = _sync
    return await connection_service.dispatch_connection_sync(
      connection_id="conn123", graph_id="kg1", user_id="usr1"
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_a_synchronous_provider_releases_the_lock(_lock):
  """SEC and external return a status string; nothing else will free the lock."""
  await _dispatch("sec", "nothing to sync")
  assert _lock.called, (
    "a provider that returns synchronously must release the sync lock, or the "
    "next sync on this connection 409s for the lock's full TTL"
  )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_an_async_provider_keeps_the_lock_for_its_run(_lock):
  """QuickBooks hands the lock to qb_load; releasing here would let a second
  sync race the run this one just started."""
  await _dispatch("quickbooks", "run-xyz")
  assert not _lock.called
