import asyncio as real_asyncio

import pytest

from robosystems.operations.graph.tasks import graph_tier_upgrade


@pytest.fixture(autouse=True)
def no_tier_upgrade_poll_sleep(monkeypatch):
  """Make the tier-upgrade poll loops wait no real time.

  `_wait_for_reattachment` and `_drain_instance` poll DynamoDB/ASG on
  `REATTACH_POLL_INTERVAL_SECONDS` (10s) and `DRAIN_POLL_INTERVAL_SECONDS`
  (5s). Tests drive those loops with mocks that flip state after one
  iteration, so each paid a full real interval.

  This neuters the sleep rather than zeroing the interval constants,
  because the loops also use the interval to advance `elapsed` against
  `REATTACH_TIMEOUT_SECONDS`. Zeroing the constant would stop `elapsed`
  from advancing and spin forever on any test where the volume never
  attaches; no-oping the sleep keeps the termination arithmetic identical
  to production. Rebinding the module's `asyncio` name (rather than
  `asyncio.sleep`) keeps the patch scoped to this one module.
  """

  class _NoSleepAsyncio:
    def __getattr__(self, name):
      return getattr(real_asyncio, name)

    @staticmethod
    async def sleep(_seconds):
      return None

  monkeypatch.setattr(graph_tier_upgrade, "asyncio", _NoSleepAsyncio())
