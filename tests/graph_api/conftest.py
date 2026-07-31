import asyncio as real_asyncio

import pytest

from robosystems.graph_api.client.base import BaseGraphClient
from robosystems.graph_api.core import task_sse


@pytest.fixture(autouse=True)
def no_client_retry_delay(request, monkeypatch):
  """Collapse the Graph API client's exponential backoff to zero.

  `_execute_with_retry` awaits `asyncio.sleep(_calculate_retry_delay(...))`.
  With the shipped defaults (`retry_delay=1.0`, `retry_backoff=2.0`,
  `max_retries=3`) an exhausted retry chain waits 1 + 2 + 4 = 7s, which is
  what made the timeout/retry tests here the slowest in the suite.

  Patching the delay -- rather than the config -- keeps `test_config.py`'s
  assertions on the shipped defaults (`retry_delay == 1.0`) meaningful, and
  keeps `asyncio.sleep` itself real so the retry loop's await semantics are
  unchanged.

  Tests that assert on the backoff curve itself opt out with
  `@pytest.mark.real_retry_delay`.
  """
  if request.node.get_closest_marker("real_retry_delay"):
    return
  monkeypatch.setattr(
    BaseGraphClient, "_calculate_retry_delay", lambda self, attempt: 0.0
  )


@pytest.fixture(autouse=True)
def no_task_sse_poll_sleep(monkeypatch):
  """Drop the SSE generator's 2s poll interval.

  `generate_task_sse_events` polls task state on a hardcoded
  `asyncio.sleep(2)`. Nothing asserts on that cadence -- the heartbeat
  test already drives elapsed time through a mocked `time.time` -- so the
  wait was pure wall-clock. Loop iteration counts are unchanged, which
  keeps that test's finite `side_effect` sequence lined up.
  """

  class _NoSleepAsyncio:
    def __getattr__(self, name):
      return getattr(real_asyncio, name)

    @staticmethod
    async def sleep(_seconds):
      return None

  monkeypatch.setattr(task_sse, "asyncio", _NoSleepAsyncio())
