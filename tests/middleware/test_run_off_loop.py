"""`run_off_loop` must keep the runner limiter honest under native timeouts.

Regression for the tier-B robustness review §2.4: anyio's `to_thread.run_sync`
shields only anyio cancellation, so `asyncio.wait_for` / `asyncio.timeout`
raise straight through it and anyio releases the `CapacityLimiter` token while
the worker thread — and its DB connection — is still running. That uncouples
the limiter from the real thread count; under timeouts the extensions pool
overflows. The fix shields the thread run, tying the token to the thread's
lifetime, not the caller's.
"""

from __future__ import annotations

import asyncio
import threading

import anyio
import pytest

from robosystems.middleware import operations


@pytest.fixture
def fresh_limiter():
  """Isolate the module-global runner limiter for the test."""
  prev = operations._runner_limiter
  operations._runner_limiter = anyio.CapacityLimiter(5)
  try:
    yield operations._runner_limiter
  finally:
    operations._runner_limiter = prev


@pytest.mark.asyncio
@pytest.mark.unit
async def test_timeout_holds_the_runner_token_until_the_thread_completes(
  fresh_limiter,
):
  limiter = fresh_limiter
  started = threading.Event()
  release = threading.Event()

  def slow_work():
    started.set()
    # Hold the worker thread open past the caller's timeout.
    release.wait(5.0)
    return "committed-late"

  assert limiter.borrowed_tokens == 0

  # The caller times out while the thread is still running.
  with pytest.raises((asyncio.TimeoutError, TimeoutError)):
    await asyncio.wait_for(operations.run_off_loop(slow_work), 0.2)

  # The thread is still live, so the token must NOT have been released — this
  # is the assertion that fails against the pre-fix code (token drops to 0 at
  # the timeout while the thread runs on).
  assert started.is_set()
  assert limiter.borrowed_tokens == 1, (
    "runner token released while the worker thread was still running — "
    "the limiter no longer bounds the real thread count"
  )

  # Let the thread finish; the token is released on thread completion.
  release.set()
  for _ in range(500):
    if limiter.borrowed_tokens == 0:
      break
    await asyncio.sleep(0.01)
  assert limiter.borrowed_tokens == 0


@pytest.mark.asyncio
@pytest.mark.unit
async def test_late_failure_after_timeout_is_logged_not_lost(fresh_limiter):
  """A worker that raises *after* the caller timed out must reach the logger,
  not surface as an unretrieved-exception stderr traceback from an orphan task
  (parity with execute_operation's abandoned-outcome handling)."""
  from unittest.mock import patch

  release = threading.Event()

  def slow_then_fail():
    release.wait(5.0)
    raise RuntimeError("late boom")

  with patch.object(operations.logger, "warning") as mock_warning:
    with pytest.raises((asyncio.TimeoutError, TimeoutError)):
      await asyncio.wait_for(operations.run_off_loop(slow_then_fail), 0.2)

    release.set()
    for _ in range(500):
      if mock_warning.called:
        break
      await asyncio.sleep(0.01)

  assert mock_warning.called, "a post-cancellation worker failure was not logged"
  logged = " ".join(str(a) for a in mock_warning.call_args.args)
  assert "late boom" in logged or "RuntimeError" in logged


@pytest.mark.asyncio
@pytest.mark.unit
async def test_normal_completion_releases_the_token(fresh_limiter):
  limiter = fresh_limiter

  def quick_work():
    return 42

  assert await operations.run_off_loop(quick_work) == 42
  assert limiter.borrowed_tokens == 0


@pytest.mark.asyncio
@pytest.mark.unit
async def test_coroutine_func_is_awaited_directly(fresh_limiter):
  async def coro_work():
    return "async"

  assert await operations.run_off_loop(coro_work) == "async"
