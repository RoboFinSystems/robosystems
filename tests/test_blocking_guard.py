"""The blocking-call guard in tests/_blocking_guard.py, tested on itself."""

import asyncio

import bcrypt
import pytest

from tests import _blocking_guard as guard

pytestmark = pytest.mark.unit

_HASH = bcrypt.hashpw(b"pw", bcrypt.gensalt(rounds=4))


async def _inline_platform_coroutine() -> bool:
  return bcrypt.checkpw(b"pw", _HASH)


async def _offloaded_platform_coroutine() -> bool:
  return await asyncio.to_thread(bcrypt.checkpw, b"pw", _HASH)


@pytest.fixture
def this_file_is_platform(monkeypatch):
  monkeypatch.setattr(guard, "PLATFORM_ROOTS", [__file__])


# Each test runs its own guard, so the autouse one never sees the violation.


@pytest.mark.asyncio
async def test_inline_call_from_platform_coroutine_is_recorded(this_file_is_platform):
  with guard.guard_blocking_calls() as violations:
    assert await _inline_platform_coroutine() is True
  assert violations == [
    "bcrypt.checkpw called on the event loop from "
    "test_blocking_guard.py:_inline_platform_coroutine"
  ]


@pytest.mark.asyncio
async def test_swallowed_call_is_still_recorded(this_file_is_platform):
  async def _swallowing_platform_coroutine():
    try:
      return bcrypt.checkpw(b"pw", _HASH)
    except Exception:
      return None

  with guard.guard_blocking_calls() as violations:
    await _swallowing_platform_coroutine()
  assert len(violations) == 1


@pytest.mark.asyncio
async def test_offloaded_call_passes(this_file_is_platform):
  with guard.guard_blocking_calls() as violations:
    assert await _offloaded_platform_coroutine() is True
  assert violations == []


@pytest.mark.asyncio
async def test_call_from_a_test_body_is_not_platform_code():
  with guard.guard_blocking_calls() as violations:
    assert bcrypt.checkpw(b"pw", _HASH) is True
  assert violations == []


@pytest.mark.asyncio
async def test_allowlisted_caller_passes(this_file_is_platform, monkeypatch):
  monkeypatch.setattr(
    guard,
    "ALLOWLIST",
    {"test_blocking_guard.py:_inline_platform_coroutine": "self-test"},
  )
  with guard.guard_blocking_calls() as violations:
    await _inline_platform_coroutine()
  assert violations == []
