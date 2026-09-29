"""Fail a test when platform async code reaches a known-blocking call on the loop.

The API runs one uvicorn worker, so a blocking call inside a coroutine stalls
every other request in the process. The fix is ``asyncio.to_thread`` (or a
plain ``def`` handler); this guard catches the ones that forget.

A call is a violation when a coroutine or async generator defined under
``robosystems/`` is on the calling thread's stack. Offloaded work runs on a
worker thread whose stack holds no coroutine frames, and test bodies that call
these directly are not platform code, so neither trips it.

The denylist is deliberately narrow: bcrypt, OpenSearch and ``requests``.
Sync SQLAlchemy, Redis and boto3 are out until the executor-pool decision in
the blocking-call spec is made; the codebase uses them on the loop by design.
"""

from __future__ import annotations

import inspect
import sys
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import patch

import bcrypt
import requests
from opensearchpy.transport import Transport

PLATFORM_ROOTS: list[str] = [
  str(Path(__file__).resolve().parent.parent / "robosystems")
]

# "robosystems/path/to/module.py:function" of the platform coroutine that
# reaches a blocking call → why it is tolerated, and until when. Every entry is
# a known defect, not an exemption.
ALLOWLIST: dict[str, str] = {}

_ASYNC_FLAGS = inspect.CO_COROUTINE | inspect.CO_ASYNC_GENERATOR


def _platform_coroutine_on_stack() -> str | None:
  frame = sys._getframe(2)
  while frame is not None:
    code = frame.f_code
    if code.co_flags & _ASYNC_FLAGS and any(
      code.co_filename.startswith(root) for root in PLATFORM_ROOTS
    ):
      root = next(r for r in PLATFORM_ROOTS if code.co_filename.startswith(r))
      relative = code.co_filename[len(str(Path(root).parent)) + 1 :]
      return f"{relative}:{code.co_name}"
    frame = frame.f_back
  return None


def _guard(
  name: str, real: Callable[..., Any], violations: list[str]
) -> Callable[..., Any]:
  def guarded(*args: Any, **kwargs: Any) -> Any:
    caller = _platform_coroutine_on_stack()
    if caller is not None and caller not in ALLOWLIST:
      violations.append(f"{name} called on the event loop from {caller}")
    return real(*args, **kwargs)

  guarded.__blocking_guard_original__ = real  # type: ignore[attr-defined]
  return guarded


@contextmanager
def guard_blocking_calls() -> Iterator[list[str]]:
  """Yield the violations seen while active.

  Recorded rather than raised: platform code that catches ``Exception`` (the
  API-key check does) would swallow a raise and the test would pass.
  """
  violations: list[str] = []
  targets = [
    (bcrypt, "hashpw", "bcrypt.hashpw"),
    (bcrypt, "checkpw", "bcrypt.checkpw"),
    (requests.Session, "request", "requests.Session.request"),
    (Transport, "perform_request", "OpenSearch Transport.perform_request"),
  ]
  with ExitStack() as stack:
    for owner, attr, label in targets:
      current = getattr(owner, attr)
      # A nested guard (the self-tests) wraps the original, not the outer guard.
      real = getattr(current, "__blocking_guard_original__", current)
      guarded = _guard(label, real, violations)
      stack.enter_context(patch.object(owner, attr, guarded))
    yield violations
