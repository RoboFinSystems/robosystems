"""A maintenance pause on graph writes.

A deliberate writer roll sets `/robosystems/{env}/features/GRAPH_WRITES_PAUSED_UNTIL`
to an ISO timestamp, then drains in-flight writes before replacing instances.
New writes refuse until that time, so none starts on a writer about to go. The
timestamp lapses on its own, so a cancelled roll cannot pause writes for good.
"""

from __future__ import annotations

from datetime import UTC, datetime

from robosystems.logger import logger

PAUSE_PARAMETER = "GRAPH_WRITES_PAUSED_UNTIL"


class GraphWritesPausedError(RuntimeError):
  """Graph writes are paused for maintenance; the caller should retry later."""

  def __init__(self, until: datetime) -> None:
    super().__init__(
      f"Graph writes are paused for maintenance until {until.isoformat()}; "
      "retry after that."
    )
    self.until = until


def graph_writes_paused_until(now: datetime | None = None) -> datetime | None:
  """When the pause ends, or None when writes are allowed.

  Read uncached. Fails open: a missing, past or unreadable value allows writes,
  because the pause is a guard for planned rolls, not a lock.
  """
  from robosystems.config.parameter_store import get_parameter_manager

  raw = get_parameter_manager().get_parameter_uncached(PAUSE_PARAMETER).strip()
  if not raw:
    return None
  try:
    until = datetime.fromisoformat(raw.replace("Z", "+00:00"))
  except ValueError:
    logger.warning(f"Ignoring unparseable {PAUSE_PARAMETER}: {raw!r}")
    return None
  if until.tzinfo is None:
    until = until.replace(tzinfo=UTC)
  return until if until > (now or datetime.now(UTC)) else None


def assert_graph_writes_allowed() -> None:
  """Raise GraphWritesPausedError while a maintenance pause is in force."""
  until = graph_writes_paused_until()
  if until is not None:
    raise GraphWritesPausedError(until)


async def refuse_while_writes_paused() -> None:
  """For an API that starts a graph write: 503 + Retry-After during a pause,
  instead of queuing work the worker would refuse."""
  import asyncio

  from fastapi import HTTPException, status

  until = await asyncio.to_thread(graph_writes_paused_until)
  if until is None:
    return
  retry_after = max(60, int((until - datetime.now(UTC)).total_seconds()))
  raise HTTPException(
    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
    detail={
      "error": "Graph writes are paused for maintenance",
      "retry_after_seconds": retry_after,
    },
    headers={"Retry-After": str(retry_after)},
  )
