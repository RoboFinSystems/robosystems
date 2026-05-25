"""Tests for ``MappingOperator.run`` — the bounded, credit-constrained loop.

The loop re-invokes ``_run_single_pass`` until the CoA is fully mapped, a
pass makes no progress, the graph runs out of credits, or the pass cap is
hit. These tests drive it with a stubbed single pass + credit check so the
control flow is exercised without Bedrock or a database.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from robosystems.operations.operators.base import OperatorResult
from robosystems.operations.operators.implementations.mapping.operator import (
  MAX_MAPPING_PASSES,
  MappingOperator,
)


def _operator() -> MappingOperator:
  # Bypass __init__ — we only exercise run() against stubbed helpers.
  return MappingOperator.__new__(MappingOperator)


def _pass(mapped=0, flagged=0, skipped=0, coverage=0.0) -> OperatorResult:
  return OperatorResult(
    content="pass",
    metadata={
      "mapped": mapped,
      "flagged": flagged,
      "skipped": skipped,
      "coverage_percent": coverage,
    },
  )


class _Ctx:
  graph_id = "kg_test"
  user_id = "user_test"

  def __init__(self):
    # run() awaits ctx.progress.is_cancelled() before each pass.
    self.progress = AsyncMock()
    self.progress.is_cancelled = AsyncMock(return_value=False)


@pytest.mark.asyncio
async def test_stops_when_fully_mapped():
  """coverage == 100 after a pass → stop_reason 'complete', one pass."""
  op = _operator()
  op._has_credit_budget = lambda ctx: True

  calls = 0

  async def one_pass(ctx):
    nonlocal calls
    calls += 1
    return _pass(mapped=12, coverage=100.0)

  op._run_single_pass = one_pass

  result = await op.run(_Ctx())

  assert calls == 1
  assert result.metadata["stop_reason"] == "complete"
  assert result.metadata["mapped"] == 12
  assert result.metadata["passes"] == 1


@pytest.mark.asyncio
async def test_stops_on_no_progress():
  """A pass that maps/flags nothing new → stop_reason 'no_progress' rather
  than re-attempting the same unmappable elements (and burning credits)."""
  op = _operator()
  op._has_credit_budget = lambda ctx: True

  calls = 0

  async def stuck_pass(ctx):
    nonlocal calls
    calls += 1
    return _pass(mapped=0, flagged=0, skipped=5, coverage=70.0)

  op._run_single_pass = stuck_pass

  result = await op.run(_Ctx())

  assert calls == 1
  assert result.metadata["stop_reason"] == "no_progress"
  assert result.metadata["skipped"] == 5


@pytest.mark.asyncio
async def test_converges_over_multiple_passes():
  """Partial passes accumulate mapped counts and stop once coverage hits 100."""
  op = _operator()
  op._has_credit_budget = lambda ctx: True

  scripted = iter(
    [
      _pass(mapped=10, coverage=55.0),
      _pass(mapped=8, flagged=2, coverage=88.0),
      _pass(mapped=4, coverage=100.0),
    ]
  )

  async def scripted_pass(ctx):
    return next(scripted)

  op._run_single_pass = scripted_pass

  result = await op.run(_Ctx())

  assert result.metadata["passes"] == 3
  assert result.metadata["stop_reason"] == "complete"
  assert result.metadata["mapped"] == 22
  assert result.metadata["flagged"] == 2


@pytest.mark.asyncio
async def test_stops_when_credits_exhausted_before_first_pass():
  """No credits → loop never runs a pass; reports 'insufficient_credits'."""
  op = _operator()
  op._has_credit_budget = lambda ctx: False

  calls = 0

  async def one_pass(ctx):
    nonlocal calls
    calls += 1
    return _pass(mapped=1, coverage=10.0)

  op._run_single_pass = one_pass

  result = await op.run(_Ctx())

  assert calls == 0
  assert result.metadata["stop_reason"] == "insufficient_credits"
  assert result.metadata["passes"] == 0
  assert result.metadata["mapped"] == 0


@pytest.mark.asyncio
async def test_credits_exhausted_mid_loop():
  """Credits run out after the first pass → stop, keeping first-pass progress."""
  op = _operator()
  budget = iter([True, False])
  op._has_credit_budget = lambda ctx: next(budget)

  async def making_progress(ctx):
    return _pass(mapped=6, coverage=40.0)

  op._run_single_pass = making_progress

  result = await op.run(_Ctx())

  assert result.metadata["stop_reason"] == "insufficient_credits"
  assert result.metadata["passes"] == 1
  assert result.metadata["mapped"] == 6


@pytest.mark.asyncio
async def test_pass_cap_bounds_a_slowly_progressing_loop():
  """Always-progressing-but-never-complete passes stop at the cap, so the
  loop can't run forever even if every pass maps one more element."""
  op = _operator()
  op._has_credit_budget = lambda ctx: True

  async def drip(ctx):
    return _pass(mapped=1, coverage=50.0)

  op._run_single_pass = drip

  result = await op.run(_Ctx())

  assert result.metadata["passes"] == MAX_MAPPING_PASSES
  assert result.metadata["stop_reason"] == "pass_cap_reached"
  assert result.metadata["mapped"] == MAX_MAPPING_PASSES


@pytest.mark.asyncio
async def test_stops_when_cancelled_before_pass():
  """A cancellation between passes stops the loop without starting a new
  (paid) pass — no AI spend after the user cancels."""
  op = _operator()
  op._has_credit_budget = lambda ctx: True

  calls = 0

  async def one_pass(ctx):
    nonlocal calls
    calls += 1
    return _pass(mapped=5, coverage=50.0)

  op._run_single_pass = one_pass

  ctx = _Ctx()
  ctx.progress.is_cancelled = AsyncMock(return_value=True)

  result = await op.run(ctx)

  assert calls == 0
  assert result.metadata["stop_reason"] == "cancelled"
  assert result.metadata["passes"] == 0
  assert result.metadata["mapped"] == 0
