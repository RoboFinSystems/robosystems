"""Cold report-model builds are single-flight per report and one at a time
per process: each holds a whole report in memory."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from robosystems.operations.roboledger.views import information_blocks as ib

MODULE = "robosystems.operations.roboledger.views.information_blocks"


class _Builds:
  def __init__(self):
    self.calls: list[str] = []
    self.running = 0
    self.peak = 0

  async def build(self, key, graph_id, report_id, cache):
    self.calls.append(report_id)
    self.running += 1
    self.peak = max(self.peak, self.running)
    await asyncio.sleep(0.05)
    self.running -= 1
    return f"model:{report_id}"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_concurrent_loads_of_one_report_share_a_build():
  builds = _Builds()
  with (
    patch(f"{MODULE}._cache", return_value=None),
    patch(f"{MODULE}._build_and_cache", new=builds.build),
  ):
    results = await asyncio.gather(
      *(ib.load_report_model("kg0000000000000001", "rpt_a") for _ in range(3))
    )
  assert builds.calls == ["rpt_a"]
  assert {model for model, _ in results} == {"model:rpt_a"}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_different_reports_build_one_at_a_time():
  builds = _Builds()
  with (
    patch(f"{MODULE}._cache", return_value=None),
    patch(f"{MODULE}._build_and_cache", new=builds.build),
  ):
    await asyncio.gather(
      *(ib.load_report_model("kg0000000000000001", f"rpt_{i}") for i in range(3))
    )
  assert sorted(builds.calls) == ["rpt_0", "rpt_1", "rpt_2"]
  assert builds.peak == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_a_caller_that_leaves_does_not_cancel_the_shared_build():
  builds = _Builds()
  with (
    patch(f"{MODULE}._cache", return_value=None),
    patch(f"{MODULE}._build_and_cache", new=builds.build),
  ):
    first = asyncio.create_task(ib.load_report_model("kg0000000000000001", "rpt_x"))
    second = asyncio.create_task(ib.load_report_model("kg0000000000000001", "rpt_x"))
    await asyncio.sleep(0.01)
    first.cancel()
    model, _ = await second
  assert model == "model:rpt_x"
  assert builds.calls == ["rpt_x"]
