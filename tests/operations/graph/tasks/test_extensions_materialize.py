"""Worker-path extensions materialization task.

The blue/green gate refuses to swap a `partial` build, leaving the old
graph generation active. The worker task (the path every manual API and
MCP materialize takes) must treat that exactly like the Dagster path
does — as a failure that leaves `graph_stale` set so the staleness
sensor retries — rather than marking the graph fresh over a build that
never swapped in.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.operations.graph.tasks.extensions_materialize import (
  ExtensionsMaterializeTask,
)


def _make_task() -> ExtensionsMaterializeTask:
  manager = MagicMock()
  manager.emit_progress = AsyncMock()
  task = ExtensionsMaterializeTask(
    task_id="op_test123",
    graph_id="kg01234567890abcdef",
    user_id="user_1",
    params={"rebuild": True, "lock_key": None},
    manager=manager,
  )
  return task


def _result(status: str) -> SimpleNamespace:
  return SimpleNamespace(
    status=status,
    errors=["FactSet: staging count mismatch"] if status != "success" else [],
    duration_ms=42.0,
    tables_staged=3,
    tables_materialized=["Entity", "Element"],
    total_rows=100,
  )


def _db_session_gen(db: MagicMock):
  yield db


class TestPartialBuildNotMarkedFresh:
  @pytest.mark.asyncio
  async def test_partial_short_circuits_before_mark_fresh(self):
    """A partial build must not clear staleness — the swap was refused."""
    task = _make_task()
    db = MagicMock()

    materializer = MagicMock()
    materializer.materialize = AsyncMock(return_value=_result("partial"))

    with (
      patch(
        "robosystems.operations.extensions.materialize.ExtensionsMaterializer",
        return_value=materializer,
      ),
      patch(
        "robosystems.database.get_db_session",
        return_value=_db_session_gen(db),
      ),
    ):
      result = await task.execute()

    assert result["status"] == "partial"
    assert result["errors"]
    # The graph row must never be touched: no query, no mark_fresh.
    db.query.assert_not_called()

  @pytest.mark.asyncio
  async def test_error_short_circuits_before_mark_fresh(self):
    task = _make_task()
    db = MagicMock()

    materializer = MagicMock()
    materializer.materialize = AsyncMock(return_value=_result("error"))

    with (
      patch(
        "robosystems.operations.extensions.materialize.ExtensionsMaterializer",
        return_value=materializer,
      ),
      patch(
        "robosystems.database.get_db_session",
        return_value=_db_session_gen(db),
      ),
    ):
      result = await task.execute()

    assert result["status"] == "error"
    db.query.assert_not_called()

  @pytest.mark.asyncio
  async def test_success_marks_fresh(self):
    """Guard against overcorrection: a clean build still clears staleness."""
    task = _make_task()
    db = MagicMock()
    graph = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = graph

    materializer = MagicMock()
    materializer.materialize = AsyncMock(return_value=_result("success"))

    with (
      patch(
        "robosystems.operations.extensions.materialize.ExtensionsMaterializer",
        return_value=materializer,
      ),
      patch(
        "robosystems.database.get_db_session",
        return_value=_db_session_gen(db),
      ),
      patch(
        "robosystems.dagster.reporting.report_asset_materialization",
        new=AsyncMock(),
      ),
    ):
      result = await task.execute()

    assert result["status"] == "success"
    graph.mark_fresh.assert_called_once()
    assert graph.mark_fresh.call_args.kwargs["session"] is db
    # Compare-and-clear anchor: the snapshot time travels with the call so a
    # write that landed during the build keeps the graph stale.
    assert graph.mark_fresh.call_args.kwargs["started_at"] is not None
