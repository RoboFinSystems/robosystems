"""Tests for the storage reclaim sweep (transient artifacts + orphan estates)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.operations.graph.storage_reclaim import reclaim_instance_storage

GRAPH_ID = "kg_test"


def _db_with_subgraphs(*graph_ids: str) -> MagicMock:
  """A session whose registry reports these ids as live subgraphs."""
  db = MagicMock()
  db.query.return_value.filter.return_value.all.return_value = [
    (graph_id,) for graph_id in graph_ids
  ]
  return db


def _breakdown() -> dict:
  return {
    "graph_id": GRAPH_ID,
    "total_bytes": 1200,
    "items": [
      {"type": "graph", "id": GRAPH_ID, "bytes": 100},
      {"type": "memory", "id": f"{GRAPH_ID}_memory", "bytes": 10},
      {"type": "vectors", "id": f"{GRAPH_ID}_memory", "bytes": 10},
      {"type": "subgraph", "id": f"{GRAPH_ID}_dev", "bytes": 200},
      {"type": "subgraph", "id": f"{GRAPH_ID}_gone", "bytes": 300},
      {"type": "vectors", "id": f"{GRAPH_ID}_gone", "bytes": 40},
      {"type": "staging", "id": f"{GRAPH_ID}_gone", "bytes": 50},
      {"type": "transient", "id": f"{GRAPH_ID}-wip", "bytes": 490},
    ],
  }


def _client(breakdown: dict | None = None) -> AsyncMock:
  client = AsyncMock()
  client.get_storage_breakdown.return_value = (
    breakdown if breakdown is not None else _breakdown()
  )
  return client


async def _run(client: AsyncMock, db: MagicMock, dry_run: bool = False) -> dict:
  with patch(
    "robosystems.graph_api.client.factory.get_graph_client",
    new=AsyncMock(return_value=client),
  ):
    return await reclaim_instance_storage(GRAPH_ID, db, dry_run=dry_run)


@pytest.mark.unit
class TestReclaimTargeting:
  @pytest.mark.asyncio
  async def test_reclaims_transients_and_orphan_estates_only(self):
    """Live databases are never targets; an orphan's estate is one delete."""
    client = _client()
    result = await _run(client, _db_with_subgraphs(f"{GRAPH_ID}_dev"))

    deleted = {call.args[0] for call in client.delete_database.await_args_list}
    assert deleted == {f"{GRAPH_ID}-wip", f"{GRAPH_ID}_gone"}

    # Estate folded: lbug 300 + vectors 40 + staging 50 as one 390-byte entry.
    by_id = {e["id"]: e for e in result["reclaimed"]}
    assert by_id[f"{GRAPH_ID}_gone"]["bytes"] == 390
    assert result["bytes_freed"] == 490 + 390
    assert result["skipped"] == []

  @pytest.mark.asyncio
  async def test_transient_delete_preserves_the_bases_staging(self):
    """A `-wip` shares its base's name in DuckDB/Lance terms — only the
    lbug artifact goes; an orphan takes its whole estate with it."""
    client = _client()
    await _run(client, _db_with_subgraphs(f"{GRAPH_ID}_dev"))

    kwargs_by_id = {
      call.args[0]: call.kwargs for call in client.delete_database.await_args_list
    }
    assert kwargs_by_id[f"{GRAPH_ID}-wip"]["preserve_duckdb"] is True
    assert kwargs_by_id[f"{GRAPH_ID}_gone"]["preserve_duckdb"] is False

  @pytest.mark.asyncio
  async def test_item_failure_skips_and_continues(self):
    """A 409 (build in flight) or 404 is a per-item outcome, not an abort."""
    client = _client()
    client.delete_database.side_effect = [
      RuntimeError("409: materialization in progress"),
      {"status": "success"},
    ]

    result = await _run(client, _db_with_subgraphs(f"{GRAPH_ID}_dev"))

    assert len(result["reclaimed"]) == 1
    assert len(result["skipped"]) == 1
    assert "409" in result["skipped"][0]["reason"]
    assert result["bytes_freed"] == result["reclaimed"][0]["bytes"]

  @pytest.mark.asyncio
  async def test_registry_failure_never_deletes_orphans(self):
    """No registry confirmation, no orphan deletion — the sweep fails safe.

    Unlabelled `{parent}_*` items keep type `subgraph` and are not targets;
    only the transient artifact (whose classification needs no registry) is
    reclaimed.
    """
    db = MagicMock()
    db.query.side_effect = RuntimeError("registry unavailable")
    client = _client()

    result = await _run(client, db)

    deleted = {call.args[0] for call in client.delete_database.await_args_list}
    assert deleted == {f"{GRAPH_ID}-wip"}
    assert result["bytes_freed"] == 490

  @pytest.mark.asyncio
  async def test_dry_run_deletes_nothing(self):
    client = _client()
    result = await _run(client, _db_with_subgraphs(f"{GRAPH_ID}_dev"), dry_run=True)

    client.delete_database.assert_not_awaited()
    assert result["bytes_freed"] == 0
    assert {e["id"] for e in result["would_reclaim"]} == {
      f"{GRAPH_ID}-wip",
      f"{GRAPH_ID}_gone",
    }
    assert result["bytes_reclaimable"] == 490 + 390

  @pytest.mark.asyncio
  async def test_client_closed_even_on_breakdown_failure(self):
    client = _client()
    client.get_storage_breakdown.side_effect = RuntimeError("instance unreachable")

    with pytest.raises(RuntimeError):
      await _run(client, _db_with_subgraphs())

    client.close.assert_awaited_once()
