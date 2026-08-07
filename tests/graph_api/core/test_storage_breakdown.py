"""Tests for itemized graph storage measurement.

The bugs this replaces both produced a confident zero, so these tests care
most about two things: that a directory-shaped database is actually measured,
and that the total spans all three roots rather than the primary file alone.
"""

from pathlib import Path

import pytest

from robosystems.graph_api.core.storage_breakdown import (
  compute_storage_breakdown,
  path_size_bytes,
)

GRAPH_ID = "kg19f54bda80f39fbbac0f"
OTHER_ID = "kg00000000000000000000"


@pytest.fixture
def roots(tmp_path, monkeypatch):
  """Three instance-local storage roots, patched into env."""
  lbug = tmp_path / "lbug-dbs"
  lance = tmp_path / "lance"
  staging = tmp_path / "staging"
  for root in (lbug, lance, staging):
    root.mkdir()

  monkeypatch.setattr(
    "robosystems.graph_api.core.storage_breakdown.env.LBUG_DATABASE_PATH", str(lbug)
  )
  monkeypatch.setattr(
    "robosystems.graph_api.core.storage_breakdown.env.LANCE_INDEX_PATH", str(lance)
  )
  monkeypatch.setattr(
    "robosystems.graph_api.core.storage_breakdown.env.DUCKDB_STAGING_PATH",
    str(staging),
  )
  return {"lbug": lbug, "lance": lance, "staging": staging}


def _write(path: Path, size: int) -> None:
  path.parent.mkdir(parents=True, exist_ok=True)
  path.write_bytes(b"x" * size)


class TestPathSizeBytes:
  def test_sizes_a_file(self, tmp_path):
    _write(tmp_path / "db.lbug", 512)
    assert path_size_bytes(tmp_path / "db.lbug") == 512

  def test_sizes_a_directory_recursively(self, tmp_path):
    """The regression that mattered: directory-shaped databases read as 0."""
    _write(tmp_path / "db.lbug" / "data" / "a.bin", 300)
    _write(tmp_path / "db.lbug" / "b.bin", 200)
    assert path_size_bytes(tmp_path / "db.lbug") == 500

  def test_missing_path_is_zero_not_an_error(self, tmp_path):
    assert path_size_bytes(tmp_path / "nope.lbug") == 0


class TestComputeStorageBreakdown:
  def test_counts_all_three_roots(self, roots):
    """Total must span lbug + vectors + staging, not the graph file alone."""
    _write(roots["lbug"] / f"{GRAPH_ID}.lbug", 1000)
    _write(roots["lance"] / GRAPH_ID / "index.bin", 70)
    _write(roots["staging"] / f"{GRAPH_ID}.duckdb", 1800)

    result = compute_storage_breakdown(GRAPH_ID)

    assert result["total_bytes"] == 2870
    assert {i["type"] for i in result["items"]} == {"graph", "vectors", "staging"}

  def test_staging_can_exceed_the_graph_itself(self, roots):
    """Guards the undercount: the primary file is a minority of the footprint."""
    _write(roots["lbug"] / f"{GRAPH_ID}.lbug", 100)
    _write(roots["staging"] / f"{GRAPH_ID}.duckdb", 900)

    result = compute_storage_breakdown(GRAPH_ID)
    graph_bytes = next(i["bytes"] for i in result["items"] if i["type"] == "graph")

    assert graph_bytes == 100
    assert result["total_bytes"] == 1000

  def test_classifies_memory_and_subgraphs(self, roots):
    _write(roots["lbug"] / f"{GRAPH_ID}.lbug", 10)
    _write(roots["lbug"] / f"{GRAPH_ID}_memory.lbug", 20)
    _write(roots["lbug"] / f"{GRAPH_ID}_dev.lbug", 30)

    items = {i["id"]: i["type"] for i in compute_storage_breakdown(GRAPH_ID)["items"]}

    assert items[GRAPH_ID] == "graph"
    assert items[f"{GRAPH_ID}_memory"] == "memory"
    assert items[f"{GRAPH_ID}_dev"] == "subgraph"

  def test_classifies_blue_green_artifacts_as_transient(self, roots):
    """A build artifact must not read as the subgraph it was built for.

    `{parent}_{name}-wip` matches the subgraph prefix, so the classifier's
    fallthrough labelled it `subgraph` — and `/usage`, which sums by type,
    reported a footprint the subgraph list (which sums by registered id) could
    never match.
    """
    _write(roots["lbug"] / f"{GRAPH_ID}.lbug", 10)
    _write(roots["lbug"] / f"{GRAPH_ID}_dev.lbug", 20)
    _write(roots["lbug"] / f"{GRAPH_ID}_dev-wip.lbug", 40)
    _write(roots["lbug"] / f"{GRAPH_ID}-prev.lbug", 80)

    items = {i["id"]: i["type"] for i in compute_storage_breakdown(GRAPH_ID)["items"]}

    assert items[f"{GRAPH_ID}_dev"] == "subgraph"
    assert items[f"{GRAPH_ID}_dev-wip"] == "transient"
    assert items[f"{GRAPH_ID}-prev"] == "transient"

  def test_subgraph_type_sums_to_live_subgraphs_only(self, roots):
    """The `subgraph` bucket is what the subgraph list is reconciled against."""
    _write(roots["lbug"] / f"{GRAPH_ID}_dev.lbug", 20)
    _write(roots["lbug"] / f"{GRAPH_ID}_dev.lbug.wal", 5)
    _write(roots["lbug"] / f"{GRAPH_ID}_dev-wip.lbug", 40)

    items = compute_storage_breakdown(GRAPH_ID)["items"]
    subgraph_bytes = sum(i["bytes"] for i in items if i["type"] == "subgraph")

    assert subgraph_bytes == 25

  def test_counts_the_parents_own_build_artifacts(self, roots):
    """`{parent}-wip` is hyphenated, so it escaped the ownership test entirely.

    A subgraph's artifact was counted and the parent's was not — the same
    disk, visible or invisible depending on which database produced it.
    """
    _write(roots["lbug"] / f"{GRAPH_ID}.lbug", 100)
    _write(roots["lbug"] / f"{GRAPH_ID}-wip.lbug", 700)

    result = compute_storage_breakdown(GRAPH_ID)

    assert result["total_bytes"] == 800

  def test_folds_wal_into_its_database(self, roots):
    """A WAL is the same logical object as its database to a quota reader."""
    _write(roots["lbug"] / f"{GRAPH_ID}.lbug", 100)
    _write(roots["lbug"] / f"{GRAPH_ID}.lbug.wal", 50)

    items = compute_storage_breakdown(GRAPH_ID)["items"]
    graph_items = [i for i in items if i["type"] == "graph"]

    assert len(graph_items) == 1
    assert graph_items[0]["bytes"] == 150

  def test_measures_directory_shaped_databases(self, roots):
    _write(roots["lbug"] / f"{GRAPH_ID}.lbug" / "chunk.bin", 4096)

    result = compute_storage_breakdown(GRAPH_ID)

    assert result["total_bytes"] == 4096

  def test_excludes_other_tenants(self, roots):
    """The cap denominator must not absorb a neighbour's disk."""
    _write(roots["lbug"] / f"{GRAPH_ID}.lbug", 100)
    _write(roots["lbug"] / f"{OTHER_ID}.lbug", 999)
    _write(roots["lance"] / OTHER_ID / "index.bin", 999)
    _write(roots["staging"] / f"{OTHER_ID}.duckdb", 999)

    result = compute_storage_breakdown(GRAPH_ID)

    assert result["total_bytes"] == 100
    assert all(i["id"].startswith(GRAPH_ID) for i in result["items"])

  def test_prefix_match_requires_the_underscore_boundary(self, roots):
    """`kg1abc` must not swallow `kg1abcdef`."""
    _write(roots["lbug"] / f"{GRAPH_ID}.lbug", 100)
    _write(roots["lbug"] / f"{GRAPH_ID}extra.lbug", 500)

    result = compute_storage_breakdown(GRAPH_ID)

    assert result["total_bytes"] == 100

  def test_empty_instance_reports_zero_cleanly(self, roots):
    result = compute_storage_breakdown(GRAPH_ID)

    assert result["total_bytes"] == 0
    assert result["items"] == []
    assert result["graph_id"] == GRAPH_ID

  def test_missing_roots_do_not_raise(self, tmp_path, monkeypatch):
    """A node without vectors or staging configured still reports its graph."""
    lbug = tmp_path / "lbug-dbs"
    lbug.mkdir()
    _write(lbug / f"{GRAPH_ID}.lbug", 42)

    monkeypatch.setattr(
      "robosystems.graph_api.core.storage_breakdown.env.LBUG_DATABASE_PATH", str(lbug)
    )
    monkeypatch.setattr(
      "robosystems.graph_api.core.storage_breakdown.env.LANCE_INDEX_PATH",
      str(tmp_path / "absent-lance"),
    )
    monkeypatch.setattr(
      "robosystems.graph_api.core.storage_breakdown.env.DUCKDB_STAGING_PATH",
      str(tmp_path / "absent-staging"),
    )

    assert compute_storage_breakdown(GRAPH_ID)["total_bytes"] == 42

  def test_total_always_equals_the_sum_of_items(self, roots):
    _write(roots["lbug"] / f"{GRAPH_ID}.lbug", 11)
    _write(roots["lbug"] / f"{GRAPH_ID}_memory.lbug", 22)
    _write(roots["lbug"] / f"{GRAPH_ID}_sub.lbug", 33)
    _write(roots["lance"] / GRAPH_ID / "i.bin", 44)
    _write(roots["lance"] / f"{GRAPH_ID}_memory" / "i.bin", 55)
    _write(roots["staging"] / f"{GRAPH_ID}.duckdb", 66)

    result = compute_storage_breakdown(GRAPH_ID)

    assert result["total_bytes"] == sum(i["bytes"] for i in result["items"])
    assert result["total_bytes"] == 231

  def test_rejects_an_unsafe_graph_id(self, roots):
    from fastapi import HTTPException

    with pytest.raises(HTTPException):
      compute_storage_breakdown("../../etc/passwd")
