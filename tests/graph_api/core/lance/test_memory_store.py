"""Tests for LanceMemoryStore — per-graph incremental semantic-memory CRUD.

Exercises create-if-not-exists, add, search (incl. empty-table), get, list,
update (merge_insert), delete, and the id/vector/predicate guards. No network
or Postgres — a temp lance base directory only.
"""

import uuid

import pytest
from fastapi import HTTPException

VECTOR_DIM = 384


def _vec(*leading: float) -> list[float]:
  """A 384-dim unit-ish vector with the given leading components."""
  v = [0.0] * VECTOR_DIM
  for i, x in enumerate(leading):
    v[i] = float(x)
  return v


def _mem_id() -> str:
  return f"mem_{uuid.uuid4().hex}"


def _row(
  text: str, vec: list[float], *, source: str = "api", memory_type: str = "note"
):
  return {
    "id": _mem_id(),
    "vector": vec,
    "text": text,
    "created_at_ms": 1_700_000_000_000,
    "updated_at_ms": 1_700_000_000_000,
    "created_by": "user_1",
    "source": source,
    "memory_type": memory_type,
    "tags": ["t1"],
    "source_ref": None,
    "provenance": None,
  }


@pytest.fixture()
def store(tmp_path):
  from robosystems.graph_api.core.lance import LanceMemoryStore

  return LanceMemoryStore(base_path=str(tmp_path / "lance"))


GRAPH = "kgtestgraph000001"


class TestEmptyAndExistence:
  def test_absent_table_reports_not_exists(self, store):
    assert store.exists(GRAPH) is False
    assert store.count(GRAPH) == 0

  def test_search_empty_returns_empty(self, store):
    out = store.search(GRAPH, _vec(1.0), limit=5)
    assert out == {"results": [], "total": 0, "execution_time_ms": 0.0}

  def test_get_absent_returns_none(self, store):
    assert store.get(GRAPH, _mem_id()) is None

  def test_list_empty_returns_empty(self, store):
    assert store.list(GRAPH) == {"results": [], "total": 0}

  def test_delete_absent_reports_not_deleted(self, store):
    assert store.delete(GRAPH, _mem_id()) == {"deleted": False}


class TestAddAndSearch:
  def test_add_creates_table_and_counts(self, store):
    res = store.add_rows(
      GRAPH, [_row("apple", _vec(1.0)), _row("banana", _vec(0.0, 1.0))]
    )
    assert res["added"] == 2
    assert res["total"] == 2
    assert store.exists(GRAPH) is True

  def test_search_orders_by_cosine_and_hides_vector(self, store):
    store.add_rows(
      GRAPH,
      [
        _row("apple", _vec(1.0)),
        _row("banana", _vec(0.0, 1.0)),
        _row("apricot", _vec(0.9, 0.1)),
      ],
    )
    out = store.search(GRAPH, _vec(1.0), limit=2)
    assert out["total"] == 2
    texts = [r["text"] for r in out["results"]]
    assert texts[0] == "apple"  # exact match ranks first
    assert "banana" not in texts  # farthest excluded by limit
    for r in out["results"]:
      assert "vector" not in r
      assert "distance" in r

  def test_search_prefilter_by_source(self, store):
    store.add_rows(
      GRAPH,
      [
        _row("api-one", _vec(1.0), source="api"),
        _row("mcp-one", _vec(0.99, 0.01), source="mcp"),
      ],
    )
    out = store.search(GRAPH, _vec(1.0), limit=10, where="source = 'api'")
    assert {r["source"] for r in out["results"]} == {"api"}

  def test_add_rejects_bad_vector_dim(self, store):
    bad = _row("bad", [0.1, 0.2, 0.3])
    with pytest.raises(ValueError):
      store.add_rows(GRAPH, bad if isinstance(bad, list) else [bad])

  def test_add_rejects_bad_id(self, store):
    row = _row("x", _vec(1.0))
    row["id"] = "notamemoryid"
    with pytest.raises(ValueError):
      store.add_rows(GRAPH, [row])


class TestGetListUpdateDelete:
  def test_get_roundtrip(self, store):
    row = _row("remember me", _vec(1.0))
    store.add_rows(GRAPH, [row])
    got = store.get(GRAPH, row["id"])
    assert got is not None
    assert got["text"] == "remember me"
    assert "vector" not in got

  def test_list_returns_all(self, store):
    store.add_rows(GRAPH, [_row(f"m{i}", _vec(float(i) + 1.0)) for i in range(3)])
    out = store.list(GRAPH, limit=10)
    assert out["total"] == 3
    assert len(out["results"]) == 3

  def test_list_pagination_offset(self, store):
    store.add_rows(GRAPH, [_row(f"m{i}", _vec(float(i) + 1.0)) for i in range(5)])
    page = store.list(GRAPH, limit=2, offset=2)
    assert page["total"] == 5
    assert len(page["results"]) == 2

  def test_update_replaces_text(self, store):
    row = _row("old text", _vec(1.0))
    store.add_rows(GRAPH, [row])
    updated = {**row, "text": "new text", "vector": _vec(0.5, 0.5)}
    store.update(GRAPH, row["id"], updated)
    got = store.get(GRAPH, row["id"])
    assert got["text"] == "new text"
    assert store.count(GRAPH) == 1  # upsert, not insert

  def test_delete_removes_row(self, store):
    row = _row("bye", _vec(1.0))
    store.add_rows(GRAPH, [row])
    assert store.delete(GRAPH, row["id"]) == {"deleted": True}
    assert store.count(GRAPH) == 0
    assert store.get(GRAPH, row["id"]) is None

  def test_update_missing_id_does_not_resurrect(self, store):
    store.add_rows(GRAPH, [_row("keep", _vec(1.0))])
    ghost = _mem_id()
    res = store.update(GRAPH, ghost, _row("resurrected", _vec(0.5)))
    assert res["updated"] is False
    assert store.get(GRAPH, ghost) is None
    assert store.count(GRAPH) == 1  # no insert of the ghost row

  def test_list_filtered_total_reflects_filter(self, store):
    store.add_rows(
      GRAPH,
      [
        _row("a", _vec(1.0), memory_type="fact"),
        _row("b", _vec(2.0), memory_type="note"),
        _row("c", _vec(3.0), memory_type="note"),
      ],
    )
    out = store.list(GRAPH, where="memory_type = 'note'")
    assert out["total"] == 2
    assert len(out["results"]) == 2
    assert all(r["memory_type"] == "note" for r in out["results"])


class TestPredicateAndIdGuards:
  def test_get_rejects_injection_id(self, store):
    store.add_rows(GRAPH, [_row("x", _vec(1.0))])
    with pytest.raises(ValueError):
      store.get(GRAPH, "mem_' OR '1'='1")

  def test_delete_rejects_injection_id(self, store):
    store.add_rows(GRAPH, [_row("x", _vec(1.0))])
    with pytest.raises(ValueError):
      store.delete(GRAPH, "'; DROP TABLE memory; --")

  def test_path_traversal_graph_id_rejected(self, store):
    with pytest.raises(HTTPException):
      store.add_rows("../../etc", [_row("x", _vec(1.0))])
