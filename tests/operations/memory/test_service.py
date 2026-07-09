"""Tests for the MemoryService kernel (transport-independent).

Uses a fake GraphClient and a stub embedding service — no network, no fastembed
model load. Verifies embed→add wiring, distance→score mapping, predicate building
from allowlisted fields, provenance roundtrip, and update read-modify-write.
"""

import pytest

from robosystems.models.api.memory import (
  MemoryCreateRequest,
  MemoryRecallRequest,
  MemoryUpdateRequest,
)
from robosystems.operations.memory.service import (
  MemoryService,
  _build_predicate,
  get_memory_service,
)

GRAPH = "kgtest000000000001"
DIM = 384


class _StubEmbedder:
  def __init__(self):
    self.calls: list[str] = []

  def embed_single(self, text: str) -> list[float]:
    self.calls.append(text)
    # deterministic non-zero vector
    return [0.1] * DIM


class _FakeClient:
  """Records calls; returns canned rows."""

  def __init__(self):
    self.added: list[dict] = []
    self.updated: list[tuple] = []
    self.deleted: list[str] = []
    self.searched: list[dict] = []
    self.store: dict[str, dict] = {}

  async def memory_add(self, graph_id, records):
    for r in records:
      self.added.append(r)
      self.store[r["id"]] = r
    return {"added": len(records), "total": len(self.store)}

  async def memory_search(self, graph_id, embedding, limit=10, where=None, select=None):
    self.searched.append({"where": where, "limit": limit})
    # return two hits with distances
    return [
      {"id": "mem_" + "a" * 32, "text": "closest", "tags": ["x"], "distance": 0.0},
      {"id": "mem_" + "b" * 32, "text": "farther", "tags": None, "distance": 0.25},
    ]

  async def memory_get(self, graph_id, memory_id):
    return self.store.get(memory_id)

  async def memory_list(self, graph_id, where=None, limit=100, offset=0):
    rows = list(self.store.values())
    return {"results": rows, "total": len(rows)}

  async def memory_update(self, graph_id, memory_id, row):
    self.updated.append((memory_id, row))
    self.store[memory_id] = {**row, "id": memory_id}
    return {"id": memory_id, "updated": True}

  async def memory_delete(self, graph_id, memory_id):
    self.deleted.append(memory_id)
    existed = memory_id in self.store
    self.store.pop(memory_id, None)
    return {"deleted": existed}


@pytest.fixture()
def service():
  svc = MemoryService(_FakeClient())
  svc._embedding_service = _StubEmbedder()  # type: ignore[assignment]
  return svc


class TestRemember:
  async def test_remember_embeds_and_persists(self, service):
    req = MemoryCreateRequest(
      text="fiscal year ends March 31", provenance={"src": "op"}
    )
    rec = await service.remember(GRAPH, req, created_by="user_1")

    embedder = service._embedding_service
    assert embedder.calls == ["fiscal year ends March 31"]
    assert rec.id.startswith("mem_")
    assert rec.text == "fiscal year ends March 31"
    assert rec.created_by == "user_1"
    assert rec.provenance == {"src": "op"}  # dict roundtrips through JSON
    assert rec.created_at is not None

    added = service.client.added[0]
    assert len(added["vector"]) == DIM
    assert added["provenance"] == '{"src": "op"}'  # stored as JSON string


class TestRecall:
  async def test_recall_maps_distance_to_score(self, service):
    resp = await service.recall(GRAPH, MemoryRecallRequest(query="when?", k=5))
    assert resp.total == 2
    assert resp.hits[0].score == 1.0  # distance 0.0
    assert resp.hits[1].score == pytest.approx(0.75)  # distance 0.25
    assert resp.hits[0].source_type == "memory"
    assert resp.hits[0].snippet == "closest"

  async def test_recall_builds_allowlisted_predicate(self, service):
    await service.recall(
      GRAPH, MemoryRecallRequest(query="q", memory_type="fact", source="mcp")
    )
    where = service.client.searched[-1]["where"]
    assert where == "memory_type = 'fact' AND source = 'mcp'"


class TestGovernance:
  async def test_get_missing_returns_none(self, service):
    assert await service.get_memory(GRAPH, "mem_" + "c" * 32) is None

  async def test_list_maps_records(self, service):
    await service.remember(GRAPH, MemoryCreateRequest(text="a"))
    await service.remember(GRAPH, MemoryCreateRequest(text="b"))
    out = await service.list_memories(GRAPH)
    assert out.total == 2
    assert {m.text for m in out.memories} == {"a", "b"}

  async def test_forget_reports_deleted(self, service):
    rec = await service.remember(GRAPH, MemoryCreateRequest(text="bye"))
    resp = await service.forget(GRAPH, rec.id)
    assert resp.deleted is True
    assert resp.id == rec.id

  async def test_update_read_modify_write_reembeds(self, service):
    rec = await service.remember(GRAPH, MemoryCreateRequest(text="old"))
    service._embedding_service.calls.clear()
    updated = await service.update_memory(
      GRAPH, rec.id, MemoryUpdateRequest(text="new")
    )
    assert updated is not None
    assert updated.text == "new"
    # re-embedded the new text
    assert service._embedding_service.calls == ["new"]
    # preserved original created_by/created_at fields
    memory_id, row = service.client.updated[-1]
    assert memory_id == rec.id
    assert len(row["vector"]) == DIM

  async def test_update_missing_returns_none(self, service):
    out = await service.update_memory(
      GRAPH, "mem_" + "d" * 32, MemoryUpdateRequest(text="x")
    )
    assert out is None


class TestPredicateAndGate:
  def test_build_predicate_none_when_empty(self):
    assert _build_predicate() is None

  def test_build_predicate_escapes_quotes(self):
    where = _build_predicate(source="a'b")
    assert where == "source = 'a''b'"

  def test_gate_returns_none_when_disabled(self, monkeypatch):
    monkeypatch.setattr(
      "robosystems.operations.memory.service.env.SEMANTIC_MEMORY_ENABLED", False
    )
    assert get_memory_service(_FakeClient()) is None

  def test_gate_returns_service_when_enabled(self, monkeypatch):
    monkeypatch.setattr(
      "robosystems.operations.memory.service.env.SEMANTIC_MEMORY_ENABLED", True
    )
    assert isinstance(get_memory_service(_FakeClient()), MemoryService)
