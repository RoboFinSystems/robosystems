"""Artifact loaders must work in a process that has imported networkit.

icebug bundles a second libarrow beside pyarrow's; the two collide on Arrow's
global filesystem registry, so once networkit is loaded `pq.read_table(path)`
raises ArrowKeyError forever. The knowledge builders import networkit, so any
loader reading their output has to take a file handle. This test poisons the
registry the same way and asserts the loaders still return data — the previous
tests all mocked the read, so a bare path passed them and failed in production.
"""

from __future__ import annotations

from pathlib import Path

import networkit  # noqa: F401  — poisons the Arrow registry, exactly as prod does
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from robosystems.adapters.sec.enrichment import SemanticEnricher

pytestmark = pytest.mark.unit


def _write(path: Path, columns: dict) -> None:
  with open(path, "wb") as f:
    pq.write_table(pa.table(columns), f)


@pytest.fixture
def artifacts(tmp_path, monkeypatch):
  _write(
    tmp_path / "element_knowledge.parquet",
    {
      "qname": ["us-gaap:Assets"],
      "primary_statement": ["BalanceSheet"],
      "bfs_depth": [1],
      "pagerank": [0.9],
      "core_number": [2],
      "neighborhood_agreement": [0.5],
      "filing_count": [3],
      "disclosure_type": ["x"],
    },
  )
  _write(
    tmp_path / "structure_profiles.parquet",
    {
      "canonical_type": ["BalanceSheet"],
      "qname": ["us-gaap:Assets"],
      "frequency": [0.8],
      "structure_count": [4],
    },
  )
  monkeypatch.setattr(
    "robosystems.config.storage.shared.get_artifact_path",
    lambda name: str(tmp_path / f"{name}.parquet"),
  )
  return tmp_path


def _enricher() -> SemanticEnricher:
  # __new__, not __init__: construction loads an embedding model, and the read
  # path under test does not need one.
  return SemanticEnricher.__new__(SemanticEnricher)


def test_element_knowledge_loads(artifacts):
  result = _enricher()._load_element_knowledge()
  assert result is not None, "bare-path read — ArrowKeyError swallowed to None"
  assert result["us-gaap:Assets"]["pagerank"] == 0.9


def test_structure_profiles_load(artifacts):
  result = _enricher()._load_structure_profiles()
  assert result is not None, "bare-path read — ArrowKeyError swallowed to None"
  assert result["BalanceSheet"]["us-gaap:Assets"] == pytest.approx(0.8)


def test_consolidation_reads_parquet_from_disk(tmp_path):
  """The consolidation read attributes any failure to file corruption, so a
  registry collision there discards real rows under a misleading label."""
  from robosystems.adapters.sec.processors.consolidation import (
    consolidate_parquet_from_disk,
  )

  table_dir = tmp_path / "nodes/Entity"
  table_dir.mkdir(parents=True)
  _write(table_dir / "a.parquet", {"identifier": ["e1"], "name": ["Acme"]})

  out = consolidate_parquet_from_disk(tmp_path, "nodes/Entity")
  assert out is not None, "every file read as corrupt — rows silently dropped"
  from io import BytesIO

  assert pq.read_table(BytesIO(out)).num_rows == 1
