"""Tests for the SEC dataset card: snapshot stats capture and rendering."""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from robosystems.adapters.sec.pipeline import dataset_card
from robosystems.adapters.sec.pipeline.dataset_card import (
  capture_snapshot_stats,
  render_dataset_card,
)

# The 2026-08-31 snapshot, as its card was written by hand.
STATS = {
  "filers": 8525,
  "filers_with_ticker": 6875,
  "filings": 76816,
  "forms": {
    "10-Q": 45241,
    "10-K": 16394,
    "DEF 14A": 9962,
    "20-F": 3006,
    "S-1": 1792,
    "40-F": 419,
    "S-3": 1,
    "": 1,
  },
  "facts": 84094356,
  "numeric_facts": 72966418,
  "nodes": 294505525,
  "node_counts": {},
  "node_types": 14,
  "relationship_types": 22,
  "coverage_start": "2024-01-02",
  "coverage_end": "2026-08-28",
}


def _snapshot(**overrides):
  snapshot = {
    "snapshot_at": datetime(2026, 8, 31, 2, 0, tzinfo=UTC),
    "compressed_size_bytes": 42_247_675_436,
    "original_size_bytes": 139_865_260_032,
    "engine_version": "0.18.1",
    "storage_version": 42,
    "stats": STATS,
  }
  snapshot.update(overrides)
  return snapshot


@pytest.mark.unit
class TestRenderDatasetCard:
  def test_renders_the_snapshot_figures(self):
    card = render_dataset_card(_snapshot())

    assert "**8,525 filers, 76,816 filings, 84.1 million facts**" in card
    assert "**294.5 million nodes**, in one **130.3 GiB**" in card
    assert "| **Snapshot** | 2026-08-31 (filings through 2026-08-28)" in card
    assert "filings dated 2024-01-02 → 2026-08-28" in card
    assert "8,525 entities (6,875 with a ticker)" in card
    assert "84.1 million XBRL facts (73.0 million numeric)" in card
    assert "14 node types · 22 relationship types · 294.5 million nodes" in card
    assert "LadybugDB **0.18.1** (storage format v42)" in card
    assert (
      "`sec.lbug.zst` 39.3 GiB (42,247,675,436 bytes) → "
      "`sec.lbug` 130.3 GiB (139,865,260,032 bytes)"
    ) in card
    assert "You need **~170 GiB free** while decompressing (39.3 + 130.3;" in card
    assert "this is 130 GiB on disk" in card
    assert card.count("pip install ladybug==0.18.1") == 2
    assert "Hugging Face, snapshot 2026-08-31." in card
    assert "{{" not in card

  def test_forms_line_lists_major_forms_and_sums_the_rest(self):
    card = render_dataset_card(_snapshot())
    assert (
      "| **Forms** | 10-Q 45,241 · 10-K 16,394 · DEF 14A 9,962 · 20-F 3,006 · "
      "S-1 1,792 · 40-F 419 (and 2 other filings) |"
    ) in card

  def test_forms_line_singular_and_none(self):
    one = {**STATS, "forms": {"10-K": 500, "S-3": 1}}
    assert "10-K 500 (and 1 other filing) |" in render_dataset_card(
      _snapshot(stats=one)
    )
    clean = {**STATS, "forms": {"10-K": 500, "10-Q": 300}}
    assert "| **Forms** | 10-K 500 · 10-Q 300 |" in render_dataset_card(
      _snapshot(stats=clean)
    )

  def test_unknown_storage_version(self):
    card = render_dataset_card(_snapshot(storage_version=None))
    assert "(storage format unknown)" in card

  def test_unfilled_placeholder_raises(self, tmp_path):
    template = tmp_path / "card.md"
    template.write_text("{{filers}} and {{not_a_value}}")
    with patch.object(dataset_card, "CARD_TEMPLATE_PATH", template):
      with pytest.raises(ValueError, match="not_a_value"):
        render_dataset_card(_snapshot())


def _client(responses):
  """A graph client whose query() answers by matching the Cypher text."""

  async def query(cypher, graph_id, timeout=None):
    assert graph_id == "sec"
    assert timeout == dataset_card.STATS_QUERY_TIMEOUT_SECONDS
    for fragment, rows in responses.items():
      if fragment in cypher:
        return {"data": rows}
    raise AssertionError(f"Unexpected query: {cypher}")

  client = AsyncMock()
  client.query.side_effect = query
  return client


@pytest.mark.unit
class TestCaptureSnapshotStats:
  def test_counts(self):
    client = _client(
      {
        "show_tables": [
          {"name": "Entity", "type": "NODE"},
          {"name": "Report", "type": "NODE"},
          {"name": "Fact", "type": "NODE"},
          {"name": "ENTITY_HAS_REPORT", "type": "REL"},
          {"name": "REPORT_HAS_FACT", "type": "REL"},
        ],
        "MATCH (n:Entity)": [{"n": 10}],
        "MATCH (n:Report)": [{"n": 25}],
        "MATCH (n:Fact)": [{"n": 1000}],
        "r.form AS form": [{"form": "10-K", "n": 20}, {"form": None, "n": 5}],
        "min(r.filing_date)": [{"first": "2024-01-02", "last": "2026-08-28"}],
        "e.ticker IS NOT NULL": [{"n": 8}],
        "f.numeric_value IS NOT NULL": [{"n": 900}],
      }
    )

    stats = asyncio.run(capture_snapshot_stats(client))

    assert stats == {
      "filers": 10,
      "filers_with_ticker": 8,
      "filings": 25,
      "forms": {"10-K": 20, "": 5},
      "facts": 1000,
      "numeric_facts": 900,
      "nodes": 1035,
      "node_counts": {"Entity": 10, "Fact": 1000, "Report": 25},
      "node_types": 3,
      "relationship_types": 2,
      "coverage_start": "2024-01-02",
      "coverage_end": "2026-08-28",
    }

  def test_rejects_an_unexpected_table_name(self):
    client = _client({"show_tables": [{"name": "Bad) DETACH", "type": "NODE"}]})
    with pytest.raises(ValueError, match="Unexpected table name"):
      asyncio.run(capture_snapshot_stats(client))
