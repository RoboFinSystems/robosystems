"""Tests for SEC text search indexing helpers.

Covers:
- _get_indexed_accessions() composite aggregation pagination
- _part_document_ids() for unsplit sections and for parts
"""

import hashlib
from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest

from robosystems.adapters.sec.pipeline.text_index import (
  _get_indexed_accessions,
  _part_document_ids,
)


@pytest.mark.unit
class TestGetIndexedAccessions:
  """Tests for _get_indexed_accessions composite aggregation."""

  def test_returns_accessions_from_single_page(self):
    """Single page of results (< 10000 buckets)."""
    os_client = MagicMock()
    os_client.client.search.return_value = {
      "aggregations": {
        "accessions": {
          "buckets": [
            {"key": {"accession": "0001045810-25-000014"}, "doc_count": 3},
            {"key": {"accession": "0001045810-25-000015"}, "doc_count": 1},
          ]
        }
      }
    }

    result = _get_indexed_accessions(os_client, "sec")

    assert result == {"0001045810-25-000014", "0001045810-25-000015"}
    # Verify graph_id filter was sent
    call_body = os_client.client.search.call_args.kwargs["body"]
    filters = call_body["query"]["bool"]["filter"]
    assert {"term": {"graph_id": "sec"}} in filters

  def test_paginates_multiple_pages(self):
    """Composite agg paginates when buckets == page size."""
    os_client = MagicMock()

    # First page: exactly 10000 buckets (triggers pagination)
    page1_buckets = [
      {"key": {"accession": f"acc-{i:05d}"}, "doc_count": 1} for i in range(10000)
    ]
    # Second page: fewer than 10000 (terminates)
    page2_buckets = [
      {"key": {"accession": "acc-10000"}, "doc_count": 1},
      {"key": {"accession": "acc-10001"}, "doc_count": 1},
    ]

    os_client.client.search.side_effect = [
      {"aggregations": {"accessions": {"buckets": page1_buckets}}},
      {"aggregations": {"accessions": {"buckets": page2_buckets}}},
    ]

    result = _get_indexed_accessions(os_client, "sec")

    assert len(result) == 10002
    assert "acc-00000" in result
    assert "acc-09999" in result
    assert "acc-10001" in result
    # Second call should include "after" cursor
    second_call_body = os_client.client.search.call_args_list[1].kwargs["body"]
    assert "after" in second_call_body["aggs"]["accessions"]["composite"]

  def test_returns_empty_set_on_empty_index(self):
    """Empty index returns empty set."""
    os_client = MagicMock()
    os_client.client.search.return_value = {
      "aggregations": {"accessions": {"buckets": []}}
    }

    result = _get_indexed_accessions(os_client, "sec")

    assert result == set()

  def test_returns_empty_set_on_exception(self):
    """OpenSearch errors return empty set (graceful degradation)."""
    os_client = MagicMock()
    os_client.client.search.side_effect = Exception("connection refused")

    result = _get_indexed_accessions(os_client, "sec")

    assert result == set()

  def test_returns_empty_set_on_index_not_found(self):
    """Index not found (first run) returns empty set."""
    from opensearchpy import NotFoundError

    os_client = MagicMock()
    os_client.client.search.side_effect = NotFoundError(
      404, "index_not_found_exception"
    )

    result = _get_indexed_accessions(os_client, "sec")

    assert result == set()

  def test_filters_by_graph_id(self):
    """Verify graph_id is passed as mandatory filter."""
    os_client = MagicMock()
    os_client.client.search.return_value = {
      "aggregations": {"accessions": {"buckets": []}}
    }

    _get_indexed_accessions(os_client, "custom_graph")

    call_body = os_client.client.search.call_args.kwargs["body"]
    filters = call_body["query"]["bool"]["filter"]
    assert {"term": {"graph_id": "custom_graph"}} in filters

  def test_filters_by_source_type(self):
    """Verify source_type filter is added when specified."""
    os_client = MagicMock()
    os_client.client.search.return_value = {
      "aggregations": {"accessions": {"buckets": []}}
    }

    _get_indexed_accessions(os_client, "sec", source_type="narrative_section")

    call_body = os_client.client.search.call_args.kwargs["body"]
    filters = call_body["query"]["bool"]["filter"]
    assert {"term": {"graph_id": "sec"}} in filters
    assert {"term": {"source_type": "narrative_section"}} in filters

  def test_no_source_type_filter_when_none(self):
    """Verify no source_type filter when not specified."""
    os_client = MagicMock()
    os_client.client.search.return_value = {
      "aggregations": {"accessions": {"buckets": []}}
    }

    _get_indexed_accessions(os_client, "sec")

    call_body = os_client.client.search.call_args.kwargs["body"]
    filters = call_body["query"]["bool"]["filter"]
    assert len(filters) == 1  # Only graph_id


@dataclass
class _Section:
  section_id: str
  part: int = 1
  part_count: int = 1


def _legacy_id(*keys: str) -> str:
  return hashlib.sha256(":".join(keys).encode()).hexdigest()[:16]


@pytest.mark.unit
class TestPartDocumentIds:
  """Document ids for section parts."""

  def test_unsplit_section_keeps_the_id_it_always_had(self):
    doc_id, parent, next_id = _part_document_ids(
      "sec", "narr", "0000066740-25-000006", _Section("item_7")
    )
    assert doc_id == _legacy_id("sec", "narr", "0000066740-25-000006", "item_7")
    assert parent is None and next_id is None

  def test_parts_share_the_unsplit_id_as_parent_and_chain_by_next(self):
    parts = [_Section("item_7", part=i, part_count=3) for i in (1, 2, 3)]
    ids = [_part_document_ids("sec", "narr", "acc", s) for s in parts]

    unsplit = _legacy_id("sec", "narr", "acc", "item_7")
    assert {parent for _, parent, _ in ids} == {unsplit}
    assert len({doc_id for doc_id, _, _ in ids}) == 3
    assert unsplit not in {doc_id for doc_id, _, _ in ids}
    assert ids[0][2] == ids[1][0]
    assert ids[1][2] == ids[2][0]
    assert ids[2][2] is None

  def test_sources_and_sections_do_not_collide(self):
    narr = _part_document_ids("sec", "narr", "acc", _Section("item_7"))[0]
    ixbrl = _part_document_ids("sec", "ixbrl", "acc", _Section("item_7"))[0]
    other = _part_document_ids("sec", "narr", "acc", _Section("item_7a"))[0]
    assert len({narr, ixbrl, other}) == 3


@pytest.mark.unit
class TestCell:
  """The entity lookup's cells: a missing ticker must fall through to the CIK,
  not index as the string "<NA>" (U.S. Steel, and every ticker-less filer)."""

  def test_missing_values_are_empty(self):
    import numpy as np
    import pandas as pd

    from robosystems.adapters.sec.pipeline.text_index import _cell

    assert _cell(None) == ""
    assert _cell(pd.NA) == ""
    assert _cell(np.nan) == ""
    assert _cell(float("nan")) == ""

  def test_values_are_text(self):
    from robosystems.adapters.sec.pipeline.text_index import _cell

    assert _cell("MMM") == "MMM"
    assert _cell(66740) == "66740"
