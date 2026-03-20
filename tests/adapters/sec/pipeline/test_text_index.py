"""Tests for SEC text search indexing helpers.

Covers:
- _get_indexed_accessions() composite aggregation pagination
- _derive_section_label() element name cleanup
- _partition_year() partition key parsing
"""

from unittest.mock import MagicMock

import pytest

from robosystems.adapters.sec.pipeline.text_index import (
  _derive_section_label,
  _get_indexed_accessions,
  _partition_year,
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
    assert call_body["query"] == {"term": {"graph_id": "sec"}}

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
    assert call_body["query"] == {"term": {"graph_id": "custom_graph"}}


@pytest.mark.unit
class TestDeriveSectionLabel:
  """Tests for _derive_section_label element name cleanup."""

  def test_strips_text_block_suffix(self):
    assert _derive_section_label("Risk Factors [Text Block]") == "Risk Factors"

  def test_strips_textblock_suffix(self):
    assert (
      _derive_section_label("ManagementDiscussionAndAnalysisTextBlock")
      == "Management Discussion And Analysis"
    )

  def test_splits_camel_case(self):
    assert (
      _derive_section_label("BusinessDescriptionAndBasisOfPresentation")
      == "Business Description And Basis Of Presentation"
    )

  def test_preserves_already_clean_label(self):
    assert _derive_section_label("Revenue") == "Revenue"

  def test_handles_empty_string(self):
    assert _derive_section_label("") == ""


@pytest.mark.unit
class TestPartitionYear:
  """Tests for _partition_year key parsing."""

  def test_extracts_year_from_filed_partition(self):
    assert (
      _partition_year("sec/processed/filed=2025-Q1/nodes/Entity/part.parquet") == 2025
    )

  def test_extracts_year_from_year_partition(self):
    assert _partition_year("sec/year=2024/0001045810/filing.zip") == 2024

  def test_returns_zero_for_no_partition(self):
    assert _partition_year("sec/some/random/path.parquet") == 0

  def test_handles_filed_partition_different_quarters(self):
    assert (
      _partition_year("sec/processed/filed=2024-Q4/nodes/Fact/part.parquet") == 2024
    )
