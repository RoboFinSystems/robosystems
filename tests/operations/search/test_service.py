"""Tests for SearchService."""

from unittest.mock import MagicMock

import pytest

from robosystems.models.api.search import SearchRequest
from robosystems.operations.search.service import SearchService


@pytest.fixture
def mock_client():
  return MagicMock()


@pytest.fixture
def mock_embedding_service():
  svc = MagicMock()
  svc.embed_single.return_value = [0.1] * 384
  svc.embed_batch.return_value = [[0.1] * 384]
  return svc


@pytest.fixture
def service(mock_client, mock_embedding_service):
  svc = SearchService(mock_client)
  svc._embedding_service = mock_embedding_service
  return svc


class TestSearchDocuments:
  def test_returns_search_response(self, service, mock_client):
    mock_client.search.return_value = {
      "hits": {
        "total": {"value": 1},
        "hits": [
          {
            "_id": "doc1",
            "_score": 0.95,
            "_source": {
              "source_type": "narrative_section",
              "entity_ticker": "NVDA",
              "section_label": "Risk Factors",
              "content_length": 5000,
            },
            "highlight": {"content": ["...tariff risk exposure..."]},
          }
        ],
      }
    }

    request = SearchRequest(query="tariff risk")
    response = service.search_documents("sec", request)

    assert response.total == 1
    assert response.graph_id == "sec"
    assert response.query == "tariff risk"
    assert len(response.hits) == 1
    assert response.hits[0].document_id == "doc1"
    assert response.hits[0].snippet == "...tariff risk exposure..."
    assert response.hits[0].entity_ticker == "NVDA"

  def test_default_uses_bm25(self, service, mock_client, mock_embedding_service):
    """Default search uses BM25-only (no embedding)."""
    mock_client.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

    request = SearchRequest(query="supply chain risk")
    service.search_documents("sec", request)

    mock_embedding_service.embed_single.assert_not_called()
    mock_client.search.assert_called_once()
    mock_client.search_hybrid.assert_not_called()

  def test_semantic_flag_uses_hybrid(
    self, service, mock_client, mock_embedding_service
  ):
    """semantic=True uses hybrid search with embedding."""
    mock_client.search_hybrid.return_value = {
      "hits": {"total": {"value": 0}, "hits": []}
    }

    request = SearchRequest(query="supply chain risk", semantic=True)
    service.search_documents("sec", request)

    mock_embedding_service.embed_single.assert_called_once_with("supply chain risk")
    mock_client.search_hybrid.assert_called_once()
    call_kwargs = mock_client.search_hybrid.call_args.kwargs
    assert call_kwargs["query_embedding"] == [0.1] * 384

  def test_empty_results(self, service, mock_client):
    mock_client.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

    request = SearchRequest(query="nonexistent topic")
    response = service.search_documents("sec", request)

    assert response.total == 0
    assert len(response.hits) == 0

  def test_snippet_fallback_when_no_highlight(self, service, mock_client):
    mock_client.search.return_value = {
      "hits": {
        "total": {"value": 1},
        "hits": [
          {
            "_id": "doc1",
            "_score": 0.5,
            "_source": {
              "source_type": "xbrl_textblock",
              "section_label": "Business Description",
            },
          }
        ],
      }
    }

    request = SearchRequest(query="test")
    response = service.search_documents("sec", request)

    # Fallback uses section_label
    assert response.hits[0].snippet == "Business Description"

  def test_user_doc_includes_parent_document_id(self, service, mock_client):
    """User docs (udoc_ prefix) should have parent_document_id extracted."""
    mock_client.search.return_value = {
      "hits": {
        "total": {"value": 1},
        "hits": [
          {
            "_id": "udoc_doc_01ABC123_3",
            "_score": 0.8,
            "_source": {
              "source_type": "uploaded_doc",
              "document_title": "Depreciation Policy",
              "section_label": "Method",
              "folder": "policies",
              "tags": ["depreciation"],
            },
            "highlight": {"content": ["...straight-line depreciation..."]},
          }
        ],
      }
    }

    request = SearchRequest(query="depreciation")
    response = service.search_documents("kg_test", request)

    assert response.hits[0].parent_document_id == "doc_01ABC123"
    assert response.hits[0].document_id == "udoc_doc_01ABC123_3"

  def test_sec_doc_has_no_parent_document_id(self, service, mock_client):
    """SEC docs (doc_ prefix) should have parent_document_id=None."""
    mock_client.search.return_value = {
      "hits": {
        "total": {"value": 1},
        "hits": [
          {
            "_id": "doc_sec_nvda_10k_2024_5",
            "_score": 0.9,
            "_source": {
              "source_type": "narrative_section",
              "entity_ticker": "NVDA",
              "section_label": "Risk Factors",
            },
            "highlight": {"content": ["...risk..."]},
          }
        ],
      }
    }

    request = SearchRequest(query="risk")
    response = service.search_documents("sec", request)

    assert response.hits[0].parent_document_id is None

  def test_passes_filters(self, service, mock_client):
    mock_client.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

    request = SearchRequest(
      query="test",
      entity="AAPL",
      form_type="10-K",
      fiscal_year=2024,
    )
    service.search_documents("sec", request)

    call_kwargs = mock_client.search.call_args.kwargs
    assert call_kwargs["filters"]["entity"] == "AAPL"
    assert call_kwargs["filters"]["form_type"] == "10-K"
    assert call_kwargs["filters"]["fiscal_year"] == 2024


class TestGetDocumentSection:
  def test_returns_section_when_found(self, service, mock_client):
    mock_client.get_document.return_value = {
      "graph_id": "sec",
      "source_type": "narrative_section",
      "entity_ticker": "NVDA",
      "section_label": "MD&A",
      "content": "Full text of management discussion...",
      "content_length": 3500,
    }

    result = service.get_document_section("sec", "doc1")

    assert result is not None
    assert result.entity_ticker == "NVDA"
    assert result.content == "Full text of management discussion..."

  def test_returns_none_when_not_found(self, service, mock_client):
    mock_client.get_document.return_value = None

    result = service.get_document_section("sec", "nonexistent")
    assert result is None


class TestSectionParts:
  """Parts of a long SEC section carry part, part_count, the shared parent
  id and the next part's id through both the hit and the section."""

  def test_hit_carries_part_fields(self, service, mock_client):
    mock_client.search.return_value = {
      "hits": {
        "total": {"value": 1},
        "hits": [
          {
            "_id": "3f1c2a9b8d7e6f50",
            "_score": 0.9,
            "_source": {
              "source_type": "narrative_section",
              "section_id": "item_7",
              "section_label": "MD&A (2/6)",
              "part": 2,
              "part_count": 6,
              "parent_document_id": "a1b2c3d4e5f60718",
              "next_document_id": "9e8d7c6b5a493827",
            },
            "highlight": {"content": ["...liquidity..."]},
          }
        ],
      }
    }

    hit = service.search_documents("sec", SearchRequest(query="liquidity")).hits[0]

    assert (hit.part, hit.part_count) == (2, 6)
    assert hit.parent_document_id == "a1b2c3d4e5f60718"
    assert hit.next_document_id == "9e8d7c6b5a493827"
    assert hit.section_label == "MD&A (2/6)"

  def test_unsplit_section_has_defaults(self, service, mock_client):
    mock_client.search.return_value = {
      "hits": {
        "total": {"value": 1},
        "hits": [
          {
            "_id": "doc1",
            "_score": 0.9,
            "_source": {"source_type": "ixbrl_disclosure", "section_label": "Goodwill"},
          }
        ],
      }
    }

    hit = service.search_documents("sec", SearchRequest(query="goodwill")).hits[0]

    assert (hit.part, hit.part_count) == (1, 1)
    assert hit.parent_document_id is None
    assert hit.next_document_id is None

  def test_section_carries_part_fields(self, service, mock_client):
    mock_client.get_document.return_value = {
      "graph_id": "sec",
      "source_type": "ixbrl_disclosure",
      "section_id": "us-gaap:CommitmentsAndContingenciesDisclosureTextBlock",
      "section_label": "Commitments And Contingencies Disclosure (3/6)",
      "part": 3,
      "part_count": 6,
      "parent_document_id": "a1b2c3d4e5f60718",
      "next_document_id": "9e8d7c6b5a493827",
      "content": "PFAS litigation...",
      "content_length": 24000,
    }

    section = service.get_document_section("sec", "3f1c2a9b8d7e6f50")

    assert section is not None
    assert (section.part, section.part_count) == (3, 6)
    assert section.parent_document_id == "a1b2c3d4e5f60718"
    assert section.next_document_id == "9e8d7c6b5a493827"
