"""Tests for SearchService."""

from unittest.mock import MagicMock

import pytest

from robosystems.models.api.search import SearchRequest
from robosystems.operations.search.service import SearchService


@pytest.fixture
def mock_client():
  return MagicMock()


@pytest.fixture
def service(mock_client):
  return SearchService(mock_client)


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
