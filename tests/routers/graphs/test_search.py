"""Tests for the search router.

This test suite covers:
- search_documents endpoint (POST /search)
- get_document_section endpoint (GET /search/{document_id})
- Service unavailability (503)
- Document not found (404)
"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.api.search import (
  DocumentSection,
  SearchRequest,
  SearchResponse,
)
from robosystems.routers.graphs.search import (
  _require_search_service,
  get_document_section,
  search_documents,
)

MODULE = "robosystems.routers.graphs.search"


def _make_search_response(**overrides):
  """Helper to build a SearchResponse."""
  defaults = {
    "total": 1,
    "hits": [
      {
        "document_id": "abc123",
        "score": 0.95,
        "source_type": "narrative_section",
        "entity_ticker": "NVDA",
        "entity_name": "NVIDIA Corporation",
        "section_label": "Risk Factors",
        "filing_date": "2025-01-26",
        "fiscal_year": 2025,
        "form_type": "10-K",
        "snippet": "...artificial intelligence risks...",
        "content_length": 5000,
      }
    ],
    "query": "artificial intelligence",
    "graph_id": "sec",
  }
  defaults.update(overrides)
  return SearchResponse(**defaults)


def _make_document_section(**overrides):
  """Helper to build a DocumentSection."""
  defaults = {
    "document_id": "abc123",
    "graph_id": "sec",
    "source_type": "narrative_section",
    "entity_ticker": "NVDA",
    "entity_name": "NVIDIA Corporation",
    "section_id": "item_1a",
    "section_label": "Risk Factors",
    "content": "Our business faces significant risks...",
    "content_length": 5000,
    "filing_date": "2025-01-26",
    "fiscal_year": 2025,
    "form_type": "10-K",
    "accession_number": "0001045810-25-000014",
  }
  defaults.update(overrides)
  return DocumentSection(**defaults)


class TestRequireSearchService:
  """Tests for _require_search_service helper."""

  @pytest.mark.unit
  def test_returns_service_when_available(self):
    mock_service = MagicMock()
    with patch(f"{MODULE}.get_search_service", return_value=mock_service):
      result = _require_search_service()
      assert result is mock_service

  @pytest.mark.unit
  def test_raises_503_when_unavailable(self):
    with patch(f"{MODULE}.get_search_service", return_value=None):
      with pytest.raises(HTTPException) as exc_info:
        _require_search_service()
      assert exc_info.value.status_code == 503
      assert "not available" in exc_info.value.detail


@pytest.mark.asyncio
class TestSearchDocuments:
  """Tests for search_documents endpoint."""

  @pytest.mark.unit
  async def test_returns_search_response(self):
    mock_service = MagicMock()
    expected = _make_search_response()
    mock_service.search_documents.return_value = expected

    with patch(f"{MODULE}.get_search_service", return_value=mock_service):
      request = SearchRequest(query="artificial intelligence")
      result = await search_documents(
        graph_id="sec",
        request=request,
        req=MagicMock(),
        current_user=MagicMock(),
      )

    assert result.total == 1
    assert result.graph_id == "sec"
    assert result.hits[0].document_id == "abc123"
    mock_service.search_documents.assert_called_once_with("sec", request)

  @pytest.mark.unit
  async def test_passes_graph_id_to_service(self):
    mock_service = MagicMock()
    mock_service.search_documents.return_value = _make_search_response(
      graph_id="custom_graph"
    )

    with patch(f"{MODULE}.get_search_service", return_value=mock_service):
      request = SearchRequest(query="revenue")
      await search_documents(
        graph_id="custom_graph",
        request=request,
        req=MagicMock(),
        current_user=MagicMock(),
      )

    call_args = mock_service.search_documents.call_args
    assert call_args.args[0] == "custom_graph"

  @pytest.mark.unit
  async def test_passes_filters_through(self):
    mock_service = MagicMock()
    mock_service.search_documents.return_value = _make_search_response()

    with patch(f"{MODULE}.get_search_service", return_value=mock_service):
      request = SearchRequest(
        query="risk",
        entity="AAPL",
        form_type="10-K",
        fiscal_year=2025,
        size=5,
      )
      await search_documents(
        graph_id="sec",
        request=request,
        req=MagicMock(),
        current_user=MagicMock(),
      )

    passed_request = mock_service.search_documents.call_args.args[1]
    assert passed_request.entity == "AAPL"
    assert passed_request.form_type == "10-K"
    assert passed_request.fiscal_year == 2025
    assert passed_request.size == 5

  @pytest.mark.unit
  async def test_raises_503_when_service_unavailable(self):
    with patch(f"{MODULE}.get_search_service", return_value=None):
      with pytest.raises(HTTPException) as exc_info:
        await search_documents(
          graph_id="sec",
          request=SearchRequest(query="test"),
          req=MagicMock(),
          current_user=MagicMock(),
        )
      assert exc_info.value.status_code == 503


@pytest.mark.asyncio
class TestGetDocumentSection:
  """Tests for get_document_section endpoint."""

  @pytest.mark.unit
  async def test_returns_document_section(self):
    mock_service = MagicMock()
    expected = _make_document_section()
    mock_service.get_document_section.return_value = expected

    with patch(f"{MODULE}.get_search_service", return_value=mock_service):
      result = await get_document_section(
        graph_id="sec",
        document_id="abc123",
        req=MagicMock(),
        current_user=MagicMock(),
      )

    assert result.document_id == "abc123"
    assert result.section_label == "Risk Factors"
    assert "significant risks" in result.content
    mock_service.get_document_section.assert_called_once_with("sec", "abc123")

  @pytest.mark.unit
  async def test_raises_404_when_not_found(self):
    mock_service = MagicMock()
    mock_service.get_document_section.return_value = None

    with patch(f"{MODULE}.get_search_service", return_value=mock_service):
      with pytest.raises(HTTPException) as exc_info:
        await get_document_section(
          graph_id="sec",
          document_id="nonexistent",
          req=MagicMock(),
          current_user=MagicMock(),
        )
      assert exc_info.value.status_code == 404
      assert "not found" in exc_info.value.detail

  @pytest.mark.unit
  async def test_passes_graph_id_to_service(self):
    mock_service = MagicMock()
    mock_service.get_document_section.return_value = _make_document_section(
      graph_id="custom_graph"
    )

    with patch(f"{MODULE}.get_search_service", return_value=mock_service):
      await get_document_section(
        graph_id="custom_graph",
        document_id="abc123",
        req=MagicMock(),
        current_user=MagicMock(),
      )

    call_args = mock_service.get_document_section.call_args
    assert call_args.args[0] == "custom_graph"
    assert call_args.args[1] == "abc123"

  @pytest.mark.unit
  async def test_raises_503_when_service_unavailable(self):
    with patch(f"{MODULE}.get_search_service", return_value=None):
      with pytest.raises(HTTPException) as exc_info:
        await get_document_section(
          graph_id="sec",
          document_id="abc123",
          req=MagicMock(),
          current_user=MagicMock(),
        )
      assert exc_info.value.status_code == 503
