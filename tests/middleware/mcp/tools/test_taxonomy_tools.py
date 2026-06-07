"""Tests for taxonomy mapping MCP read tools.

The write tool (`create-mapping-association`) is registrar-generated
from `OperationSpec` in `routers/extensions/roboledger/operations.py`
and covered by `tests/middleware/mcp/test_registrar.py` plus the
ops-layer tests under `tests/operations/roboledger/taxonomies/`.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.taxonomy_tools import (
  GetMappingSummaryTool,
  GetUnmappedElementsTool,
  SuggestMappingTool,
)
from robosystems.models.api.extensions.taxonomies import (
  ElementResponse,
  MappingCoverageResponse,
  UnmappedElementResponse,
)

MODULE = "robosystems.middleware.mcp.tools.taxonomy_tools"


@pytest.fixture
def mock_graph_client():
  client = MagicMock()
  client.graph_id = "kg01234567890abcdef"
  return client


@contextmanager
def _patch_session():
  """Patch the extensions_session context manager imported into the tool module."""
  session = MagicMock()
  cm = MagicMock()
  cm.__enter__ = MagicMock(return_value=session)
  cm.__exit__ = MagicMock(return_value=False)
  with patch(f"{MODULE}.extensions_session", return_value=cm):
    yield session


def _unmapped(id="elem_1", code="1000", name="Cash", trait="asset"):
  return UnmappedElementResponse(
    id=id,
    code=code,
    name=name,
    trait=trait,
    balance_type="debit",
    external_source="quickbooks",
  )


def _element(
  id="elem_1",
  code="1000",
  name="Cash",
  trait="asset",
  qname=None,
  source="quickbooks",
  is_abstract=False,
  depth=0,
):
  return ElementResponse(
    id=id,
    code=code,
    name=name,
    description=None,
    qname=qname,
    namespace=None,
    trait=trait,
    sub_classification=None,
    balance_type="debit",
    period_type="instant",
    is_abstract=is_abstract,
    element_type="monetary",
    source=source,
    taxonomy_id=None,
    parent_id=None,
    depth=depth,
    is_active=True,
    external_id=None,
    external_source=source,
  )


# ── GetUnmappedElementsTool ──────────────────────────────────────────────


class TestGetUnmappedElementsTool:
  def test_tool_definition(self, mock_graph_client):
    tool = GetUnmappedElementsTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "get-unmapped-elements"
    assert "inputSchema" in defn

  @pytest.mark.asyncio
  async def test_returns_unmapped_elements(self, mock_graph_client):
    tool = GetUnmappedElementsTool(mock_graph_client)

    unmapped = [
      _unmapped(id="elem_2", name="Revenue"),
      _unmapped(id="elem_3", name="Rent"),
    ]

    with (
      _patch_session(),
      patch(f"{MODULE}.list_unmapped_elements", return_value=unmapped),
      patch(f"{MODULE}.count_coa_elements", return_value=3),
    ):
      result = await tool.execute({})

    assert result["unmapped_count"] == 2
    assert result["total_coa_elements"] == 3
    assert len(result["elements"]) == 2
    assert result["elements"][0]["name"] == "Revenue"

  @pytest.mark.asyncio
  async def test_all_mapped_returns_empty(self, mock_graph_client):
    tool = GetUnmappedElementsTool(mock_graph_client)

    with (
      _patch_session(),
      patch(f"{MODULE}.list_unmapped_elements", return_value=[]),
      patch(f"{MODULE}.count_coa_elements", return_value=1),
    ):
      result = await tool.execute({})

    assert result["unmapped_count"] == 0
    assert result["elements"] == []

  @pytest.mark.asyncio
  async def test_handles_error(self, mock_graph_client):
    tool = GetUnmappedElementsTool(mock_graph_client)

    with patch(
      f"{MODULE}.extensions_session",
      side_effect=ValueError("bad graph"),
    ):
      result = await tool.execute({})

    assert "error" in result


# ── SuggestMappingTool ───────────────────────────────────────────────────


class TestSuggestMappingTool:
  def test_tool_definition(self, mock_graph_client):
    tool = SuggestMappingTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "suggest-mapping"
    assert "element_id" in defn["inputSchema"]["properties"]

  @pytest.mark.asyncio
  async def test_returns_candidates_by_classification(self, mock_graph_client):
    tool = SuggestMappingTool(mock_graph_client)
    source = _element(
      id="elem_1", name="Product Revenue", trait="revenue", source="quickbooks"
    )
    # suggest-mapping returns rs-gaap candidates only
    # (the Reporting Style picker targets rs-gaap Networks).
    candidates = [
      _element(
        id="gaap_1",
        qname="rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
        name="Revenue From Contract With Customer (Excluding Assessed Tax)",
        source="rs-gaap",
        trait="revenue",
        depth=2,
      ),
      _element(
        id="gaap_2",
        qname="rs-gaap:SalesRevenueNet",
        name="Sales Revenue, Net",
        source="rs-gaap",
        trait="revenue",
        depth=2,
      ),
    ]

    with (
      _patch_session(),
      patch(f"{MODULE}.get_element", return_value=source),
      patch(f"{MODULE}.suggest_mapping_candidates", return_value=candidates),
    ):
      result = await tool.execute({"element_id": "elem_1"})

    assert result["source_element"]["name"] == "Product Revenue"
    assert len(result["candidates"]) == 2
    assert result["candidates"][0]["qname"].startswith("rs-gaap:")
    assert all(c["source"] == "rs-gaap" for c in result["candidates"])

  @pytest.mark.asyncio
  async def test_returns_error_for_missing_element(self, mock_graph_client):
    tool = SuggestMappingTool(mock_graph_client)
    with _patch_session(), patch(f"{MODULE}.get_element", return_value=None):
      result = await tool.execute({"element_id": "nonexistent"})

    assert "error" in result


# ── GetMappingSummaryTool ────────────────────────────────────────────────


class TestGetMappingSummaryTool:
  def test_tool_definition(self, mock_graph_client):
    tool = GetMappingSummaryTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "get-mapping-summary"
    assert "mapping_id" in defn["inputSchema"]["properties"]

  @pytest.mark.asyncio
  async def test_returns_coverage_stats(self, mock_graph_client):
    tool = GetMappingSummaryTool(mock_graph_client)
    coverage = MappingCoverageResponse(
      mapping_id="struct_map_1",
      total_coa_elements=5,
      mapped_count=3,
      unmapped_count=2,
      coverage_percent=60.0,
      high_confidence=1,
      medium_confidence=1,
      low_confidence=1,
    )

    with (
      _patch_session(),
      patch(f"{MODULE}.get_mapping_coverage", return_value=coverage),
    ):
      result = await tool.execute({"mapping_id": "struct_map_1"})

    assert result["total_coa_elements"] == 5
    assert result["mapped_count"] == 3
    assert result["unmapped_count"] == 2
    assert result["coverage_percent"] == 60.0
    assert result["confidence_distribution"]["high"] == 1
    assert result["confidence_distribution"]["medium"] == 1
    assert result["confidence_distribution"]["low"] == 1
    # 3 mapped - (1 high + 1 med + 1 low) = 0 manual
    assert result["confidence_distribution"]["manual"] == 0

  @pytest.mark.asyncio
  async def test_handles_zero_coa_elements(self, mock_graph_client):
    tool = GetMappingSummaryTool(mock_graph_client)
    coverage = MappingCoverageResponse(
      mapping_id="struct_map_1",
      total_coa_elements=0,
      mapped_count=0,
      unmapped_count=0,
      coverage_percent=0.0,
      high_confidence=0,
      medium_confidence=0,
      low_confidence=0,
    )

    with (
      _patch_session(),
      patch(f"{MODULE}.get_mapping_coverage", return_value=coverage),
    ):
      result = await tool.execute({"mapping_id": "struct_map_1"})

    assert result["coverage_percent"] == 0.0
    assert result["mapped_count"] == 0
