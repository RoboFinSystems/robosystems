"""Tests for taxonomy mapping MCP tools."""

from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.taxonomy_tools import (
  CreateMappingAssociationTool,
  GetMappingSummaryTool,
  GetUnmappedElementsTool,
  SuggestMappingTool,
)

MODULE = "robosystems.middleware.mcp.tools.taxonomy_tools"


@pytest.fixture
def mock_graph_client():
  client = MagicMock()
  client.graph_id = "kg01234567890abcdef"
  return client


def _make_element(
  id="elem_1",
  code="1000",
  name="Cash",
  classification="asset",
  balance_type="debit",
  source="quickbooks",
  is_active=True,
  is_abstract=False,
  external_source="quickbooks",
  qname=None,
  namespace=None,
  depth=0,
):
  e = MagicMock()
  e.id = id
  e.code = code
  e.name = name
  e.classification = classification
  e.balance_type = balance_type
  e.source = source
  e.is_active = is_active
  e.is_abstract = is_abstract
  e.external_source = external_source
  e.qname = qname
  e.namespace = namespace
  e.depth = depth
  return e


def _make_association(
  id="assoc_1",
  from_element_id="elem_1",
  confidence=0.95,
):
  a = MagicMock()
  a.id = id
  a.from_element_id = from_element_id
  a.confidence = confidence
  return a


def _mock_session():
  return MagicMock()


def _patch_extensions(mock_session):
  mock_ctx = MagicMock()
  mock_ctx.__enter__ = MagicMock(return_value=mock_session)
  mock_ctx.__exit__ = MagicMock(return_value=False)
  return patch(
    "robosystems.db.extensions.extensions_session",
    return_value=mock_ctx,
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
    session = _mock_session()

    coa = [
      _make_element(id="elem_1", name="Cash"),
      _make_element(id="elem_2", name="Revenue"),
      _make_element(id="elem_3", name="Rent"),
    ]

    call_count = 0

    def mock_execute(query):
      nonlocal call_count
      call_count += 1
      result = MagicMock()
      if call_count == 1:
        result.scalars.return_value.all.return_value = coa
      else:
        result.scalars.return_value.all.return_value = ["elem_1"]
      return result

    session.execute.side_effect = mock_execute

    with _patch_extensions(session):
      result = await tool.execute({})

    assert result["unmapped_count"] == 2
    assert result["total_coa_elements"] == 3
    assert len(result["elements"]) == 2
    assert result["elements"][0]["name"] == "Revenue"

  @pytest.mark.asyncio
  async def test_all_mapped_returns_empty(self, mock_graph_client):
    tool = GetUnmappedElementsTool(mock_graph_client)
    session = _mock_session()

    coa = [_make_element(id="elem_1")]
    call_count = 0

    def mock_execute(query):
      nonlocal call_count
      call_count += 1
      result = MagicMock()
      if call_count == 1:
        result.scalars.return_value.all.return_value = coa
      else:
        result.scalars.return_value.all.return_value = ["elem_1"]
      return result

    session.execute.side_effect = mock_execute

    with _patch_extensions(session):
      result = await tool.execute({})

    assert result["unmapped_count"] == 0
    assert result["elements"] == []

  @pytest.mark.asyncio
  async def test_handles_error(self, mock_graph_client):
    tool = GetUnmappedElementsTool(mock_graph_client)

    with patch(
      "robosystems.db.extensions.extensions_session",
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
    session = _mock_session()

    source_elem = _make_element(
      id="elem_1", name="Product Revenue", classification="revenue"
    )
    candidates = [
      _make_element(
        id="gaap_1",
        qname="sfac6:Revenues",
        name="Revenues",
        source="sfac6",
        classification="revenue",
        is_abstract=True,
        depth=0,
      ),
      _make_element(
        id="gaap_2",
        qname="us-gaap:Revenues",
        name="Revenue",
        source="us-gaap",
        classification="revenue",
        depth=1,
      ),
    ]

    call_count = 0

    def mock_execute(query):
      nonlocal call_count
      call_count += 1
      result = MagicMock()
      if call_count == 1:
        result.scalar_one_or_none.return_value = source_elem
      else:
        result.scalars.return_value.all.return_value = candidates
      return result

    session.execute.side_effect = mock_execute

    with _patch_extensions(session):
      result = await tool.execute({"element_id": "elem_1"})

    assert result["source_element"]["name"] == "Product Revenue"
    assert len(result["candidates"]) == 2
    assert result["candidates"][0]["qname"] == "sfac6:Revenues"

  @pytest.mark.asyncio
  async def test_returns_error_for_missing_element(self, mock_graph_client):
    tool = SuggestMappingTool(mock_graph_client)
    session = _mock_session()
    session.execute.return_value.scalar_one_or_none.return_value = None

    with _patch_extensions(session):
      result = await tool.execute({"element_id": "nonexistent"})

    assert "error" in result


# ── CreateMappingAssociationTool ─────────────────────────────────────────


class TestCreateMappingAssociationTool:
  def test_tool_definition(self, mock_graph_client):
    tool = CreateMappingAssociationTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "create-mapping-association"
    assert "mapping_id" in defn["inputSchema"]["properties"]
    assert "from_element_id" in defn["inputSchema"]["properties"]
    assert "to_element_id" in defn["inputSchema"]["properties"]

  @pytest.mark.asyncio
  async def test_creates_association(self, mock_graph_client):
    tool = CreateMappingAssociationTool(mock_graph_client)
    session = _mock_session()

    from_elem = _make_element(id="elem_1", name="Cash", code="1000")
    to_elem = _make_element(id="gaap_1", name="Revenue", qname="us-gaap:Revenues")

    call_count = 0

    def mock_execute(query):
      nonlocal call_count
      call_count += 1
      result = MagicMock()
      if call_count == 1:
        result.scalar_one_or_none.return_value = from_elem
      else:
        result.scalar_one_or_none.return_value = to_elem
      return result

    session.execute.side_effect = mock_execute

    with _patch_extensions(session):
      result = await tool.execute(
        {
          "mapping_id": "struct_map_1",
          "from_element_id": "elem_1",
          "to_element_id": "gaap_1",
        }
      )

    assert result["created"] is True
    assert result["from_element"] == "Cash"
    assert result["to_element"] == "Revenue"
    session.add.assert_called_once()

  @pytest.mark.asyncio
  async def test_returns_error_for_missing_element(self, mock_graph_client):
    tool = CreateMappingAssociationTool(mock_graph_client)
    session = _mock_session()
    session.execute.return_value.scalar_one_or_none.return_value = None

    with _patch_extensions(session):
      result = await tool.execute(
        {
          "mapping_id": "struct_map_1",
          "from_element_id": "nonexistent",
          "to_element_id": "gaap_1",
        }
      )

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
    session = _mock_session()

    assocs = [
      _make_association(id="a1", from_element_id="elem_1", confidence=0.95),
      _make_association(id="a2", from_element_id="elem_2", confidence=0.80),
      _make_association(id="a3", from_element_id="elem_3", confidence=0.50),
    ]

    call_count = 0

    def mock_execute(query):
      nonlocal call_count
      call_count += 1
      result = MagicMock()
      if call_count == 1:
        result.scalar.return_value = 5
      else:
        result.scalars.return_value.all.return_value = assocs
      return result

    session.execute.side_effect = mock_execute

    with _patch_extensions(session):
      result = await tool.execute({"mapping_id": "struct_map_1"})

    assert result["total_coa_elements"] == 5
    assert result["mapped_count"] == 3
    assert result["unmapped_count"] == 2
    assert result["coverage_percent"] == 60.0
    assert result["confidence_distribution"]["high"] == 1
    assert result["confidence_distribution"]["medium"] == 1
    assert result["confidence_distribution"]["low"] == 1

  @pytest.mark.asyncio
  async def test_handles_zero_coa_elements(self, mock_graph_client):
    tool = GetMappingSummaryTool(mock_graph_client)
    session = _mock_session()

    call_count = 0

    def mock_execute(query):
      nonlocal call_count
      call_count += 1
      result = MagicMock()
      if call_count == 1:
        result.scalar.return_value = 0
      else:
        result.scalars.return_value.all.return_value = []
      return result

    session.execute.side_effect = mock_execute

    with _patch_extensions(session):
      result = await tool.execute({"mapping_id": "struct_map_1"})

    assert result["coverage_percent"] == 0.0
    assert result["mapped_count"] == 0
