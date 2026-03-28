"""Unit tests for taxonomy, structure, mapping, and element endpoints."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.routers.ledger.taxonomies import (
  create_mapping_association,
  create_structure,
  create_taxonomy,
  delete_mapping_association,
  get_mapped_trial_balance,
  get_mapping_coverage,
  get_mapping_detail,
  get_reporting_taxonomy,
  list_elements,
  list_mappings,
  list_structures,
  list_taxonomies,
  list_unmapped_elements,
)

MODULE = "robosystems.routers.ledger.taxonomies"
GRAPH_ID = "kg01234567890abcdef"


def _make_user():
  user = MagicMock()
  user.id = "usr_test123"
  return user


def _make_taxonomy(
  id="tax_1",
  name="US GAAP Reporting",
  taxonomy_type="reporting",
  version="2024",
  standard="us-gaap",
  namespace_uri=None,
  is_shared=True,
  is_active=True,
  is_locked=True,
  description=None,
  source_taxonomy_id=None,
  target_taxonomy_id=None,
):
  t = MagicMock()
  t.id = id
  t.name = name
  t.description = description
  t.taxonomy_type = taxonomy_type
  t.version = version
  t.standard = standard
  t.namespace_uri = namespace_uri
  t.is_shared = is_shared
  t.is_active = is_active
  t.is_locked = is_locked
  t.source_taxonomy_id = source_taxonomy_id
  t.target_taxonomy_id = target_taxonomy_id
  return t


def _make_structure(
  id="struct_1",
  name="Income Statement",
  structure_type="income_statement",
  taxonomy_id="tax_1",
  is_active=True,
  description=None,
):
  s = MagicMock()
  s.id = id
  s.name = name
  s.description = description
  s.structure_type = structure_type
  s.taxonomy_id = taxonomy_id
  s.is_active = is_active
  return s


def _make_element(
  id="elem_1",
  code="1000",
  name="Cash",
  classification="asset",
  balance_type="debit",
  period_type="duration",
  is_abstract=False,
  element_type="concept",
  source="quickbooks",
  qname=None,
  namespace=None,
  sub_classification=None,
  taxonomy_id=None,
  parent_id=None,
  depth=0,
  is_active=True,
  external_id=None,
  external_source="quickbooks",
  description=None,
):
  e = MagicMock()
  e.id = id
  e.code = code
  e.name = name
  e.description = description
  e.classification = classification
  e.sub_classification = sub_classification
  e.balance_type = balance_type
  e.period_type = period_type
  e.is_abstract = is_abstract
  e.element_type = element_type
  e.source = source
  e.qname = qname
  e.namespace = namespace
  e.taxonomy_id = taxonomy_id
  e.parent_id = parent_id
  e.depth = depth
  e.is_active = is_active
  e.external_id = external_id
  e.external_source = external_source
  return e


def _make_association(
  id="assoc_1",
  structure_id="struct_map_1",
  from_element_id="elem_1",
  to_element_id="elem_gaap_revenues",
  association_type="mapping",
  order_value=1.0,
  weight=None,
  confidence=0.95,
  suggested_by="ai",
  approved_by=None,
):
  a = MagicMock()
  a.id = id
  a.structure_id = structure_id
  a.from_element_id = from_element_id
  a.to_element_id = to_element_id
  a.association_type = association_type
  a.order_value = order_value
  a.weight = weight
  a.confidence = confidence
  a.suggested_by = suggested_by
  a.approved_by = approved_by
  return a


def _mock_session():
  mock_session = MagicMock()
  return mock_session


def _patch_extensions(mock_session):
  mock_ctx = MagicMock()
  mock_ctx.__enter__ = MagicMock(return_value=mock_session)
  mock_ctx.__exit__ = MagicMock(return_value=False)
  return patch(f"{MODULE}.extensions_session", return_value=mock_ctx)


# ── Taxonomy Tests ────────────────────────────────────────────────────────


class TestListTaxonomies:
  @pytest.mark.asyncio
  async def test_returns_taxonomies(self):
    session = _mock_session()
    session.execute.return_value.scalars.return_value.all.return_value = [
      _make_taxonomy(),
      _make_taxonomy(
        id="tax_2",
        name="QB CoA",
        taxonomy_type="chart_of_accounts",
        standard=None,
        is_shared=False,
        is_locked=False,
      ),
    ]

    with _patch_extensions(session):
      result = await list_taxonomies(
        graph_id=GRAPH_ID, current_user=_make_user(), _rate_limit=None
      )

    assert len(result.taxonomies) == 2
    assert result.taxonomies[0].name == "US GAAP Reporting"
    assert result.taxonomies[1].taxonomy_type == "chart_of_accounts"

  @pytest.mark.asyncio
  async def test_filter_by_type(self):
    session = _mock_session()
    session.execute.return_value.scalars.return_value.all.return_value = [
      _make_taxonomy(taxonomy_type="reporting"),
    ]

    with _patch_extensions(session):
      result = await list_taxonomies(
        graph_id=GRAPH_ID,
        taxonomy_type="reporting",
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.taxonomies) == 1

  @pytest.mark.asyncio
  async def test_returns_404_on_missing_schema(self):
    with patch(f"{MODULE}.extensions_session", side_effect=ValueError("bad")):
      with pytest.raises(HTTPException) as exc_info:
        await list_taxonomies(
          graph_id=GRAPH_ID, current_user=_make_user(), _rate_limit=None
        )
      assert exc_info.value.status_code == 404


class TestCreateTaxonomy:
  @pytest.mark.asyncio
  async def test_creates_taxonomy(self):
    session = _mock_session()
    mock_tax = _make_taxonomy(
      name="QB → GAAP Mapping",
      taxonomy_type="mapping",
      standard=None,
      is_shared=False,
      is_locked=False,
    )

    with (
      _patch_extensions(session),
      patch(f"{MODULE}.Taxonomy", return_value=mock_tax),
    ):
      from robosystems.models.api.extensions.taxonomies import (
        CreateTaxonomyRequest,
      )

      body = CreateTaxonomyRequest(
        name="QB → GAAP Mapping",
        taxonomy_type="mapping",
      )
      result = await create_taxonomy(
        body=body,
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.name == "QB → GAAP Mapping"
    assert result.taxonomy_type == "mapping"
    session.add.assert_called_once()
    session.flush.assert_called_once()


class TestGetReportingTaxonomy:
  @pytest.mark.asyncio
  async def test_returns_reporting_taxonomy(self):
    session = _mock_session()
    session.execute.return_value.scalar_one_or_none.return_value = _make_taxonomy()

    with _patch_extensions(session):
      result = await get_reporting_taxonomy(
        graph_id=GRAPH_ID, current_user=_make_user(), _rate_limit=None
      )

    assert result.standard == "us-gaap"
    assert result.is_locked is True

  @pytest.mark.asyncio
  async def test_returns_404_when_not_found(self):
    session = _mock_session()
    session.execute.return_value.scalar_one_or_none.return_value = None

    with _patch_extensions(session):
      with pytest.raises(HTTPException) as exc_info:
        await get_reporting_taxonomy(
          graph_id=GRAPH_ID, current_user=_make_user(), _rate_limit=None
        )
      assert exc_info.value.status_code == 404


# ── Element Tests ─────────────────────────────────────────────────────────


class TestListElements:
  @pytest.mark.asyncio
  async def test_returns_elements_with_pagination(self):
    session = _mock_session()
    session.execute.return_value.scalar.return_value = 2
    session.execute.return_value.scalars.return_value.all.return_value = [
      _make_element(id="elem_1", code="1000", name="Cash"),
      _make_element(
        id="elem_2",
        code="4000",
        name="Revenue",
        classification="revenue",
        balance_type="credit",
      ),
    ]

    with _patch_extensions(session):
      result = await list_elements(
        graph_id=GRAPH_ID,
        limit=100,
        offset=0,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.elements) == 2
    assert result.elements[0].name == "Cash"
    assert result.elements[1].classification == "revenue"


class TestListUnmappedElements:
  @pytest.mark.asyncio
  async def test_returns_unmapped_elements(self):
    session = _mock_session()
    coa_elements = [
      _make_element(id="elem_1", name="Cash"),
      _make_element(id="elem_2", name="Revenue"),
      _make_element(id="elem_3", name="Rent Expense"),
    ]

    # First call: CoA elements. Second call: mapped IDs (only elem_1 is mapped)
    call_count = 0

    def mock_execute(query):
      nonlocal call_count
      call_count += 1
      result = MagicMock()
      if call_count == 1:
        result.scalars.return_value.all.return_value = coa_elements
      else:
        result.scalars.return_value.all.return_value = ["elem_1"]
      return result

    session.execute.side_effect = mock_execute

    with _patch_extensions(session):
      result = await list_unmapped_elements(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result) == 2
    assert result[0].name == "Revenue"
    assert result[1].name == "Rent Expense"


# ── Structure Tests ───────────────────────────────────────────────────────


class TestListStructures:
  @pytest.mark.asyncio
  async def test_returns_structures(self):
    session = _mock_session()
    session.execute.return_value.scalars.return_value.all.return_value = [
      _make_structure(),
      _make_structure(
        id="struct_2", name="Balance Sheet", structure_type="balance_sheet"
      ),
    ]

    with _patch_extensions(session):
      result = await list_structures(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.structures) == 2


class TestCreateStructure:
  @pytest.mark.asyncio
  async def test_creates_structure(self):
    session = _mock_session()
    mock_struct = _make_structure(
      name="QB → GAAP Mapping",
      structure_type="coa_mapping",
    )

    with (
      _patch_extensions(session),
      patch(f"{MODULE}.Structure", return_value=mock_struct),
    ):
      from robosystems.models.api.extensions.taxonomies import (
        CreateStructureRequest,
      )

      body = CreateStructureRequest(
        name="QB → GAAP Mapping",
        structure_type="coa_mapping",
        taxonomy_id="tax_1",
      )
      result = await create_structure(
        body=body,
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.name == "QB → GAAP Mapping"
    assert result.structure_type == "coa_mapping"
    session.add.assert_called_once()


# ── Mapping Tests ─────────────────────────────────────────────────────────


class TestListMappings:
  @pytest.mark.asyncio
  async def test_returns_only_coa_mappings(self):
    session = _mock_session()
    session.execute.return_value.scalars.return_value.all.return_value = [
      _make_structure(
        id="struct_map_1", name="QB → GAAP", structure_type="coa_mapping"
      ),
    ]

    with _patch_extensions(session):
      result = await list_mappings(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.structures) == 1
    assert result.structures[0].structure_type == "coa_mapping"


class TestGetMappingDetail:
  @pytest.mark.asyncio
  async def test_returns_mapping_with_associations(self):
    session = _mock_session()

    structure = _make_structure(
      id="struct_map_1", name="QB → GAAP", structure_type="coa_mapping"
    )

    # First call: structure. Second call: associations with element names.
    call_count = 0

    def mock_execute(query):
      nonlocal call_count
      call_count += 1
      result = MagicMock()
      if call_count == 1:
        result.scalar_one_or_none.return_value = structure
      else:
        assoc = _make_association()
        result.all.return_value = [
          (assoc, "Cash", "qb:1000", "Revenue", "us-gaap:Revenues")
        ]
      return result

    session.execute.side_effect = mock_execute

    with _patch_extensions(session):
      result = await get_mapping_detail(
        graph_id=GRAPH_ID,
        mapping_id="struct_map_1",
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.name == "QB → GAAP"
    assert result.total_associations == 1
    assert result.associations[0].from_element_name == "Cash"
    assert result.associations[0].to_element_qname == "us-gaap:Revenues"

  @pytest.mark.asyncio
  async def test_returns_404_for_missing_mapping(self):
    session = _mock_session()
    session.execute.return_value.scalar_one_or_none.return_value = None

    with _patch_extensions(session):
      with pytest.raises(HTTPException) as exc_info:
        await get_mapping_detail(
          graph_id=GRAPH_ID,
          mapping_id="nonexistent",
          current_user=_make_user(),
          _rate_limit=None,
        )
      assert exc_info.value.status_code == 404


class TestCreateMappingAssociation:
  @pytest.mark.asyncio
  async def test_creates_association(self):
    session = _mock_session()

    structure = _make_structure(structure_type="coa_mapping")
    from_elem = _make_element(id="elem_1", name="Cash", qname="qb:1000")
    to_elem = _make_element(id="elem_gaap_1", name="Revenue", qname="us-gaap:Revenues")

    call_count = 0

    def mock_execute(query):
      nonlocal call_count
      call_count += 1
      result = MagicMock()
      if call_count == 1:
        result.scalar_one_or_none.return_value = structure
      elif call_count == 2:
        result.scalar_one_or_none.return_value = from_elem
      else:
        result.scalar_one_or_none.return_value = to_elem
      return result

    session.execute.side_effect = mock_execute

    with _patch_extensions(session):
      from robosystems.models.api.extensions.taxonomies import (
        CreateAssociationRequest,
      )

      body = CreateAssociationRequest(
        from_element_id="elem_1",
        to_element_id="elem_gaap_1",
      )
      result = await create_mapping_association(
        body=body,
        graph_id=GRAPH_ID,
        mapping_id="struct_1",
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.from_element_name == "Cash"
    assert result.to_element_name == "Revenue"
    session.add.assert_called_once()

  @pytest.mark.asyncio
  async def test_returns_400_for_missing_source_element(self):
    session = _mock_session()

    structure = _make_structure()
    call_count = 0

    def mock_execute(query):
      nonlocal call_count
      call_count += 1
      result = MagicMock()
      if call_count == 1:
        result.scalar_one_or_none.return_value = structure
      else:
        result.scalar_one_or_none.return_value = None
      return result

    session.execute.side_effect = mock_execute

    with _patch_extensions(session):
      from robosystems.models.api.extensions.taxonomies import (
        CreateAssociationRequest,
      )

      body = CreateAssociationRequest(
        from_element_id="nonexistent",
        to_element_id="elem_gaap_1",
      )
      with pytest.raises(HTTPException) as exc_info:
        await create_mapping_association(
          body=body,
          graph_id=GRAPH_ID,
          mapping_id="struct_1",
          current_user=_make_user(),
          _rate_limit=None,
        )
      assert exc_info.value.status_code == 400


class TestDeleteMappingAssociation:
  @pytest.mark.asyncio
  async def test_deletes_association(self):
    session = _mock_session()
    session.query.return_value.filter.return_value.delete.return_value = 1

    with _patch_extensions(session):
      await delete_mapping_association(
        graph_id=GRAPH_ID,
        mapping_id="struct_1",
        association_id="assoc_1",
        current_user=_make_user(),
        _rate_limit=None,
      )

    session.query.return_value.filter.return_value.delete.assert_called_once()

  @pytest.mark.asyncio
  async def test_returns_404_for_missing_association(self):
    session = _mock_session()
    session.query.return_value.filter.return_value.delete.return_value = 0

    with _patch_extensions(session):
      with pytest.raises(HTTPException) as exc_info:
        await delete_mapping_association(
          graph_id=GRAPH_ID,
          mapping_id="struct_1",
          association_id="nonexistent",
          current_user=_make_user(),
          _rate_limit=None,
        )
      assert exc_info.value.status_code == 404


class TestGetMappingCoverage:
  @pytest.mark.asyncio
  async def test_returns_coverage_stats(self):
    session = _mock_session()

    assoc_high = _make_association(confidence=0.95)
    assoc_med = _make_association(
      id="assoc_2", confidence=0.80, from_element_id="elem_2"
    )
    assoc_low = _make_association(
      id="assoc_3", confidence=0.50, from_element_id="elem_3"
    )

    call_count = 0

    def mock_execute(query):
      nonlocal call_count
      call_count += 1
      result = MagicMock()
      if call_count == 1:
        result.scalar.return_value = 5  # total CoA elements
      else:
        result.scalars.return_value.all.return_value = [
          assoc_high,
          assoc_med,
          assoc_low,
        ]
      return result

    session.execute.side_effect = mock_execute

    with _patch_extensions(session):
      result = await get_mapping_coverage(
        graph_id=GRAPH_ID,
        mapping_id="struct_map_1",
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.total_coa_elements == 5
    assert result.mapped_count == 3
    assert result.unmapped_count == 2
    assert result.coverage_percent == 60.0
    assert result.high_confidence == 1
    assert result.medium_confidence == 1
    assert result.low_confidence == 1


# ── Element Filter Tests ──────────────────────────────────────────────────


class TestListElementsFilters:
  @pytest.mark.asyncio
  async def test_filter_by_source(self):
    session = _mock_session()
    session.execute.return_value.scalar.return_value = 1
    session.execute.return_value.scalars.return_value.all.return_value = [
      _make_element(source="us-gaap", qname="us-gaap:Revenues"),
    ]

    with _patch_extensions(session):
      result = await list_elements(
        graph_id=GRAPH_ID,
        source="us-gaap",
        limit=100,
        offset=0,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.elements) == 1
    assert result.elements[0].source == "us-gaap"

  @pytest.mark.asyncio
  async def test_filter_by_classification(self):
    session = _mock_session()
    session.execute.return_value.scalar.return_value = 1
    session.execute.return_value.scalars.return_value.all.return_value = [
      _make_element(classification="revenue"),
    ]

    with _patch_extensions(session):
      result = await list_elements(
        graph_id=GRAPH_ID,
        classification="revenue",
        limit=100,
        offset=0,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.elements) == 1
    assert result.elements[0].classification == "revenue"

  @pytest.mark.asyncio
  async def test_filter_by_is_abstract(self):
    session = _mock_session()
    session.execute.return_value.scalar.return_value = 1
    session.execute.return_value.scalars.return_value.all.return_value = [
      _make_element(is_abstract=True, element_type="abstract"),
    ]

    with _patch_extensions(session):
      result = await list_elements(
        graph_id=GRAPH_ID,
        is_abstract=True,
        limit=100,
        offset=0,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.elements) == 1
    assert result.elements[0].is_abstract is True

  @pytest.mark.asyncio
  async def test_empty_result(self):
    session = _mock_session()
    session.execute.return_value.scalar.return_value = 0
    session.execute.return_value.scalars.return_value.all.return_value = []

    with _patch_extensions(session):
      result = await list_elements(
        graph_id=GRAPH_ID,
        source="ifrs",
        limit=100,
        offset=0,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.elements) == 0
    assert result.pagination.total == 0


# ── Association Edge Cases ────────────────────────────────────────────────


class TestCreateAssociationEdgeCases:
  @pytest.mark.asyncio
  async def test_creates_with_confidence_score(self):
    session = _mock_session()
    structure = _make_structure(structure_type="coa_mapping")
    from_elem = _make_element(id="elem_1", name="Cash")
    to_elem = _make_element(id="gaap_1", name="Revenue", qname="us-gaap:Revenues")

    call_count = 0

    def mock_execute(query):
      nonlocal call_count
      call_count += 1
      result = MagicMock()
      if call_count == 1:
        result.scalar_one_or_none.return_value = structure
      elif call_count == 2:
        result.scalar_one_or_none.return_value = from_elem
      else:
        result.scalar_one_or_none.return_value = to_elem
      return result

    session.execute.side_effect = mock_execute

    with _patch_extensions(session):
      from robosystems.models.api.extensions.taxonomies import (
        CreateAssociationRequest,
      )

      body = CreateAssociationRequest(
        from_element_id="elem_1",
        to_element_id="gaap_1",
        confidence=0.85,
        suggested_by="ai",
      )
      result = await create_mapping_association(
        body=body,
        graph_id=GRAPH_ID,
        mapping_id="struct_1",
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.confidence == 0.85
    assert result.suggested_by == "ai"

  @pytest.mark.asyncio
  async def test_returns_404_for_missing_mapping_structure(self):
    session = _mock_session()
    session.execute.return_value.scalar_one_or_none.return_value = None

    with _patch_extensions(session):
      from robosystems.models.api.extensions.taxonomies import (
        CreateAssociationRequest,
      )

      body = CreateAssociationRequest(
        from_element_id="elem_1",
        to_element_id="gaap_1",
      )
      with pytest.raises(HTTPException) as exc_info:
        await create_mapping_association(
          body=body,
          graph_id=GRAPH_ID,
          mapping_id="nonexistent",
          current_user=_make_user(),
          _rate_limit=None,
        )
      assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_returns_400_for_missing_target_element(self):
    session = _mock_session()
    structure = _make_structure()
    from_elem = _make_element(id="elem_1")

    call_count = 0

    def mock_execute(query):
      nonlocal call_count
      call_count += 1
      result = MagicMock()
      if call_count == 1:
        result.scalar_one_or_none.return_value = structure
      elif call_count == 2:
        result.scalar_one_or_none.return_value = from_elem
      else:
        result.scalar_one_or_none.return_value = None  # target missing
      return result

    session.execute.side_effect = mock_execute

    with _patch_extensions(session):
      from robosystems.models.api.extensions.taxonomies import (
        CreateAssociationRequest,
      )

      body = CreateAssociationRequest(
        from_element_id="elem_1",
        to_element_id="nonexistent",
      )
      with pytest.raises(HTTPException) as exc_info:
        await create_mapping_association(
          body=body,
          graph_id=GRAPH_ID,
          mapping_id="struct_1",
          current_user=_make_user(),
          _rate_limit=None,
        )
      assert exc_info.value.status_code == 400


# ── Mapped Trial Balance Tests ────────────────────────────────────────────


class TestGetMappedTrialBalance:
  @pytest.mark.asyncio
  async def test_returns_mapped_rows(self):
    session = _mock_session()

    # Mock raw SQL result
    mock_row = MagicMock()
    mock_row.reporting_element_id = "gaap_revenues"
    mock_row.qname = "us-gaap:Revenues"
    mock_row.reporting_name = "Revenue"
    mock_row.classification = "revenue"
    mock_row.balance_type = "credit"
    mock_row.total_debits = 0
    mock_row.total_credits = 50000  # 500.00 in dollars

    session.execute.return_value = [mock_row]

    with _patch_extensions(session):
      result = await get_mapped_trial_balance(
        graph_id=GRAPH_ID,
        mapping_id="struct_map_1",
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result["mapping_id"] == "struct_map_1"
    assert len(result["rows"]) == 1
    assert result["rows"][0]["qname"] == "us-gaap:Revenues"
    assert result["rows"][0]["total_credits"] == 500.0
    assert result["rows"][0]["net_balance"] == -500.0  # debits - credits

  @pytest.mark.asyncio
  async def test_returns_empty_when_no_data(self):
    session = _mock_session()
    session.execute.return_value = []

    with _patch_extensions(session):
      result = await get_mapped_trial_balance(
        graph_id=GRAPH_ID,
        mapping_id="struct_map_1",
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result["rows"] == []

  @pytest.mark.asyncio
  async def test_returns_404_on_missing_schema(self):
    with patch(f"{MODULE}.extensions_session", side_effect=ValueError("bad")):
      with pytest.raises(HTTPException) as exc_info:
        await get_mapped_trial_balance(
          graph_id=GRAPH_ID,
          mapping_id="struct_map_1",
          current_user=_make_user(),
          _rate_limit=None,
        )
      assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_multiple_reporting_concepts(self):
    session = _mock_session()

    row_revenue = MagicMock()
    row_revenue.reporting_element_id = "gaap_revenues"
    row_revenue.qname = "us-gaap:Revenues"
    row_revenue.reporting_name = "Revenue"
    row_revenue.classification = "revenue"
    row_revenue.balance_type = "credit"
    row_revenue.total_debits = 0
    row_revenue.total_credits = 100000

    row_cogs = MagicMock()
    row_cogs.reporting_element_id = "gaap_cogs"
    row_cogs.qname = "us-gaap:CostOfRevenue"
    row_cogs.reporting_name = "Cost of Revenue"
    row_cogs.classification = "expense"
    row_cogs.balance_type = "debit"
    row_cogs.total_debits = 40000
    row_cogs.total_credits = 0

    session.execute.return_value = [row_revenue, row_cogs]

    with _patch_extensions(session):
      result = await get_mapped_trial_balance(
        graph_id=GRAPH_ID,
        mapping_id="struct_map_1",
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result["rows"]) == 2
    assert result["rows"][0]["reporting_name"] == "Revenue"
    assert result["rows"][0]["total_credits"] == 1000.0
    assert result["rows"][1]["reporting_name"] == "Cost of Revenue"
    assert result["rows"][1]["total_debits"] == 400.0
