"""Taxonomy, structure, and mapping endpoints.

CRUD for the taxonomy system: taxonomies, structures, element associations,
mapping coverage, and unmapped element discovery.
"""

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy import func, select, text
from sqlalchemy.exc import ProgrammingError

from robosystems.db.extensions import extensions_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import create_pagination_info
from robosystems.models.api.extensions.taxonomies import (
  CreateAssociationRequest,
  CreateStructureRequest,
  CreateTaxonomyRequest,
  ElementAssociationResponse,
  ElementListResponse,
  ElementResponse,
  MappingCoverageResponse,
  MappingDetailResponse,
  StructureListResponse,
  StructureResponse,
  TaxonomyListResponse,
  TaxonomyResponse,
  UnmappedElementResponse,
)
from robosystems.models.extensions import (
  Element,
  ElementAssociation,
  Structure,
  Taxonomy,
)
from robosystems.models.iam import User
from robosystems.utils.ulid import generate_prefixed_ulid

router = APIRouter()


def _ledger_404():
  return HTTPException(
    status_code=404,
    detail="Ledger not initialized. Connect a data source first.",
  )


# ── Taxonomies ────────────────────────────────────────────────────────────


@router.get(
  "/taxonomies",
  response_model=TaxonomyListResponse,
  operation_id="listTaxonomies",
  summary="List Taxonomies",
  tags=["Ledger"],
)
async def list_taxonomies(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  taxonomy_type: str | None = Query(None, description="Filter by type"),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      query = select(Taxonomy).where(Taxonomy.is_active.is_(True))
      if taxonomy_type:
        query = query.where(Taxonomy.taxonomy_type == taxonomy_type)
      rows = session.execute(query.order_by(Taxonomy.name)).scalars().all()
      return TaxonomyListResponse(
        taxonomies=[
          TaxonomyResponse(
            id=r.id,
            name=r.name,
            description=r.description,
            taxonomy_type=r.taxonomy_type,
            version=r.version,
            standard=r.standard,
            namespace_uri=r.namespace_uri,
            is_shared=r.is_shared,
            is_active=r.is_active,
            is_locked=r.is_locked,
            source_taxonomy_id=r.source_taxonomy_id,
            target_taxonomy_id=r.target_taxonomy_id,
          )
          for r in rows
        ]
      )
  except (ValueError, ProgrammingError):
    raise _ledger_404()


@router.post(
  "/taxonomies",
  response_model=TaxonomyResponse,
  status_code=201,
  operation_id="createTaxonomy",
  summary="Create Taxonomy",
  tags=["Ledger"],
)
async def create_taxonomy(
  body: CreateTaxonomyRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      taxonomy = Taxonomy(
        id=generate_prefixed_ulid("tax"),
        name=body.name,
        description=body.description,
        taxonomy_type=body.taxonomy_type,
        version=body.version,
        source_taxonomy_id=body.source_taxonomy_id,
        target_taxonomy_id=body.target_taxonomy_id,
        created_by=str(current_user.id),
      )
      session.add(taxonomy)
      session.flush()
      return TaxonomyResponse(
        id=taxonomy.id,
        name=taxonomy.name,
        description=taxonomy.description,
        taxonomy_type=taxonomy.taxonomy_type,
        version=taxonomy.version,
        standard=taxonomy.standard,
        namespace_uri=taxonomy.namespace_uri,
        is_shared=taxonomy.is_shared,
        is_active=taxonomy.is_active,
        is_locked=taxonomy.is_locked,
        source_taxonomy_id=taxonomy.source_taxonomy_id,
        target_taxonomy_id=taxonomy.target_taxonomy_id,
      )
  except (ValueError, ProgrammingError):
    raise _ledger_404()


@router.get(
  "/taxonomies/reporting",
  response_model=TaxonomyResponse,
  operation_id="getReportingTaxonomy",
  summary="Get Reporting Taxonomy",
  tags=["Ledger"],
)
async def get_reporting_taxonomy(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Get the shared US GAAP reporting taxonomy (read-only)."""
  try:
    with extensions_session(graph_id) as session:
      row = session.execute(
        select(Taxonomy).where(
          Taxonomy.standard == "us-gaap",
          Taxonomy.taxonomy_type == "reporting",
          Taxonomy.is_locked.is_(True),
        )
      ).scalar_one_or_none()
      if not row:
        raise HTTPException(status_code=404, detail="Reporting taxonomy not found")
      return TaxonomyResponse(
        id=row.id,
        name=row.name,
        description=row.description,
        taxonomy_type=row.taxonomy_type,
        version=row.version,
        standard=row.standard,
        namespace_uri=row.namespace_uri,
        is_shared=row.is_shared,
        is_active=row.is_active,
        is_locked=row.is_locked,
        source_taxonomy_id=row.source_taxonomy_id,
        target_taxonomy_id=row.target_taxonomy_id,
      )
  except (ValueError, ProgrammingError):
    raise _ledger_404()


# ── Elements (enhanced with taxonomy filtering) ──────────────────────────


@router.get(
  "/elements",
  response_model=ElementListResponse,
  operation_id="listElements",
  summary="List Elements",
  tags=["Ledger"],
)
async def list_elements(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  taxonomy_id: str | None = Query(None, description="Filter by taxonomy"),
  source: str | None = Query(None, description="Filter by source"),
  classification: str | None = Query(None, description="Filter by classification"),
  is_abstract: bool | None = Query(None, description="Filter by abstract"),
  limit: int = Query(100, ge=1, le=1000),
  offset: int = Query(0, ge=0),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      query = select(Element).where(Element.is_active.is_(True))
      count_query = (
        select(func.count()).select_from(Element).where(Element.is_active.is_(True))
      )

      if taxonomy_id:
        query = query.where(Element.taxonomy_id == taxonomy_id)
        count_query = count_query.where(Element.taxonomy_id == taxonomy_id)
      if source:
        query = query.where(Element.source == source)
        count_query = count_query.where(Element.source == source)
      if classification:
        query = query.where(Element.classification == classification)
        count_query = count_query.where(Element.classification == classification)
      if is_abstract is not None:
        query = query.where(Element.is_abstract == is_abstract)
        count_query = count_query.where(Element.is_abstract == is_abstract)

      total = session.execute(count_query).scalar() or 0
      rows = (
        session.execute(
          query.order_by(Element.depth, Element.code, Element.qname)
          .offset(offset)
          .limit(limit)
        )
        .scalars()
        .all()
      )

      return ElementListResponse(
        elements=[_element_to_response(r) for r in rows],
        pagination=create_pagination_info(total, limit, offset),
      )
  except (ValueError, ProgrammingError):
    raise _ledger_404()


@router.get(
  "/elements/unmapped",
  response_model=list[UnmappedElementResponse],
  operation_id="listUnmappedElements",
  summary="List Unmapped Elements",
  tags=["Ledger"],
)
async def list_unmapped_elements(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  mapping_id: str | None = Query(
    None, description="Mapping structure to check against"
  ),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """List CoA elements that are not yet mapped to the reporting taxonomy."""
  try:
    with extensions_session(graph_id) as session:
      # Get CoA elements (source = quickbooks, xero, native, import)
      coa_sources = ("quickbooks", "xero", "plaid", "native", "import")
      coa_query = select(Element).where(
        Element.source.in_(coa_sources),
        Element.is_active.is_(True),
        Element.is_abstract.is_(False),
      )
      coa_elements = session.execute(coa_query).scalars().all()

      # Get mapped element IDs (from_element_id in mapping associations)
      mapped_ids: set[str] = set()
      if mapping_id:
        mapped_query = select(ElementAssociation.from_element_id).where(
          ElementAssociation.structure_id == mapping_id,
          ElementAssociation.association_type == "mapping",
        )
        mapped_ids = set(session.execute(mapped_query).scalars().all())
      else:
        # Check all mapping structures
        mapped_query = select(ElementAssociation.from_element_id).where(
          ElementAssociation.association_type == "mapping",
        )
        mapped_ids = set(session.execute(mapped_query).scalars().all())

      unmapped = [e for e in coa_elements if e.id not in mapped_ids]

      return [
        UnmappedElementResponse(
          id=e.id,
          code=e.code,
          name=e.name,
          classification=e.classification,
          balance_type=e.balance_type,
          external_source=e.external_source,
        )
        for e in unmapped
      ]
  except (ValueError, ProgrammingError):
    raise _ledger_404()


# ── Structures ────────────────────────────────────────────────────────────


@router.get(
  "/structures",
  response_model=StructureListResponse,
  operation_id="listStructures",
  summary="List Structures",
  tags=["Ledger"],
)
async def list_structures(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  taxonomy_id: str | None = Query(None, description="Filter by taxonomy"),
  structure_type: str | None = Query(None, description="Filter by type"),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      query = select(Structure).where(Structure.is_active.is_(True))
      if taxonomy_id:
        query = query.where(Structure.taxonomy_id == taxonomy_id)
      if structure_type:
        query = query.where(Structure.structure_type == structure_type)
      rows = session.execute(query.order_by(Structure.name)).scalars().all()
      return StructureListResponse(
        structures=[
          StructureResponse(
            id=r.id,
            name=r.name,
            description=r.description,
            structure_type=r.structure_type,
            taxonomy_id=r.taxonomy_id,
            is_active=r.is_active,
          )
          for r in rows
        ]
      )
  except (ValueError, ProgrammingError):
    raise _ledger_404()


@router.post(
  "/structures",
  response_model=StructureResponse,
  status_code=201,
  operation_id="createStructure",
  summary="Create Structure",
  tags=["Ledger"],
)
async def create_structure(
  body: CreateStructureRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      structure = Structure(
        id=generate_prefixed_ulid("struct"),
        name=body.name,
        description=body.description,
        structure_type=body.structure_type,
        taxonomy_id=body.taxonomy_id,
        created_by=str(current_user.id),
      )
      session.add(structure)
      session.flush()
      return StructureResponse(
        id=structure.id,
        name=structure.name,
        description=structure.description,
        structure_type=structure.structure_type,
        taxonomy_id=structure.taxonomy_id,
        is_active=structure.is_active,
      )
  except (ValueError, ProgrammingError):
    raise _ledger_404()


# ── Mappings ──────────────────────────────────────────────────────────────


@router.get(
  "/mappings",
  response_model=StructureListResponse,
  operation_id="listMappings",
  summary="List Mapping Structures",
  tags=["Ledger"],
)
async def list_mappings(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """List all mapping structures (structure_type = 'coa_mapping')."""
  try:
    with extensions_session(graph_id) as session:
      rows = (
        session.execute(
          select(Structure)
          .where(
            Structure.structure_type == "coa_mapping",
            Structure.is_active.is_(True),
          )
          .order_by(Structure.name)
        )
        .scalars()
        .all()
      )
      return StructureListResponse(
        structures=[
          StructureResponse(
            id=r.id,
            name=r.name,
            description=r.description,
            structure_type=r.structure_type,
            taxonomy_id=r.taxonomy_id,
            is_active=r.is_active,
          )
          for r in rows
        ]
      )
  except (ValueError, ProgrammingError):
    raise _ledger_404()


@router.get(
  "/mappings/{mapping_id}",
  response_model=MappingDetailResponse,
  operation_id="getMappingDetail",
  summary="Get Mapping Detail",
  tags=["Ledger"],
)
async def get_mapping_detail(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  mapping_id: str = Path(...),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Get a mapping structure with all its associations."""
  try:
    with extensions_session(graph_id) as session:
      structure = session.execute(
        select(Structure).where(Structure.id == mapping_id)
      ).scalar_one_or_none()
      if not structure:
        raise HTTPException(status_code=404, detail="Mapping not found")

      # Get associations with element names
      from_elem = Element.__table__.alias("from_elem")
      to_elem = Element.__table__.alias("to_elem")

      assoc_rows = session.execute(
        select(
          ElementAssociation,
          from_elem.c.name.label("from_name"),
          from_elem.c.qname.label("from_qname"),
          to_elem.c.name.label("to_name"),
          to_elem.c.qname.label("to_qname"),
        )
        .join(from_elem, ElementAssociation.from_element_id == from_elem.c.id)
        .join(to_elem, ElementAssociation.to_element_id == to_elem.c.id)
        .where(ElementAssociation.structure_id == mapping_id)
        .order_by(ElementAssociation.order_value)
      ).all()

      associations = [
        ElementAssociationResponse(
          id=a.id,
          structure_id=a.structure_id,
          from_element_id=a.from_element_id,
          from_element_name=from_name,
          from_element_qname=from_qname,
          to_element_id=a.to_element_id,
          to_element_name=to_name,
          to_element_qname=to_qname,
          association_type=a.association_type,
          order_value=a.order_value,
          weight=a.weight,
          confidence=a.confidence,
          suggested_by=a.suggested_by,
          approved_by=a.approved_by,
        )
        for a, from_name, from_qname, to_name, to_qname in assoc_rows
      ]

      return MappingDetailResponse(
        id=structure.id,
        name=structure.name,
        structure_type=structure.structure_type,
        taxonomy_id=structure.taxonomy_id,
        associations=associations,
        total_associations=len(associations),
      )
  except (ValueError, ProgrammingError):
    raise _ledger_404()


@router.post(
  "/mappings/{mapping_id}/associations",
  response_model=ElementAssociationResponse,
  status_code=201,
  operation_id="createMappingAssociation",
  summary="Create Mapping Association",
  tags=["Ledger"],
)
async def create_mapping_association(
  body: CreateAssociationRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  mapping_id: str = Path(...),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Add a mapping association (CoA element → reporting concept)."""
  try:
    with extensions_session(graph_id) as session:
      # Verify structure exists
      structure = session.execute(
        select(Structure).where(Structure.id == mapping_id)
      ).scalar_one_or_none()
      if not structure:
        raise HTTPException(status_code=404, detail="Mapping not found")

      # Verify elements exist
      from_elem = session.execute(
        select(Element).where(Element.id == body.from_element_id)
      ).scalar_one_or_none()
      if not from_elem:
        raise HTTPException(status_code=400, detail="Source element not found")

      to_elem = session.execute(
        select(Element).where(Element.id == body.to_element_id)
      ).scalar_one_or_none()
      if not to_elem:
        raise HTTPException(status_code=400, detail="Target element not found")

      assoc = ElementAssociation(
        id=generate_prefixed_ulid("assoc"),
        structure_id=mapping_id,
        from_element_id=body.from_element_id,
        to_element_id=body.to_element_id,
        association_type=body.association_type,
        order_value=body.order_value,
        weight=body.weight,
        confidence=body.confidence,
        suggested_by=body.suggested_by,
        created_by=str(current_user.id),
      )
      session.add(assoc)
      session.flush()

      return ElementAssociationResponse(
        id=assoc.id,
        structure_id=assoc.structure_id,
        from_element_id=assoc.from_element_id,
        from_element_name=from_elem.name,
        from_element_qname=from_elem.qname,
        to_element_id=assoc.to_element_id,
        to_element_name=to_elem.name,
        to_element_qname=to_elem.qname,
        association_type=assoc.association_type,
        order_value=assoc.order_value,
        weight=assoc.weight,
        confidence=assoc.confidence,
        suggested_by=assoc.suggested_by,
        approved_by=assoc.approved_by,
      )
  except (ValueError, ProgrammingError):
    raise _ledger_404()


@router.delete(
  "/mappings/{mapping_id}/associations/{association_id}",
  status_code=204,
  operation_id="deleteMappingAssociation",
  summary="Delete Mapping Association",
  tags=["Ledger"],
)
async def delete_mapping_association(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  mapping_id: str = Path(...),
  association_id: str = Path(...),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Remove a mapping association."""
  try:
    with extensions_session(graph_id) as session:
      deleted = (
        session.query(ElementAssociation)
        .filter(
          ElementAssociation.id == association_id,
          ElementAssociation.structure_id == mapping_id,
        )
        .delete(synchronize_session=False)
      )
      if not deleted:
        raise HTTPException(status_code=404, detail="Association not found")
  except (ValueError, ProgrammingError):
    raise _ledger_404()


@router.get(
  "/mappings/{mapping_id}/coverage",
  response_model=MappingCoverageResponse,
  operation_id="getMappingCoverage",
  summary="Mapping Coverage",
  tags=["Ledger"],
)
async def get_mapping_coverage(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  mapping_id: str = Path(...),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Get mapping coverage stats."""
  try:
    with extensions_session(graph_id) as session:
      # Count CoA elements (non-abstract, active, from connectors/native)
      coa_sources = ("quickbooks", "xero", "plaid", "native", "import")
      total_coa = (
        session.execute(
          select(func.count())
          .select_from(Element)
          .where(
            Element.source.in_(coa_sources),
            Element.is_active.is_(True),
            Element.is_abstract.is_(False),
          )
        ).scalar()
        or 0
      )

      # Count mapped elements
      mapping_assocs = (
        session.execute(
          select(ElementAssociation).where(
            ElementAssociation.structure_id == mapping_id,
            ElementAssociation.association_type == "mapping",
          )
        )
        .scalars()
        .all()
      )

      mapped_count = len({a.from_element_id for a in mapping_assocs})
      unmapped_count = total_coa - mapped_count

      # Confidence distribution
      high = sum(
        1 for a in mapping_assocs if a.confidence is not None and a.confidence > 0.90
      )
      medium = sum(
        1
        for a in mapping_assocs
        if a.confidence is not None and 0.70 <= a.confidence <= 0.90
      )
      low = sum(
        1 for a in mapping_assocs if a.confidence is not None and a.confidence < 0.70
      )

      return MappingCoverageResponse(
        mapping_id=mapping_id,
        total_coa_elements=total_coa,
        mapped_count=mapped_count,
        unmapped_count=unmapped_count,
        coverage_percent=((mapped_count / total_coa * 100) if total_coa > 0 else 0.0),
        high_confidence=high,
        medium_confidence=medium,
        low_confidence=low,
      )
  except (ValueError, ProgrammingError):
    raise _ledger_404()


# ── Mapped Trial Balance ──────────────────────────────────────────────────


@router.get(
  "/trial-balance/mapped",
  operation_id="getMappedTrialBalance",
  summary="Mapped Trial Balance",
  tags=["Ledger"],
)
async def get_mapped_trial_balance(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  mapping_id: str = Query(..., description="Mapping structure ID"),
  start_date: str | None = Query(None),
  end_date: str | None = Query(None),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Trial balance rolled up to reporting concepts via mapping associations."""
  try:
    with extensions_session(graph_id) as session:
      result = session.execute(
        text("""
                    SELECT
                        target.id AS reporting_element_id,
                        target.qname,
                        target.name AS reporting_name,
                        target.classification,
                        target.balance_type,
                        COALESCE(SUM(li.debit_amount), 0) AS total_debits,
                        COALESCE(SUM(li.credit_amount), 0) AS total_credits
                    FROM elements source_elem
                    JOIN line_items li ON li.element_id = source_elem.id
                    JOIN entries e ON e.id = li.entry_id
                    JOIN element_associations mapping
                        ON mapping.from_element_id = source_elem.id
                        AND mapping.association_type = 'mapping'
                        AND mapping.structure_id = :mapping_id
                    JOIN elements target ON target.id = mapping.to_element_id
                    WHERE e.status = 'posted'
                        AND (e.posting_date >= :start_date OR :start_date IS NULL)
                        AND (e.posting_date <= :end_date OR :end_date IS NULL)
                    GROUP BY target.id, target.qname, target.name,
                             target.classification, target.balance_type
                    ORDER BY target.qname
                """),
        {
          "mapping_id": mapping_id,
          "start_date": start_date,
          "end_date": end_date,
        },
      )

      rows = []
      for row in result:
        debits = float(row.total_debits) / 100.0
        credits = float(row.total_credits) / 100.0
        rows.append(
          {
            "reporting_element_id": row.reporting_element_id,
            "qname": row.qname,
            "reporting_name": row.reporting_name,
            "classification": row.classification,
            "balance_type": row.balance_type,
            "total_debits": debits,
            "total_credits": credits,
            "net_balance": debits - credits,
          }
        )

      return {"rows": rows, "mapping_id": mapping_id}
  except (ValueError, ProgrammingError):
    raise _ledger_404()


# ── Helpers ───────────────────────────────────────────────────────────────


def _element_to_response(row: Element) -> ElementResponse:
  return ElementResponse(
    id=row.id,
    code=row.code,
    name=row.name,
    description=row.description,
    qname=row.qname,
    namespace=row.namespace,
    classification=row.classification,
    sub_classification=row.sub_classification,
    balance_type=row.balance_type,
    period_type=row.period_type,
    is_abstract=row.is_abstract,
    element_type=row.element_type,
    source=row.source,
    taxonomy_id=row.taxonomy_id,
    parent_id=row.parent_id,
    depth=row.depth,
    is_active=row.is_active,
    external_id=row.external_id,
    external_source=row.external_source,
  )
