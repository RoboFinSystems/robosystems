import time
import uuid


from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.models.iam import User
from robosystems.models.iam.graph import Graph, GraphTier
from robosystems.database import get_db_session
from robosystems.models.api.views import (
  CreateViewRequest,
  SaveViewRequest,
  SaveViewResponse,
  ViewMetadata,
  ViewResponse,
  ViewSourceType,
)
from robosystems.operations.views import (
  FactGridBuilder,
  aggregate_trial_balance,
  apply_element_mapping,
  get_mapping_structure,
  query_facts_with_aspects,
  save_view_as_report,
)

router = APIRouter(prefix="/views", tags=["views"])


def get_graph_tier(graph_id: str, session: Session) -> GraphTier:
  """Detect graph tier from graph_id."""
  graph = session.query(Graph).filter(Graph.graph_id == graph_id).first()
  if graph and graph.graph_tier:
    tier_map = {
      "kuzu-standard": GraphTier.KUZU_STANDARD,
      "kuzu-large": GraphTier.KUZU_LARGE,
      "kuzu-xlarge": GraphTier.KUZU_XLARGE,
      "kuzu-shared": GraphTier.KUZU_SHARED,
    }
    return tier_map.get(graph.graph_tier.lower(), GraphTier.KUZU_STANDARD)
  return GraphTier.KUZU_STANDARD


@router.post("", operation_id="create_view")
async def create_view(
  graph_id: str,
  request: CreateViewRequest,
  req: Request,
  current_user: User = Depends(get_current_user),
  session: Session = Depends(get_db_session),
):
  """
  Generate financial report view from data source (dual-mode support).

  **Mode 1: Transaction Aggregation (generate_from_transactions)**
  - Aggregates raw transaction data to trial balance
  - Creates facts on-demand
  - Shows real-time reporting from source of truth

  **Mode 2: Existing Facts (pivot_existing_facts)**
  - Queries existing Fact nodes
  - Supports multi-dimensional analysis
  - Works with SEC filings and pre-computed facts

  Both modes:
  - Build FactGrid from data
  - Generate pivot table presentation
  - Return consistent response format
  """
  start_time = time.time()

  try:
    requested_dimensions = []
    if request.view_config:
      all_axes = (request.view_config.rows or []) + (request.view_config.columns or [])
      for axis in all_axes:
        if axis.type == "dimension" and axis.dimension_axis:
          requested_dimensions.append(axis.dimension_axis)

    if request.source.type == ViewSourceType.TRANSACTIONS:
      fact_data = await aggregate_trial_balance(
        graph_id=graph_id,
        period_start=request.source.period_start,
        period_end=request.source.period_end,
        entity_id=request.source.entity_id,
        requested_dimensions=requested_dimensions or None,
      )
      source = "trial_balance_aggregation"
      period_start = request.source.period_start
      period_end = request.source.period_end

    elif request.source.type == ViewSourceType.FACT_SET:
      fact_data = await query_facts_with_aspects(
        graph_id=graph_id,
        fact_set_id=request.source.fact_set_id,
        period_start=request.source.period_start,
        period_end=request.source.period_end,
        entity_id=request.source.entity_id,
        requested_dimensions=requested_dimensions or None,
      )
      source = "fact_set_query"
      period_start = request.source.period_start
      period_end = request.source.period_end

    else:
      raise HTTPException(
        status_code=400,
        detail=f"Unsupported source type: {request.source.type}",
      )

    if request.mapping_structure_id:
      tier = get_graph_tier(graph_id, session)
      mapping = await get_mapping_structure(
        graph_id, request.mapping_structure_id, tier
      )
      if not mapping:
        raise HTTPException(
          status_code=404,
          detail=f"Mapping structure not found: {request.mapping_structure_id}",
        )
      fact_data = apply_element_mapping(fact_data, mapping.structure)

    builder = FactGridBuilder()
    fact_grid = builder.build(
      fact_data=fact_data,
      view_config=request.view_config,
      source=source,
    )

    pivot_table = builder.generate_pivot_table(fact_grid, request.view_config)

    construction_time_ms = (time.time() - start_time) * 1000

    metadata = ViewMetadata(
      view_id=str(uuid.uuid4()),
      facts_processed=fact_grid.metadata.fact_count,
      construction_time_ms=construction_time_ms,
      source=source,
      period_start=period_start,
      period_end=period_end,
    )

    return ViewResponse(
      metadata=metadata,
      presentations={"pivot_table": pivot_table},
    )

  except Exception as e:
    raise HTTPException(
      status_code=500,
      detail=f"Failed to create view: {str(e)}",
    ) from e


@router.post("/save", operation_id="save_view")
async def save_view(
  graph_id: str,
  request: SaveViewRequest,
  req: Request,
  current_user: User = Depends(get_current_user),
  session: Session = Depends(get_db_session),
) -> SaveViewResponse:
  """
  Save or update view as materialized report in the graph.

  Converts computed view results into persistent Report, Fact, and Structure nodes.
  This establishes what data exists in the subgraph, which then defines what
  needs to be exported for publishing to the parent graph.

  **Create Mode** (no report_id provided):
  - Generates new report_id from entity + period + report type
  - Creates new Report, Facts, and Structures

  **Update Mode** (report_id provided):
  - Deletes all existing Facts and Structures for the report
  - Updates Report metadata
  - Creates fresh Facts and Structures from current view
  - Useful for refreshing reports with updated data or view configurations

  **This is NOT publishing** - it only creates nodes in the subgraph workspace.
  Publishing (export → parquet → parent ingest) happens separately.

  Creates/Updates:
  - Report node with metadata
  - Fact nodes with all aspects (period, entity, element, unit)
  - PresentationStructure nodes (how facts are displayed)
  - CalculationStructure nodes (how facts roll up)

  Returns:
  - report_id: Unique identifier used as parquet export prefix
  - parquet_export_prefix: Filename prefix for future exports
  - All created facts and structures
  """
  try:
    response = await save_view_as_report(graph_id, request)
    return response

  except Exception as e:
    raise HTTPException(
      status_code=500,
      detail=f"Failed to save view: {str(e)}",
    ) from e


# Element mapping endpoints have been removed.
# These operations are now handled via client-side extensions
# that write to subgraph workspaces using the public /query endpoint.
#
# See:
# - robosystems-python-client/robosystems_client/extensions/element_mapping_client.py
# - robosystems-python-client/robosystems_client/extensions/subgraph_workspace_client.py
#
# Architecture:
# 1. Create subgraph workspace (write-enabled)
# 2. Use ElementMappingClient to write mappings to subgraph via /query
# 3. Apply mappings client-side when generating views
# 4. Export subgraph → parquet → S3 → incremental ingest to main graph
