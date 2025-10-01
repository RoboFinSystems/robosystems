"""Schema export endpoint."""

import yaml
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, Request, status, Path, Query
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.iam import User
from robosystems.models.api.graph import SchemaExportResponse
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.database import get_db_session

router = APIRouter()


@router.get(
  "/export",
  response_model=SchemaExportResponse,
  operation_id="exportGraphSchema",
  summary="Export Graph Schema",
  description="Export the schema of an existing graph in JSON, YAML, or Cypher format",
  status_code=status.HTTP_200_OK,
)
async def export_graph_schema(
  request: Request,
  graph_id: str = Path(..., description="The graph ID to export schema from"),
  format: str = Query(
    "json",
    description="Export format: json, yaml, or cypher",
    regex="^(json|yaml|cypher)$",
  ),
  include_data_stats: bool = Query(
    False, description="Include statistics about actual data in the graph"
  ),
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
):
  """
  Export the schema of an existing graph.

  This endpoint retrieves the schema definition from a graph and exports it
  in the requested format (JSON, YAML, or Cypher DDL).
  """
  try:
    # Verify user has access to the graph (handle both user graphs and shared repositories)
    from robosystems.models.iam import UserGraph
    from robosystems.middleware.graph.multitenant_utils import MultiTenantUtils
    from robosystems.models.iam.user_repository import UserRepository

    # Determine graph type and validate access accordingly
    identity = MultiTenantUtils.get_graph_identity(graph_id)

    if identity.is_shared_repository:
      # Check shared repository access
      if not UserRepository.user_has_access(str(current_user.id), graph_id, db):
        raise HTTPException(
          status_code=status.HTTP_403_FORBIDDEN,
          detail=f"Access denied to shared repository {graph_id}",
        )
      # Create synthetic UserGraph for access control
      user_graph = UserGraph()
      user_graph.user_id = str(current_user.id)
      user_graph.graph_id = graph_id
      user_graph.role = "reader"

    elif identity.is_user_graph:
      # Check user graph access
      user_graph = (
        db.query(UserGraph)
        .filter_by(user_id=current_user.id, graph_id=graph_id)
        .first()
      )
      if not user_graph:
        raise HTTPException(
          status_code=status.HTTP_403_FORBIDDEN,
          detail=f"Access denied to user graph {graph_id}",
        )

    else:
      # Unknown graph type
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Invalid graph identifier: {graph_id}",
      )

    # Try to get graph metadata from the graph itself
    try:
      from robosystems.middleware.graph import get_graph_repository

      repository = await get_graph_repository(graph_id, operation_type="read")

      # Query for GraphMetadata
      result = await repository.execute_query(
        "MATCH (m:GraphMetadata {graph_id: $graph_id}) RETURN m", {"graph_id": graph_id}
      )

      if result and len(result) > 0:
        graph_meta = result[0].get("m", {})
        schema_name = graph_meta.get("custom_schema_name") or f"{graph_id}_schema"
        schema_version = graph_meta.get("custom_schema_version") or "1.0.0"
        schema_type = graph_meta.get("schema_type") or "standard"
      else:
        # Fallback if no metadata found
        schema_name = f"{graph_id}_schema"
        schema_version = "1.0.0"
        schema_type = "standard"
    except Exception as e:
      logger.warning(f"Could not retrieve graph metadata: {e}")
      # Fallback values
      schema_name = f"{graph_id}_schema"
      schema_version = "1.0.0"
      schema_type = "standard"

    # Create a basic schema definition
    # In production, this would be retrieved from the actual schema storage
    nodes = []
    relationships = []

    # Add a placeholder message
    logger.warning(f"Schema export not fully implemented for graph {graph_id}")

    # Build schema definition
    schema_def = {
      "name": schema_name,
      "version": schema_version,
      "description": f"Exported schema from graph {graph_id}",
      "type": schema_type,
      "nodes": nodes,
      "relationships": relationships,
      "metadata": {
        "graph_id": graph_id,
        "export_note": "Full schema export requires accessing stored schema definitions",
      },
    }

    # Get data statistics if requested
    data_stats = None
    if include_data_stats:
      # For now, skip data statistics to avoid query issues
      # In production, we would use proper Kuzu queries to get node/relationship counts
      data_stats = {
        "node_counts": {},
        "total_nodes": 0,
        "message": "Data statistics not yet implemented",
      }

    # Format output based on requested format
    if format == "yaml":
      schema_output = yaml.dump(schema_def, default_flow_style=False)
    elif format == "cypher":
      # Generate Cypher DDL
      from robosystems.schemas.custom import (
        CustomSchemaManager,
      )

      manager = CustomSchemaManager()
      schema = manager.create_from_dict(schema_def)
      schema_output = schema.to_cypher()
    else:  # json
      schema_output = schema_def

    return SchemaExportResponse(
      graph_id=graph_id,
      schema_definition=schema_output,
      format=format,
      exported_at=datetime.now(timezone.utc).isoformat(),
      data_stats=data_stats,
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Schema export error: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to export schema: {str(e)}",
    )
