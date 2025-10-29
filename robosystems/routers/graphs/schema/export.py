"""Schema export endpoint."""

import yaml
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, Request, status, Path, Query
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.iam import User
from robosystems.models.api.graph import SchemaExportResponse
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.database import get_db_session

router = APIRouter()


@router.get(
  "/schema/export",
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
  current_user: User = Depends(get_current_user_with_graph),
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

    # Get declared schema from PostgreSQL GraphSchema table
    from robosystems.models.iam import Graph, GraphSchema

    schema_record = GraphSchema.get_active_schema(graph_id, db)

    if not schema_record:
      # Try to reconstruct from Graph metadata if no schema record exists
      graph = Graph.get_by_id(graph_id, db)
      if not graph:
        raise HTTPException(
          status_code=status.HTTP_404_NOT_FOUND,
          detail=f"No schema found for graph {graph_id}",
        )

      # Use graph metadata as fallback
      schema_name = f"{graph.graph_name}_schema"
      schema_version = "1.0.0"
      schema_type = "extensions" if graph.schema_extensions else "base"
      schema_def = {
        "name": schema_name,
        "version": schema_version,
        "description": f"Schema for {graph.graph_name}",
        "type": schema_type,
        "extensions": graph.schema_extensions or [],
        "metadata": {
          "graph_id": graph_id,
          "base_schema": graph.base_schema or "base",
          "note": "Schema record not found, reconstructed from graph metadata",
        },
      }
    else:
      # Use stored schema from GraphSchema table
      schema_name = schema_record.custom_schema_name or f"{graph_id}_schema"
      schema_version = str(schema_record.schema_version)
      schema_type = schema_record.schema_type

      # Use stored schema_json if available
      if schema_record.schema_json:
        schema_def = schema_record.schema_json
        # Ensure it has required fields
        if "name" not in schema_def:
          schema_def["name"] = schema_name  # type: ignore[index]
        if "version" not in schema_def:
          schema_def["version"] = schema_version  # type: ignore[index]
        if "type" not in schema_def:
          schema_def["type"] = schema_type  # type: ignore[index]
      else:
        # Construct from DDL if JSON not available
        schema_def = {
          "name": schema_name,
          "version": schema_version,
          "description": f"Exported schema from graph {graph_id}",
          "type": schema_type,
          "ddl": schema_record.schema_ddl,
          "metadata": {
            "graph_id": graph_id,
            "schema_version": schema_record.schema_version,
            "created_at": schema_record.created_at.isoformat(),
          },
        }

    # Get data statistics if requested
    data_stats = None
    if include_data_stats:
      # Get runtime statistics from the graph
      try:
        from .utils import get_schema_info
        from robosystems.middleware.graph.dependencies import (
          get_universal_repository,
        )

        # Use existing session parameter for repository auth
        repository = await get_universal_repository(graph_id, "read")
        runtime_schema = await get_schema_info(repository)

        data_stats = {
          "node_labels_count": len(runtime_schema.get("node_labels", [])),
          "relationship_types_count": len(runtime_schema.get("relationship_types", [])),
          "node_properties_count": len(runtime_schema.get("node_properties", {})),
        }
      except HTTPException:
        raise
      except Exception as e:
        logger.warning(f"Could not retrieve data statistics for {graph_id}: {e}")
        data_stats = {
          "message": "Data statistics unavailable",
        }

    # Format output based on requested format
    if format == "yaml":
      schema_output = yaml.dump(schema_def, default_flow_style=False)
    elif format == "cypher":
      # Use stored DDL if available from GraphSchema
      if schema_record and schema_record.schema_ddl:
        schema_output = schema_record.schema_ddl
      elif "ddl" in schema_def:
        schema_output = schema_def["ddl"]
      else:
        # Try to generate from schema_def if it has nodes/relationships
        try:
          from robosystems.schemas.custom import CustomSchemaManager

          manager = CustomSchemaManager()
          schema = manager.create_from_dict(schema_def)
          schema_output = schema.to_cypher()
        except Exception as e:
          logger.error(f"Failed to generate Cypher DDL: {e}")
          raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Cannot generate Cypher format for this schema",
          )
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
