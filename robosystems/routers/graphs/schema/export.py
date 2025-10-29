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
  summary="Export Declared Graph Schema",
  description="""Export the declared schema definition of an existing graph.

## What This Returns

This endpoint returns the **original schema definition** that was used to create the graph:
- The schema as it was **declared** during graph creation
- Complete node and relationship definitions
- Property types and constraints
- Schema metadata (name, version, type)

## Runtime vs Declared Schema

**Use this endpoint** (`/schema/export`) when you need:
- The original schema definition used to create the graph
- Schema in a specific format (JSON, YAML, Cypher DDL)
- Schema for documentation or version control
- Schema to replicate in another graph

**Use `/schema` instead** when you need:
- What data is ACTUALLY in the database right now
- What properties exist on real nodes (discovered from data)
- Current runtime database structure for querying

## Export Formats

### JSON Format (`format=json`)
Returns structured JSON with nodes, relationships, and properties.
Best for programmatic access and API integration.

### YAML Format (`format=yaml`)
Returns human-readable YAML with comments.
Best for documentation and configuration management.

### Cypher DDL Format (`format=cypher`)
Returns Cypher CREATE statements for recreating the schema.
Best for database migration and replication.

## Data Statistics

Set `include_data_stats=true` to include:
- Node counts by label
- Relationship counts by type
- Total nodes and relationships

This combines declared schema with runtime statistics.

This operation is included - no credit consumption required.""",
  status_code=status.HTTP_200_OK,
  responses={
    200: {
      "description": "Schema exported successfully",
      "model": SchemaExportResponse,
    },
    403: {"description": "Access denied to graph"},
    404: {"description": "Schema not found for graph"},
    500: {"description": "Failed to export schema"},
  },
)
async def export_graph_schema(
  request: Request,
  graph_id: str = Path(
    ...,
    description="The graph ID to export schema from",
    examples=["sec", "kg1a2b3c4d5"],
  ),
  format: str = Query(
    "json",
    description="Export format: json, yaml, or cypher",
    regex="^(json|yaml|cypher)$",
    openapi_examples={
      "json": {
        "summary": "JSON Format",
        "description": "Structured JSON format for programmatic access",
        "value": "json",
      },
      "yaml": {
        "summary": "YAML Format",
        "description": "Human-readable YAML format for documentation",
        "value": "yaml",
      },
      "cypher": {
        "summary": "Cypher DDL",
        "description": "Cypher CREATE statements for database migration",
        "value": "cypher",
      },
    },
  ),
  include_data_stats: bool = Query(
    False,
    description="Include statistics about actual data in the graph (node counts, relationship counts)",
    openapi_examples={
      "without_stats": {
        "summary": "Schema Only",
        "description": "Export schema definition without data statistics",
        "value": False,
      },
      "with_stats": {
        "summary": "Schema + Statistics",
        "description": "Export schema with node and relationship counts",
        "value": True,
      },
    },
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
