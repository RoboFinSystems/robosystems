"""Schema export endpoint."""

from datetime import UTC, datetime

import yaml
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, status
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.api.graphs.schema import SchemaExportResponse
from robosystems.models.core import User

router = APIRouter()


@router.get(
  "/schema/export",
  response_model=SchemaExportResponse,
  operation_id="exportGraphSchema",
  summary="Export Declared Graph Schema",
  description="Returns the original schema definition from graph creation, not the runtime state. Set `include_data_stats=true` to add live node/relationship counts. Use `/schema` to inspect what's actually in the database.",
  responses={**RESOURCE_ERROR_RESPONSES},
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
  from robosystems.middleware.billing.enforcement import require_graph_access

  require_graph_access(graph_id, db, require_write=False)

  try:
    from robosystems.models.core import Graph, GraphSchema

    schema_record = GraphSchema.get_active_schema(graph_id, db)

    if not schema_record:
      # No schema record: reconstruct from Graph metadata.
      logger.info(
        f"No GraphSchema record found for {graph_id}, falling back to Graph metadata"
      )
      graph = Graph.get_by_id(graph_id, db)
      if not graph:
        raise HTTPException(
          status_code=status.HTTP_404_NOT_FOUND,
          detail=f"No schema found for graph {graph_id}",
        )

      logger.debug(
        f"Reconstructing schema from Graph metadata for {graph_id}: "
        f"type={graph.graph_type}, extensions={graph.schema_extensions}"
      )
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
      schema_name = schema_record.custom_schema_name or f"{graph_id}_schema"
      schema_version = str(schema_record.schema_version)
      schema_type = schema_record.schema_type

      if schema_record.schema_json:
        schema_def = schema_record.schema_json
        if "name" not in schema_def:
          schema_def["name"] = schema_name  # type: ignore[index]
        if "version" not in schema_def:
          schema_def["version"] = schema_version  # type: ignore[index]
        if "type" not in schema_def:
          schema_def["type"] = schema_type  # type: ignore[index]

        # Extension-based schemas: rebuild the full schema from the Python definitions.
        if schema_type == "extensions" and "nodes" not in schema_def:
          logger.info(
            f"Extension-based schema for {graph_id} missing nodes, "
            "reconstructing from Python definitions"
          )
          from robosystems.schemas.loader import get_schema_loader

          extensions_list = schema_def.get("extensions", [])
          logger.debug(f"Loading schema extensions for {graph_id}: {extensions_list}")
          base_schema = schema_def.get("base", "entity")

          loader = get_schema_loader(extensions=extensions_list)

          nodes = []
          for node in loader.nodes.values():
            node_dict = {
              "name": node.name,
              "properties": [
                {
                  "name": prop.name,
                  "type": prop.type,
                  "is_primary_key": prop.is_primary_key,
                }
                for prop in node.properties
              ],
            }
            for i, prop in enumerate(node.properties):
              if not prop.nullable and not prop.is_primary_key:
                node_dict["properties"][i]["is_required"] = True

            nodes.append(node_dict)

          relationships = []
          for rel in loader.relationships.values():
            rel_dict = {
              "name": rel.name,
              "from_node": rel.from_node,
              "to_node": rel.to_node,
              "properties": [
                {
                  "name": prop.name,
                  "type": prop.type,
                }
                for prop in rel.properties
              ]
              if rel.properties
              else [],
            }
            relationships.append(rel_dict)

          schema_def["nodes"] = nodes  # type: ignore[index]
          schema_def["relationships"] = relationships  # type: ignore[index]
          schema_def["extends"] = base_schema  # type: ignore[index]
          if "base" in schema_def:
            del schema_def["base"]  # type: ignore[index]
      else:
        # Construct from DDL if JSON not available
        logger.info(f"No schema_json found for {graph_id}, falling back to DDL export")
        logger.debug(
          f"Exporting DDL schema for {graph_id}: "
          f"type={schema_type}, version={schema_version}"
        )
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

    data_stats = None
    if include_data_stats:
      try:
        from robosystems.middleware.graph.router import get_universal_repository

        from .utils import get_schema_info

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
          from robosystems.schemas.runtime.custom import CustomSchemaManager

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
      exported_at=datetime.now(UTC).isoformat(),
      data_stats=data_stats,
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Schema export error: {e}", exc_info=True)
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to export schema.",
    )
