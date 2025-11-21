"""
Data operation MCP tools for financial reporting.

Provides tools for:
- Fact grid construction
- File ingestion with DuckDB staging
- Element mapping
- Query operations
- Graph materialization
"""

from typing import Any, Dict

from robosystems.logger import logger

MAX_ASSOCIATION_PREVIEW = 50
MAX_STAGING_QUERY_LIMIT = 10000


class BuildFactGridTool:
  """Build multidimensional fact grid from graph data."""

  def __init__(self, kuzu_client):
    self.client = kuzu_client

  def get_tool_definition(self) -> Dict[str, Any]:
    return {
      "name": "build-fact-grid",
      "description": "Construct multidimensional fact grid from graph data. Retrieves facts based on elements, periods, and optional dimensions, then builds a structured grid for analysis.",
      "inputSchema": {
        "type": "object",
        "properties": {
          "elements": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Element URIs or identifiers to include in the grid",
          },
          "periods": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Period end dates (YYYY-MM-DD format) or quarters (YYYY-QN)",
          },
          "dimensions": {
            "type": "object",
            "description": "Optional dimensional filters (e.g., segment, geography)",
            "default": {},
          },
          "rows": {
            "type": "array",
            "description": "Optional axis configuration for rows",
            "default": [],
          },
          "columns": {
            "type": "array",
            "description": "Optional axis configuration for columns",
            "default": [],
          },
        },
        "required": ["elements", "periods"],
      },
    }

  async def execute(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute fact grid construction using FactGridBuilder.

    Args:
        arguments: Tool arguments with elements, periods, dimensions

    Returns:
        Dict with fact grid data and metadata
    """
    elements = arguments.get("elements", [])
    periods = arguments.get("periods", [])
    rows = arguments.get("rows", [])
    columns = arguments.get("columns", [])

    if not elements:
      return {
        "error": "missing_elements",
        "message": "At least one element is required",
      }

    if not periods:
      return {"error": "missing_periods", "message": "At least one period is required"}

    # Validate rows and columns structure
    if rows and not isinstance(rows, list):
      return {"error": "invalid_rows", "message": "Rows must be a list"}

    if columns and not isinstance(columns, list):
      return {"error": "invalid_columns", "message": "Columns must be a list"}

    # Validate each row/column config is a dict with required fields
    for i, row in enumerate(rows):
      if not isinstance(row, dict):
        return {
          "error": "invalid_row_config",
          "message": f"Row {i} must be a dictionary with axis configuration",
        }

    for i, col in enumerate(columns):
      if not isinstance(col, dict):
        return {
          "error": "invalid_column_config",
          "message": f"Column {i} must be a dictionary with axis configuration",
        }

    try:
      graph_id = self.client.graph_id

      # Build parameterized Cypher query to prevent injection
      query = """
      MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element)
      MATCH (f)-[:FACT_HAS_PERIOD]->(p:Period)
      WHERE el.uri IN $elements
        AND p.period_end IN $periods
      OPTIONAL MATCH (f)-[:FACT_HAS_DIMENSION]->(d:Dimension)
      RETURN
        el.uri as element_id,
        p.period_end as period_end,
        f.numeric_value as value,
        f.unit_ref as unit,
        d.value as dimension_member
      """

      # Execute query through Graph API with parameters
      from robosystems.middleware.graph import get_universal_repository

      repository = await get_universal_repository(graph_id, "read")
      parameters = {"elements": elements, "periods": periods}
      result = await repository.execute_query(query, parameters)

      # Convert to DataFrame (lazy import pandas)
      import pandas as pd

      if not result:
        fact_data = pd.DataFrame()
      else:
        fact_data = pd.DataFrame(result)

      # Build fact grid using existing FactGridBuilder
      from robosystems.operations.views.fact_grid_builder import FactGridBuilder
      from robosystems.models.api.views import ViewConfig, ViewAxisConfig

      # Create view config
      row_configs = [ViewAxisConfig(**r) for r in rows] if rows else []
      column_configs = [ViewAxisConfig(**c) for c in columns] if columns else []

      view_config = ViewConfig(rows=row_configs, columns=column_configs)

      builder = FactGridBuilder()
      fact_grid = builder.build(
        fact_data=fact_data, view_config=view_config, source="mcp_tool"
      )

      logger.info(
        f"Built fact grid with {fact_grid.metadata.fact_count} facts across {fact_grid.metadata.dimension_count} dimensions"
      )

      return {
        "success": True,
        "fact_count": fact_grid.metadata.fact_count,
        "dimension_count": fact_grid.metadata.dimension_count,
        "dimensions": [
          {
            "type": d.type.value,
            "values": d.values[:10] if len(d.values) > 10 else d.values,
            "total_values": len(d.values),
          }
          for d in fact_grid.dimensions
        ],
        "construction_time_ms": fact_grid.metadata.construction_time_ms,
        "message": f"Built fact grid with {fact_grid.metadata.fact_count} facts",
      }

    except Exception as e:
      logger.error(f"Failed to build fact grid: {e}")
      import traceback

      logger.error(traceback.format_exc())
      return {"error": "construction_failed", "message": str(e)}


class IngestFileTool:
  """Upload file and stage in DuckDB for immediate querying."""

  def __init__(self, kuzu_client):
    self.client = kuzu_client

  def get_tool_definition(self) -> Dict[str, Any]:
    return {
      "name": "ingest-file",
      "description": "Upload financial data file and immediately stage it in DuckDB for querying. Returns operation_id for monitoring progress via SSE.",
      "inputSchema": {
        "type": "object",
        "properties": {
          "file_path": {
            "type": "string",
            "description": "Local file path to upload (CSV, Excel, Parquet)",
          },
          "table_name": {
            "type": "string",
            "description": "Target table name in DuckDB",
          },
          "ingest_to_graph": {
            "type": "boolean",
            "description": "If true, automatically materialize to graph database after staging",
            "default": False,
          },
        },
        "required": ["file_path", "table_name"],
      },
    }

  async def execute(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute file ingestion with DuckDB staging.

    Args:
        arguments: Tool arguments with file_path, table_name

    Returns:
        Dict with operation_id for monitoring
    """
    file_path = arguments.get("file_path")
    table_name = arguments.get("table_name")
    ingest_to_graph = arguments.get("ingest_to_graph", False)

    if not file_path:
      return {"error": "missing_file_path", "message": "file_path is required"}

    if not table_name:
      return {"error": "missing_table_name", "message": "table_name is required"}

    try:
      graph_id = self.client.graph_id

      # This would typically upload the file to S3 and trigger the staging task
      # For now, return a message that this requires client SDK support
      return {
        "error": "client_sdk_required",
        "message": "File upload requires robosystems-python-client SDK. Use client.upload_file() method instead.",
        "example": f"""
from robosystems_client import RoboSystemsClient

client = RoboSystemsClient(graph_id="{graph_id}")
result = client.upload_file(
    file_path="{file_path}",
    table_name="{table_name}",
    ingest_to_graph={ingest_to_graph}
)
print(f"Operation ID: {{result['operation_id']}}")
        """,
      }

    except Exception as e:
      logger.error(f"Failed to ingest file: {e}")
      return {"error": "ingestion_failed", "message": str(e)}


class MapElementsTool:
  """Map Chart of Accounts to XBRL taxonomy elements."""

  def __init__(self, kuzu_client):
    self.client = kuzu_client

  def get_tool_definition(self) -> Dict[str, Any]:
    return {
      "name": "map-elements",
      "description": "Map source elements (Chart of Accounts) to target taxonomy elements (US-GAAP). Retrieves existing mapping structure or creates new associations.",
      "inputSchema": {
        "type": "object",
        "properties": {
          "structure_id": {
            "type": "string",
            "description": "Mapping structure identifier (optional for retrieval)",
          },
          "source_elements": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Source element URIs to map",
          },
          "target_taxonomy": {
            "type": "string",
            "description": "Target taxonomy URI (e.g., 'us-gaap')",
            "default": "us-gaap",
          },
        },
      },
    }

  async def execute(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute element mapping retrieval or creation.

    Args:
        arguments: Tool arguments with structure_id or source_elements

    Returns:
        Dict with mapping structure and associations
    """
    structure_id = arguments.get("structure_id")
    source_elements = arguments.get("source_elements", [])
    target_taxonomy = arguments.get("target_taxonomy", "us-gaap")

    try:
      graph_id = self.client.graph_id

      # If structure_id provided, retrieve existing mapping
      if structure_id:
        from robosystems.operations.views.element_mapping import get_mapping_structure
        from robosystems.models.iam.graph import GraphTier

        # Get tier from client if available
        tier = getattr(self.client, "tier", GraphTier.KUZU_STANDARD)

        mapping_response = await get_mapping_structure(
          graph_id=graph_id, structure_id=structure_id, tier=tier
        )

        if not mapping_response:
          return {
            "error": "mapping_not_found",
            "message": f"Mapping structure '{structure_id}' not found",
          }

        logger.info(
          f"Retrieved mapping structure '{structure_id}' with {mapping_response.association_count} associations"
        )

        return {
          "success": True,
          "structure_id": structure_id,
          "association_count": mapping_response.association_count,
          "associations": [
            {
              "source": assoc.source_element,
              "target": assoc.target_element,
              "aggregation": assoc.aggregation_method.value,
              "weight": assoc.weight,
            }
            for assoc in mapping_response.structure.associations[
              :MAX_ASSOCIATION_PREVIEW
            ]
          ],
          "message": f"Retrieved {mapping_response.association_count} element associations",
        }

      # Otherwise, suggest using client SDK for mapping creation
      return {
        "info": "mapping_creation_requires_client",
        "message": "Creating new element mappings requires robosystems-python-client SDK",
        "example": f"""
from robosystems_client.extensions.element_mapping_client import ElementMappingClient

client = ElementMappingClient(graph_id="{graph_id}")
mapping = client.create_mapping_structure(
    name="My Mapping",
    source_elements={source_elements[:5] if source_elements else []},
    target_taxonomy="{target_taxonomy}"
)
        """,
      }

    except Exception as e:
      logger.error(f"Failed to map elements: {e}")
      import traceback

      logger.error(traceback.format_exc())
      return {"error": "mapping_failed", "message": str(e)}


class QueryStagingTool:
  """Query DuckDB staging tables before graph materialization."""

  def __init__(self, kuzu_client):
    self.client = kuzu_client

  def get_tool_definition(self) -> Dict[str, Any]:
    return {
      "name": "query-staging",
      "description": "Execute SQL query against DuckDB staging tables before materialization to graph. Useful for data validation and preview.",
      "inputSchema": {
        "type": "object",
        "properties": {
          "sql": {
            "type": "string",
            "description": "SQL query to execute (DuckDB SQL syntax)",
          },
          "limit": {
            "type": "integer",
            "description": "Maximum rows to return",
            "default": 100,
          },
        },
        "required": ["sql"],
      },
    }

  async def execute(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute SQL query against DuckDB staging.

    Args:
        arguments: Tool arguments with sql query

    Returns:
        Dict with query results
    """
    sql = arguments.get("sql")
    limit = arguments.get("limit", 100)

    if not sql:
      return {"error": "missing_sql", "message": "SQL query is required"}

    # Validate limit is an integer within safe bounds
    if not isinstance(limit, int) or limit < 1 or limit > MAX_STAGING_QUERY_LIMIT:
      return {
        "error": "invalid_limit",
        "message": f"Limit must be an integer between 1 and {MAX_STAGING_QUERY_LIMIT}",
      }

    try:
      graph_id = self.client.graph_id

      # Safely add LIMIT clause if not present
      sql_upper = sql.upper()
      if "LIMIT" not in sql_upper:
        # Validate limit as integer to prevent injection
        # limit is already validated above, so this is safe
        sql = f"{sql.rstrip().rstrip(';')} LIMIT {int(limit)}"

      # Execute via Graph API client
      from robosystems.graph_api.client.factory import get_graph_client

      client = await get_graph_client(graph_id, "read")
      result = await client.query_table(graph_id, sql)

      logger.info(f"Query returned {len(result.get('rows', []))} rows")

      return {
        "success": True,
        "columns": result.get("columns", []),
        "rows": result.get("rows", []),
        "row_count": len(result.get("rows", [])),
        "execution_time_ms": result.get("execution_time_ms", 0),
      }

    except Exception as e:
      logger.error(f"Failed to query staging: {e}")
      import traceback

      logger.error(traceback.format_exc())
      return {"error": "query_failed", "message": str(e)}


class MaterializeGraphTool:
  """Trigger materialization from DuckDB staging to Kuzu graph."""

  def __init__(self, kuzu_client):
    self.client = kuzu_client

  def get_tool_definition(self) -> Dict[str, Any]:
    return {
      "name": "materialize-graph",
      "description": "Trigger materialization of DuckDB staging tables to Kuzu graph database. Converts tabular data to graph nodes and relationships.",
      "inputSchema": {
        "type": "object",
        "properties": {
          "table_name": {
            "type": "string",
            "description": "Table name to materialize from DuckDB",
          },
          "file_id": {
            "type": "string",
            "description": "Optional specific file ID to materialize (for selective materialization)",
          },
        },
        "required": ["table_name"],
      },
    }

  async def execute(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute graph materialization.

    Args:
        arguments: Tool arguments with table_name

    Returns:
        Dict with operation_id for monitoring
    """
    table_name = arguments.get("table_name")
    file_id = arguments.get("file_id")

    if not table_name:
      return {"error": "missing_table_name", "message": "table_name is required"}

    # Validate table name format (alphanumeric and underscores only)
    import re

    if not re.match(r"^[a-zA-Z0-9_]+$", table_name):
      return {
        "error": "invalid_table_name",
        "message": "Table name must contain only letters, numbers, and underscores",
      }

    try:
      graph_id = self.client.graph_id

      # Get table info from database
      from robosystems.database import get_db_session
      from robosystems.models.iam import GraphTable, GraphFile

      db_gen = get_db_session()
      db = next(db_gen)
      try:
        table = (
          db.query(GraphTable)
          .filter(GraphTable.graph_id == graph_id, GraphTable.table_name == table_name)
          .first()
        )

        if not table:
          return {
            "error": "table_not_found",
            "message": f"Table '{table_name}' not found in graph {graph_id}",
          }

        # If file_id specified, materialize just that file
        if file_id:
          file = GraphFile.get_by_id(file_id, db)
          if not file:
            return {"error": "file_not_found", "message": f"File '{file_id}' not found"}

          # Queue materialization task
          from robosystems.tasks.table_operations.graph_materialization import (
            materialize_file_to_graph,
          )

          task = materialize_file_to_graph.delay(  # type: ignore[attr-defined]
            file_id=file_id, graph_id=graph_id, table_name=table_name
          )

          logger.info(f"Queued materialization task {task.id} for file {file_id}")

          return {
            "success": True,
            "task_id": task.id,
            "file_id": file_id,
            "table_name": table_name,
            "message": f"Queued materialization for file {file_id}",
          }

        # Otherwise materialize all files for the table
        files = GraphFile.get_all_for_table(table.id, db)
        staged_files = [
          f
          for f in files
          if f.duckdb_status == "staged" or f.upload_status == "uploaded"
        ]

        if not staged_files:
          return {
            "error": "no_files_to_materialize",
            "message": f"No staged files found for table '{table_name}'",
          }

        # Queue materialization for each file
        from robosystems.tasks.table_operations.graph_materialization import (
          materialize_file_to_graph,
        )

        task_ids = []
        for file in staged_files:
          task = materialize_file_to_graph.delay(  # type: ignore[attr-defined]
            file_id=file.id, graph_id=graph_id, table_name=table_name
          )
          task_ids.append(task.id)

        logger.info(
          f"Queued {len(task_ids)} materialization tasks for table '{table_name}'"
        )

        return {
          "success": True,
          "task_ids": task_ids,
          "file_count": len(staged_files),
          "table_name": table_name,
          "message": f"Queued materialization for {len(staged_files)} files",
        }

      finally:
        try:
          next(db_gen)
        except StopIteration:
          pass

    except Exception as e:
      logger.error(f"Failed to materialize graph: {e}")
      import traceback

      logger.error(traceback.format_exc())
      return {"error": "materialization_failed", "message": str(e)}
