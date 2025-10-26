from typing import List
from sqlalchemy.orm import Session

from robosystems.models.iam import GraphTable
from robosystems.schemas.parser import parse_cypher_schema
from robosystems.config import env
from robosystems.logger import logger


class TableService:
  """
  Service for managing DuckDB staging tables tied to graph schema.

  Tables are schema-level constructs that map to graph node types.
  Users manage files, not tables.
  """

  def __init__(self, session: Session):
    self.session = session

  def create_tables_from_schema(
    self, graph_id: str, user_id: str, schema_ddl: str
  ) -> List[GraphTable]:
    """
    Automatically create DuckDB staging tables from graph schema.

    Parses the schema DDL to extract node types and creates a
    corresponding DuckDB external table for each node type.

    Args:
        graph_id: Graph database identifier
        user_id: User who owns the graph
        schema_ddl: Cypher DDL defining the graph schema

    Returns:
        List of created GraphTable objects
    """
    logger.info(f"Auto-creating DuckDB tables from schema for graph {graph_id}")

    # Parse schema to extract node types
    try:
      node_types = parse_cypher_schema(schema_ddl)
    except Exception as e:
      logger.error(f"Failed to parse schema DDL for graph {graph_id}: {e}")
      raise ValueError(f"Invalid schema DDL: {str(e)}") from e

    if not node_types:
      logger.warning(f"No node types found in schema for graph {graph_id}")
      return []

    logger.info(
      f"Found {len(node_types)} node types in schema: {[n.name for n in node_types]}"
    )

    created_tables = []

    for node_type in node_types:
      # Check if table already exists
      existing_table = GraphTable.get_by_name(graph_id, node_type.name, self.session)
      if existing_table:
        logger.info(
          f"Table {node_type.name} already exists for graph {graph_id}, skipping"
        )
        created_tables.append(existing_table)
        continue

      # Create GraphTable record
      # DuckDB will use glob pattern: s3://bucket/user-staging/{user_id}/{graph_id}/{table_name}/*.parquet
      table = GraphTable.create(
        graph_id=graph_id,
        table_name=node_type.name,
        table_type="external",
        schema_json=node_type.to_dict(),
        target_node_type=node_type.name,
        session=self.session,
        commit=False,
      )

      logger.info(
        f"Created DuckDB staging table '{node_type.name}' for graph {graph_id} "
        f"with {len(node_type.properties)} properties"
      )

      created_tables.append(table)

    logger.info(
      f"Auto-created {len(created_tables)} DuckDB staging tables for graph {graph_id}"
    )

    return created_tables

  def get_s3_pattern_for_table(
    self, graph_id: str, table_name: str, user_id: str
  ) -> str:
    """
    Generate S3 glob pattern for a table's files.

    Args:
        graph_id: Graph database identifier
        table_name: Table name (matches node type)
        user_id: User who owns the graph

    Returns:
        S3 glob pattern for all files in this table
    """
    bucket = env.AWS_S3_BUCKET
    return f"s3://{bucket}/user-staging/{user_id}/{graph_id}/{table_name}/**/*.parquet"

  def delete_table(self, graph_id: str, table_name: str) -> None:
    """
    Delete a table (should only be used on schema changes).

    This is an admin operation. Normal users manage files, not tables.

    Args:
        graph_id: Graph database identifier
        table_name: Table name to delete
    """
    logger.warning(
      f"Deleting table {table_name} from graph {graph_id} - "
      f"this should only happen on schema changes!"
    )

    table = GraphTable.get_by_name(graph_id, table_name, self.session)
    if not table:
      logger.warning(f"Table {table_name} not found for graph {graph_id}")
      return

    # Delete associated files first
    from robosystems.models.iam import GraphFile

    files = GraphFile.get_all_for_table(table.id, self.session)
    for file in files:
      self.session.delete(file)

    # Delete table
    self.session.delete(table)
    self.session.commit()

    logger.info(f"Deleted table {table_name} and {len(files)} associated files")
