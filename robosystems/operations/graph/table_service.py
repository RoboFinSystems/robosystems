from typing import Literal

from sqlalchemy.orm import Session

from robosystems.config import env
from robosystems.logger import logger
from robosystems.models.core import GraphTable
from robosystems.schemas.runtime.parser import parse_cypher_schema


def infer_table_type(table_name: str) -> Literal["node", "relationship"]:
  """Classify a table by its name alone.

  ALL_UPPERCASE_WITH_UNDERSCORES (``PERSON_WORKS_FOR_COMPANY``) is a
  relationship; anything else (``Company``, ``Person``) is a node.
  """
  if table_name.isupper() and "_" in table_name:
    return "relationship"
  return "node"


class TableService:
  """DuckDB staging tables, derived from the graph schema.

  Tables are schema-level and created automatically from the DDL — the
  user-facing unit is the file, not the table.
  """

  def __init__(self, session: Session):
    self.session = session

  def create_tables_from_schema(
    self, graph_id: str, user_id: str, schema_ddl: str
  ) -> list[GraphTable]:
    """Create one staging table per node and relationship type in the DDL.

    Idempotent — an existing table is returned as-is. Rows are added to the
    session without committing, so the caller owns the transaction. Raises
    ValueError on unparseable DDL.
    """
    from robosystems.schemas.runtime.parser import parse_relationship_types

    logger.info(f"Auto-creating DuckDB tables from schema for graph {graph_id}")

    try:
      node_types = parse_cypher_schema(schema_ddl)
      relationship_types = parse_relationship_types(schema_ddl)
    except Exception as e:
      logger.error(f"Failed to parse schema DDL for graph {graph_id}: {e}")
      raise ValueError(f"Invalid schema DDL: {e!s}") from e

    if not node_types and not relationship_types:
      logger.warning(
        f"No node or relationship types found in schema for graph {graph_id}"
      )
      return []

    logger.info(
      f"Found {len(node_types)} node types in schema: {[n.name for n in node_types]}"
    )
    logger.info(
      f"Found {len(relationship_types)} relationship types in schema: {relationship_types}"
    )

    created_tables = []

    for node_type in node_types:
      existing_table = GraphTable.get_by_name(graph_id, node_type.name, self.session)
      if existing_table:
        logger.info(
          f"Table {node_type.name} already exists for graph {graph_id}, skipping"
        )
        created_tables.append(existing_table)
        continue

      table = GraphTable.create(
        graph_id=graph_id,
        table_name=node_type.name,
        table_type="node",
        schema_json=node_type.to_dict(),
        target_node_type=node_type.name,
        session=self.session,
        commit=False,
      )

      logger.info(
        f"Created DuckDB staging table '{node_type.name}' (node) for graph {graph_id} "
        f"with {len(node_type.properties)} properties"
      )

      created_tables.append(table)

    for rel_type in relationship_types:
      existing_table = GraphTable.get_by_name(graph_id, rel_type, self.session)
      if existing_table:
        logger.info(f"Table {rel_type} already exists for graph {graph_id}, skipping")
        created_tables.append(existing_table)
        continue

      table = GraphTable.create(
        graph_id=graph_id,
        table_name=rel_type,
        table_type="relationship",
        schema_json={"name": rel_type, "properties": []},
        target_node_type=None,
        session=self.session,
        commit=False,
      )

      logger.info(
        f"Created DuckDB staging table '{rel_type}' (relationship) for graph {graph_id}"
      )

      created_tables.append(table)

    logger.info(
      f"Auto-created {len(created_tables)} DuckDB staging tables for graph {graph_id}"
    )

    return created_tables

  def get_s3_pattern_for_table(
    self, graph_id: str, table_name: str, user_id: str
  ) -> str:
    """S3 glob matching every parquet file staged for one table."""
    bucket = env.USER_DATA_BUCKET
    return f"s3://{bucket}/user-staging/{user_id}/{graph_id}/{table_name}/**/*.parquet"

  def delete_table(self, graph_id: str, table_name: str) -> None:
    """Delete a staging table and every file record attached to it.

    Admin-only, for schema changes. Commits. The S3 objects themselves are not
    removed — only the file records.
    """
    logger.warning(
      f"Deleting table {table_name} from graph {graph_id} - "
      f"this should only happen on schema changes!"
    )

    table = GraphTable.get_by_name(graph_id, table_name, self.session)
    if not table:
      logger.warning(f"Table {table_name} not found for graph {graph_id}")
      return

    from robosystems.models.core import GraphFile

    files = GraphFile.get_all_for_table(table.id, self.session)
    for file in files:
      self.session.delete(file)

    self.session.delete(table)
    self.session.commit()

    logger.info(f"Deleted table {table_name} and {len(files)} associated files")
