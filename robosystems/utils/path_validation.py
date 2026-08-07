"""
Build on-disk paths from user-supplied identifiers without letting them escape.

`graph_id` and `table_name` reach the filesystem from request input, so every
path here is constructed in two steps: reject identifiers that are not plain
names, then resolve the result and confirm it still sits under its base
directory. Some of these paths are passed to `shutil.rmtree`, so a traversal is
a delete primitive, not just a read.

All validators raise `HTTPException` (400) rather than returning a flag, so
callers cannot proceed on an unvalidated value by forgetting to check.
"""

import re
from pathlib import Path

from fastapi import HTTPException, status

from robosystems.logger import logger


def validate_graph_id(graph_id: str) -> str:
  """Return `graph_id` if it is alphanumeric plus underscore and hyphen."""
  if not graph_id:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST, detail="graph_id cannot be empty"
    )

  if ".." in graph_id or "/" in graph_id or "\\" in graph_id or "\x00" in graph_id:
    logger.warning(f"Path traversal attempt detected in graph_id: {graph_id[:50]}")
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Invalid graph_id: contains illegal characters",
    )

  if not re.match(r"^[a-zA-Z0-9_-]+$", graph_id):
    logger.warning(f"Invalid graph_id format: {graph_id[:50]}")
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Invalid graph_id format: only alphanumeric, underscore, and hyphen allowed",
    )

  return graph_id


def get_lbug_database_path(graph_id: str, base_path: str | None = None) -> Path:
  """Path to a graph's LadybugDB database file.

  `base_path` defaults to `env.LBUG_DATABASE_PATH`.
  """
  from robosystems.config import env

  validated_id = validate_graph_id(graph_id)

  base = Path(base_path if base_path else env.LBUG_DATABASE_PATH)
  db_path = base / f"{validated_id}.lbug"

  try:
    resolved_path = db_path.resolve()
    resolved_base = base.resolve()
    resolved_path.relative_to(resolved_base)
  except (ValueError, RuntimeError) as e:
    logger.error(
      f"Path validation failed for graph_id {graph_id}: {e}",
      extra={"graph_id": graph_id, "base_path": str(base)},
    )
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Invalid graph_id: path outside base directory",
    )

  return db_path


def validate_table_name(table_name: str) -> str:
  """Return `table_name` if it is a bare SQL-style identifier.

  Stricter than `validate_graph_id`: must start with a letter or underscore.
  """
  if not table_name:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST, detail="table_name cannot be empty"
    )

  if (
    ".." in table_name
    or "/" in table_name
    or "\\" in table_name
    or "\x00" in table_name
  ):
    logger.warning(f"Path traversal attempt detected in table_name: {table_name[:50]}")
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Invalid table_name: contains illegal characters",
    )

  if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table_name):
    logger.warning(f"Invalid table_name format: {table_name[:50]}")
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Invalid table_name format: must start with a letter or underscore and "
      "contain only alphanumeric characters and underscores",
    )

  return table_name


def get_lance_index_path(
  graph_id: str,
  table_name: str | None = None,
  base_path: str | None = None,
) -> Path:
  """Path to a graph's LanceDB index directory, or one table's within it.

  Both components are validated. `base_path` defaults to `env.LANCE_INDEX_PATH`.
  """
  from robosystems.config import env

  validated_id = validate_graph_id(graph_id)

  base = Path(base_path if base_path else env.LANCE_INDEX_PATH)
  index_path = base / validated_id
  if table_name is not None:
    index_path = index_path / validate_table_name(table_name)

  try:
    resolved_path = index_path.resolve()
    resolved_base = base.resolve()
    resolved_path.relative_to(resolved_base)
  except (ValueError, RuntimeError) as e:
    logger.error(
      f"Lance path validation failed for graph_id {graph_id}: {e}",
      extra={"graph_id": graph_id, "base_path": str(base)},
    )
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Invalid graph_id: path outside base directory",
    )

  return index_path


def get_duckdb_staging_path(graph_id: str, base_path: str | None = None) -> Path:
  """Path to a graph's DuckDB staging database file.

  `base_path` defaults to `env.DUCKDB_STAGING_PATH`.
  """
  from robosystems.config import env

  validated_id = validate_graph_id(graph_id)

  base = Path(base_path if base_path else env.DUCKDB_STAGING_PATH)
  db_path = base / f"{validated_id}.duckdb"

  try:
    resolved_path = db_path.resolve()
    resolved_base = base.resolve()
    resolved_path.relative_to(resolved_base)
  except (ValueError, RuntimeError) as e:
    logger.error(
      f"DuckDB path validation failed for graph_id {graph_id}: {e}",
      extra={"graph_id": graph_id, "base_path": str(base)},
    )
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Invalid graph_id: path outside base directory",
    )

  return db_path
