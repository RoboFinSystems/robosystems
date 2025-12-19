import re
from pathlib import Path

from fastapi import HTTPException, status

from robosystems.logger import logger


def validate_graph_id(graph_id: str) -> str:
  """
  Validate graph_id to prevent path traversal attacks.

  Args:
      graph_id: Graph database identifier to validate

  Returns:
      The validated graph_id

  Raises:
      HTTPException: If graph_id contains illegal characters or patterns
  """
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
  """
  Get validated LadybugDB database path for a graph_id.

  This function provides centralized path construction with security validation
  to prevent path traversal attacks.

  Args:
      graph_id: Graph database identifier
      base_path: Optional override for base directory (defaults to env config)

  Returns:
      Validated Path object for the LadybugDB database

  Raises:
      HTTPException: If graph_id is invalid or path is outside base directory
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


def get_duckdb_staging_path(graph_id: str, base_path: str | None = None) -> Path:
  """
  Get validated DuckDB staging database path for a graph_id.

  Args:
      graph_id: Graph database identifier
      base_path: Optional override for base directory (defaults to env config)

  Returns:
      Validated Path object for the DuckDB staging database

  Raises:
      HTTPException: If graph_id is invalid or path is outside base directory
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
