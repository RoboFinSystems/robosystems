"""
Database operation utilities.

Functions for database name resolution, creation, and management.
"""

import asyncio
from typing import Any

import redis
from httpx import HTTPStatusError

from robosystems.config import env
from robosystems.graph_api.client import GraphClient
from robosystems.graph_api.client.exceptions import GraphClientError
from robosystems.logger import logger

from .validation import is_shared_repository, validate_graph_id


def is_multitenant_mode() -> bool:
  """
  Check if the system is running in multi-tenant mode.

  For graph databases, multi-tenant is always enabled via clusters.

  Returns:
      bool: True if multi-tenant mode is enabled, False otherwise
  """
  return True


def get_database_name(graph_id: str | None = None) -> str:
  """
  Get the appropriate database name based on multi-tenant mode and graph_id.

  Special handling for shared repositories - always routes to their specific
  database names regardless of multi-tenant mode.

  Args:
      graph_id: Optional graph identifier for multi-tenant mode

  Returns:
      str: Database name to use
  """
  if graph_id and is_shared_repository(graph_id):
    repository_name = get_repository_database_name(graph_id)
    logger.debug(f"Routing to shared repository database: '{repository_name}'")
    return repository_name

  if is_multitenant_mode():
    if graph_id and graph_id != "default":
      validated_graph_id = validate_graph_id(graph_id)
      logger.debug(
        f"Multi-tenant mode: using database '{validated_graph_id}' for graph_id '{graph_id}'"
      )
      return validated_graph_id
    else:
      logger.debug(
        "Multi-tenant mode: no graph_id provided or 'default' used, using default database"
      )
      return "default"
  else:
    logger.debug("Single-tenant mode: using default database")
    return "default"


def get_repository_database_name(repository_id: str) -> str:
  """
  Get the database name for a shared repository.

  Args:
      repository_id: Repository identifier (e.g., 'sec', 'industry')

  Returns:
      str: Database name for the repository

  Raises:
      ValueError: If repository_id is not a known shared repository
  """
  from ..types import GraphTypeRegistry

  if repository_id not in GraphTypeRegistry.SHARED_REPOSITORIES:
    raise ValueError(f"Unknown shared repository: {repository_id}")

  return GraphTypeRegistry.SHARED_REPOSITORIES[repository_id]


def list_shared_repositories() -> list[str]:
  """
  Get a list of all known shared repository identifiers.

  Returns:
      List[str]: List of repository IDs
  """
  from ..types import GraphTypeRegistry

  return GraphTypeRegistry.list_shared_repositories()


def log_database_operation(
  operation: str, database_name: str, graph_id: str | None = None
) -> None:
  """
  Log database operations for observability.

  Args:
      operation: Description of the operation being performed
      database_name: Name of the database being accessed
      graph_id: Optional graph identifier
  """
  if graph_id:
    logger.info(f"{operation} - Database: {database_name}, Graph ID: {graph_id}")
  else:
    logger.info(f"{operation} - Database: {database_name}")


def get_database_path_for_graph(graph_id: str) -> str:
  """
  Get the database file path for a graph (LadybugDB single-file format).

  Args:
      graph_id: Graph identifier

  Returns:
      str: Database file path (.lbug extension)
  """
  from robosystems.operations.lbug.path_utils import get_lbug_database_path

  db_path = get_lbug_database_path(graph_id)
  return str(db_path)


def get_max_databases_per_node() -> int:
  """
  Get the maximum number of databases per graph node.

  Returns:
      int: Maximum databases per node
  """
  return env.LBUG_MAX_DATABASES_PER_NODE


async def ensure_database_with_schema(
  graph_url: str,
  db_name: str,
  schema_name: str,
  api_key: str | None = None,
  lock_timeout: int = 300,
) -> dict[str, Any]:
  """
  Ensure database exists with proper schema.

  Args:
      graph_url: Graph API endpoint URL
      db_name: Database name to create
      schema_name: Schema name to apply (e.g., "sec", "entity")
      api_key: Optional API key for authentication
      lock_timeout: Unused parameter (kept for backwards compatibility)

  Returns:
      Dict with creation status and database info

  Raises:
      RuntimeError: If creation fails
  """
  headers = {}
  if api_key:
    headers["Authorization"] = f"Bearer {api_key}"

  async with GraphClient(base_url=graph_url, headers=headers) as client:
    try:
      db_info = await client.get_database_info(db_name)
      if db_info.get("is_healthy", False):
        logger.info(f"Database {db_name} already exists and is ready")
        return {"created": False, "status": "exists", "database": db_info}
    except GraphClientError as e:
      if "not found" in str(e).lower():
        logger.info(f"Database {db_name} not found, will create")
      else:
        logger.error(f"LadybugClient error checking database: {e}")
        raise RuntimeError(f"Failed to check database existence: {e}")
    except HTTPStatusError as e:
      if e.response.status_code == 404:
        logger.info(f"Database {db_name} not found, will create")
      else:
        logger.error(
          f"HTTP error checking database: {e.response.status_code} - {e.response.text}"
        )
        raise RuntimeError(f"Failed to check database existence: {e}")
    except Exception as e:
      logger.error(f"Unexpected error checking database: {type(e).__name__}: {e}")
      raise

    schema_type = "custom"
    repository_name = None

    if is_shared_repository(db_name):
      schema_type = "shared"
      repository_name = db_name
    elif schema_name in ["sec", "sec_xbrl", "sec_filings"]:
      schema_type = "shared"
      repository_name = "sec"
    elif schema_name in ["entity", "entity_financials"]:
      schema_type = "entity"
    elif schema_name in ["industry", "economic"]:
      schema_type = "shared"
      repository_name = schema_name

    try:
      logger.info(
        f"Creating database {db_name} with schema type: {schema_type}, "
        f"repository: {repository_name}"
      )

      create_result = await client.create_database(
        graph_id=db_name, schema_type=schema_type, repository_name=repository_name
      )

      logger.info(f"Successfully created database {db_name}")

      max_wait = 30
      wait_interval = 2
      total_waited = 0

      while total_waited < max_wait:
        try:
          db_info = await client.get_database_info(db_name)
          if db_info.get("is_healthy", False):
            logger.info(f"Database {db_name} is ready")
            return {
              "created": True,
              "status": "created",
              "database": db_info,
              "create_result": create_result,
            }
        except Exception as e:
          logger.debug(f"Database not ready yet: {e}")

        await asyncio.sleep(wait_interval)
        total_waited += wait_interval

      raise RuntimeError(
        f"Database {db_name} creation timed out after {max_wait} seconds"
      )

    except Exception as e:
      logger.error(f"Failed to create database {db_name}: {e}")
      raise RuntimeError(f"Database creation failed: {e}")


def ensure_database_with_schema_sync(
  graph_url: str,
  db_name: str,
  schema_name: str,
  api_key: str | None = None,
  redis_client: redis.Redis | None = None,
) -> dict[str, Any]:
  """
  Synchronous wrapper for ensure_database_with_schema.

  Args:
      graph_url: Graph API endpoint URL
      db_name: Database name to create
      schema_name: Schema name to apply
      api_key: Optional API key for authentication
      redis_client: Optional Redis client for locking (unused, for compatibility)

  Returns:
      Dict with creation status and database info
  """
  loop = asyncio.new_event_loop()
  asyncio.set_event_loop(loop)
  try:
    return loop.run_until_complete(
      ensure_database_with_schema(
        graph_url=graph_url, db_name=db_name, schema_name=schema_name, api_key=api_key
      )
    )
  finally:
    loop.close()
