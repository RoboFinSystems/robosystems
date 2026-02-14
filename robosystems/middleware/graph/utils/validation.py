"""
Graph ID validation utilities.

Functions for validating graph identifiers and database names.
"""

import re

from robosystems.logger import logger


def is_shared_repository(graph_id: str | None) -> bool:
  """
  Check if the given graph_id refers to a registered shared repository.

  Note: This checks exact parent repository IDs only (e.g., "sec").
  For checking subgraphs too (e.g., "sec_historical"), use
  ``is_shared_repository_or_subgraph`` from the config registry.

  Args:
      graph_id: Graph identifier to check

  Returns:
      bool: True if this is a shared repository
  """
  from robosystems.config.shared_repositories import (
    is_shared_repository as _registry_check,
  )

  return _registry_check(graph_id)


def validate_graph_id(graph_id: str) -> str:
  """
  Validate graph_id meets database naming requirements.

  Graph database names must:
  - Not be empty
  - Be at most 64 characters long
  - Contain only alphanumeric characters, underscores, and hyphens
  - Not be reserved names

  Args:
      graph_id: The graph identifier to validate

  Returns:
      str: The validated graph_id

  Raises:
      ValueError: If graph_id doesn't meet requirements
  """
  if not graph_id:
    raise ValueError("graph_id cannot be empty")

  if is_shared_repository(graph_id):
    return graph_id

  if ".." in graph_id or "/" in graph_id or "\\" in graph_id:
    raise ValueError("graph_id contains invalid path characters")

  if len(graph_id) > 64:
    raise ValueError(f"graph_id too long: {len(graph_id)} characters (max 64)")

  if not re.match(r"^[a-zA-Z0-9_-]+$", graph_id):
    raise ValueError(
      "graph_id contains invalid characters (use only alphanumeric, underscore, hyphen)"
    )

  if graph_id.startswith("-") or graph_id.endswith("-"):
    raise ValueError("graph_id cannot start or end with hyphen")

  if graph_id.startswith("_") or graph_id.endswith("_"):
    raise ValueError("graph_id cannot start or end with underscore")

  reserved_names = {"system", "ladybug", "default"}
  if graph_id.lower() in reserved_names:
    raise ValueError(f"graph_id '{graph_id}' is a reserved name")

  return graph_id


def validate_database_creation(graph_id: str) -> str:
  """
  Validate that a new database can be created for the given graph_id.

  This combines graph_id validation with database limit checking.

  Args:
      graph_id: The graph identifier for the new database

  Returns:
      str: The validated graph_id

  Raises:
      ValueError: If graph_id is invalid
      RuntimeError: If database limit would be exceeded
  """
  validated_graph_id = validate_graph_id(graph_id)

  logger.info(f"Validated database creation for graph_id: {validated_graph_id}")
  return validated_graph_id
