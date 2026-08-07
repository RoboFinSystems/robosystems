"""Graph ID validation utilities.

Functions for validating graph identifiers and database names.
"""

import re

from robosystems.logger import logger


def is_shared_repository(graph_id: str | None) -> bool:
  """Check if the given graph_id refers to a registered shared repository.

  Note: This checks exact parent repository IDs only (e.g., "sec").
  For checking subgraphs too (e.g., "sec_historical"), use
  ``is_shared_repository_or_subgraph`` from the config registry.
  """
  from robosystems.config.shared_repositories import (
    is_shared_repository as _registry_check,
  )

  return _registry_check(graph_id)


def is_shared_repository_or_subgraph(graph_id: str | None) -> bool:
  """Check if the given graph_id is a shared repository OR a subgraph of one.

  This checks both parent IDs (e.g., "sec") and subgraph IDs (e.g., "sec_historical").
  """
  from robosystems.config.shared_repositories import (
    is_shared_repository_or_subgraph as _registry_check,
  )

  return _registry_check(graph_id)


def validate_graph_id(graph_id: str) -> str:
  """Validate graph_id meets database naming requirements.

  Graph database names must:
  - Not be empty
  - Be at most 64 characters long
  - Contain only alphanumeric characters, underscores, and hyphens
  - Not be reserved names
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
  """Validate that a new database can be created for the given graph_id.

  This combines graph_id validation with database limit checking.
  """
  validated_graph_id = validate_graph_id(graph_id)

  logger.info(f"Validated database creation for graph_id: {validated_graph_id}")
  return validated_graph_id
