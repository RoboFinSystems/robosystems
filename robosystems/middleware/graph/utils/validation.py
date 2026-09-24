"""Graph identifier and database-name validation."""

import re

from robosystems.logger import logger


def is_shared_repository(graph_id: str | None) -> bool:
  """Exact repository IDs only ("sec", not "sec_historical")."""
  from robosystems.config.shared_repositories import (
    is_shared_repository as _registry_check,
  )

  return _registry_check(graph_id)


def is_shared_repository_or_subgraph(graph_id: str | None) -> bool:
  from robosystems.config.shared_repositories import (
    is_shared_repository_or_subgraph as _registry_check,
  )

  return _registry_check(graph_id)


def validate_graph_id(graph_id: str) -> str:
  """Return graph_id or raise ValueError; shared repository IDs pass as-is."""
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
  """Validate the ID for a new database. No capacity check happens here."""
  validated_graph_id = validate_graph_id(graph_id)

  logger.info(f"Validated database creation for graph_id: {validated_graph_id}")
  return validated_graph_id
