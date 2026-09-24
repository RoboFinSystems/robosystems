"""Graph categories, types and ID formats.

Graph IDs: `kg[a-f0-9]{16,}` for user graphs, a registry name (`sec`, …) for
shared repositories, and `{parent}_{name}` for subgraphs, where the name is
1-20 ASCII alphanumerics.
"""

import re
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from ...config.graph_tier import GraphTier
from ...logger import get_logger

logger = get_logger(__name__)


SHARED_REPO_WRITE_ERROR_MESSAGE = (
  "Shared repositories are read-only. File uploads and data ingestion are not allowed. "
  "Shared repositories provide reference data that cannot be modified."
)

SHARED_REPO_DELETE_ERROR_MESSAGE = (
  "Shared repositories are read-only. File deletion is not allowed. "
  "Shared repositories provide reference data that cannot be modified."
)


class GraphCategory(str, Enum):
  """High-level graph categories."""

  USER = "user"
  SHARED = "shared"
  SYSTEM = "system"


class UserGraphType(str, Enum):
  ENTITY = "entity"  # RoboSystems schema
  CUSTOM = "custom"


class AccessPattern(str, Enum):
  """Graph database access patterns (authorization level)."""

  READ_WRITE = "read_write"  # user graphs
  READ_ONLY = "read_only"  # shared repositories
  RESTRICTED = "restricted"  # system graphs


class ConnectionPattern(str, Enum):
  """How to connect to the database."""

  DIRECT_FILE = "direct_file"  # local development only
  API_WRITER = "api_writer"
  API_READER = "api_reader"  # via ALB
  API_AUTO = "api_auto"


class NodeType(str, Enum):
  """Node types in the cluster architecture."""

  WRITER = "writer"
  SHARED_MASTER = "shared_master"
  SHARED_REPLICA = "shared_replica"


class RepositoryType(str, Enum):
  """Infrastructure-level classification."""

  ENTITY = "entity"
  SHARED = "shared"


class GraphIdentity(BaseModel):
  """Complete graph identity with category and type information."""

  graph_id: str = Field(..., description="Unique graph identifier")
  category: GraphCategory = Field(..., description="High-level graph category")
  graph_type: str | None = Field(None, description="Specific type within category")
  graph_tier: GraphTier | None = Field(None, description="Graph tier for routing")
  access_pattern: AccessPattern | None = Field(
    None, description="Access pattern for this graph"
  )

  @property
  def is_user_graph(self) -> bool:
    """Check if this is a user-created graph."""
    return self.category == GraphCategory.USER

  @property
  def is_shared_repository(self) -> bool:
    """Check if this is a shared repository graph."""
    return self.category == GraphCategory.SHARED

  @property
  def is_system_graph(self) -> bool:
    """Check if this is a system internal graph."""
    return self.category == GraphCategory.SYSTEM

  def get_access_pattern(self) -> AccessPattern:
    """Get the access pattern for this graph type."""
    if self.access_pattern:
      return self.access_pattern

    if self.is_shared_repository:
      return AccessPattern.READ_ONLY
    elif self.is_user_graph:
      return AccessPattern.READ_WRITE
    else:
      return AccessPattern.RESTRICTED

  def get_routing_info(self) -> dict[str, Any]:
    """Get routing information for this graph."""
    access = self.get_access_pattern()

    if self.is_shared_repository:
      return {
        "cluster_type": "shared_writer",
        "access_mode": access.value,
        "cache_enabled": True,
        "ttl_seconds": 3600,
        "graph_tier": GraphTier.LADYBUG_STANDARD,
      }
    elif self.is_user_graph:
      return {
        "cluster_type": "user_writer",
        "access_mode": access.value,
        "cache_enabled": False,
        "requires_allocation": True,
        "graph_tier": self.graph_tier or GraphTier.LADYBUG_STANDARD,
      }
    else:
      return {
        "cluster_type": "system",
        "access_mode": access.value,
        "cache_enabled": False,
        "graph_tier": GraphTier.LADYBUG_STANDARD,
      }


class GraphTypeRegistry:
  """Registry for graph type mappings and validation."""

  @classmethod
  def _get_shared_repo_ids(cls) -> list[str]:
    from ...config.shared_repositories import get_all_repository_ids

    return get_all_repository_ids()

  @classmethod
  def get_graph_id_pattern(cls) -> str:
    """`kg` + lowercase hex, or a shared repository name.

    New IDs carry 20 hex characters; 16+ is accepted for older graphs.
    """
    repo_names = "|".join(cls._get_shared_repo_ids())
    return f"^(kg[a-f0-9]{{16,}}|{repo_names})$"

  USER_GRAPH_PATTERNS = [
    (
      re.compile(r"^kg[a-f0-9]{16,}$"),
      None,  # Type comes from metadata, not the ID.
    ),
  ]

  @classmethod
  def identify_graph(
    cls,
    graph_id: str,
    session: Any | None = None,
    graph_tier: GraphTier | None = None,
  ) -> GraphIdentity:
    """Identify a graph from its database row, else from its ID."""
    # The registry is authoritative for what is shared: shared-repo subgraph
    # rows carry is_repository=False.
    from ...config.shared_repositories import (
      is_shared_repository_or_subgraph as _is_shared_repo_or_sub,
    )

    if session:
      from ...models.core import Graph

      graph = Graph.get_by_id(graph_id, session)
      if graph:
        if graph.is_repository or _is_shared_repo_or_sub(graph_id):
          try:
            tier = (
              GraphTier(graph.graph_tier)
              if graph.graph_tier
              else GraphTier.LADYBUG_SHARED
            )
          except ValueError:
            logger.warning(
              f"Invalid graph_tier '{graph.graph_tier}' for {graph_id}, using LADYBUG_SHARED"
            )
            tier = GraphTier.LADYBUG_SHARED

          return GraphIdentity(
            graph_id=graph_id,
            category=GraphCategory.SHARED,
            graph_type=str(graph.repository_type)
            if graph.repository_type
            else "repository",
            graph_tier=tier,
            access_pattern=AccessPattern.READ_ONLY,
          )
        else:
          try:
            tier = (
              GraphTier(graph.graph_tier)
              if graph.graph_tier
              else graph_tier or GraphTier.LADYBUG_STANDARD
            )
          except ValueError:
            logger.warning(
              f"Invalid graph_tier '{graph.graph_tier}' for {graph_id}, using fallback"
            )
            tier = graph_tier or GraphTier.LADYBUG_STANDARD

          return GraphIdentity(
            graph_id=graph_id,
            category=GraphCategory.USER,
            graph_type=str(graph.graph_type)
            if graph.graph_type
            else UserGraphType.CUSTOM.value,
            graph_tier=tier,
            access_pattern=AccessPattern.READ_WRITE,
          )

    if _is_shared_repo_or_sub(graph_id):
      return GraphIdentity(
        graph_id=graph_id,
        category=GraphCategory.SHARED,
        graph_type=graph_id,
        graph_tier=GraphTier.LADYBUG_SHARED,
        access_pattern=AccessPattern.READ_ONLY,
      )

    if graph_id in ["system", "metadata", "config"]:
      return GraphIdentity(
        graph_id=graph_id,
        category=GraphCategory.SYSTEM,
        graph_type="internal",
        graph_tier=GraphTier.LADYBUG_STANDARD,
        access_pattern=AccessPattern.RESTRICTED,
      )

    return GraphIdentity(
      graph_id=graph_id,
      category=GraphCategory.USER,
      graph_type=UserGraphType.CUSTOM.value,
      graph_tier=graph_tier or GraphTier.LADYBUG_STANDARD,
      access_pattern=AccessPattern.READ_WRITE,
    )

  @classmethod
  def is_valid_graph_id(cls, graph_id: str, category: GraphCategory) -> bool:
    """Validate graph ID based on category."""
    if category == GraphCategory.SHARED:
      return graph_id in cls._get_shared_repo_ids()
    elif category == GraphCategory.USER:
      return bool(re.match(r"^[a-zA-Z0-9_-]+$", graph_id)) and len(graph_id) <= 64
    else:
      return graph_id in ["system", "metadata", "config"]

  @classmethod
  def list_shared_repositories(cls) -> list[str]:
    return cls._get_shared_repo_ids()


def _build_graph_id_pattern() -> str:
  return GraphTypeRegistry.get_graph_id_pattern()


def _build_graph_or_subgraph_id_pattern() -> str:
  """User graphs, shared repos, either with a subgraph suffix, or `library`.

  `library` is the read-only shared taxonomy library, open to any
  authenticated user.
  """
  repo_names = GraphTypeRegistry._get_shared_repo_ids()
  repo_patterns = "|".join(rf"{name}(?:_[a-zA-Z0-9]{{1,20}})?" for name in repo_names)
  return r"^(kg[a-f0-9]{16,}(?:_[a-zA-Z0-9]{1,20})?|" + repo_patterns + r"|library)$"


# Computed on first access: the registry's adapter imports circle back here.
_lazy_patterns: dict[str, str] = {}


def __getattr__(name: str) -> str:
  if name == "GRAPH_ID_PATTERN":
    if "GRAPH_ID_PATTERN" not in _lazy_patterns:
      _lazy_patterns["GRAPH_ID_PATTERN"] = _build_graph_id_pattern()
    return _lazy_patterns["GRAPH_ID_PATTERN"]
  if name == "GRAPH_OR_SUBGRAPH_ID_PATTERN":
    if "GRAPH_OR_SUBGRAPH_ID_PATTERN" not in _lazy_patterns:
      _lazy_patterns["GRAPH_OR_SUBGRAPH_ID_PATTERN"] = (
        _build_graph_or_subgraph_id_pattern()
      )
    return _lazy_patterns["GRAPH_OR_SUBGRAPH_ID_PATTERN"]
  raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# The name part only ("dev"), not the full ID.
SUBGRAPH_NAME_PATTERN = r"^[a-zA-Z0-9]{1,20}$"


def is_subgraph_id(graph_id: str) -> bool:
  """`{parent}_{name}` where the parent is a user graph or a shared repository."""
  if not graph_id or graph_id in GraphTypeRegistry._get_shared_repo_ids():
    return False
  if "_" not in graph_id:
    return False
  parts = graph_id.split("_", 1)
  parent_part = parts[0]
  subgraph_part = parts[1] if len(parts) > 1 else ""

  if not subgraph_part or len(subgraph_part) > 20:
    return False
  if not re.fullmatch(r"[a-zA-Z0-9]+", subgraph_part):
    return False

  if parent_part.startswith("kg") and len(parent_part) >= 18:
    hex_part = parent_part[2:]
    if all(c in "0123456789abcdef" for c in hex_part):
      return True

  if parent_part in GraphTypeRegistry._get_shared_repo_ids():
    return True

  return False


def parse_graph_id(graph_id: str) -> tuple[str, str | None]:
  """Split into (parent graph ID, subgraph name or None)."""
  if is_subgraph_id(graph_id):
    parts = graph_id.split("_", 1)
    return parts[0], parts[1]
  return graph_id, None


def construct_subgraph_id(parent_graph_id: str, subgraph_name: str) -> str:
  if not parent_graph_id:
    raise ValueError("parent_graph_id cannot be empty")
  if not subgraph_name:
    raise ValueError("subgraph_name cannot be empty")
  if "_" in parent_graph_id:
    raise ValueError(f"parent_graph_id cannot contain underscore: {parent_graph_id}")
  if not re.match(SUBGRAPH_NAME_PATTERN, subgraph_name):
    raise ValueError(
      f"subgraph_name must be alphanumeric (1-20 chars): {subgraph_name}"
    )

  return f"{parent_graph_id}_{subgraph_name}"
