"""
Common types and enums for the graph middleware.

This module defines the type system for distinguishing between different categories
and types of graphs in the system, providing clear separation between user-created
graphs and shared repository graphs.

Graph ID Formats:
    - Parent graphs: kg[a-f0-9]{16,} (e.g., kg1234567890abcdef)
    - Subgraph IDs: {parent_id}_{subgraph_name} (e.g., kg1234567890abcdef_dev)
    - Shared repositories: Fixed names (sec, industry, economic)

Subgraph Naming Rules:
    - Alphanumeric characters only: [a-zA-Z0-9]
    - Length: 1-20 characters
    - No special characters, hyphens, or underscores
    - Case-sensitive

Examples:
    >>> is_subgraph_id("kg1234567890abcdef_dev")
    True
    >>> parse_graph_id("kg1234567890abcdef_staging")
    ("kg1234567890abcdef", "staging")
    >>> construct_subgraph_id("kg1234567890abcdef", "prod")
    "kg1234567890abcdef_prod"
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

  USER = "user"  # User-created graphs (customer data)
  SHARED = "shared"  # Shared repository graphs (public/reference data)
  SYSTEM = "system"  # System internal graphs (metadata, config)


class UserGraphType(str, Enum):
  """Types of user-created graphs."""

  ENTITY = "entity"  # Business entity graphs using RoboSystems schema
  CUSTOM = "custom"  # Custom schema graphs


class AccessPattern(str, Enum):
  """Graph database access patterns (authorization level)."""

  READ_WRITE = "read_write"  # Full read/write access (user graphs)
  READ_ONLY = "read_only"  # Read-only access (shared repositories)
  RESTRICTED = "restricted"  # Restricted access (system graphs)


class ConnectionPattern(str, Enum):
  """Database connection patterns (how to connect to the database)."""

  DIRECT_FILE = "direct_file"  # Direct file access (local development only)
  API_WRITER = "api_writer"  # API access to writer node
  API_READER = "api_reader"  # API access to reader node (via ALB)
  API_AUTO = "api_auto"  # API access with automatic routing


class NodeType(str, Enum):
  """Node types in the cluster architecture."""

  WRITER = "writer"  # Writer for all graphs (entity and shared repositories)
  SHARED_MASTER = "shared_master"  # Shared repository master writer
  SHARED_REPLICA = "shared_replica"  # Shared repository read-only replica


class RepositoryType(str, Enum):
  """Types of repositories (infrastructure-level classification)."""

  ENTITY = "entity"  # User/entity-specific graphs
  SHARED = "shared"  # Shared repositories (SEC, industry, etc.)


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
        "ttl_seconds": 3600,  # Cache for 1 hour
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
    """Get shared repository IDs from the registry."""
    from ...config.shared_repositories import get_all_repository_ids

    return get_all_repository_ids()

  @classmethod
  def get_graph_id_pattern(cls) -> str:
    """
    Build graph ID validation pattern for API endpoints.

    Format: kg + 20 hex characters (lowercase hex from ULID generation)
    Special cases: Shared repository names from registry
    Regex accepts 16+ chars to remain compatible with older graph IDs.

    Returns:
        Regex pattern string for validating graph IDs
    """
    repo_names = "|".join(cls._get_shared_repo_ids())
    return f"^(kg[a-f0-9]{{16,}}|{repo_names})$"

  # Patterns for identifying graph types
  USER_GRAPH_PATTERNS = [
    (
      re.compile(r"^kg[a-f0-9]{16,}$"),
      None,  # Type determined by metadata, not ID pattern
    ),  # All user graphs use kg prefix with ULID hex
  ]

  @classmethod
  def identify_graph(
    cls,
    graph_id: str,
    session: Any | None = None,
    graph_tier: GraphTier | None = None,
  ) -> GraphIdentity:
    """
    Identify a graph from its ID using database lookup.

    Args:
        graph_id: The graph identifier
        session: Optional database session for lookup
        graph_tier: Optional graph tier override

    Returns:
        GraphIdentity with category and type information
    """
    # The registry is authoritative for what is shared. Shared-repo subgraph
    # rows are created with is_repository=False (subgraph_service), so trusting
    # the row alone classified sec_historical as a READ_WRITE user graph.
    from ...config.shared_repositories import (
      is_shared_repository_or_subgraph as _is_shared_repo_or_sub,
    )

    # Try database lookup first if session provided
    if session:
      from ...models.core import Graph

      graph = Graph.get_by_id(graph_id, session)
      if graph:
        # Found in database - use actual metadata
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
          # User graph
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

    # Fallback: pattern-based detection (for cases without session)
    # Check if it's a known shared repository
    if _is_shared_repo_or_sub(graph_id):
      return GraphIdentity(
        graph_id=graph_id,
        category=GraphCategory.SHARED,
        graph_type=graph_id,
        graph_tier=GraphTier.LADYBUG_SHARED,
        access_pattern=AccessPattern.READ_ONLY,
      )

    # Check if it's a system graph
    if graph_id in ["system", "metadata", "config"]:
      return GraphIdentity(
        graph_id=graph_id,
        category=GraphCategory.SYSTEM,
        graph_type="internal",
        graph_tier=GraphTier.LADYBUG_STANDARD,
        access_pattern=AccessPattern.RESTRICTED,
      )

    # Default to user graph
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
      # User graphs must follow naming conventions
      return bool(re.match(r"^[a-zA-Z0-9_-]+$", graph_id)) and len(graph_id) <= 64
    else:
      return graph_id in ["system", "metadata", "config"]

  @classmethod
  def list_shared_repositories(cls) -> list[str]:
    """Get list of all available shared repositories."""
    return cls._get_shared_repo_ids()


def _build_graph_id_pattern() -> str:
  """Build graph ID pattern from registry (called lazily on first access)."""
  return GraphTypeRegistry.get_graph_id_pattern()


def _build_graph_or_subgraph_id_pattern() -> str:
  """Build graph-or-subgraph ID pattern from registry (called lazily on first access).

  Supports:
  - User graphs: kg[hex]{16,}
  - User subgraphs: kg[hex]{16,}_[alnum]{1,20}
  - Shared repos: sec, industry, etc.
  - Shared repo subgraphs: sec_historical, etc.
  - Taxonomy library sentinel: `library` — routes to the shared taxonomy
    library. Read-only, accessible to any authenticated user. Currently
    backed by the extensions DB `public` schema; schema name is an
    implementation detail, not part of the API identity.
  """
  repo_names = GraphTypeRegistry._get_shared_repo_ids()
  # Build pattern for shared repos with optional subgraph suffix
  repo_patterns = "|".join(rf"{name}(?:_[a-zA-Z0-9]{{1,20}})?" for name in repo_names)
  return r"^(kg[a-f0-9]{16,}(?:_[a-zA-Z0-9]{1,20})?|" + repo_patterns + r"|library)$"


# Lazy pattern cache — patterns are computed on first access to avoid circular
# imports (the registry triggers adapter imports that circle back here).
_lazy_patterns: dict[str, str] = {}


def __getattr__(name: str) -> str:
  """PEP 562 module-level __getattr__ for lazy pattern computation."""
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


# Subgraph name pattern (for subgraph creation/management endpoints)
# Just the name part (e.g., "dev", "staging", "prod1"), not the full ID
SUBGRAPH_NAME_PATTERN = r"^[a-zA-Z0-9]{1,20}$"


def is_subgraph_id(graph_id: str) -> bool:
  """
  Check if graph_id is a subgraph ID.

  Subgraph IDs have a parent_subgraph format where the parent is either
  a user graph (kg[hex]{16,}) or a shared repository ID.

  Args:
      graph_id: The graph identifier to check

  Returns:
      True if graph_id is a subgraph ID, False otherwise

  Examples:
      >>> is_subgraph_id("kg0123456789abcdef_dev")
      True
      >>> is_subgraph_id("sec_historical")
      True
      >>> is_subgraph_id("kg0123456789abcdef")
      False
      >>> is_subgraph_id("sec")
      False
      >>> is_subgraph_id("_")
      False
  """
  if not graph_id or graph_id in GraphTypeRegistry._get_shared_repo_ids():
    return False
  if "_" not in graph_id:
    return False
  parts = graph_id.split("_", 1)
  parent_part = parts[0]
  subgraph_part = parts[1] if len(parts) > 1 else ""

  # Subgraph name must be non-empty and match pattern
  if not subgraph_part or len(subgraph_part) > 20:
    return False
  if not all(c.isalnum() for c in subgraph_part):
    return False

  # Parent must match the kg[hex]{16,} pattern OR be a shared repo
  if parent_part.startswith("kg") and len(parent_part) >= 18:
    # Validate parent is all lowercase hex after "kg"
    hex_part = parent_part[2:]
    if all(c in "0123456789abcdef" for c in hex_part):
      return True

  # Check if parent is a shared repository
  if parent_part in GraphTypeRegistry._get_shared_repo_ids():
    return True

  return False


def parse_graph_id(graph_id: str) -> tuple[str, str | None]:
  """
  Parse graph_id into parent graph ID and optional subgraph name.

  Args:
      graph_id: The graph identifier to parse

  Returns:
      Tuple of (parent_graph_id, subgraph_name)
      - For parent graphs: (graph_id, None)
      - For subgraphs: (parent_id, subgraph_name)
      - For shared repos: (graph_id, None)

  Examples:
      >>> parse_graph_id("kg0123456789abcdef_dev")
      ("kg0123456789abcdef", "dev")
      >>> parse_graph_id("kg0123456789abcdef")
      ("kg0123456789abcdef", None)
      >>> parse_graph_id("sec")
      ("sec", None)
      >>> parse_graph_id("sec_historical")
      ("sec", "historical")
  """
  if is_subgraph_id(graph_id):
    parts = graph_id.split("_", 1)
    return parts[0], parts[1]
  return graph_id, None


def construct_subgraph_id(parent_graph_id: str, subgraph_name: str) -> str:
  """
  Construct a full subgraph ID from parent graph ID and subgraph name.

  Args:
      parent_graph_id: The parent graph identifier
      subgraph_name: The subgraph name

  Returns:
      Full subgraph ID in format: parent_id_subgraph_name

  Examples:
      >>> construct_subgraph_id("kg0123456789abcdef", "dev")
      "kg0123456789abcdef_dev"

  Raises:
      ValueError: If parent_graph_id or subgraph_name are invalid
  """
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
