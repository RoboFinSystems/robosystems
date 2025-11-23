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

from enum import Enum
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field
import re

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


class SharedRepositoryType(str, Enum):
  """Types of shared repository graphs."""

  SEC = "sec"  # SEC public entity filings
  INDUSTRY = "industry"  # Industry benchmarking data
  ECONOMIC = "economic"  # Economic indicators
  REGULATORY = "regulatory"  # International regulatory filings
  MARKET = "market"  # Market data and securities
  ESG = "esg"  # Environmental, Social, Governance data
  STOCK = "stock"  # Stock price history and market data
  REFERENCE = "reference"  # General reference data


class AccessPattern(str, Enum):
  """Graph database access patterns."""

  READ_WRITE = "read_write"  # Full read/write access (user graphs)
  READ_ONLY = "read_only"  # Read-only access (shared repositories)
  RESTRICTED = "restricted"  # Restricted access (system graphs)


class GraphIdentity(BaseModel):
  """Complete graph identity with category and type information."""

  graph_id: str = Field(..., description="Unique graph identifier")
  category: GraphCategory = Field(..., description="High-level graph category")
  graph_type: Optional[str] = Field(None, description="Specific type within category")
  graph_tier: Optional[GraphTier] = Field(None, description="Graph tier for routing")
  access_pattern: Optional[AccessPattern] = Field(
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

  def get_routing_info(self) -> Dict[str, Any]:
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

  # Shared repository mappings (static, well-known)
  SHARED_REPOSITORIES: Dict[str, SharedRepositoryType] = {
    "sec": SharedRepositoryType.SEC,
    "industry": SharedRepositoryType.INDUSTRY,
    "economic": SharedRepositoryType.ECONOMIC,
    "regulatory": SharedRepositoryType.REGULATORY,
    "market": SharedRepositoryType.MARKET,
    "esg": SharedRepositoryType.ESG,
    "stock": SharedRepositoryType.STOCK,
    "reference": SharedRepositoryType.REFERENCE,
  }

  @classmethod
  def get_graph_id_pattern(cls) -> str:
    """
    Build graph ID validation pattern for API endpoints.

    Format: kg + 16+ hex characters (lowercase hex from ULID generation)
    Special cases: Shared repository names from SHARED_REPOSITORIES
    Current generation:
      - Generic graphs: kg + 16 chars (ULID) = 16 chars after prefix
      - Entity graphs: kg + 14 chars (ULID) + 4 chars (entity hash) = 18 chars after prefix

    Returns:
        Regex pattern string for validating graph IDs
    """
    repo_names = "|".join(cls.SHARED_REPOSITORIES.keys())
    return f"^(kg[a-f0-9]{{16,}}|{repo_names})$"

  # Patterns for identifying graph types
  USER_GRAPH_PATTERNS = [
    (
      re.compile(r"^kg[a-f0-9]{16,}$"),
      None,  # Type determined by metadata, not ID pattern
    ),  # All user graphs use kg prefix with UUID
  ]

  @classmethod
  def identify_graph(
    cls,
    graph_id: str,
    session: Optional[Any] = None,
    graph_tier: Optional[GraphTier] = None,
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
    # Try database lookup first if session provided
    if session:
      from ...models.iam import Graph

      graph = Graph.get_by_id(graph_id, session)
      if graph:
        # Found in database - use actual metadata
        if graph.is_repository:
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
    if graph_id in cls.SHARED_REPOSITORIES:
      return GraphIdentity(
        graph_id=graph_id,
        category=GraphCategory.SHARED,
        graph_type=cls.SHARED_REPOSITORIES[graph_id].value,
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
      return graph_id in cls.SHARED_REPOSITORIES
    elif category == GraphCategory.USER:
      # User graphs must follow naming conventions
      return bool(re.match(r"^[a-zA-Z0-9_-]+$", graph_id)) and len(graph_id) <= 64
    else:
      return graph_id in ["system", "metadata", "config"]

  @classmethod
  def list_shared_repositories(cls) -> List[str]:
    """Get list of all available shared repositories."""
    return list(cls.SHARED_REPOSITORIES.keys())


# Convenience constants for API endpoint validation
# Parent graphs and shared repositories only (for subgraph management and DynamoDB registry)
GRAPH_ID_PATTERN = GraphTypeRegistry.get_graph_id_pattern()

# Parent graphs, subgraphs, and shared repositories (for general graph-scoped endpoints)
# Accepts: kg[hex]{16,} OR kg[hex]{16,}_[alphanumeric]{1,20} OR shared repo names
GRAPH_OR_SUBGRAPH_ID_PATTERN = (
  r"^(kg[a-f0-9]{16,}(?:_[a-zA-Z0-9]{1,20})?|"
  + "|".join(GraphTypeRegistry.SHARED_REPOSITORIES.keys())
  + r")$"
)

# Subgraph name pattern (for subgraph creation/management endpoints)
# Just the name part (e.g., "dev", "staging", "prod1"), not the full ID
SUBGRAPH_NAME_PATTERN = r"^[a-zA-Z0-9]{1,20}$"


def is_subgraph_id(graph_id: str) -> bool:
  """
  Check if graph_id is a subgraph ID.

  Subgraph IDs must match the pattern: kg[a-f0-9]{16,}_[a-zA-Z0-9]{1,20}
  where the parent part follows the standard graph ID format.

  Args:
      graph_id: The graph identifier to check

  Returns:
      True if graph_id is a subgraph ID, False otherwise

  Examples:
      >>> is_subgraph_id("kg0123456789abcdef_dev")
      True
      >>> is_subgraph_id("kg0123456789abcdef")
      False
      >>> is_subgraph_id("sec")
      False
      >>> is_subgraph_id("tenant1_entity")
      False
      >>> is_subgraph_id("_")
      False
  """
  if not graph_id or graph_id in GraphTypeRegistry.SHARED_REPOSITORIES:
    return False
  if "_" not in graph_id:
    return False
  parts = graph_id.split("_", 1)
  parent_part = parts[0]
  subgraph_part = parts[1] if len(parts) > 1 else ""

  # Parent must match the kg[hex]{16,} pattern
  if not parent_part.startswith("kg") or len(parent_part) < 18:
    return False

  # Validate parent is all lowercase hex after "kg"
  hex_part = parent_part[2:]
  if not all(c in "0123456789abcdef" for c in hex_part):
    return False

  # Subgraph name must be non-empty and match pattern
  if not subgraph_part or len(subgraph_part) > 20:
    return False
  if not all(c.isalnum() for c in subgraph_part):
    return False

  return True


def parse_graph_id(graph_id: str) -> tuple[str, Optional[str]]:
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
