"""
Common types and enums for the graph middleware.

This module defines the type system for distinguishing between different categories
and types of graphs in the system, providing clear separation between user-created
graphs and shared repository graphs.
"""

from enum import Enum
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field
import re

from ...models.iam.graph_credits import GraphTier


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
        "graph_tier": GraphTier.KUZU_STANDARD,
      }
    elif self.is_user_graph:
      return {
        "cluster_type": "user_writer",
        "access_mode": access.value,
        "cache_enabled": False,
        "requires_allocation": True,
        "graph_tier": self.graph_tier or GraphTier.KUZU_STANDARD,
      }
    else:
      return {
        "cluster_type": "system",
        "access_mode": access.value,
        "cache_enabled": False,
        "graph_tier": GraphTier.KUZU_STANDARD,
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

  # Patterns for identifying graph types
  USER_GRAPH_PATTERNS = [
    (
      re.compile(r"^kg[a-f0-9]{16,}$"),
      None,  # Type determined by metadata, not ID pattern
    ),  # All user graphs use kg prefix with UUID
  ]

  @classmethod
  def identify_graph(
    cls, graph_id: str, graph_tier: Optional[GraphTier] = None
  ) -> GraphIdentity:
    """
    Identify a graph from its ID.

    Args:
        graph_id: The graph identifier
        graph_tier: Optional graph tier override

    Returns:
        GraphIdentity with category and type information
    """
    # Check if it's a shared repository
    if graph_id in cls.SHARED_REPOSITORIES:
      return GraphIdentity(
        graph_id=graph_id,
        category=GraphCategory.SHARED,
        graph_type=cls.SHARED_REPOSITORIES[graph_id].value,
        graph_tier=GraphTier.KUZU_STANDARD,
        access_pattern=AccessPattern.READ_ONLY,
      )

    # Check if it's a system graph
    if graph_id in ["system", "metadata", "config"]:
      return GraphIdentity(
        graph_id=graph_id,
        category=GraphCategory.SYSTEM,
        graph_type="internal",
        graph_tier=GraphTier.KUZU_STANDARD,
        access_pattern=AccessPattern.RESTRICTED,
      )

    # Check user graph patterns
    for pattern, graph_type in cls.USER_GRAPH_PATTERNS:
      if pattern.match(graph_id):
        return GraphIdentity(
          graph_id=graph_id,
          category=GraphCategory.USER,
          graph_type=UserGraphType.CUSTOM.value,  # Default to custom for all user graphs
          graph_tier=graph_tier or GraphTier.KUZU_STANDARD,
          access_pattern=AccessPattern.READ_WRITE,
        )

    # Default to custom user graph
    return GraphIdentity(
      graph_id=graph_id,
      category=GraphCategory.USER,
      graph_type=UserGraphType.CUSTOM.value,
      graph_tier=graph_tier or GraphTier.KUZU_STANDARD,
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
