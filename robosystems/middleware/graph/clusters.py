"""
Cluster Configuration for Writer/Reader Architecture

DEPRECATION WARNING: This module is being phased out in favor of the dynamic
allocation manager (KuzuAllocationManager) which properly tracks instance
allocations in DynamoDB. The hardcoded configurations here do not work
correctly in production environments with multiple EC2 instances.

For entity graphs, use:
  from robosystems.graph_api.client import get_kuzu_client
  client = await get_kuzu_client(graph_id)

This module defines cluster configurations for single-writer, multiple-reader architecture.

Key features:
- User graphs: Single writer node per cluster, no readers initially
- Shared repositories (SEC): Dedicated writer + scalable reader cluster with ALB
- API-only access: All databases accessed via REST API, no direct file access
- S3 sync: Daily database compression and upload for shared repositories
"""

from typing import Dict, List, Optional
from enum import Enum
from dataclasses import dataclass, field

from .types import GraphTier
from robosystems.logger import logger
from robosystems.config import env


class NodeType(str, Enum):
  """Node types in the cluster architecture."""

  WRITER = "writer"  # Writer for all graphs (entity and shared repositories)
  SHARED_MASTER = "shared_master"  # Shared repository master writer
  SHARED_REPLICA = "shared_replica"  # Shared repository read-only replica


class RepositoryType(str, Enum):
  """Types of repositories."""

  ENTITY = "entity"  # User/entity-specific graphs
  SHARED = "shared"  # Shared repositories (SEC, industry, etc.)


@dataclass
class NodeConfig:
  """Configuration for a single node."""

  node_id: str  # Unique identifier for this node
  node_type: NodeType  # Type of node (writer/reader)
  api_base_url: str  # REST API endpoint for this node
  region: str  # AWS region
  tier: GraphTier  # Service tier
  max_databases: int = 200  # Max databases this node can handle
  current_databases: int = 0  # Current database count
  repository_type: RepositoryType = RepositoryType.ENTITY
  s3_sync_enabled: bool = False  # Whether this node syncs to S3
  s3_bucket: Optional[str] = None  # S3 bucket for database syncing
  health_check_url: Optional[str] = None  # Health check endpoint

  @property
  def utilization_percent(self) -> float:
    """Calculate current utilization percentage."""
    if self.max_databases == 0:
      return 100.0
    return (self.current_databases / self.max_databases) * 100

  @property
  def capacity_remaining(self) -> int:
    """Calculate remaining database capacity."""
    return max(0, self.max_databases - self.current_databases)

  @property
  def is_available(self) -> bool:
    """Check if node has available capacity."""
    return self.capacity_remaining > 0


@dataclass
class ClusterConfig:
  """Configuration for a cluster (writer + optional readers)."""

  cluster_id: str  # Unique cluster identifier
  repository_type: RepositoryType  # Type of repository this cluster serves
  writer_node: NodeConfig  # Primary writer node
  reader_nodes: List[NodeConfig] = field(default_factory=list)  # Reader nodes
  alb_endpoint: Optional[str] = None  # ALB endpoint for readers
  region: str = "us-east-1"  # AWS region
  tier: GraphTier = GraphTier.KUZU_STANDARD

  @property
  def max_databases(self) -> int:
    """Total database capacity of this cluster."""
    return self.writer_node.max_databases

  @property
  def current_databases(self) -> int:
    """Current database count in this cluster."""
    return self.writer_node.current_databases

  @property
  def utilization_percent(self) -> float:
    """Calculate cluster utilization percentage."""
    return self.writer_node.utilization_percent

  @property
  def capacity_remaining(self) -> int:
    """Calculate remaining cluster capacity."""
    return self.writer_node.capacity_remaining

  @property
  def is_available(self) -> bool:
    """Check if cluster has available capacity."""
    return self.writer_node.is_available

  def get_read_endpoint(self) -> str:
    """Get the optimal read endpoint (ALB if available, otherwise writer)."""
    if self.alb_endpoint and self.reader_nodes:
      return self.alb_endpoint
    return self.writer_node.api_base_url

  def get_write_endpoint(self) -> str:
    """Get the write endpoint (always the writer node)."""
    return self.writer_node.api_base_url


def load_cluster_configs_from_environment() -> Dict[str, ClusterConfig]:
  """
  Load cluster configurations from environment variables.

  Supports:
  - KUZU_API_URL: Graph API endpoint for all operations
  """
  configs = {}

  # Writer configuration
  # This is the direct connection to the graph database instance
  writer_url = env.GRAPH_API_URL

  # Log the API URL for debugging
  logger.info(f"Configuring Kuzu cluster with API URL: {writer_url}")

  configs["entity-shared-us-east-1"] = ClusterConfig(
    cluster_id="entity-shared-us-east-1",
    repository_type=RepositoryType.ENTITY,
    writer_node=NodeConfig(
      node_id="entity-writer-local",
      node_type=NodeType.WRITER,
      api_base_url=writer_url,
      region="us-east-1",
      tier=GraphTier.KUZU_STANDARD,
      max_databases=200,
      repository_type=RepositoryType.ENTITY,
    ),
    region="us-east-1",
    tier=GraphTier.KUZU_STANDARD,
  )

  # Shared repositories now use the same infrastructure as entity graphs
  # They are distinguished by metadata, not by separate instances
  # All databases (entity and shared) are allocated dynamically on the same writer clusters

  logger.info(f"Loaded cluster configs from environment: {list(configs.keys())}")
  return configs


def load_cluster_configs() -> Dict[str, ClusterConfig]:
  """
  Load Kuzu cluster configurations from environment variables.

  Future implementation will:
  1. Load from AWS Secrets Manager
  2. Query AWS ECS/EC2 for dynamic cluster discovery
  3. Read from configuration files
  4. Support runtime cluster registration

  Currently uses environment variables for all environments.
  """

  # Always use environment-based configuration
  # This simplifies deployment and makes configuration more consistent
  return load_cluster_configs_from_environment()


def get_cluster_for_entity_graphs(
  tier: GraphTier, region: str = "us-east-1"
) -> ClusterConfig:
  """
  Get the best available Kuzu cluster for entity graph creation.

  Args:
      tier: Customer tier (shared, enterprise, premium)
      region: Preferred AWS region

  Returns:
      ClusterConfig for the optimal cluster

  Raises:
      ValueError: If no suitable cluster is found
  """
  clusters = load_cluster_configs()

  # Filter by entity repository type, tier, and region
  candidates = [
    cluster
    for cluster in clusters.values()
    if (
      cluster.repository_type == RepositoryType.ENTITY
      and cluster.tier == tier
      and cluster.region == region
    )
  ]

  if not candidates:
    # Fallback to any entity cluster of the right tier
    candidates = [
      cluster
      for cluster in clusters.values()
      if (cluster.repository_type == RepositoryType.ENTITY and cluster.tier == tier)
    ]

  if not candidates:
    raise ValueError(f"No Kuzu entity clusters available for tier {tier}")

  # Select cluster with most available capacity
  available_clusters = [c for c in candidates if c.is_available]
  if not available_clusters:
    raise ValueError(f"No capacity available in tier {tier} Kuzu entity clusters")

  best_cluster = min(available_clusters, key=lambda c: c.utilization_percent)

  logger.info(
    f"Selected Kuzu cluster {best_cluster.cluster_id} for tier {tier} "
    f"(capacity: {best_cluster.capacity_remaining}/{best_cluster.max_databases})"
  )

  return best_cluster


def get_cluster_for_shared_repository(
  repository_name: str, operation_type: str = "read", region: str = "us-east-1"
) -> ClusterConfig:
  """
  Get the Kuzu cluster configuration for a shared repository.

  Since shared repositories now use the same infrastructure as entity graphs,
  this delegates to get_cluster_for_entity_graphs.

  Args:
      repository_name: Name of shared repository (e.g., 'sec', 'industry')
      operation_type: 'read' or 'write' (ignored - all operations go to writers)
      region: Preferred AWS region

  Returns:
      ClusterConfig for the shared repository

  Raises:
      ValueError: If repository cluster not found
  """
  # Shared repositories now use the standard tier
  # They are allocated on the same clusters as entity graphs
  logger.info(
    f"Routing shared repository '{repository_name}' to standard tier entity clusters"
  )
  return get_cluster_for_entity_graphs(GraphTier.KUZU_STANDARD, region)


def get_cluster_by_id(cluster_id: str) -> ClusterConfig:
  """
  Get Kuzu cluster configuration by cluster ID.

  Args:
      cluster_id: Unique cluster identifier

  Returns:
      ClusterConfig for the specified cluster

  Raises:
      ValueError: If cluster ID not found
  """
  clusters = load_cluster_configs()

  if cluster_id not in clusters:
    raise ValueError(f"Kuzu cluster '{cluster_id}' not found")

  return clusters[cluster_id]
