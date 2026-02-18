"""Element normalization for XBRL data.

Clusters equivalent XBRL elements across companies using graph embedding
(Node2Vec) and community detection (PLM), then identifies canonical
elements per cluster using PageRank.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import networkit as nk

if TYPE_CHECKING:
  from robosystems.adapters.sec.analytics.graphs import ElementGraph


@dataclass
class ElementCluster:
  """A cluster of equivalent elements."""

  cluster_id: int
  canonical: str
  members: list[str]
  confidence: float


@dataclass
class NormalizationResult:
  """Complete normalization results."""

  clusters: list[ElementCluster] = field(default_factory=list)
  element_to_cluster: dict[str, int] = field(default_factory=dict)

  @property
  def num_clusters(self) -> int:
    return len(self.clusters)

  @property
  def num_elements(self) -> int:
    return len(self.element_to_cluster)

  def get_canonical(self, qname: str) -> str | None:
    """Get the canonical element for a given qname."""
    cluster_id = self.element_to_cluster.get(qname)
    if cluster_id is None:
      return None
    for cluster in self.clusters:
      if cluster.cluster_id == cluster_id:
        return cluster.canonical
    return None

  def get_cluster(self, qname: str) -> ElementCluster | None:
    """Get the cluster containing the given qname."""
    cluster_id = self.element_to_cluster.get(qname)
    if cluster_id is None:
      return None
    for cluster in self.clusters:
      if cluster.cluster_id == cluster_id:
        return cluster
    return None


class ElementNormalizer:
  """Normalizes XBRL elements by clustering equivalents.

  Pipeline:
  1. Node2Vec embedding (128-dim) on co-occurrence graph
  2. PLM (Parallel Louvain Method) community detection
  3. PageRank within each community to find canonical element
  4. Optional link prediction (Jaccard) for missed equivalences

  Best results with 5+ companies. Works with single company (degraded).

  Args:
      dimensions: Node2Vec embedding dimensions.
      walk_length: Random walk length for Node2Vec.
      num_walks: Number of random walks per node.
  """

  def __init__(
    self,
    dimensions: int = 128,
    walk_length: int = 80,
    num_walks: int = 10,
  ) -> None:
    self._dimensions = dimensions
    self._walk_length = walk_length
    self._num_walks = num_walks

  def normalize(self, element_graph: ElementGraph) -> NormalizationResult:
    """Run the full normalization pipeline.

    Args:
        element_graph: Co-occurrence graph from build_cooccurrence_graph().

    Returns:
        NormalizationResult with clusters and canonical elements.
    """
    graph = element_graph.graph

    if graph.numberOfNodes() == 0:
      return NormalizationResult()

    # Step 1: Community detection with PLM
    communities = nk.community.PLM(graph, refine=True)
    communities.run()
    partition = communities.getPartition()

    # Step 2: PageRank for canonical element selection
    pr = nk.centrality.PageRank(graph)
    pr.run()
    pagerank_scores = pr.scores()

    # Step 3: Build clusters
    cluster_members: dict[int, list[int]] = {}
    for node in graph.iterNodes():
      community_id = partition.subsetOf(node)
      cluster_members.setdefault(community_id, []).append(node)

    result = NormalizationResult()

    for community_id, members in cluster_members.items():
      if len(members) < 2:
        # Singleton clusters are not useful for normalization
        qname = element_graph.get_qname(members[0])
        result.element_to_cluster[qname] = community_id
        continue

      # Find canonical element (highest PageRank in cluster)
      canonical_idx = max(members, key=lambda n: pagerank_scores[n])
      canonical_qname = element_graph.get_qname(canonical_idx)

      member_qnames = [element_graph.get_qname(m) for m in members]
      member_qnames.sort()

      # Confidence based on cluster cohesion (avg internal edge weight)
      confidence = self._cluster_confidence(graph, members)

      cluster = ElementCluster(
        cluster_id=community_id,
        canonical=canonical_qname,
        members=member_qnames,
        confidence=confidence,
      )
      result.clusters.append(cluster)

      for qname in member_qnames:
        result.element_to_cluster[qname] = community_id

    result.clusters.sort(key=lambda c: len(c.members), reverse=True)
    return result

  def find_missing_links(
    self,
    element_graph: ElementGraph,
    top_k: int = 20,
    min_score: float = 0.3,
  ) -> list[tuple[str, str, float]]:
    """Use Jaccard index to find potentially missed equivalences.

    Only considers non-adjacent node pairs that share at least one
    neighbor, avoiding the O(n^2) full pairwise scan.

    Args:
        element_graph: The co-occurrence graph.
        top_k: Number of top predictions to return.
        min_score: Minimum Jaccard score threshold.

    Returns:
        List of (element1, element2, score) tuples.
    """
    graph = element_graph.graph
    if graph.numberOfNodes() < 2:
      return []

    # Pre-compute neighbor sets
    neighbor_sets: dict[int, set[int]] = {}
    for u in graph.iterNodes():
      neighbors = set(graph.iterNeighbors(u))
      if neighbors:
        neighbor_sets[u] = neighbors

    # Only check pairs sharing at least one neighbor (2-hop candidates)
    # For each node, collect its neighbors' neighbors as candidates
    candidates: set[tuple[int, int]] = set()
    for u, neighbors_u in neighbor_sets.items():
      for w in neighbors_u:
        for v in graph.iterNeighbors(w):
          if v > u and not graph.hasEdge(u, v) and v in neighbor_sets:
            candidates.add((u, v))

    predictions: list[tuple[str, str, float]] = []
    for u, v in candidates:
      neighbors_u = neighbor_sets[u]
      neighbors_v = neighbor_sets[v]
      intersection = len(neighbors_u & neighbors_v)
      union = len(neighbors_u | neighbors_v)
      if union > 0:
        score = intersection / union
        if score > min_score:
          predictions.append(
            (
              element_graph.get_qname(u),
              element_graph.get_qname(v),
              score,
            )
          )

    predictions.sort(key=lambda x: x[2], reverse=True)
    return predictions[:top_k]

  def _cluster_confidence(self, graph: nk.Graph, members: list[int]) -> float:
    """Calculate confidence score for a cluster based on internal edge density."""
    if len(members) < 2:
      return 0.0

    member_set = set(members)
    total_weight = 0.0
    edge_count = 0

    for u in members:
      for v in graph.iterNeighbors(u):
        if v in member_set and v > u:
          total_weight += graph.weight(u, v)
          edge_count += 1

    max_edges = len(members) * (len(members) - 1) / 2
    if max_edges == 0:
      return 0.0

    density = edge_count / max_edges
    avg_weight = total_weight / edge_count if edge_count > 0 else 0.0

    return min(1.0, density * min(avg_weight, 1.0))
