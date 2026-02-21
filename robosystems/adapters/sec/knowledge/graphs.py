"""Graph construction utilities for SEC knowledge artifacts.

Builds icebug (networkit) graphs from extracted arc data,
handling node indexing (qname <-> integer ID mapping).
"""

from __future__ import annotations

from dataclasses import dataclass, field

import networkit as nk


@dataclass
class ElementGraph:
  """An icebug graph with element-to-index mapping.

  Attributes:
      graph: The networkit directed weighted graph.
      elements: Ordered list of element qnames (index = node ID).
      element_to_idx: Mapping from qname to node index.
  """

  graph: nk.Graph
  elements: list[str] = field(default_factory=list)
  element_to_idx: dict[str, int] = field(default_factory=dict)

  def get_qname(self, node_id: int) -> str:
    """Get the qname for a node index."""
    return self.elements[node_id]

  def get_idx(self, qname: str) -> int | None:
    """Get the node index for a qname, or None if not found."""
    return self.element_to_idx.get(qname)

  @property
  def num_nodes(self) -> int:
    return self.graph.numberOfNodes()

  @property
  def num_edges(self) -> int:
    return self.graph.numberOfEdges()


def _ensure_node(
  qname: str,
  elements: list[str],
  element_to_idx: dict[str, int],
  graph: nk.Graph,
) -> int:
  """Get or create a node index for the given qname."""
  if qname in element_to_idx:
    return element_to_idx[qname]
  idx = graph.addNode()
  elements.append(qname)
  element_to_idx[qname] = idx
  return idx


def build_element_graph_from_edges(
  edges: list[tuple[str, str, float, str]],
) -> ElementGraph:
  """Build a directed weighted graph from pre-deduplicated edge tuples.

  Calculation arcs use their XBRL weight. Presentation arcs get a
  default weight of 0.5 and are only added for edges not already
  present from calculation arcs.

  Args:
      edges: List of (parent_qname, child_qname, weight, association_type) tuples,
             already deduplicated by DuckDB SQL.

  Returns:
      ElementGraph with the constructed graph and index mappings.
  """
  graph = nk.Graph(0, weighted=True, directed=True)
  elements: list[str] = []
  element_to_idx: dict[str, int] = {}
  seen_edges: set[tuple[int, int]] = set()

  # Process calculation arcs first (primary signal)
  for parent_qname, child_qname, weight, assoc_type in edges:
    if assoc_type != "Calculation":
      continue
    parent_idx = _ensure_node(parent_qname, elements, element_to_idx, graph)
    child_idx = _ensure_node(child_qname, elements, element_to_idx, graph)
    edge_key = (parent_idx, child_idx)
    if edge_key not in seen_edges:
      graph.addEdge(parent_idx, child_idx, abs(weight))
      seen_edges.add(edge_key)

  # Then presentation arcs (supplementary, lower weight)
  for parent_qname, child_qname, weight, assoc_type in edges:
    if assoc_type != "Presentation":
      continue
    parent_idx = _ensure_node(parent_qname, elements, element_to_idx, graph)
    child_idx = _ensure_node(child_qname, elements, element_to_idx, graph)
    edge_key = (parent_idx, child_idx)
    if edge_key not in seen_edges:
      graph.addEdge(parent_idx, child_idx, 0.5)
      seen_edges.add(edge_key)

  return ElementGraph(graph=graph, elements=elements, element_to_idx=element_to_idx)
