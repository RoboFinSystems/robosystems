"""Graph construction utilities for SEC analytics.

Builds icebug (networkit) graphs from extracted arc data,
handling node indexing (qname <-> integer ID mapping).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import networkit as nk

if TYPE_CHECKING:
  from robosystems.adapters.sec.analytics.extractors import CalcArc, PresArc


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


def build_element_graph(
  calc_arcs: list[CalcArc],
  pres_arcs: list[PresArc] | None = None,
) -> ElementGraph:
  """Build a directed weighted graph from calculation and presentation arcs.

  Calculation arcs get weight from their XBRL weight field.
  Presentation arcs get a default weight of 0.5 and are only added
  for edges not already present from calculation arcs.

  Args:
      calc_arcs: Calculation arcs (primary signal).
      pres_arcs: Presentation arcs (supplementary signal).

  Returns:
      ElementGraph with the constructed graph and index mappings.
  """
  graph = nk.Graph(0, weighted=True, directed=True)
  elements: list[str] = []
  element_to_idx: dict[str, int] = {}
  seen_edges: set[tuple[int, int]] = set()

  for arc in calc_arcs:
    parent_idx = _ensure_node(arc.parent_qname, elements, element_to_idx, graph)
    child_idx = _ensure_node(arc.child_qname, elements, element_to_idx, graph)
    edge_key = (parent_idx, child_idx)
    if edge_key not in seen_edges:
      graph.addEdge(parent_idx, child_idx, abs(arc.weight))
      seen_edges.add(edge_key)

  if pres_arcs:
    for arc in pres_arcs:
      parent_idx = _ensure_node(arc.parent_qname, elements, element_to_idx, graph)
      child_idx = _ensure_node(arc.child_qname, elements, element_to_idx, graph)
      edge_key = (parent_idx, child_idx)
      if edge_key not in seen_edges:
        graph.addEdge(parent_idx, child_idx, 0.5)
        seen_edges.add(edge_key)

  return ElementGraph(graph=graph, elements=elements, element_to_idx=element_to_idx)


def build_cooccurrence_graph(
  multi_company_arcs: list[list[CalcArc]],
) -> ElementGraph:
  """Build an undirected weighted co-occurrence graph from multiple companies.

  Two elements are connected if they share a parent in any company's
  calculation tree. Edge weight = number of companies where they co-occur.

  Args:
      multi_company_arcs: List of arc lists, one per company.

  Returns:
      ElementGraph with undirected co-occurrence graph.
  """
  graph = nk.Graph(0, weighted=True, directed=False)
  elements: list[str] = []
  element_to_idx: dict[str, int] = {}
  edge_weights: dict[tuple[int, int], float] = {}

  for company_arcs in multi_company_arcs:
    # Group children by parent
    parent_children: dict[str, set[str]] = {}
    for arc in company_arcs:
      parent_children.setdefault(arc.parent_qname, set()).add(arc.child_qname)

    # Create co-occurrence edges between siblings
    for children in parent_children.values():
      child_list = sorted(children)
      for i, c1 in enumerate(child_list):
        idx1 = _ensure_node(c1, elements, element_to_idx, graph)
        for c2 in child_list[i + 1 :]:
          idx2 = _ensure_node(c2, elements, element_to_idx, graph)
          edge_key = (min(idx1, idx2), max(idx1, idx2))
          edge_weights[edge_key] = edge_weights.get(edge_key, 0.0) + 1.0

  for (u, v), weight in edge_weights.items():
    if graph.hasEdge(u, v):
      graph.setWeight(u, v, weight)
    else:
      graph.addEdge(u, v, weight)

  return ElementGraph(graph=graph, elements=elements, element_to_idx=element_to_idx)
