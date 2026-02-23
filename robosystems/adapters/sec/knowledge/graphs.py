"""Graph construction utilities for SEC knowledge artifacts.

Builds icebug graphs from DuckDB staging data using zero-copy
Arrow → CSR ingestion, handling node indexing (qname <-> integer ID mapping).
"""

from __future__ import annotations

from dataclasses import dataclass, field

import networkit as nk
import numpy as np
import pyarrow as pa


@dataclass
class ElementGraph:
  """An icebug graph with element-to-index mapping.

  Attributes:
      graph: The icebug directed weighted graph (CSR-backed).
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
  # Collect unique qnames and assign stable integer IDs
  qname_set: set[str] = set()
  for parent_qname, child_qname, _weight, _assoc_type in edges:
    qname_set.add(parent_qname)
    qname_set.add(child_qname)

  elements = sorted(qname_set)
  element_to_idx = {q: i for i, q in enumerate(elements)}
  n = len(elements)

  if n == 0:
    graph = nk.Graph(0, weighted=True, directed=True)
    return ElementGraph(graph=graph, elements=[], element_to_idx={})

  # Build COO edge list: calculation arcs first, then presentation supplements
  src_list: list[int] = []
  dst_list: list[int] = []
  wt_list: list[float] = []
  seen: set[tuple[int, int]] = set()

  for parent_qname, child_qname, weight, assoc_type in edges:
    if assoc_type != "Calculation":
      continue
    s = element_to_idx[parent_qname]
    d = element_to_idx[child_qname]
    if (s, d) not in seen:
      src_list.append(s)
      dst_list.append(d)
      wt_list.append(abs(weight))
      seen.add((s, d))

  for parent_qname, child_qname, _weight, assoc_type in edges:
    if assoc_type != "Presentation":
      continue
    s = element_to_idx[parent_qname]
    d = element_to_idx[child_qname]
    if (s, d) not in seen:
      src_list.append(s)
      dst_list.append(d)
      wt_list.append(0.5)
      seen.add((s, d))

  graph = _build_csr_graph(n, src_list, dst_list, wt_list)
  return ElementGraph(graph=graph, elements=elements, element_to_idx=element_to_idx)


def build_element_graph_from_arrow(
  nodes: pa.Array,
  edges: pa.Table,
) -> ElementGraph:
  """Build a directed weighted graph from Arrow arrays via zero-copy CSR.

  Uses icebug's Graph.fromCSR() for zero-copy Arrow ingestion,
  avoiding per-element Python loops entirely.

  Args:
      nodes: Arrow string array of qnames, ordered by node ID.
      edges: Arrow table with columns (src: int64, dst: int64, weight: float64),
             already deduped with calc-first priority and sorted by (src, dst).

  Returns:
      ElementGraph with the constructed graph and index mappings.
  """
  elements = nodes.to_pylist()
  element_to_idx = {q: i for i, q in enumerate(elements)}
  n = len(elements)

  if n == 0:
    graph = nk.Graph(0, weighted=True, directed=True)
    return ElementGraph(graph=graph, elements=[], element_to_idx={})

  src_np = edges.column("src").to_numpy().astype(np.int64)
  dst_np = edges.column("dst").to_numpy().astype(np.int64)
  wt_np = edges.column("weight").to_numpy().astype(np.float64)

  graph = _build_csr_graph(n, src_np, dst_np, wt_np)
  return ElementGraph(graph=graph, elements=elements, element_to_idx=element_to_idx)


def _build_csr_graph(
  n: int,
  src: list[int] | np.ndarray,
  dst: list[int] | np.ndarray,
  weights: list[float] | np.ndarray,
) -> nk.Graph:
  """Build an icebug CSR graph from COO edge data.

  Converts COO (src, dst, weight) arrays to CSR format and
  constructs the graph via Graph.fromCSR() with Arrow zero-copy.
  """
  src_np = np.asarray(src, dtype=np.int64)
  dst_np = np.asarray(dst, dtype=np.int64)
  wt_np = np.asarray(weights, dtype=np.float64)

  if len(src_np) == 0:
    return nk.Graph(n, weighted=True, directed=True)

  # Sort by (src, dst) for outgoing CSR
  order = np.lexsort((dst_np, src_np))
  out_src = src_np[order]
  out_dst = dst_np[order]
  out_wt = wt_np[order]

  # Outgoing CSR indptr
  out_indptr = np.zeros(n + 1, dtype=np.int64)
  np.add.at(out_indptr[1:], out_src, 1)
  np.cumsum(out_indptr, out=out_indptr)

  # Sort by (dst, src) for incoming CSR
  order_in = np.lexsort((src_np, dst_np))
  in_dst = dst_np[order_in]  # node receiving the edge
  in_src = src_np[order_in]  # node sending the edge
  in_wt = wt_np[order_in]

  # Incoming CSR indptr
  in_indptr = np.zeros(n + 1, dtype=np.int64)
  np.add.at(in_indptr[1:], in_dst, 1)
  np.cumsum(in_indptr, out=in_indptr)

  return nk.Graph.fromCSR(
    n=n,
    directed=True,
    out_indices=pa.array(out_dst),
    out_indptr=pa.array(out_indptr),
    out_weights=pa.array(out_wt),
    in_indices=pa.array(in_src),
    in_indptr=pa.array(in_indptr),
    in_weights=pa.array(in_wt),
  )
