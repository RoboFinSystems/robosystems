"""Build icebug (networkit) element graphs, with qname ↔ node-id mapping."""

from __future__ import annotations

from dataclasses import dataclass, field

import networkit as nk
import numpy as np
import pyarrow as pa


@dataclass
class ElementGraph:
  """A directed weighted CSR graph; ``elements[i]`` is node i's qname."""

  graph: nk.Graph
  elements: list[str] = field(default_factory=list)
  element_to_idx: dict[str, int] = field(default_factory=dict)

  def get_qname(self, node_id: int) -> str:
    return self.elements[node_id]

  def get_idx(self, qname: str) -> int | None:
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
  """Graph from deduplicated (parent, child, weight, association_type) tuples.

  Calculation arcs keep |weight|; presentation arcs get 0.5 and only fill
  pairs no calculation arc covers.
  """
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
  """Graph from ``ArcExtractor.extract_graph_arrow`` output, with no per-edge Python."""
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
  """CSR graph (outgoing and incoming) from COO edge arrays."""
  src_np = np.asarray(src, dtype=np.int64)
  dst_np = np.asarray(dst, dtype=np.int64)
  wt_np = np.asarray(weights, dtype=np.float64)

  if len(src_np) == 0:
    return nk.Graph(n, weighted=True, directed=True)

  order = np.lexsort((dst_np, src_np))
  out_src = src_np[order]
  out_dst = dst_np[order]
  out_wt = wt_np[order]

  out_indptr = np.zeros(n + 1, dtype=np.int64)
  np.add.at(out_indptr[1:], out_src, 1)
  np.cumsum(out_indptr, out=out_indptr)

  order_in = np.lexsort((src_np, dst_np))
  in_dst = dst_np[order_in]
  in_src = src_np[order_in]
  in_wt = wt_np[order_in]

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
