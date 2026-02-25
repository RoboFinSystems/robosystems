"""Tests for graph construction utilities.

Tests ElementGraph dataclass, build_element_graph_from_edges,
build_element_graph_from_arrow, and the internal _build_csr_graph function.
"""

import networkit as nk
import numpy as np
import pyarrow as pa
import pytest

from robosystems.adapters.sec.knowledge.graphs import (
  ElementGraph,
  _build_csr_graph,
  build_element_graph_from_arrow,
  build_element_graph_from_edges,
)

pytestmark = pytest.mark.unit


class TestElementGraph:
  """Tests for the ElementGraph dataclass."""

  def test_get_qname_valid_index(self):
    """get_qname returns the qname for a valid node index."""
    graph = nk.Graph(3, weighted=True, directed=True)
    elements = ["us-gaap:Assets", "us-gaap:Liabilities", "us-gaap:Revenue"]
    element_to_idx = {q: i for i, q in enumerate(elements)}

    eg = ElementGraph(graph=graph, elements=elements, element_to_idx=element_to_idx)
    assert eg.get_qname(0) == "us-gaap:Assets"
    assert eg.get_qname(1) == "us-gaap:Liabilities"
    assert eg.get_qname(2) == "us-gaap:Revenue"

  def test_get_qname_out_of_range(self):
    """get_qname raises IndexError for out-of-range index."""
    graph = nk.Graph(2, weighted=True, directed=True)
    elements = ["a", "b"]
    eg = ElementGraph(graph=graph, elements=elements, element_to_idx={"a": 0, "b": 1})
    with pytest.raises(IndexError):
      eg.get_qname(5)

  def test_get_idx_found(self):
    """get_idx returns the index for a known qname."""
    graph = nk.Graph(2, weighted=True, directed=True)
    elements = ["us-gaap:Assets", "us-gaap:Revenue"]
    element_to_idx = {"us-gaap:Assets": 0, "us-gaap:Revenue": 1}

    eg = ElementGraph(graph=graph, elements=elements, element_to_idx=element_to_idx)
    assert eg.get_idx("us-gaap:Assets") == 0
    assert eg.get_idx("us-gaap:Revenue") == 1

  def test_get_idx_not_found(self):
    """get_idx returns None for an unknown qname."""
    graph = nk.Graph(1, weighted=True, directed=True)
    eg = ElementGraph(
      graph=graph, elements=["us-gaap:Assets"], element_to_idx={"us-gaap:Assets": 0}
    )
    assert eg.get_idx("us-gaap:NonExistent") is None

  def test_num_nodes(self):
    """num_nodes returns the graph node count."""
    graph = nk.Graph(5, weighted=True, directed=True)
    eg = ElementGraph(graph=graph, elements=["a", "b", "c", "d", "e"])
    assert eg.num_nodes == 5

  def test_num_edges(self):
    """num_edges returns the graph edge count."""
    graph = nk.Graph(3, weighted=True, directed=True)
    graph.addEdge(0, 1, 1.0)
    graph.addEdge(1, 2, 0.5)
    eg = ElementGraph(graph=graph)
    assert eg.num_edges == 2

  def test_empty_graph(self):
    """ElementGraph works with an empty graph."""
    graph = nk.Graph(0, weighted=True, directed=True)
    eg = ElementGraph(graph=graph, elements=[], element_to_idx={})
    assert eg.num_nodes == 0
    assert eg.num_edges == 0

  def test_default_factory_fields(self):
    """Default elements and element_to_idx are empty."""
    graph = nk.Graph(0, weighted=True, directed=True)
    eg = ElementGraph(graph=graph)
    assert eg.elements == []
    assert eg.element_to_idx == {}


class TestBuildElementGraphFromEdges:
  """Tests for build_element_graph_from_edges function."""

  def test_basic_graph_construction(self):
    """Builds a graph from simple edge tuples."""
    edges = [
      ("us-gaap:Assets", "us-gaap:Cash", 1.0, "Calculation"),
      ("us-gaap:Assets", "us-gaap:Receivables", 1.0, "Calculation"),
    ]
    result = build_element_graph_from_edges(edges)

    assert result.num_nodes == 3
    assert result.num_edges == 2
    assert len(result.elements) == 3

  def test_elements_sorted(self):
    """Element list is sorted alphabetically."""
    edges = [
      ("z_parent", "a_child", 1.0, "Calculation"),
      ("m_parent", "b_child", 0.5, "Presentation"),
    ]
    result = build_element_graph_from_edges(edges)
    assert result.elements == sorted(result.elements)

  def test_element_to_idx_consistent(self):
    """element_to_idx maps each qname to its sorted position."""
    edges = [
      ("b", "a", 1.0, "Calculation"),
      ("c", "b", 1.0, "Calculation"),
    ]
    result = build_element_graph_from_edges(edges)

    for i, qname in enumerate(result.elements):
      assert result.element_to_idx[qname] == i

  def test_calc_edges_first(self):
    """Calculation edges are added before Presentation edges."""
    edges = [
      ("parent", "child", 1.0, "Calculation"),
      ("parent", "child", 0.5, "Presentation"),  # Duplicate, should be skipped
    ]
    result = build_element_graph_from_edges(edges)

    parent_idx = result.get_idx("parent")
    child_idx = result.get_idx("child")
    assert parent_idx is not None
    assert child_idx is not None

    # Only one edge should exist (calc wins)
    assert result.num_edges == 1
    assert result.graph.weight(parent_idx, child_idx) == 1.0

  def test_presentation_only_edges(self):
    """Presentation-only edges get weight 0.5."""
    edges = [
      ("parent", "child", None, "Presentation"),
    ]
    result = build_element_graph_from_edges(edges)

    parent_idx = result.get_idx("parent")
    child_idx = result.get_idx("child")
    assert result.graph.weight(parent_idx, child_idx) == 0.5

  def test_presentation_supplements_calc(self):
    """Presentation edges for different src-dst pairs are added alongside calc."""
    edges = [
      ("A", "B", 1.0, "Calculation"),
      ("A", "C", None, "Presentation"),
    ]
    result = build_element_graph_from_edges(edges)

    assert result.num_edges == 2

    a_idx = result.get_idx("A")
    b_idx = result.get_idx("B")
    c_idx = result.get_idx("C")

    assert result.graph.weight(a_idx, b_idx) == 1.0
    assert result.graph.weight(a_idx, c_idx) == 0.5

  def test_negative_weight_abs(self):
    """Negative calculation weights are converted to absolute values."""
    edges = [
      ("parent", "child", -2.5, "Calculation"),
    ]
    result = build_element_graph_from_edges(edges)

    parent_idx = result.get_idx("parent")
    child_idx = result.get_idx("child")
    assert result.graph.weight(parent_idx, child_idx) == 2.5

  def test_empty_edges(self):
    """Empty edge list produces an empty graph."""
    result = build_element_graph_from_edges([])

    assert result.num_nodes == 0
    assert result.num_edges == 0
    assert result.elements == []
    assert result.element_to_idx == {}

  def test_directed_graph(self):
    """Resulting graph is directed."""
    edges = [("A", "B", 1.0, "Calculation")]
    result = build_element_graph_from_edges(edges)

    assert result.graph.isDirected()

  def test_weighted_graph(self):
    """Resulting graph is weighted."""
    edges = [("A", "B", 1.0, "Calculation")]
    result = build_element_graph_from_edges(edges)

    assert result.graph.isWeighted()

  def test_self_loop_handled(self):
    """Self-loops are included in the graph."""
    edges = [("A", "A", 1.0, "Calculation")]
    result = build_element_graph_from_edges(edges)

    assert result.num_nodes == 1
    assert result.num_edges == 1

  def test_dedup_within_calc(self):
    """Duplicate (same src, dst) calc edges are deduplicated."""
    edges = [
      ("A", "B", 1.0, "Calculation"),
      ("A", "B", 2.0, "Calculation"),  # Duplicate src-dst
    ]
    result = build_element_graph_from_edges(edges)

    assert result.num_edges == 1
    a_idx = result.get_idx("A")
    b_idx = result.get_idx("B")
    # First one wins (1.0)
    assert result.graph.weight(a_idx, b_idx) == 1.0

  def test_large_edge_set(self):
    """Handles a larger set of edges."""
    edges = []
    for i in range(100):
      edges.append((f"parent_{i}", f"child_{i}", 1.0, "Calculation"))
    result = build_element_graph_from_edges(edges)

    assert result.num_nodes == 200
    assert result.num_edges == 100


class TestBuildElementGraphFromArrow:
  """Tests for build_element_graph_from_arrow function."""

  def test_basic_arrow_construction(self):
    """Builds a graph from Arrow arrays."""
    nodes = pa.array(["A", "B", "C"])
    edges = pa.table(
      {
        "src": pa.array([0, 0], type=pa.int64()),
        "dst": pa.array([1, 2], type=pa.int64()),
        "weight": pa.array([1.0, 0.5], type=pa.float64()),
      }
    )

    result = build_element_graph_from_arrow(nodes, edges)

    assert result.num_nodes == 3
    assert result.num_edges == 2
    assert result.elements == ["A", "B", "C"]

  def test_element_to_idx_mapping(self):
    """Element to index mapping is correct."""
    nodes = pa.array(["alpha", "beta", "gamma"])
    edges = pa.table(
      {
        "src": pa.array([0], type=pa.int64()),
        "dst": pa.array([1], type=pa.int64()),
        "weight": pa.array([1.0], type=pa.float64()),
      }
    )

    result = build_element_graph_from_arrow(nodes, edges)

    assert result.get_idx("alpha") == 0
    assert result.get_idx("beta") == 1
    assert result.get_idx("gamma") == 2

  def test_empty_arrow_graph(self):
    """Empty node array produces an empty graph."""
    nodes = pa.array([], type=pa.string())
    edges = pa.table(
      {
        "src": pa.array([], type=pa.int64()),
        "dst": pa.array([], type=pa.int64()),
        "weight": pa.array([], type=pa.float64()),
      }
    )

    result = build_element_graph_from_arrow(nodes, edges)

    assert result.num_nodes == 0
    assert result.num_edges == 0
    assert result.elements == []
    assert result.element_to_idx == {}

  def test_directed_weighted(self):
    """Result graph is directed and weighted."""
    nodes = pa.array(["A", "B"])
    edges = pa.table(
      {
        "src": pa.array([0], type=pa.int64()),
        "dst": pa.array([1], type=pa.int64()),
        "weight": pa.array([2.5], type=pa.float64()),
      }
    )

    result = build_element_graph_from_arrow(nodes, edges)

    assert result.graph.isDirected()
    assert result.graph.isWeighted()
    assert result.graph.weight(0, 1) == 2.5

  def test_nodes_without_edges(self):
    """Nodes with no edges are still present in the graph."""
    nodes = pa.array(["A", "B", "C", "D"])
    edges = pa.table(
      {
        "src": pa.array([0], type=pa.int64()),
        "dst": pa.array([1], type=pa.int64()),
        "weight": pa.array([1.0], type=pa.float64()),
      }
    )

    result = build_element_graph_from_arrow(nodes, edges)

    assert result.num_nodes == 4
    assert result.num_edges == 1
    assert result.get_idx("C") is not None
    assert result.get_idx("D") is not None

  def test_multiple_edges(self):
    """Multiple edges are correctly added."""
    nodes = pa.array(["A", "B", "C"])
    edges = pa.table(
      {
        "src": pa.array([0, 0, 1], type=pa.int64()),
        "dst": pa.array([1, 2, 2], type=pa.int64()),
        "weight": pa.array([1.0, 0.5, 0.8], type=pa.float64()),
      }
    )

    result = build_element_graph_from_arrow(nodes, edges)

    assert result.num_edges == 3
    assert result.graph.weight(0, 1) == 1.0
    assert result.graph.weight(0, 2) == 0.5
    assert result.graph.weight(1, 2) == 0.8


class TestBuildCSRGraph:
  """Tests for the internal _build_csr_graph function."""

  def test_basic_csr_construction(self):
    """Builds a CSR graph from COO edge data."""
    n = 3
    src = [0, 0, 1]
    dst = [1, 2, 2]
    weights = [1.0, 0.5, 0.8]

    graph = _build_csr_graph(n, src, dst, weights)

    assert graph.numberOfNodes() == 3
    assert graph.numberOfEdges() == 3
    assert graph.isDirected()
    assert graph.isWeighted()

  def test_empty_edges(self):
    """Empty edge lists produce a graph with nodes but no edges."""
    graph = _build_csr_graph(5, [], [], [])

    assert graph.numberOfNodes() == 5
    assert graph.numberOfEdges() == 0
    assert graph.isDirected()
    assert graph.isWeighted()

  def test_zero_nodes(self):
    """Zero-node graph is valid."""
    graph = _build_csr_graph(0, [], [], [])

    assert graph.numberOfNodes() == 0
    assert graph.numberOfEdges() == 0

  def test_numpy_arrays(self):
    """Accepts numpy arrays as input."""
    n = 3
    src = np.array([0, 1], dtype=np.int64)
    dst = np.array([1, 2], dtype=np.int64)
    weights = np.array([1.0, 2.0], dtype=np.float64)

    graph = _build_csr_graph(n, src, dst, weights)

    assert graph.numberOfNodes() == 3
    assert graph.numberOfEdges() == 2

  def test_python_lists(self):
    """Accepts Python lists as input."""
    graph = _build_csr_graph(2, [0], [1], [1.0])

    assert graph.numberOfNodes() == 2
    assert graph.numberOfEdges() == 1

  def test_weights_preserved(self):
    """Edge weights are preserved in the graph."""
    graph = _build_csr_graph(3, [0, 1], [1, 2], [3.14, 2.71])

    assert abs(graph.weight(0, 1) - 3.14) < 1e-10
    assert abs(graph.weight(1, 2) - 2.71) < 1e-10

  def test_single_edge(self):
    """Single edge graph works correctly."""
    graph = _build_csr_graph(2, [0], [1], [1.5])

    assert graph.numberOfNodes() == 2
    assert graph.numberOfEdges() == 1
    assert graph.weight(0, 1) == 1.5

  def test_incoming_edges_traversable(self):
    """Incoming edges are correctly indexed for traversal."""
    # Build: 0->2, 1->2 (so node 2 has 2 incoming edges)
    graph = _build_csr_graph(3, [0, 1], [2, 2], [1.0, 1.0])

    # Verify that node 2 has incoming neighbors
    in_neighbors = list(graph.iterInNeighbors(2))
    assert sorted(in_neighbors) == [0, 1]

  def test_outgoing_edges_traversable(self):
    """Outgoing edges are correctly indexed for traversal."""
    # Build: 0->1, 0->2 (so node 0 has 2 outgoing edges)
    graph = _build_csr_graph(3, [0, 0], [1, 2], [1.0, 0.5])

    out_neighbors = list(graph.iterNeighbors(0))
    assert sorted(out_neighbors) == [1, 2]


class TestEdgeAndArrowEquivalence:
  """Tests that edge-based and Arrow-based construction produce equivalent graphs."""

  def test_same_graph_structure(self):
    """Both construction methods produce the same graph topology."""
    edges = [
      ("A", "B", 1.0, "Calculation"),
      ("A", "C", 0.8, "Calculation"),
      ("B", "C", 0.5, "Presentation"),
    ]

    # Edge-based
    graph_edges = build_element_graph_from_edges(edges)

    # Arrow-based (simulate what DuckDB would produce)
    elements_sorted = sorted({"A", "B", "C"})
    idx_map = {q: i for i, q in enumerate(elements_sorted)}

    nodes = pa.array(elements_sorted)
    edge_data = pa.table(
      {
        "src": pa.array([idx_map["A"], idx_map["A"], idx_map["B"]], type=pa.int64()),
        "dst": pa.array([idx_map["B"], idx_map["C"], idx_map["C"]], type=pa.int64()),
        "weight": pa.array([1.0, 0.8, 0.5], type=pa.float64()),
      }
    )
    graph_arrow = build_element_graph_from_arrow(nodes, edge_data)

    assert graph_edges.num_nodes == graph_arrow.num_nodes
    assert graph_edges.num_edges == graph_arrow.num_edges
    assert sorted(graph_edges.elements) == sorted(graph_arrow.elements)

  def test_same_edge_weights(self):
    """Both methods preserve the same edge weights."""
    edges = [
      ("X", "Y", 2.5, "Calculation"),
      ("Y", "Z", 0.5, "Presentation"),
    ]

    graph_edges = build_element_graph_from_edges(edges)

    elements_sorted = sorted({"X", "Y", "Z"})
    idx_map = {q: i for i, q in enumerate(elements_sorted)}

    nodes = pa.array(elements_sorted)
    edge_data = pa.table(
      {
        "src": pa.array([idx_map["X"], idx_map["Y"]], type=pa.int64()),
        "dst": pa.array([idx_map["Y"], idx_map["Z"]], type=pa.int64()),
        "weight": pa.array([2.5, 0.5], type=pa.float64()),
      }
    )
    graph_arrow = build_element_graph_from_arrow(nodes, edge_data)

    # Check edge weights match
    for qname in graph_edges.elements:
      edge_idx = graph_edges.get_idx(qname)
      arrow_idx = graph_arrow.get_idx(qname)
      assert edge_idx is not None
      assert arrow_idx is not None

      edge_neighbors = sorted(graph_edges.graph.iterNeighbors(edge_idx))
      arrow_neighbors = sorted(graph_arrow.graph.iterNeighbors(arrow_idx))

      assert len(edge_neighbors) == len(arrow_neighbors)

      for en, an in zip(edge_neighbors, arrow_neighbors, strict=True):
        edge_w = graph_edges.graph.weight(edge_idx, en)
        arrow_w = graph_arrow.graph.weight(arrow_idx, an)
        assert abs(edge_w - arrow_w) < 1e-10


class TestGraphAnalyticsCompatibility:
  """Tests that constructed graphs work with networkit analytics."""

  def test_pagerank_runs(self):
    """PageRank can run on constructed graph."""
    edges = [
      ("A", "B", 1.0, "Calculation"),
      ("B", "C", 1.0, "Calculation"),
      ("C", "A", 1.0, "Calculation"),
    ]
    result = build_element_graph_from_edges(edges)

    pr = nk.centrality.PageRank(result.graph, damp=0.85, tol=1e-6)
    pr.run()
    scores = pr.scores()

    assert len(scores) == 3
    assert all(s > 0 for s in scores)

  def test_core_decomposition_runs(self):
    """Core decomposition can run on constructed graph."""
    edges = [
      ("A", "B", 1.0, "Calculation"),
      ("B", "C", 1.0, "Calculation"),
      ("A", "C", 1.0, "Calculation"),
    ]
    result = build_element_graph_from_edges(edges)

    undirected = nk.graphtools.toUndirected(result.graph)
    core = nk.centrality.CoreDecomposition(undirected)
    core.run()
    scores = core.scores()

    assert len(scores) == 3

  def test_bfs_traversal(self):
    """BFS traversal works on constructed graph."""
    edges = [
      ("root", "child1", 1.0, "Calculation"),
      ("root", "child2", 1.0, "Calculation"),
      ("child1", "grandchild", 1.0, "Calculation"),
    ]
    result = build_element_graph_from_edges(edges)

    root_idx = result.get_idx("root")
    assert root_idx is not None

    bfs = nk.distance.BFS(result.graph, root_idx, storePaths=True)
    bfs.run()

    grandchild_idx = result.get_idx("grandchild")
    assert grandchild_idx is not None
    # Grandchild should be reachable at distance 2
    assert bfs.distance(grandchild_idx) == 2

  def test_disconnected_nodes_pagerank(self):
    """PageRank handles graph with isolated nodes."""
    # Build a graph with a disconnected node (D has no edges)
    edges = [
      ("A", "B", 1.0, "Calculation"),
      ("B", "C", 1.0, "Calculation"),
    ]
    result = build_element_graph_from_edges(edges)

    # D is not in the graph, but A, B, C are.
    # All should have PageRank scores.
    pr = nk.centrality.PageRank(result.graph, damp=0.85, tol=1e-6)
    pr.run()
    scores = pr.scores()

    assert len(scores) == 3
    assert all(s >= 0 for s in scores)
