"""Statement classification for XBRL elements.

Classifies elements into financial statement categories (Income Statement,
Balance Sheet, Cash Flow Statement, Statement of Equity) using BFS
traversal from known root elements through calculation/presentation graphs.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

import networkit as nk

if TYPE_CHECKING:
  from robosystems.adapters.sec.analytics.graphs import ElementGraph


class StatementType(str, Enum):
  """Financial statement categories."""

  INCOME_STATEMENT = "IncomeStatement"
  BALANCE_SHEET = "BalanceSheet"
  CASH_FLOW = "CashFlow"
  EQUITY = "Equity"


@dataclass
class Classification:
  """Classification result for a single element."""

  statement: StatementType
  depth: int
  weight: float
  via_root: str


@dataclass
class ClassificationResult:
  """Complete classification results."""

  classifications: dict[str, list[Classification]] = field(default_factory=dict)
  unclassified: list[str] = field(default_factory=list)

  @property
  def total_classified(self) -> int:
    return len(self.classifications)

  @property
  def total_unclassified(self) -> int:
    return len(self.unclassified)

  def get_primary_statement(self, qname: str) -> StatementType | None:
    """Get the primary (shallowest) statement for an element."""
    entries = self.classifications.get(qname)
    if not entries:
      return None
    return min(entries, key=lambda c: c.depth).statement

  def get_statement_elements(self, stmt: StatementType) -> list[str]:
    """Get all elements primarily classified under a statement."""
    return [
      qname
      for qname, entries in self.classifications.items()
      if min(entries, key=lambda c: c.depth).statement == stmt
    ]

  def summary(self) -> dict[str, int]:
    """Count elements per primary statement."""
    counts: dict[str, int] = {}
    for entries in self.classifications.values():
      primary = min(entries, key=lambda c: c.depth).statement
      counts[primary.value] = counts.get(primary.value, 0) + 1
    return counts


# Known root elements for each financial statement
STATEMENT_ROOTS: dict[StatementType, list[str]] = {
  StatementType.INCOME_STATEMENT: [
    "us-gaap:NetIncomeLoss",
    "us-gaap:ProfitLoss",
    "us-gaap:IncomeLossFromContinuingOperationsIncludingPortionAttributableToNoncontrollingInterest",
  ],
  StatementType.BALANCE_SHEET: [
    "us-gaap:Assets",
    "us-gaap:LiabilitiesAndStockholdersEquity",
    "us-gaap:Liabilities",
    "us-gaap:StockholdersEquity",
  ],
  StatementType.CASH_FLOW: [
    "us-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect",
    "us-gaap:NetCashProvidedByUsedInOperatingActivities",
    "us-gaap:NetCashProvidedByUsedInInvestingActivities",
    "us-gaap:NetCashProvidedByUsedInFinancingActivities",
  ],
  StatementType.EQUITY: [
    "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
  ],
}


class StatementClassifier:
  """Classifies XBRL elements into financial statement categories.

  Uses BFS from known root elements to propagate statement classification
  through the calculation/presentation graph. Elements reachable from
  multiple statement roots receive multiple classifications.
  """

  def __init__(
    self,
    roots: dict[StatementType, list[str]] | None = None,
  ) -> None:
    self._roots = roots or STATEMENT_ROOTS

  def classify(self, element_graph: ElementGraph) -> ClassificationResult:
    """Run statement classification on the element graph.

    Performs BFS from each statement root element, tracking depth
    and cumulative weight along the path. Uses connected components
    and core decomposition for structural analysis.

    Args:
        element_graph: The element graph with index mappings.

    Returns:
        ClassificationResult with per-element classifications.
    """
    result = ClassificationResult()
    graph = element_graph.graph

    # Run BFS from each root
    for stmt_type, root_qnames in self._roots.items():
      for root_qname in root_qnames:
        root_idx = element_graph.get_idx(root_qname)
        if root_idx is None:
          continue
        self._bfs_classify(
          graph, element_graph, root_idx, root_qname, stmt_type, result
        )

    # Identify unclassified elements
    all_qnames = set(element_graph.elements)
    classified_qnames = set(result.classifications.keys())
    result.unclassified = sorted(all_qnames - classified_qnames)

    return result

  def _bfs_classify(
    self,
    graph: nk.Graph,
    element_graph: ElementGraph,
    root_idx: int,
    root_qname: str,
    stmt_type: StatementType,
    result: ClassificationResult,
  ) -> None:
    """BFS from a root node, classifying all reachable descendants."""
    visited: set[int] = set()
    queue: deque[tuple[int, int, float]] = deque()

    # Seed the root itself
    queue.append((root_idx, 0, 1.0))
    visited.add(root_idx)

    while queue:
      node_idx, depth, cum_weight = queue.popleft()
      qname = element_graph.get_qname(node_idx)

      classification = Classification(
        statement=stmt_type,
        depth=depth,
        weight=cum_weight,
        via_root=root_qname,
      )
      result.classifications.setdefault(qname, []).append(classification)

      # Traverse outgoing edges (parent -> child)
      for neighbor in graph.iterNeighbors(node_idx):
        if neighbor not in visited:
          edge_weight = graph.weight(node_idx, neighbor)
          visited.add(neighbor)
          queue.append((neighbor, depth + 1, cum_weight * edge_weight))

  def analyze_structure(self, element_graph: ElementGraph) -> dict:
    """Run structural analysis on the graph.

    Uses connected components and core decomposition to understand
    the overall structure of the element relationship graph.

    Args:
        element_graph: The element graph.

    Returns:
        Dict with structural metrics.
    """
    graph = element_graph.graph

    # Connected components (on undirected view)
    undirected = nk.graphtools.toUndirected(graph)
    cc = nk.components.ConnectedComponents(undirected)
    cc.run()

    # Core decomposition
    core = nk.centrality.CoreDecomposition(undirected)
    core.run()

    return {
      "num_nodes": graph.numberOfNodes(),
      "num_edges": graph.numberOfEdges(),
      "num_components": cc.numberOfComponents(),
      "largest_component_size": max(cc.getComponentSizes().values())
      if cc.getComponentSizes()
      else 0,
      "max_core_number": max(core.scores()) if graph.numberOfNodes() > 0 else 0,
    }
