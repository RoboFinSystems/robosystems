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
  from robosystems.adapters.sec.knowledge.graphs import ElementGraph


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


# Authoritative mapping from Seattle Method disclosure-isSECType StatementType
# disclosures to our StatementType enum. Only the 8 disclosures that the
# Seattle Method classifies as StatementType (face of financial statement) are
# included. All other disclosures (~987) are DisclosureType (notes) — their raw
# Seattle Method names flow through the disclosure_type column in element_knowledge.parquet
# for downstream use, but are NOT mapped to statements.
# Source: disclosure-isSECType arcrole from disclosure-mechanics_ALL.xsd
DISCLOSURE_TO_STATEMENT: dict[str, StatementType] = {
  # Income Statement
  "IncomeStatement": StatementType.INCOME_STATEMENT,
  "EarningsPerShareDisclosuresHierarchy": StatementType.INCOME_STATEMENT,
  "StatementOfComprehensiveIncome": StatementType.INCOME_STATEMENT,
  # Balance Sheet (BalanceSheet + its two required sub-disclosures)
  "BalanceSheet": StatementType.BALANCE_SHEET,
  "AssetsRollUp": StatementType.BALANCE_SHEET,
  "LiabilitiesAndEquityRollUp": StatementType.BALANCE_SHEET,
  # Cash Flow
  "CashFlowStatement": StatementType.CASH_FLOW,
  # Equity
  "StatementOfChangesInEquity": StatementType.EQUITY,
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

  def classify(
    self,
    element_graph: ElementGraph,
    disclosure_roots: dict[str, set[str]] | None = None,
  ) -> ClassificationResult:
    """Run statement classification on the element graph.

    Phase 1: BFS from hardcoded STATEMENT_ROOTS (weight 1.0).
    Phase 2: BFS from disclosure root elements (weight 0.9), using
    DISCLOSURE_TO_STATEMENT mapping. Only unclassified elements benefit.

    Args:
        element_graph: The element graph with index mappings.
        disclosure_roots: Optional dict from extract_disclosure_root_elements().
            Maps element qname to set of disclosure type names.

    Returns:
        ClassificationResult with per-element classifications.
    """
    result = ClassificationResult()
    graph = element_graph.graph

    # Phase 1: BFS from hardcoded roots
    for stmt_type, root_qnames in self._roots.items():
      for root_qname in root_qnames:
        root_idx = element_graph.get_idx(root_qname)
        if root_idx is None:
          continue
        self._bfs_classify(
          graph, element_graph, root_idx, root_qname, stmt_type, result
        )

    # Phase 2: BFS from disclosure root elements
    if disclosure_roots:
      self._classify_from_disclosure_roots(element_graph, disclosure_roots, result)

    # Identify unclassified elements
    all_qnames = set(element_graph.elements)
    classified_qnames = set(result.classifications.keys())
    result.unclassified = sorted(all_qnames - classified_qnames)

    return result

  def _classify_from_disclosure_roots(
    self,
    element_graph: ElementGraph,
    disclosure_roots: dict[str, set[str]],
    result: ClassificationResult,
  ) -> None:
    """Phase 2: BFS from disclosure root elements with reduced weight.

    For each disclosure root, maps its disclosure types to StatementType
    via DISCLOSURE_TO_STATEMENT. Uses weight 0.9 to preserve priority of
    hardcoded roots.
    """
    graph = element_graph.graph
    seen_seeds: set[tuple[str, StatementType]] = set()

    for qname, disclosure_types in disclosure_roots.items():
      root_idx = element_graph.get_idx(qname)
      if root_idx is None:
        continue

      for dtype in sorted(disclosure_types):
        stmt_type = DISCLOSURE_TO_STATEMENT.get(dtype)
        if stmt_type is None:
          continue

        seed_key = (qname, stmt_type)
        if seed_key in seen_seeds:
          continue
        seen_seeds.add(seed_key)

        self._bfs_classify(
          graph,
          element_graph,
          root_idx,
          qname,
          stmt_type,
          result,
          initial_weight=0.9,
        )

  def _bfs_classify(
    self,
    graph: nk.Graph,
    element_graph: ElementGraph,
    root_idx: int,
    root_qname: str,
    stmt_type: StatementType,
    result: ClassificationResult,
    initial_weight: float = 1.0,
  ) -> None:
    """BFS from a root node, classifying all reachable descendants."""
    visited: set[int] = set()
    queue: deque[tuple[int, int, float]] = deque()

    # Seed the root itself
    queue.append((root_idx, 0, initial_weight))
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
