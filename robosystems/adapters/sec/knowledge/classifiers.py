"""Statement classification for XBRL elements.

Classifies elements into financial statement categories (Income Statement,
Balance Sheet, Cash Flow Statement, Statement of Equity) using BFS
traversal from known root elements through calculation/presentation graphs.
"""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

import networkit as nk

if TYPE_CHECKING:
  from robosystems.adapters.sec.knowledge.graphs import ElementGraph

logger = logging.getLogger(__name__)


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
  """Complete classification results.

  Stores at most one Classification per (qname, StatementType) pair —
  the shallowest path wins. This keeps memory bounded at O(nodes x 4)
  instead of O(nodes x roots).
  """

  # qname -> {StatementType -> best Classification}
  classifications: dict[str, dict[StatementType, Classification]] = field(
    default_factory=dict
  )
  unclassified: list[str] = field(default_factory=list)

  @property
  def total_classified(self) -> int:
    return len(self.classifications)

  @property
  def total_unclassified(self) -> int:
    return len(self.unclassified)

  def get_primary_statement(self, qname: str) -> StatementType | None:
    """Get the primary (shallowest) statement for an element."""
    by_type = self.classifications.get(qname)
    if not by_type:
      return None
    best = min(by_type.values(), key=lambda c: c.depth)
    return best.statement

  def get_min_depth(self, qname: str) -> int | None:
    """Get the minimum BFS depth across all statement types."""
    by_type = self.classifications.get(qname)
    if not by_type:
      return None
    return min(c.depth for c in by_type.values())

  def get_all_classifications(self, qname: str) -> list[Classification]:
    """Get all classifications for an element (one per statement type)."""
    by_type = self.classifications.get(qname)
    if not by_type:
      return []
    return list(by_type.values())


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
  through the calculation/presentation graph. For each (element, statement_type)
  pair, only the shallowest classification is kept to bound memory at
  O(nodes x 4 statement types) regardless of how many roots are used.
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
    hardcoded_count = 0
    for stmt_type, root_qnames in self._roots.items():
      for root_qname in root_qnames:
        root_idx = element_graph.get_idx(root_qname)
        if root_idx is None:
          logger.debug(f"Hardcoded root not found in graph: {root_qname}")
          continue
        hardcoded_count += 1
        self._bfs_classify(
          graph, element_graph, root_idx, root_qname, stmt_type, result
        )
    logger.info(
      f"Phase 1 complete: {hardcoded_count} hardcoded roots, "
      f"{result.total_classified} elements classified"
    )

    # Phase 2: BFS from disclosure root elements
    if disclosure_roots:
      pre_count = result.total_classified
      self._classify_from_disclosure_roots(element_graph, disclosure_roots, result)
      logger.info(
        f"Phase 2 complete: {result.total_classified - pre_count} new elements "
        f"classified from disclosure roots, {result.total_classified} total"
      )

    # Identify unclassified elements
    all_qnames = set(element_graph.elements)
    classified_qnames = set(result.classifications.keys())
    result.unclassified = sorted(all_qnames - classified_qnames)

    logger.info(
      f"Classification complete: {result.total_classified} classified, "
      f"{result.total_unclassified} unclassified out of {len(all_qnames)} elements"
    )
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
    total_roots = len(disclosure_roots)
    mapped_count = 0
    skipped_unmapped = 0
    skipped_not_in_graph = 0

    for qname, disclosure_types in disclosure_roots.items():
      root_idx = element_graph.get_idx(qname)
      if root_idx is None:
        skipped_not_in_graph += 1
        continue

      for dtype in sorted(disclosure_types):
        stmt_type = DISCLOSURE_TO_STATEMENT.get(dtype)
        if stmt_type is None:
          skipped_unmapped += 1
          continue

        seed_key = (qname, stmt_type)
        if seed_key in seen_seeds:
          continue
        seen_seeds.add(seed_key)
        mapped_count += 1

        self._bfs_classify(
          graph,
          element_graph,
          root_idx,
          qname,
          stmt_type,
          result,
          initial_weight=0.9,
        )

    logger.info(
      f"Disclosure roots: {total_roots} total, {mapped_count} mapped to statements, "
      f"{skipped_unmapped} unmapped disclosure types, "
      f"{skipped_not_in_graph} not found in graph"
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
    """BFS from a root node, classifying all reachable descendants.

    Only updates a node's classification if the new path is shallower
    than any existing classification for the same statement type.
    Skips subtrees where all nodes already have shallower classifications.
    """
    visited: set[int] = set()
    queue: deque[tuple[int, int, float]] = deque()

    # Seed the root itself
    queue.append((root_idx, 0, initial_weight))
    visited.add(root_idx)

    while queue:
      node_idx, depth, cum_weight = queue.popleft()
      qname = element_graph.get_qname(node_idx)

      # Only store if this is the shallowest path for this (qname, stmt_type)
      by_type = result.classifications.get(qname)
      if by_type is not None:
        existing = by_type.get(stmt_type)
        if existing is not None and existing.depth <= depth:
          # Already have a shallower classification — skip this subtree
          continue
      else:
        by_type = {}
        result.classifications[qname] = by_type

      by_type[stmt_type] = Classification(
        statement=stmt_type,
        depth=depth,
        weight=cum_weight,
        via_root=root_qname,
      )

      # Traverse outgoing edges (parent -> child)
      for neighbor in graph.iterNeighbors(node_idx):
        if neighbor not in visited:
          edge_weight = graph.weight(node_idx, neighbor)
          visited.add(neighbor)
          queue.append((neighbor, depth + 1, cum_weight * edge_weight))
