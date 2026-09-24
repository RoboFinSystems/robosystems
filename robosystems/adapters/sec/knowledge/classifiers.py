"""Classify XBRL elements into financial statements by BFS from known root elements."""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
  import networkit as nk

  from robosystems.adapters.sec.knowledge.graphs import ElementGraph

logger = logging.getLogger(__name__)


class StatementType(str, Enum):
  INCOME_STATEMENT = "IncomeStatement"
  BALANCE_SHEET = "BalanceSheet"
  CASH_FLOW = "CashFlow"
  EQUITY = "Equity"


@dataclass
class Classification:
  statement: StatementType
  depth: int
  weight: float
  via_root: str


@dataclass
class ClassificationResult:
  """At most one Classification per (qname, StatementType), the shallowest,
  bounding memory at O(nodes x 4) rather than O(nodes x roots)."""

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
    by_type = self.classifications.get(qname)
    if not by_type:
      return None
    best = min(by_type.values(), key=lambda c: c.depth)
    return best.statement

  def get_min_depth(self, qname: str) -> int | None:
    by_type = self.classifications.get(qname)
    if not by_type:
      return None
    return min(c.depth for c in by_type.values())

  def get_all_classifications(self, qname: str) -> list[Classification]:
    by_type = self.classifications.get(qname)
    if not by_type:
      return []
    return list(by_type.values())


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


# The Seattle Method disclosures whose disclosure-isSECType is StatementType
# (face statements). Note disclosures are deliberately absent; their names
# still reach element_knowledge's disclosure_type column.
DISCLOSURE_TO_STATEMENT: dict[str, StatementType] = {
  "IncomeStatement": StatementType.INCOME_STATEMENT,
  "EarningsPerShareDisclosuresHierarchy": StatementType.INCOME_STATEMENT,
  "StatementOfComprehensiveIncome": StatementType.INCOME_STATEMENT,
  "BalanceSheet": StatementType.BALANCE_SHEET,
  "AssetsRollUp": StatementType.BALANCE_SHEET,
  "LiabilitiesAndEquityRollUp": StatementType.BALANCE_SHEET,
  "CashFlowStatement": StatementType.CASH_FLOW,
  "StatementOfChangesInEquity": StatementType.EQUITY,
}


class StatementClassifier:
  """Propagates statement classification by BFS from root elements."""

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
    """BFS from STATEMENT_ROOTS (weight 1.0), then from disclosure roots
    (qname → disclosure types, weight 0.9) mapped via DISCLOSURE_TO_STATEMENT.

    A later pass only replaces a classification it reaches at a shallower depth.
    """
    result = ClassificationResult()
    graph = element_graph.graph

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

    if disclosure_roots:
      pre_count = result.total_classified
      self._classify_from_disclosure_roots(element_graph, disclosure_roots, result)
      logger.info(
        f"Phase 2 complete: {result.total_classified - pre_count} new elements "
        f"classified from disclosure roots, {result.total_classified} total"
      )

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
    """Classify every descendant of a root, keeping only shallower paths and
    pruning subtrees already reached shallower for this statement type."""
    visited: set[int] = set()
    queue: deque[tuple[int, int, float]] = deque()

    queue.append((root_idx, 0, initial_weight))
    visited.add(root_idx)

    while queue:
      node_idx, depth, cum_weight = queue.popleft()
      qname = element_graph.get_qname(node_idx)

      by_type = result.classifications.get(qname)
      if by_type is not None:
        existing = by_type.get(stmt_type)
        if existing is not None and existing.depth <= depth:
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

      for neighbor in graph.iterNeighbors(node_idx):
        if neighbor not in visited:
          edge_weight = graph.weight(node_idx, neighbor)
          visited.add(neighbor)
          queue.append((neighbor, depth + 1, cum_weight * edge_weight))
