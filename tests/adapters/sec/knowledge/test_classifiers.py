"""Tests for disclosure-seeded statement classification.

Tests DISCLOSURE_TO_STATEMENT mapping and the Phase 2 BFS from
disclosure root elements in StatementClassifier.
"""

import networkit as nk

from robosystems.adapters.sec.knowledge.classifiers import (
  DISCLOSURE_TO_STATEMENT,
  StatementClassifier,
  StatementType,
)
from robosystems.adapters.sec.knowledge.graphs import ElementGraph


def _make_simple_graph(edges: list[tuple[str, str]]) -> ElementGraph:
  """Build a small test graph from (parent, child) pairs."""
  graph = nk.Graph(0, weighted=True, directed=True)
  elements: list[str] = []
  element_to_idx: dict[str, int] = {}

  def ensure(qname):
    if qname not in element_to_idx:
      idx = graph.addNode()
      elements.append(qname)
      element_to_idx[qname] = idx
    return element_to_idx[qname]

  for parent, child in edges:
    p = ensure(parent)
    c = ensure(child)
    graph.addEdge(p, c, 1.0)

  return ElementGraph(graph=graph, elements=elements, element_to_idx=element_to_idx)


class TestDisclosureToStatement:
  def test_income_statement_entries(self):
    assert DISCLOSURE_TO_STATEMENT["IncomeStatement"] == StatementType.INCOME_STATEMENT
    assert (
      DISCLOSURE_TO_STATEMENT["EarningsPerShareDisclosuresHierarchy"]
      == StatementType.INCOME_STATEMENT
    )

  def test_balance_sheet_entries(self):
    assert DISCLOSURE_TO_STATEMENT["AssetsRollUp"] == StatementType.BALANCE_SHEET
    assert (
      DISCLOSURE_TO_STATEMENT["LiabilitiesAndEquityRollUp"]
      == StatementType.BALANCE_SHEET
    )
    assert DISCLOSURE_TO_STATEMENT["BalanceSheet"] == StatementType.BALANCE_SHEET

  def test_cash_flow_entry(self):
    assert DISCLOSURE_TO_STATEMENT["CashFlowStatement"] == StatementType.CASH_FLOW

  def test_equity_entry(self):
    assert DISCLOSURE_TO_STATEMENT["StatementOfChangesInEquity"] == StatementType.EQUITY

  def test_only_statement_types_mapped(self):
    """Only 8 StatementType disclosures from the Seattle Method taxonomy are mapped.
    Note disclosures flow through disclosure_type column, not this mapping."""
    assert len(DISCLOSURE_TO_STATEMENT) == 8
    # Note disclosures are NOT mapped to statements
    assert "GoodwillRollForward" not in DISCLOSURE_TO_STATEMENT
    assert "InventoryNetRollUp" not in DISCLOSURE_TO_STATEMENT
    assert "DocumentInformation" not in DISCLOSURE_TO_STATEMENT


class TestDisclosureSeededBFS:
  def test_disclosure_seeds_classify_unreachable_elements(self):
    """Elements not reachable from hardcoded roots get classified via disclosure seeds."""
    # custom:TotalAssets is not in STATEMENT_ROOTS, so BFS can't reach it
    graph = _make_simple_graph([("custom:TotalAssets", "custom:CurrentAssets")])

    # Without disclosure roots, unclassified
    classifier = StatementClassifier()
    result_without = classifier.classify(graph)
    assert "custom:TotalAssets" in result_without.unclassified

    # With disclosure roots, classified via AssetsRollUp → BALANCE_SHEET
    disclosure_roots = {"custom:TotalAssets": {"AssetsRollUp"}}
    result_with = classifier.classify(graph, disclosure_roots=disclosure_roots)
    assert "custom:TotalAssets" not in result_with.unclassified
    stmt = result_with.get_primary_statement("custom:TotalAssets")
    assert stmt == StatementType.BALANCE_SHEET

  def test_disclosure_seeds_propagate_to_children(self):
    """BFS from disclosure root propagates classification to children."""
    graph = _make_simple_graph([("custom:TotalAssets", "custom:CurrentAssets")])
    disclosure_roots = {"custom:TotalAssets": {"AssetsRollUp"}}

    classifier = StatementClassifier()
    result = classifier.classify(graph, disclosure_roots=disclosure_roots)

    # Child should also be classified
    assert "custom:CurrentAssets" not in result.unclassified
    assert (
      result.get_primary_statement("custom:CurrentAssets")
      == StatementType.BALANCE_SHEET
    )

  def test_disclosure_weight_lower_than_hardcoded(self):
    """Disclosure-seeded classifications have weight 0.9, not 1.0."""
    graph = _make_simple_graph([("custom:TotalAssets", "custom:CurrentAssets")])
    disclosure_roots = {"custom:TotalAssets": {"AssetsRollUp"}}

    classifier = StatementClassifier()
    result = classifier.classify(graph, disclosure_roots=disclosure_roots)

    entries = result.classifications["custom:TotalAssets"]
    assert entries[0].weight == 0.9

  def test_hardcoded_roots_take_priority(self):
    """When an element is reachable from both hardcoded and disclosure roots,
    both classifications are present but hardcoded has higher weight."""
    graph = _make_simple_graph([("us-gaap:NetIncomeLoss", "us-gaap:Assets")])
    disclosure_roots = {"us-gaap:Assets": {"AssetsRollUp"}}

    classifier = StatementClassifier()
    result = classifier.classify(graph, disclosure_roots=disclosure_roots)

    entries = result.classifications.get("us-gaap:Assets", [])
    # Should have entries from both
    stmt_weights = {e.statement: e.weight for e in entries}
    assert StatementType.INCOME_STATEMENT in stmt_weights
    assert stmt_weights[StatementType.INCOME_STATEMENT] == 1.0

  def test_unmapped_disclosure_type_ignored(self):
    """Disclosure types not in DISCLOSURE_TO_STATEMENT are skipped."""
    graph = _make_simple_graph([("custom:Info", "custom:Detail")])
    # DocumentInformation is not mapped to any statement
    disclosure_roots = {"custom:Info": {"DocumentInformation"}}

    classifier = StatementClassifier()
    result = classifier.classify(graph, disclosure_roots=disclosure_roots)
    assert "custom:Info" in result.unclassified

  def test_none_disclosure_roots_no_effect(self):
    """Passing None for disclosure_roots preserves existing behavior."""
    graph = _make_simple_graph([("us-gaap:NetIncomeLoss", "us-gaap:Revenues")])

    classifier = StatementClassifier()
    result1 = classifier.classify(graph)
    result2 = classifier.classify(graph, disclosure_roots=None)

    assert result1.total_classified == result2.total_classified
