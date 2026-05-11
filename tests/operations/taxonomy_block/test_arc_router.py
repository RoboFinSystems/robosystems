"""Tests for ``_build_arc_router`` — the period-type-based structure
router that fixes the rs-gaap-presentation seed loader.

Without this router, all 1890 parent triples in
``rs-gaap-presentation/v1/taxonomy.jsonld`` (which lack per-arc role
qualifiers) land on a "default structure" instead of being routed into
BS / IS / CF presentation structures.
"""

from __future__ import annotations

from robosystems.operations.taxonomy_block.library_creator import (
  _build_arc_router,
)


def test_router_returns_none_when_bs_missing():
  """Without a balance_sheet structure, no period-type routing makes
  sense — caller falls back to default."""
  assert _build_arc_router({"income_statement": "is_id"}) is None
  assert _build_arc_router({"custom": "x"}) is None
  assert _build_arc_router({}) is None


def test_router_returns_none_when_is_missing():
  assert _build_arc_router({"balance_sheet": "bs_id"}) is None


def test_routes_instant_to_balance_sheet():
  router = _build_arc_router(
    {
      "balance_sheet": "bs_id",
      "income_statement": "is_id",
      "cash_flow_statement": "cf_id",
    }
  )
  assert router is not None
  assert router("rs-gaap:Assets", "instant") == "bs_id"
  assert router("rs-gaap:Liabilities", "instant") == "bs_id"
  assert router("rs-gaap:StockholdersEquity", "instant") == "bs_id"


def test_routes_duration_to_income_statement():
  router = _build_arc_router(
    {
      "balance_sheet": "bs_id",
      "income_statement": "is_id",
      "cash_flow_statement": "cf_id",
    }
  )
  assert router is not None
  assert router("rs-gaap:Revenues", "duration") == "is_id"
  assert router("rs-gaap:OperatingExpenses", "duration") == "is_id"
  assert router("rs-gaap:NetIncomeLoss", "duration") == "is_id"


def test_routes_cash_flow_patterns_to_cf():
  router = _build_arc_router(
    {
      "balance_sheet": "bs_id",
      "income_statement": "is_id",
      "cash_flow_statement": "cf_id",
    }
  )
  assert router is not None
  assert (
    router("rs-gaap:NetCashProvidedByUsedInOperatingActivities", "duration") == "cf_id"
  )
  assert router("rs-gaap:PaymentsForRepurchaseOfCommonStock", "duration") == "cf_id"
  assert router("rs-gaap:ProceedsFromIssuanceOfDebt", "duration") == "cf_id"
  assert router("rs-gaap:IncreaseDecreaseInAccountsReceivable", "duration") == "cf_id"


def test_routes_cf_pattern_to_is_when_no_cf_structure():
  """When the package declares only BS+IS but no CF, duration concepts
  with CF naming patterns still go to IS — better than dropping them."""
  router = _build_arc_router(
    {
      "balance_sheet": "bs_id",
      "income_statement": "is_id",
    }
  )
  assert router is not None
  assert (
    router("rs-gaap:NetCashProvidedByUsedInOperatingActivities", "duration") == "is_id"
  )


def test_returns_none_for_unknown_period_type():
  """Caller treats None as 'fall back to default structure'."""
  router = _build_arc_router({"balance_sheet": "bs_id", "income_statement": "is_id"})
  assert router is not None
  assert router("rs-gaap:Whatever", None) is None
  assert router("rs-gaap:Whatever", "") is None
