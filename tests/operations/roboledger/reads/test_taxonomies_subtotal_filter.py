"""Tests for the rs-gaap subtotal denylist applied in
``expand_to_rs_gaap_candidates``.

The function must never return a denylisted rollup as either a
``candidate`` or as ``rs_gaap_parent`` — a CoA arc to a rollup would
land a leaf fact on a calculated total and double-count when the
renderer sums children.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from robosystems.operations.agents.implementations.mapping.constants import (
  RS_GAAP_SUBTOTAL_DENYLIST,
)
from robosystems.operations.roboledger.reads.taxonomies import (
  expand_to_rs_gaap_candidates,
)


def _row(**kwargs):
  """Build a SQLAlchemy-row-like MagicMock."""
  m = MagicMock()
  for k, v in kwargs.items():
    setattr(m, k, v)
  return m


def _make_session(fac_qname: str, equiv_rows, child_rows=None, presentation_ids=None):
  """Build a session that returns ``fac_qname`` for the FAC lookup,
  ``presentation_ids`` for the rs-gaap-presentation set query (defaults
  to "no filter" — empty set means caller treats as unfiltered),
  ``equiv_rows`` for the fac-to-rs-gaap query, and ``child_rows`` for
  the type-subtype query."""
  child_rows = child_rows or []
  presentation_ids = presentation_ids or []
  session = MagicMock()

  fac_lookup = MagicMock()
  fac_lookup.fetchone.return_value = _row(qname=fac_qname)

  presentation_query = MagicMock()
  presentation_query.fetchall.return_value = [
    _row(element_id=eid) for eid in presentation_ids
  ]

  equiv_query = MagicMock()
  equiv_query.fetchall.return_value = equiv_rows

  child_query = MagicMock()
  child_query.fetchall.return_value = child_rows

  session.execute.side_effect = [
    fac_lookup,
    presentation_query,
    equiv_query,
    child_query,
  ]
  return session


def test_filters_denylisted_rollup_from_narrow_equivalents():
  """fac:Assets's equivalents include rs-gaap:Assets (rollup) and
  rs-gaap:AssetsNet (concept). The rollup must be filtered before the
  narrow-case parent selection runs."""
  equiv_rows = [
    _row(id="elem_assets_rollup", qname="rs-gaap:Assets", name="Assets"),
    _row(id="elem_assets_net", qname="rs-gaap:AssetsNet", name="Net Assets"),
  ]
  session = _make_session("fac:Assets", equiv_rows)

  result = expand_to_rs_gaap_candidates(session, "fac_assets_id")

  assert result is not None
  assert result["rs_gaap_parent"]["qname"] == "rs-gaap:AssetsNet"
  qnames = {c["qname"] for c in result["candidates"]}
  assert "rs-gaap:Assets" not in qnames
  assert "rs-gaap:AssetsNet" in qnames


def test_filters_denylisted_rollup_from_wide_equivalents():
  """fac:Revenues has 80+ rs-gaap equivalents including the rollup
  rs-gaap:Revenues itself. The wide-case branch must drop the rollup
  from the candidate list."""
  equiv_rows = [
    _row(id="elem_rev_rollup", qname="rs-gaap:Revenues", name="Revenues"),
    _row(id="elem_lic_rev", qname="rs-gaap:LicenseRevenue", name="License Revenue"),
    _row(id="elem_ad_rev", qname="rs-gaap:AdvertisingRevenue", name="Ad Revenue"),
    _row(id="elem_sub_rev", qname="rs-gaap:SubscriptionRevenue", name="Sub Revenue"),
    _row(id="elem_oth_rev", qname="rs-gaap:OtherSalesRevenueNet", name="Other Sales"),
  ]
  session = _make_session("fac:Revenues", equiv_rows)

  result = expand_to_rs_gaap_candidates(session, "fac_revenues_id")

  assert result is not None
  # Wide case → rs_gaap_parent stays None
  assert result["rs_gaap_parent"] is None
  qnames = {c["qname"] for c in result["candidates"]}
  assert "rs-gaap:Revenues" not in qnames
  assert "rs-gaap:LicenseRevenue" in qnames
  assert "rs-gaap:OtherSalesRevenueNet" in qnames


def test_returns_none_when_all_equivalents_are_rollups():
  """If every equivalent is a rollup, the filter empties the set —
  caller treats this as 'no expansion available'."""
  equiv_rows = [
    _row(id="elem_a", qname="rs-gaap:Assets", name="Assets"),
    _row(id="elem_b", qname="rs-gaap:AssetsCurrent", name="Current Assets"),
  ]
  session = _make_session("fac:Assets", equiv_rows)

  result = expand_to_rs_gaap_candidates(session, "fac_assets_id")

  assert result is None


def test_filters_denylisted_type_subtype_children():
  """Type-subtype children may themselves be rollups in some branches
  (e.g. some AssetsCurrent subtypes). The narrow-case child walk must
  filter these the same way as the equivalence set."""
  equiv_rows = [
    _row(
      id="elem_recv_net",
      qname="rs-gaap:ReceivablesNetCurrent",
      name="Receivables, Net",
    ),
  ]
  child_rows = [
    _row(
      id="elem_subrollup",
      qname="rs-gaap:AssetsCurrent",  # ← rollup, must be filtered
      name="Current Assets",
      parent_id="elem_recv_net",
    ),
    _row(
      id="elem_acc_recv",
      qname="rs-gaap:AccountsReceivableNetCurrent",
      name="AR, Net",
      parent_id="elem_recv_net",
    ),
  ]
  session = _make_session("fac:CurrentAssets", equiv_rows, child_rows)

  result = expand_to_rs_gaap_candidates(session, "fac_current_assets_id")

  assert result is not None
  qnames = {c["qname"] for c in result["candidates"]}
  assert "rs-gaap:AssetsCurrent" not in qnames
  assert "rs-gaap:AccountsReceivableNetCurrent" in qnames
  assert "rs-gaap:ReceivablesNetCurrent" in qnames


def test_filters_out_of_presentation_concepts():
  """When a presentation set is loaded, candidates not in it are dropped.
  Auto-mapper restricts targets to renderable concepts."""
  equiv_rows = [
    _row(id="elem_in_pres", qname="rs-gaap:CashCashEquivalents", name="Cash"),
    _row(id="elem_orphan", qname="rs-gaap:UtilitiesOperatingExpense", name="Util"),
  ]
  session = _make_session(
    "fac:Assets",
    equiv_rows,
    presentation_ids=["elem_in_pres"],  # only one is in presentation
  )

  result = expand_to_rs_gaap_candidates(session, "fac_assets_id")

  assert result is not None
  qnames = {c["qname"] for c in result["candidates"]}
  assert "rs-gaap:CashCashEquivalents" in qnames
  assert "rs-gaap:UtilitiesOperatingExpense" not in qnames


def test_empty_presentation_set_is_unfiltered():
  """When presentation set is empty (taxonomy not seeded), the filter
  is bypassed — partial deployments still function."""
  equiv_rows = [
    _row(id="elem_a", qname="rs-gaap:SomeConcept", name="Some"),
    _row(id="elem_b", qname="rs-gaap:OtherConcept", name="Other"),
  ]
  session = _make_session("fac:Assets", equiv_rows, presentation_ids=[])

  result = expand_to_rs_gaap_candidates(session, "fac_assets_id")

  assert result is not None
  qnames = {c["qname"] for c in result["candidates"]}
  assert qnames == {"rs-gaap:SomeConcept", "rs-gaap:OtherConcept"}


def test_denylist_locks_canonical_rollups():
  """Lock down the set of statement-level rollups. If any of these
  qnames stops being denylisted, this test catches it before silent
  rendering breakage."""
  # Sample of the most load-bearing rollups — full list lives in
  # constants.RS_GAAP_SUBTOTAL_DENYLIST.
  must_be_denylisted = {
    "rs-gaap:Assets",
    "rs-gaap:Liabilities",
    "rs-gaap:StockholdersEquity",
    "rs-gaap:Revenues",
    "rs-gaap:OperatingExpenses",
    "rs-gaap:NetIncomeLoss",
    "rs-gaap:GrossProfit",
  }
  assert must_be_denylisted.issubset(RS_GAAP_SUBTOTAL_DENYLIST)
