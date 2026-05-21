"""Tests for the rs-gaap subtotal denylist applied in
``expand_to_rs_gaap_candidates``.

The function must never return a denylisted rollup as either a
``candidate`` or as ``rs_gaap_parent`` — a CoA arc to a rollup would
land a leaf fact on a calculated total and double-count when the
renderer sums children.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from robosystems.operations.operators.implementations.mapping.constants import (
  RS_GAAP_SUBTOTAL_DENYLIST,
)
from robosystems.operations.roboledger.reads.taxonomies import (
  _is_rolled_up_at_render,
  _load_rollup_concepts,
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
  the rs-gaap-type-subtype query."""
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


def test_returns_none_when_all_equivalents_are_rollups_and_no_children():
  """If every equivalent is a rollup AND no traversable rs-gaap-type-subtype
  children exist, there's nothing to land on — return None so the
  caller's per-FAC fallback fires."""
  equiv_rows = [
    _row(id="elem_a", qname="rs-gaap:Assets", name="Assets"),
    _row(id="elem_b", qname="rs-gaap:AssetsCurrent", name="Current Assets"),
  ]
  session = _make_session("fac:Assets", equiv_rows)

  result = expand_to_rs_gaap_candidates(session, "fac_assets_id")

  assert result is None


def test_traverses_through_denylisted_rollup_to_reach_children():
  """The denylist blocks rollups as TARGETS, but we still traverse
  rs-gaap-type-subtype children of denylisted equivalents. fac:CurrentLiabilities
  is the canonical case: its only fac-to-rs-gaap equivalent is
  rs-gaap:LiabilitiesCurrent (denylisted rollup), but its real children
  (AccountsPayable, DebtCurrent, …) are exactly what CoA accounts must
  land on. Earlier behavior dropped the rollup equivalent up front and
  returned None, forcing every BS current-liability CoA to the per-FAC
  Other bucket."""
  equiv_rows = [
    _row(
      id="elem_liab_curr",
      qname="rs-gaap:LiabilitiesCurrent",  # denylisted rollup
      name="Liabilities, Current",
    ),
  ]
  child_rows = [
    _row(
      id="elem_ap",
      qname="rs-gaap:AccountsPayableCurrent",
      name="Accounts Payable",
      parent_id="elem_liab_curr",
    ),
    _row(
      id="elem_debt",
      qname="rs-gaap:DebtCurrent",
      name="Debt, Current",
      parent_id="elem_liab_curr",
    ),
  ]
  session = _make_session("fac:CurrentLiabilities", equiv_rows, child_rows)

  result = expand_to_rs_gaap_candidates(session, "fac_current_liabilities_id")

  assert result is not None
  qnames = {c["qname"] for c in result["candidates"]}
  # Children landed; rollup equivalent itself is NOT in the candidate set.
  assert "rs-gaap:AccountsPayableCurrent" in qnames
  assert "rs-gaap:DebtCurrent" in qnames
  assert "rs-gaap:LiabilitiesCurrent" not in qnames
  # No emittable equivalents → parent is None → refinement-pass fallback
  # falls through to FAC_TO_RS_GAAP_FALLBACK rather than landing on the
  # rollup as the "narrow parent".
  assert result["rs_gaap_parent"] is None


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


# ---------------------------------------------------------------------------
# Structure-aware rollup guard (info-block §3.7.2)
#
# The static denylist over-denies on thin Reporting Style structures: a
# concept like rs-gaap:Revenues is a rollup in the full taxonomy but, on a
# Style where it has no rendering children, it IS the leaf and a CoA account
# must be allowed to map to it. _load_rollup_concepts derives the rolled-up
# set per-Style (concepts that are PARENTS in the Style's presentation arcs);
# _is_rolled_up_at_render is the single-concept predicate over it.
# ---------------------------------------------------------------------------


def _rollup_session(parent_element_ids):
  """Session whose reporting_style_networks presentation query returns the
  given parent (rolled-up) element_ids."""
  session = MagicMock()
  q = MagicMock()
  q.fetchall.return_value = [_row(element_id=eid) for eid in parent_element_ids]
  session.execute.return_value = q
  return session


def test_load_rollup_concepts_returns_parent_element_ids():
  session = _rollup_session(["elem_assets", "elem_liabilities"])
  assert _load_rollup_concepts(session, "style_1") == {
    "elem_assets",
    "elem_liabilities",
  }


def test_is_rolled_up_at_render_true_for_parent_concept():
  """A concept that is a parent on the active Style's structures (its
  children render) is rolled up — CoA must not map to it."""
  session = _rollup_session(["elem_assets"])
  assert _is_rolled_up_at_render(session, "style_1", "elem_assets") is True


def test_is_rolled_up_at_render_false_for_leaf_on_thin_style():
  """The core fix: rs-gaap:Revenues is statically denylisted, but on a thin
  Style where it has no rendering children it is the leaf. The structure-
  aware predicate must allow it (returns False = not a rollup here)."""
  # Only Assets rolls up on this Style; the revenue concept renders as a leaf.
  session = _rollup_session(["elem_assets"])
  assert _is_rolled_up_at_render(session, "thin_style", "elem_revenues_leaf") is False


def test_is_rolled_up_at_render_false_when_style_unseeded():
  """A Style with no composed networks yields an empty rollup set, so nothing
  is treated as a rollup — callers fall back to the static denylist."""
  session = _rollup_session([])
  assert _is_rolled_up_at_render(session, "empty_style", "elem_anything") is False


def test_expand_structure_aware_allows_leaf_that_is_statically_denylisted():
  """With a Reporting Style active where rs-gaap:Revenues is a LEAF (not a
  parent in the Style's networks), expand_to_rs_gaap_candidates must offer
  it as a candidate even though it is in RS_GAAP_SUBTOTAL_DENYLIST — the
  structure-aware guard supersedes the static list when a Style is seeded."""
  equiv_rows = [
    _row(id="elem_rev", qname="rs-gaap:Revenues", name="Revenues"),
    _row(id="elem_lic_rev", qname="rs-gaap:LicenseRevenue", name="License Revenue"),
  ]
  session = MagicMock()
  fac_lookup = MagicMock()
  fac_lookup.fetchone.return_value = _row(qname="fac:Revenues")
  # reporting_style_id present → presentation_set via _load_renderable_concepts
  renderable_query = MagicMock()
  renderable_query.fetchall.return_value = [
    _row(element_id="elem_rev"),
    _row(element_id="elem_lic_rev"),
  ]
  equiv_query = MagicMock()
  equiv_query.fetchall.return_value = equiv_rows
  # rollup set: only some other concept rolls up here — Revenues is a leaf.
  rollup_query = MagicMock()
  rollup_query.fetchall.return_value = [_row(element_id="elem_other_rollup")]
  child_query = MagicMock()
  child_query.fetchall.return_value = []
  session.execute.side_effect = [
    fac_lookup,
    renderable_query,
    equiv_query,
    rollup_query,
    child_query,
  ]

  result = expand_to_rs_gaap_candidates(
    session, "fac_revenues_id", reporting_style_id="thin_style"
  )

  assert result is not None
  qnames = {c["qname"] for c in result["candidates"]}
  # Statically-denylisted, but a leaf on this Style → now offered.
  assert "rs-gaap:Revenues" in qnames
  assert "rs-gaap:LicenseRevenue" in qnames
