"""Tests for the rs-gaap subtotal guards used by ``suggest_mapping_candidates``.

Two layers keep a CoA arc off a calculated total, where a leaf fact would
double-count when the renderer sums children:

* ``RS_GAAP_SUBTOTAL_DENYLIST`` — the static list of statement-level rollups.
* ``_load_rollup_concepts`` — the per-Style derivation, which supersedes the
  static list when a Reporting Style is seeded. The static list over-denies on
  thin Styles: ``rs-gaap:Revenues`` is a rollup in the full taxonomy but IS the
  leaf on a Style where it has no rendering children, and a CoA account must be
  allowed to map to it there.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from robosystems.operations.operators.implementations.mapping.constants import (
  RS_GAAP_SUBTOTAL_DENYLIST,
)
from robosystems.operations.roboledger.reads.taxonomies import (
  _load_rollup_concepts,
)


def _row(**kwargs):
  """Build a SQLAlchemy-row-like MagicMock."""
  m = MagicMock()
  for k, v in kwargs.items():
    setattr(m, k, v)
  return m


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


def test_load_rollup_concepts_empty_when_style_unseeded():
  """A Style with no composed networks yields an empty rollup set, so nothing
  is treated as a rollup and callers fall back to the static denylist."""
  session = _rollup_session([])
  assert _load_rollup_concepts(session, "empty_style") == set()
