"""Integration tests for liquidity narrowing in ``suggest_mapping_candidates``.

Runs against the seeded library (``extensions_session("library")``), same as
the framework-seed suite. When the CoA element carries a ``liquidity`` trait
(current / noncurrent — derived from the QB ``AccountType`` at sync, or set on
the manual element-create API), the suggester drops rs-gaap candidates of the
*opposite* liquidity so the auto-map agent never surfaces a noncurrent asset
for a "Bank" account (and vice versa). Absence of liquidity is a no-op — the
filter falls back to EFS-only, backward compatible.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text as sa_text

from robosystems.db.extensions import extensions_session
from robosystems.operations.roboledger.reads.taxonomies import (
  suggest_mapping_candidates,
)

_PPE = "rs-gaap:PropertyPlantAndEquipmentNet"  # noncurrent asset
_CASH = "rs-gaap:CashAndCashEquivalentsAtCarryingValue"  # current asset


@pytest.fixture(scope="module", autouse=True)
def _require_extensions_db() -> None:
  """Skip the whole file when the extensions DB isn't reachable.

  These assertions need a live, migrated + seeded extensions library
  database. CI runs the unit suite against a bare postgres that only has
  ``robosystems_test`` (no ``extensions`` DB), so skip cleanly there
  instead of erroring out — same guard as ``test_framework_bridge_seed``.
  """
  from sqlalchemy.exc import OperationalError

  try:
    with extensions_session("library") as s:
      s.execute(sa_text("SELECT 1"))
  except OperationalError as exc:
    pytest.skip(f"extensions DB unavailable: {exc.orig}")


@pytest.fixture(scope="module")
def asset_sets() -> dict[str, set[str]]:
  with extensions_session("library") as session:
    return {
      "efs_only": {c.qname for c in suggest_mapping_candidates(session, trait="asset")},
      "current": {
        c.qname
        for c in suggest_mapping_candidates(session, trait="asset", liquidity="current")
      },
      "noncurrent": {
        c.qname
        for c in suggest_mapping_candidates(
          session, trait="asset", liquidity="noncurrent"
        )
      },
    }


@pytest.mark.integration
class TestLiquidityNarrowing:
  def test_narrowing_is_a_strict_subset(self, asset_sets: dict[str, set[str]]) -> None:
    """Each liquidity-narrowed set is a strict subset of EFS-only — adding a
    liquidity constraint only ever removes candidates, never adds."""
    assert asset_sets["current"] < asset_sets["efs_only"]
    assert asset_sets["noncurrent"] < asset_sets["efs_only"]

  def test_opposite_liquidity_is_dropped(self, asset_sets: dict[str, set[str]]) -> None:
    """A current account never surfaces a noncurrent concept, and vice versa."""
    assert _PPE not in asset_sets["current"]
    assert _PPE in asset_sets["noncurrent"]
    assert _CASH not in asset_sets["noncurrent"]
    assert _CASH in asset_sets["current"]

  def test_efs_only_unchanged_without_liquidity(
    self, asset_sets: dict[str, set[str]]
  ) -> None:
    """Omitting liquidity returns the full EFS-bucket (backward compatible),
    and the liquidity-narrowed sets never reach outside it — the union is a
    subset of EFS-only, so narrowing only ever removes candidates. (Asserting
    set *equality* would also require every rs-gaap asset to carry a liquidity
    trait; that's a seed-data assumption, not behavior of the function under
    test, and would fail as a confusing set-diff if a trait-less concept is
    ever added. The strict-subset checks above already cover the narrowing.)"""
    assert asset_sets["current"] | asset_sets["noncurrent"] <= asset_sets["efs_only"]
    # And the constraint actually bites — the narrowed sets are materially
    # smaller, not trivially equal to the whole bucket.
    assert len(asset_sets["current"]) < len(asset_sets["efs_only"])
