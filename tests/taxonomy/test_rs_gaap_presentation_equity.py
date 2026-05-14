"""Shape tests for the rs-gaap Statement of Changes in Equity presentation.

Under the rs-gaap-anchored architecture (§3.2 + the rs-gaap-everything-at-the-
reporting-layer rewrite), the roll-forward hangs directly off
``rs-gaap:StockholdersEquity`` — no fac scaffolding. The activity arcs
are the rs-gaap concepts the statement displays as change rows: NetIncomeLoss,
OCI, issuance, repurchase, share-based compensation, dividends.

These tests guard against:
- the structure being dropped on a future edit
- arcs being rewired away from the StockholdersEquity hub
- the activity concept set shrinking below the six items we ship
"""

from __future__ import annotations

from pathlib import Path

import pytest

from robosystems.taxonomy.loaders import load_taxonomy_package
from robosystems.taxonomy.model import TaxonomyPackage

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PACKAGES = _REPO_ROOT / "robosystems" / "taxonomy" / "packages"

EQUITY_STRUCTURE_NAME = (
  "rs-gaap — Statement of Changes in Equity — Roll Forward (Total)"
)
EQUITY_ROLE_URI = "https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/Equity-rollforward"
ROLLFORWARD_HUB = "rs-gaap:StockholdersEquity"


@pytest.fixture(scope="module")
def presentation() -> TaxonomyPackage:
  return load_taxonomy_package(
    _PACKAGES / "rs-gaap-presentation" / "v1" / "taxonomy.jsonld"
  )


class TestEquityPresentationStructure:
  def test_structure_exists(self, presentation: TaxonomyPackage) -> None:
    names = [s.name for s in presentation.structures]
    assert EQUITY_STRUCTURE_NAME in names

  def test_structure_type_and_pattern(self, presentation: TaxonomyPackage) -> None:
    s = next(s for s in presentation.structures if s.name == EQUITY_STRUCTURE_NAME)
    assert s.structure_type == "equity_statement"
    assert s.concept_arrangement == "roll_forward"
    assert s.role_uri == EQUITY_ROLE_URI

  def test_rollforward_hub_has_activity_arcs(
    self, presentation: TaxonomyPackage
  ) -> None:
    """The StockholdersEquity hub fans out to the six rs-gaap activity
    concepts that drive the roll-forward: net income, OCI, issuance,
    repurchase, share-based compensation, dividends."""
    arcs_from_hub = [
      a
      for a in presentation.associations
      if a.role == EQUITY_ROLE_URI and a.from_qname == ROLLFORWARD_HUB
    ]
    activity_targets = {a.to_qname for a in arcs_from_hub}

    required = {
      "rs-gaap:NetIncomeLoss",
      "rs-gaap:OtherComprehensiveIncomeLossNetOfTax",
      "rs-gaap:ProceedsFromIssuanceOfCommonStock",
      "rs-gaap:PaymentsForRepurchaseOfCommonStock",
      "rs-gaap:ShareBasedCompensation",
      "rs-gaap:PaymentsOfDividends",
    }
    missing = required - activity_targets
    assert not missing, f"rollforward missing activity arcs to: {missing}"
