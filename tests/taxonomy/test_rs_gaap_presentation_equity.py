"""Shape tests for the rs-gaap Statement of Changes in Equity presentation.

The equity statement was added in the same branch that authored the
multi-step IS, classified BS, and indirect CF. Like those, it's a
roll-forward anchored on new fac: abstracts: StatementOfChangesInEquity
[Abstract/Table/LineItems/RollForward].

These tests guard against:
- the structure being dropped on a future edit
- arcs being rewired away from the rollforward hub
- the activity concept set shrinking below the eight items we ship
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
ROLLFORWARD_HUB = "fac:StatementOfChangesInEquityRollForward"


@pytest.fixture(scope="module")
def presentation() -> TaxonomyPackage:
  return load_taxonomy_package(
    _PACKAGES / "rs-gaap-presentation" / "v1" / "taxonomy.jsonld"
  )


@pytest.fixture(scope="module")
def fac() -> TaxonomyPackage:
  return load_taxonomy_package(_PACKAGES / "fac" / "v1" / "taxonomy.jsonld")


class TestFacEquityAbstracts:
  """The four fac: abstracts the equity structure hangs off must exist."""

  REQUIRED = (
    "fac:StatementOfChangesInEquityAbstract",
    "fac:StatementOfChangesInEquityTable",
    "fac:StatementOfChangesInEquityLineItems",
    "fac:StatementOfChangesInEquityRollForward",
  )

  def test_all_present(self, fac: TaxonomyPackage) -> None:
    qnames = {e.qname for e in fac.elements}
    missing = [q for q in self.REQUIRED if q not in qnames]
    assert not missing, f"missing fac abstracts: {missing}"


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
    """The rollforward grouping must fan out to at least 7 activity items:
    beginning balance + net income + OCI + issuance + repurchase + SBC +
    dividends. (Ending balance reuses fac:Equity, same as opening.)"""
    arcs_from_hub = [
      a
      for a in presentation.associations
      if a.role == EQUITY_ROLE_URI and a.from_qname == ROLLFORWARD_HUB
    ]
    activity_targets = {a.to_qname for a in arcs_from_hub}

    required = {
      "fac:Equity",
      "rs-gaap:NetIncomeLoss",
      "rs-gaap:OtherComprehensiveIncomeLossNetOfTax",
      "rs-gaap:ProceedsFromIssuanceOfCommonStock",
      "rs-gaap:PaymentsForRepurchaseOfCommonStock",
      "rs-gaap:ShareBasedCompensation",
      "rs-gaap:PaymentsOfDividends",
    }
    missing = required - activity_targets
    assert not missing, f"rollforward missing activity arcs to: {missing}"
