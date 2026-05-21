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

import pytest

from robosystems.taxonomy import load_taxonomy_package
from robosystems.taxonomy.discovery import framework_root
from robosystems.taxonomy.model import TaxonomyPackage

_PACKAGES = framework_root("rs-gaap") / "packages"

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

  def test_block_type_and_pattern(self, presentation: TaxonomyPackage) -> None:
    s = next(s for s in presentation.structures if s.name == EQUITY_STRUCTURE_NAME)
    assert s.block_type == "equity_statement"
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


_PRES = "https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation"


def _bs_equity_children(pkg: TaxonomyPackage, role: str) -> set[str]:
  """Concepts presented under StockholdersEquity for a given BS role."""
  return {
    a.to_qname
    for a in pkg.associations
    if a.role == role and a.from_qname == "rs-gaap:StockholdersEquity"
  }


class TestEquityFormVariants:
  """Phase 4 equity-form axis: CORP balance sheet trimmed to corporate
  equity; dedicated PART/LLC balance-sheet + Statement-of-Changes
  structures present the form-appropriate capital concept."""

  def test_corp_bs_equity_dropped_partners_and_members(
    self, presentation: TaxonomyPackage
  ) -> None:
    children = _bs_equity_children(presentation, f"{_PRES}/BS-classified")
    assert "rs-gaap:PartnersCapital" not in children
    assert "rs-gaap:MembersEquity" not in children
    # corporate stack still present
    assert "rs-gaap:CommonStockValue" in children
    assert "rs-gaap:RetainedEarningsAccumulatedDeficit" in children

  def test_partnership_bs_presents_partners_capital(
    self, presentation: TaxonomyPackage
  ) -> None:
    roles = {s.role_uri for s in presentation.structures}
    assert f"{_PRES}/BS-classified-PART" in roles
    children = _bs_equity_children(presentation, f"{_PRES}/BS-classified-PART")
    assert children == {"rs-gaap:PartnersCapital"}

  def test_llc_bs_presents_members_equity(self, presentation: TaxonomyPackage) -> None:
    roles = {s.role_uri for s in presentation.structures}
    assert f"{_PRES}/BS-classified-LLC" in roles
    children = _bs_equity_children(presentation, f"{_PRES}/BS-classified-LLC")
    assert children == {"rs-gaap:MembersEquity"}

  def test_part_llc_bs_clone_full_asset_liability_section(
    self, presentation: TaxonomyPackage
  ) -> None:
    """The form-specific balance sheets are full statements — they clone
    the corporate asset/liability arcs, only the equity section differs."""
    corp_non_eq = {
      (a.from_qname, a.to_qname)
      for a in presentation.associations
      if a.role == f"{_PRES}/BS-classified"
      and a.from_qname != "rs-gaap:StockholdersEquity"
    }
    for role in (f"{_PRES}/BS-classified-PART", f"{_PRES}/BS-classified-LLC"):
      form_non_eq = {
        (a.from_qname, a.to_qname)
        for a in presentation.associations
        if a.role == role and a.from_qname != "rs-gaap:StockholdersEquity"
      }
      assert form_non_eq == corp_non_eq, role

  def test_rollforwards_root_at_form_capital_concept(
    self, presentation: TaxonomyPackage
  ) -> None:
    for role, hub in (
      (f"{_PRES}/Equity-rollforward-PART", "rs-gaap:PartnersCapital"),
      (f"{_PRES}/Equity-rollforward-LLC", "rs-gaap:MembersEquity"),
    ):
      froms = {a.from_qname for a in presentation.associations if a.role == role}
      assert froms == {hub}, role
      targets = {a.to_qname for a in presentation.associations if a.role == role}
      assert "rs-gaap:NetIncomeLoss" in targets
