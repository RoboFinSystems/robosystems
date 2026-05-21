"""Shape tests for the four Phase C packages plus the disclosures-to-
textblocks bridge.

These tests exercise the JSON-LD loader against each new artifact to
confirm:

- The package parses cleanly into a TaxonomyPackage
- Element + Structure counts match expectations (catches accidental
  changes during edits)
- Arc directionality + arcrole vocabulary land correctly (DM and DR
  arcrole strings round-trip from the @context into AssociationSpec)

The tests are loader-only — no DB access. They run without the
``ROBOLEDGER_ENABLED`` flag and without the extensions database.
"""

from __future__ import annotations

import pytest

from robosystems.taxonomy import load_taxonomy_package
from robosystems.taxonomy.discovery import framework_root
from robosystems.taxonomy.model import TaxonomyPackage

_FW_ROOT = framework_root("rs-gaap")
_PACKAGES = _FW_ROOT / "packages"
_BRIDGES = _FW_ROOT / "bridges"


@pytest.fixture(scope="module")
def disclosures() -> TaxonomyPackage:
  return load_taxonomy_package(
    _PACKAGES / "rs-gaap-disclosures" / "v1" / "taxonomy.jsonld"
  )


@pytest.fixture(scope="module")
def disclosure_mechanics() -> TaxonomyPackage:
  return load_taxonomy_package(
    _PACKAGES / "rs-gaap-disclosure-mechanics" / "v1" / "taxonomy.jsonld"
  )


@pytest.fixture(scope="module")
def reporting_checklist() -> TaxonomyPackage:
  return load_taxonomy_package(
    _PACKAGES / "rs-gaap-reporting-checklist" / "v1" / "taxonomy.jsonld"
  )


@pytest.fixture(scope="module")
def reporting_styles() -> TaxonomyPackage:
  return load_taxonomy_package(
    _PACKAGES / "rs-gaap-reporting-styles" / "v1" / "taxonomy.jsonld"
  )


@pytest.fixture(scope="module")
def disclosures_to_textblocks() -> TaxonomyPackage:
  return load_taxonomy_package(
    _BRIDGES / "rs-gaap-disclosures-to-rs-gaap-textblocks" / "v1" / "taxonomy.jsonld"
  )


class TestDisclosuresPackage:
  def test_metadata(self, disclosures: TaxonomyPackage) -> None:
    assert disclosures.standard == "rs-gaap-disclosures"
    assert disclosures.version == "v1"
    assert disclosures.taxonomy_type == "reporting_extension"

  def test_dual_element_and_structure_per_disclosure(
    self, disclosures: TaxonomyPackage
  ) -> None:
    """Each disclosure declares both element-fields (so DM arcs can
    reference it as a qname) and structure-fields (so the renderer can
    dispatch by CAP). Counts must match."""
    assert len(disclosures.elements) == len(disclosures.structures)
    assert len(disclosures.elements) >= 30  # at least 30 named disclosures

  def test_primary_statements_present(self, disclosures: TaxonomyPackage) -> None:
    qnames = {e.qname for e in disclosures.elements}
    for required in (
      "disclosures:BalanceSheet",
      "disclosures:IncomeStatement",
      "disclosures:CashFlowStatement",
      "disclosures:StatementOfChangesInEquity",
    ):
      assert required in qnames, f"Primary statement {required} missing"

  def test_cap_values_use_canonical_vocabulary(
    self, disclosures: TaxonomyPackage
  ) -> None:
    """Vocab alignment (2026-05-15) maps the seed CAPs onto Charlie's
    canonical enumeration. `component` and `hierarchy` collapsed to
    `set`; `level1_textblock` retained as the cm.xsd specialization.
    See information-block.md §3.2.1."""
    cap_values = {s.concept_arrangement for s in disclosures.structures}
    assert "set" in cap_values
    assert "level1_textblock" in cap_values
    # All values must be in the canonical 15-value CAP enumeration.
    canonical = {
      "set",
      "roll_up",
      "roll_forward",
      "roll_forward_info",
      "adjustment",
      "variance",
      "arithmetic",
      "text_block",
      "level1_textblock",
      "level2_textblock",
      "level3_textblock",
      "level4_detail",
      "table_equivalent_textblock",
      "grid",
      "compound_fact",
    }
    assert cap_values - {None} <= canonical, (
      f"Non-canonical CAP values in seed: {cap_values - canonical - {None}}"
    )


class TestDisclosureMechanicsPackage:
  def test_metadata(self, disclosure_mechanics: TaxonomyPackage) -> None:
    assert disclosure_mechanics.standard == "rs-gaap-disclosure-mechanics"
    assert disclosure_mechanics.taxonomy_type == "rules"

  def test_balance_sheet_composition_arcs(
    self, disclosure_mechanics: TaxonomyPackage
  ) -> None:
    """BalanceSheet must require both AssetsRollUp and
    LiabilitiesAndEquityRollUp via reportedDisclosure-requiresDisclosure."""
    bs_arcs = [
      a
      for a in disclosure_mechanics.associations
      if a.from_qname == "disclosures:BalanceSheet"
      and "reportedDisclosure-requiresDisclosure" in (a.arcrole or "")
    ]
    targets = {a.to_qname for a in bs_arcs}
    assert "disclosures:AssetsRollUp" in targets
    assert "disclosures:LiabilitiesAndEquityRollUp" in targets

  def test_assets_rollup_concept_binding(
    self, disclosure_mechanics: TaxonomyPackage
  ) -> None:
    """AssetsRollUp must bind to rs-gaap:Assets via
    conceptArrangementPattern-requiresConcept."""
    arcs = [
      a
      for a in disclosure_mechanics.associations
      if a.from_qname == "disclosures:AssetsRollUp"
      and "conceptArrangementPattern-requiresConcept" in (a.arcrole or "")
    ]
    targets = {a.to_qname for a in arcs}
    assert "rs-gaap:Assets" in targets

  def test_all_arcs_use_definition_association_type(
    self, disclosure_mechanics: TaxonomyPackage
  ) -> None:
    """DM arcs land on associations.association_type = 'definition'
    (widened by 0007). Anything else points at a misconfigured
    @context entry."""
    types = {a.association_type for a in disclosure_mechanics.associations}
    assert types == {"definition"}, f"Unexpected association_type values: {types}"


class TestReportingChecklistPackage:
  def test_metadata(self, reporting_checklist: TaxonomyPackage) -> None:
    assert reporting_checklist.standard == "rs-gaap-reporting-checklist"
    assert reporting_checklist.taxonomy_type == "rules"

  def test_general_purpose_report_subject_present(
    self, reporting_checklist: TaxonomyPackage
  ) -> None:
    qnames = {e.qname for e in reporting_checklist.elements}
    assert "checklist:GeneralPurposeFinancialReport" in qnames

  def test_required_disclosures_include_primary_statements(
    self, reporting_checklist: TaxonomyPackage
  ) -> None:
    required_arcs = [
      a
      for a in reporting_checklist.associations
      if "financialReport-requiresDisclosure" in (a.arcrole or "")
    ]
    targets = {a.to_qname for a in required_arcs}
    for must_have in (
      "disclosures:BalanceSheet",
      "disclosures:IncomeStatement",
      "disclosures:CashFlowStatement",
    ):
      assert must_have in targets


class TestReportingStylesPackage:
  def test_metadata(self, reporting_styles: TaxonomyPackage) -> None:
    assert reporting_styles.standard == "rs-gaap-reporting-styles"

  def test_default_style_composes_disclosures(
    self, reporting_styles: TaxonomyPackage
  ) -> None:
    arcs = [
      a
      for a in reporting_styles.associations
      if a.from_qname == "styles:DefaultStyle"
      and "reportingStyle-composesDisclosure" in (a.arcrole or "")
    ]
    assert len(arcs) >= 5  # at least the four primary statements + cover page


class TestDisclosuresToTextblocksBridge:
  def test_metadata(self, disclosures_to_textblocks: TaxonomyPackage) -> None:
    assert (
      disclosures_to_textblocks.standard == "rs-gaap-disclosures-to-rs-gaap-textblocks"
    )
    assert disclosures_to_textblocks.taxonomy_type == "mapping"

  def test_v1_is_placeholder_with_zero_arcs(
    self, disclosures_to_textblocks: TaxonomyPackage
  ) -> None:
    """v1 ships an empty arc set — rs-gaap currently has no TextBlock
    elements. Bridge content populates as either rs-gaap is extended
    or a sibling rs-gaap-to-us-gaap bridge is authored. Test pins
    this assumption so an accidental authoring drift surfaces."""
    assert len(disclosures_to_textblocks.associations) == 0
