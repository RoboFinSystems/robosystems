"""Tests for the deterministic contra-asset mapping override.

The AI refinement collapses unambiguous synthesized-detail accounts
(e.g. "Accumulated Depreciation") into the net parent
(PropertyPlantAndEquipmentNet) via the FixedAssets fallback — which
broke the balance sheet (PP&E Net went negative) and the cash-flow
capex derivation (which keys on ΔGross + the contra). The account NAME
is the unambiguous signal, so ``_deterministic_rs_gaap_override`` routes
it to the contra concept before the AI pass.
"""

from __future__ import annotations

from robosystems.operations.operators.implementations.mapping.constants import (
  RS_GAAP_NAME_PATTERN_OVERRIDES,
)
from robosystems.operations.operators.implementations.mapping.operator import (
  _deterministic_rs_gaap_override,
)

_CONTRA = (
  "rs-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment"
)


def test_accumulated_depreciation_routes_to_contra():
  assert (
    _deterministic_rs_gaap_override({"name": "Accumulated depreciation"}) == _CONTRA
  )


def test_match_is_case_insensitive_and_substring():
  for name in (
    "Accumulated Depreciation",
    "Less: Accumulated Depreciation",
    "Accumulated Depreciation - Equipment",
    "ACCUMULATED   DEPRECIATION",
  ):
    assert _deterministic_rs_gaap_override({"name": name}) == _CONTRA, name


def test_matches_on_code_when_name_absent():
  assert (
    _deterministic_rs_gaap_override(
      {"name": "", "code": "Fixed Assets:Accumulated Depreciation"}
    )
    == _CONTRA
  )


def test_accumulated_amortization_does_not_match():
  # Intangibles use the single IntangibleAssetsNetIncludingGoodwill concept;
  # forcing the PP&E contra here would be wrong.
  assert _deterministic_rs_gaap_override({"name": "Accumulated Amortization"}) is None


def test_unrelated_accounts_do_not_match():
  for name in (
    "Depreciation Expense",
    "Gross Fixed Assets",
    "Cash",
    "Accounts Receivable",
    "Prepaid Depreciation Insurance",  # "depreciation" but not "accumulated deprec"
  ):
    assert _deterministic_rs_gaap_override({"name": name}) is None, name


def test_missing_name_and_code_is_safe():
  assert _deterministic_rs_gaap_override({}) is None


def test_override_table_wires_accumulated_depreciation_to_contra():
  qnames = {q for _, q in RS_GAAP_NAME_PATTERN_OVERRIDES}
  assert _CONTRA in qnames
