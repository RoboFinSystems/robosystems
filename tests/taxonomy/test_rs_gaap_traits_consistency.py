"""Trait-consistency guards for the rs-gaap working set.

A renderable concept that lacks its ``elementsOfFinancialStatements`` (EFS)
trait silently drops out of the EFS-filtered surfaces — ``suggest_mapping_candidates``
filters on EFS, so the concept renders but can't be mapped to, and it disappears
from per-EFS-category views. These DB-free loader tests pin the bindings that
closed the audited gaps (income-statement subtotals + revenue lines that were
missing EFS), so a future regeneration of ``rs-gaap-traits`` can't silently drop
them again.
"""

from __future__ import annotations

import pytest

from robosystems.taxonomy import load_taxonomy_package
from robosystems.taxonomy.discovery import framework_root


@pytest.fixture(scope="module")
def traits_by_qname() -> dict[str, set[str]]:
  pkg = load_taxonomy_package(
    framework_root("rs-gaap") / "packages" / "rs-gaap-traits" / "v1" / "taxonomy.jsonld"
  )
  out: dict[str, set[str]] = {}
  for a in pkg.trait_assignments:
    out.setdefault(a.element_qname, set()).add(f"{a.category}={a.identifier}")
  return out


@pytest.mark.unit
@pytest.mark.parametrize(
  "qname,required",
  [
    # IS subtotals — must carry EFS=metric like GrossProfit / NetIncomeLoss,
    # or they fall out of EFS-filtered category views (OperatingIncomeLoss was
    # the reported symptom).
    ("rs-gaap:OperatingIncomeLoss", "elementsOfFinancialStatements=metric"),
    ("rs-gaap:NonoperatingIncomeExpense", "elementsOfFinancialStatements=metric"),
    # Revenue lines — must carry EFS=revenue like Revenues, or they render on the
    # income statement but are not offered as mapping candidates.
    ("rs-gaap:SalesRevenueNet", "elementsOfFinancialStatements=revenue"),
    ("rs-gaap:OtherSalesRevenueNet", "elementsOfFinancialStatements=revenue"),
  ],
)
def test_working_set_efs_bindings_present(
  traits_by_qname: dict[str, set[str]], qname: str, required: str
) -> None:
  assert required in traits_by_qname.get(qname, set()), (
    f"{qname} is missing {required} — it would drop out of EFS-filtered "
    f"mapping/category surfaces"
  )
