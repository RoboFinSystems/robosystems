"""Swap the deprecated us-gaap revenue/COGS working-set leaves to their modern
ASC 606 / ASC 705 equivalents across the rs-gaap calc, presentation, rollup-rule
and trait packages.

The IS rollups were wired to leaves us-gaap deprecated in 2018/2019:

    Revenues      = SalesRevenueNet(dep) + RevenueFromContractWithCustomer… + OtherSalesRevenueNet(dep)
    CostOfRevenue = CostOfGoodsSold(dep) + CostOfGoodsSoldExcludingDDA(dep) + OtherCostOfOperatingRevenue

After the swap (the modern equivalents are already in the catalog with clean
labels + EFS traits):

    Revenues      = RevenueFromContractWithCustomerExcludingAssessedTax            (ASC 606 primary)
    CostOfRevenue = CostOfGoodsAndServicesSold + OtherCostOfOperatingRevenue       (ASC 705)

GrossProfit (= Revenues - CostOfRevenue) and everything above it are unchanged.
The dropped leaves stay in the public catalog (mapping mirror) but fall out of
the working set, so the regenerated tenant-exclude omits them; the new COGS leaf
joins the working set and is kept. Touches four packages so the calc DAG, the
presentation render structures (multi-step + single-step), the validation
rollup rules and the EFS/flow traits all stay consistent.

Idempotent; round-trips byte-identically (indent=2) on a no-op re-run.

    uv run python -m robosystems.taxonomy.scripts.swap_deprecated_revenue_cogs [--dry-run]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from robosystems.taxonomy.discovery import framework_root

_ROOT = framework_root("rs-gaap") / "packages"
_CALC = _ROOT / "rs-gaap-calculations" / "v1" / "taxonomy.jsonld"
_PRES = _ROOT / "rs-gaap-presentation" / "v1" / "taxonomy.jsonld"
_RULES = _ROOT / "rs-gaap-rollup-rules" / "v1" / "taxonomy.jsonld"
_TRAITS = _ROOT / "rs-gaap-traits" / "v1" / "taxonomy.jsonld"

REV = "rs-gaap:Revenues"
COR = "rs-gaap:CostOfRevenue"
SALES_REV_NET = "rs-gaap:SalesRevenueNet"
OTHER_SALES_REV_NET = "rs-gaap:OtherSalesRevenueNet"
COGS = "rs-gaap:CostOfGoodsSold"
COGS_EX_DDA = "rs-gaap:CostOfGoodsSoldExcludingDepreciationDepletionAndAmortization"
REV_CONTRACT = "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"
OTHER_COR = "rs-gaap:OtherCostOfOperatingRevenue"
COGS_AND_SVC = "rs-gaap:CostOfGoodsAndServicesSold"

# Leaves removed from each rollup's working set.
DROP_REV_LEAVES = {SALES_REV_NET, OTHER_SALES_REV_NET}
DROP_COR_LEAVES = {COGS, COGS_EX_DDA}


def _load(p: Path) -> dict:
  return json.loads(p.read_text())


def _save(p: Path, doc: dict, dry_run: bool) -> None:
  if not dry_run:
    p.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n")


def _to(arc: dict) -> str:
  t = arc.get("to")
  return t.get("@id", "") if isinstance(t, dict) else ""


def _from(arc: dict) -> str:
  f = arc.get("from")
  return f.get("@id", "") if isinstance(f, dict) else ""


def swap(dry_run: bool = False) -> dict:
  counts: dict[str, int] = {}

  # ── calc ──────────────────────────────────────────────────────────────────
  calc = _load(_CALC)
  g = calc["@graph"]
  kept = []
  for n in g:
    if (
      isinstance(n, dict)
      and n.get("associationType") == "calculation"
      and (
        (_from(n) == REV and _to(n) in DROP_REV_LEAVES)
        or (_from(n) == COR and _to(n) in DROP_COR_LEAVES)
      )
    ):
      continue  # drop deprecated calc leaf
    kept.append(n)
  calc["@graph"] = kept
  # renumber survivors + add modern COGS leaf
  for n in calc["@graph"]:
    if isinstance(n, dict) and n.get("associationType") == "calculation":
      if _from(n) == REV and _to(n) == REV_CONTRACT:
        n["order"] = 100.0
      elif _from(n) == COR and _to(n) == OTHER_COR:
        n["order"] = 200.0
  calc["@graph"].append(
    {
      "@id": "_:rs-gaap-calc-is-cor-arc-cogs-services",
      "from": {"@id": COR},
      "to": {"@id": COGS_AND_SVC},
      "associationType": "calculation",
      "role": "https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-calculations/IS-COR",
      "weight": 1.0,
      "order": 100.0,
      "@type": "rs:Association",
      "arcrole": "http://www.xbrl.org/2003/arcrole/summation-item",
    }
  )
  counts["calc_arcs"] = len(calc["@graph"])
  _save(_CALC, calc, dry_run)

  # ── presentation (curated multi-step + single-step structures) ─────────────
  pres = _load(_PRES)
  g = pres["@graph"]
  kept = []
  for n in g:
    if (
      isinstance(n, dict)
      and n.get("associationType") == "presentation"
      and (
        (_from(n) == REV and _to(n) in DROP_REV_LEAVES)
        or (_from(n) == COR and _to(n) in DROP_COR_LEAVES)
      )
    ):
      continue
    kept.append(n)
  pres["@graph"] = kept
  for variant in ("multistep", "singlestep"):
    role = (
      f"https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/IS-{variant}"
    )
    pres["@graph"].append(
      {
        "@id": f"_:rs-gaap-pres-is-{variant}-arc-cor-cogs-services",
        "from": {"@id": COR},
        "to": {"@id": COGS_AND_SVC},
        "associationType": "presentation",
        "role": role,
        "order": 10210.0,
        "@type": "rs:Association",
        "arcrole": "http://www.xbrl.org/2003/arcrole/parent-child",
      }
    )
  counts["pres_arcs"] = len(pres["@graph"])
  _save(_PRES, pres, dry_run)

  # ── rollup rules ────────────────────────────────────────────────────────────
  rules = _load(_RULES)
  for n in rules["@graph"]:
    if not isinstance(n, dict):
      continue
    if n.get("@id") == "_:rs-gaap-rollup-is-rev":
      n["ruleExpression"] = (
        "$Revenues = $RevenueFromContractWithCustomerExcludingAssessedTax"
      )
      n["ruleVariables"] = [
        {"variableName": "Revenues", "variableQname": REV},
        {
          "variableName": "RevenueFromContractWithCustomerExcludingAssessedTax",
          "variableQname": REV_CONTRACT,
        },
      ]
    elif n.get("@id") == "_:rs-gaap-rollup-is-cor":
      n["ruleExpression"] = (
        "$CostOfRevenue = ($CostOfGoodsAndServicesSold + $OtherCostOfOperatingRevenue)"
      )
      n["ruleVariables"] = [
        {"variableName": "CostOfRevenue", "variableQname": COR},
        {"variableName": "CostOfGoodsAndServicesSold", "variableQname": COGS_AND_SVC},
        {"variableName": "OtherCostOfOperatingRevenue", "variableQname": OTHER_COR},
      ]
  _save(_RULES, rules, dry_run)

  # ── traits: complete the modern revenue leaf with operatingNonoperating ─────
  traits = _load(_TRAITS)
  added_trait = False
  for n in traits["@graph"]:
    if isinstance(n, dict) and n.get("@id") == REV_CONTRACT:
      ht = n.setdefault("https://robosystems.ai/vocab/hasTrait", [])
      ids = {t.get("@id") for t in ht if isinstance(t, dict)}
      if "trait:operatingNonoperating/operating" not in ids:
        # insert after EFS, before flowClassification (peer ordering)
        flow_idx = next(
          (
            i
            for i, t in enumerate(ht)
            if isinstance(t, dict)
            and str(t.get("@id", "")).startswith("trait:flowClassification/")
          ),
          len(ht),
        )
        ht.insert(flow_idx, {"@id": "trait:operatingNonoperating/operating"})
        added_trait = True
  counts["rev_contract_trait_added"] = int(added_trait)
  _save(_TRAITS, traits, dry_run)

  return counts


if __name__ == "__main__":
  ap = argparse.ArgumentParser()
  ap.add_argument("--dry-run", action="store_true")
  args = ap.parse_args()
  print(json.dumps(swap(dry_run=args.dry_run), indent=2))
