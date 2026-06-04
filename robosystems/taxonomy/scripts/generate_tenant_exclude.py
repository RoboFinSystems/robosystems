"""Generate the per-tenant rs-gaap exclusion list (Sprint 2 — copy-filter curation).

The rs-gaap catalog is a us-gaap *mapping-target mirror* (~2155 concepts) of
which only ~90 render under the seeded Reporting Styles. The full mirror stays
in the **public** library (it backs the future SEC us-gaap bridge + MappingAgent
training corpus), but a tenant graph only needs the curated subset — industry
verticals (oil & gas, insurance, banking, utilities, …) belong in *peer*
frameworks (rs-call-report, rs-statutory, rs-ferc), and XBRL dimension
members/domains are never CoA line-item targets.

This script computes the **clear-cut, zero-rollup-risk** exclusion set and writes
it to ``robosystems/taxonomy/data/tenant_exclude_rs_gaap_v1.json``. The copy path
(``writer.copy_library_into_tenant`` / ``resync_library_into_tenant``) reads that
artifact and omits the listed concepts from each tenant schema; the public
library is untouched. Promotion is reversible: drop a qname from the list and
re-sync.

Exclusion = (dimension Member/Domain + general-special-disconnected + industry
verticals) **minus** KEEP-CRITICAL (the working set + its upward rollup ancestors
+ the calc DAG + every concept referenced by a library rule). The keep-critical
subtraction guarantees we never drop a concept the render ancestor-rollup
(``_resolve_renderable_ancestor``) or a rule (target/operand) could need.

Run against a seeded library DB (mirrors ``generate_rollup_rules.py``):

    uv run python -m robosystems.taxonomy.scripts.generate_tenant_exclude

Re-run whenever the rs-gaap catalog or the default Reporting Styles change; the
artifact is the committed source of truth the copy path consumes.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from sqlalchemy import text

from robosystems.db.extensions import LIBRARY_GRAPH_ID, extensions_session

_ARTIFACT = (
  Path(__file__).resolve().parent.parent / "data" / "tenant_exclude_rs_gaap_v1.json"
)

# Industry-vertical keyword matcher. Deliberately conservative — these are
# concepts that belong in peer frameworks (rs-call-report, rs-statutory,
# rs-ferc, …), not a general-purpose GL framework. Leases (ASC 842) and income
# tax are intentionally NOT here: they apply to every entity.
_VERTICAL = re.compile(
  r"(OilAndGas|NaturalGas|Aircraft|Airline|Regulated"
  r"|AllowanceForFundsUsedDuringConstruction|PublicUtilit|Mineral|Mining"
  r"|OreReserves?|Drilling|Timber|Insurance|Reinsuranc|Policyholder|Annuit"
  r"|Ceded|NOWAccount|FederalFunds|TimeDeposit|DemandDeposit|LoansAndLeases"
  r"|AllowanceForLoanAndLease|CappingClosure|Landfill|RealEstateInvestmentTrust)",
  re.IGNORECASE,
)


def compute_exclude() -> dict:
  """Compute the exclusion set + audit metadata from the seeded public library."""
  with extensions_session(LIBRARY_GRAPH_ID) as session:

    def fetch_ids(sql: str) -> set[str]:
      return {r[0] for r in session.execute(text(sql)).fetchall()}

    qname = dict(
      session.execute(
        text(
          "SELECT e.id, e.qname FROM public.elements e "
          "JOIN public.taxonomies t ON t.id=e.taxonomy_id WHERE t.standard='rs-gaap'"
        )
      ).fetchall()
    )
    rg = set(qname)

    working = (
      fetch_ids(
        "SELECT a.to_element_id FROM public.reporting_style_networks rsn "
        "JOIN public.associations a ON a.structure_id=rsn.network_id "
        "WHERE a.association_type='presentation' AND a.to_element_id IS NOT NULL "
        "UNION SELECT a.from_element_id FROM public.reporting_style_networks rsn "
        "JOIN public.associations a ON a.structure_id=rsn.network_id "
        "WHERE a.association_type='presentation' AND a.from_element_id IS NOT NULL"
      )
      & rg
    )
    calc = (
      fetch_ids(
        "SELECT from_element_id FROM public.associations WHERE association_type='calculation' "
        "UNION SELECT to_element_id FROM public.associations WHERE association_type='calculation'"
      )
      & rg
    )

    # Concepts referenced by library rules (target + operands) — never drop
    # one, or the rules copy dangles its polymorphic FK / rule eval can't bind.
    rule_target_ids = (
      fetch_ids(
        "SELECT target_element_id FROM public.rules WHERE target_element_id IS NOT NULL"
      )
      & rg
    )
    rule_var_qnames: set[str] = set()
    for (rv,) in session.execute(
      text("SELECT rule_variables FROM public.rules WHERE rule_variables IS NOT NULL")
    ):
      blob = rv if isinstance(rv, str) else json.dumps(rv)
      rule_var_qnames |= set(re.findall(r"rs-gaap:[A-Za-z0-9]+", blob))

    gs = session.execute(
      text(
        "SELECT from_element_id, to_element_id FROM public.associations "
        "WHERE association_type='general-special'"
      )
    ).fetchall()
    eqm = session.execute(
      text(
        "SELECT from_element_id, to_element_id FROM public.associations "
        "WHERE association_type IN ('equivalence','mapping')"
      )
    ).fetchall()

  # Upward rollup closure: ancestors the renderer may walk to from the working
  # set. general-special: parent=from, child=to. equivalence/mapping: child=from,
  # parent=to. (Mirrors operations/roboledger/reports/fact_grid.py
  # ::_resolve_renderable_ancestor.)
  up: dict[str, set[str]] = {}
  for f, t in gs:
    up.setdefault(t, set()).add(f)
  for f, t in eqm:
    up.setdefault(f, set()).add(t)
  ancestors: set[str] = set()
  frontier = set(working)
  while frontier:
    nxt: set[str] = set()
    for child in frontier:
      for parent in up.get(child, ()):
        if parent not in ancestors:
          nxt.add(parent)
    ancestors |= nxt
    frontier = nxt
  ancestors &= rg

  qname_to_id = {q: i for i, q in qname.items()}
  rule_refs = rule_target_ids | {
    qname_to_id[q] for q in rule_var_qnames if q in qname_to_id
  }

  keep_critical = working | calc | ancestors | rule_refs

  # Undirected general-special connectivity to the working set.
  adj: dict[str, set[str]] = {}
  for f, t in gs:
    adj.setdefault(f, set()).add(t)
    adj.setdefault(t, set()).add(f)
  reachable = set(working)
  frontier = set(working)
  while frontier:
    nxt = set()
    for node in frontier:
      for nb in adj.get(node, ()):
        if nb not in reachable:
          nxt.add(nb)
    reachable |= nxt
    frontier = nxt
  disconnected = rg - reachable

  members = {i for i, q in qname.items() if q.endswith(("Member", "Domain"))}
  verticals = {i for i, q in qname.items() if _VERTICAL.search(q)}

  candidate = members | disconnected | verticals
  final_drop = candidate - keep_critical  # never drop a rollup-critical concept

  def cat(i: str) -> str:
    if i in members:
      return "dimension_member_domain"
    if i in disconnected:
      return "type_subtype_disconnected"
    return "industry_vertical"

  excluded = sorted(qname[i] for i in final_drop)
  return {
    "framework": "rs-gaap",
    "version": "v1",
    "policy": "tenant_exclude_clear_cut",
    "description": (
      "rs-gaap concepts kept in the public library but NOT copied into tenant "
      "schemas (Sprint 2 clear-cut curation). Dimension members/domains + "
      "general-special-disconnected + industry verticals, minus keep-critical "
      "(working set + rollup ancestors + calc DAG)."
    ),
    "counts": {
      "rs_gaap_total": len(rg),
      "working_set": len(working),
      "keep_critical": len(keep_critical),
      "excluded": len(final_drop),
      "tenant_kept": len(rg) - len(final_drop),
      "by_category": {
        "dimension_member_domain": sum(1 for i in final_drop if i in members),
        "type_subtype_disconnected": sum(
          1 for i in final_drop if i in disconnected and i not in members
        ),
        "industry_vertical": sum(
          1 for i in final_drop if cat(i) == "industry_vertical"
        ),
      },
    },
    "excluded_qnames": excluded,
    "excluded_by_category": {q: cat(i) for i in final_drop for q in (qname[i],)},
  }


def main() -> None:
  artifact = compute_exclude()
  by_cat = dict(sorted(artifact["excluded_by_category"].items()))
  # Compact, stable on-disk shape: counts + sorted qname list + category map.
  out = {
    "framework": artifact["framework"],
    "version": artifact["version"],
    "policy": artifact["policy"],
    "description": artifact["description"],
    "counts": artifact["counts"],
    "excluded_qnames": artifact["excluded_qnames"],
    "excluded_by_category": by_cat,
  }
  _ARTIFACT.write_text(json.dumps(out, indent=2) + "\n")
  c = artifact["counts"]
  print(f"Wrote {_ARTIFACT}")
  print(
    f"  rs-gaap total {c['rs_gaap_total']} → tenant keeps {c['tenant_kept']} "
    f"(excluded {c['excluded']})"
  )
  print(f"  by category: {c['by_category']}")


if __name__ == "__main__":
  main()
