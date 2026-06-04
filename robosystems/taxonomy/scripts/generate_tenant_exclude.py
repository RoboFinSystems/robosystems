"""Generate the per-tenant rs-gaap exclusion list (Sprint 2 — copy-filter curation).

The rs-gaap catalog is a us-gaap *mapping-target mirror* (~2155 concepts) of
which only ~90 render under the seeded Reporting Styles. The full mirror stays
in the **public** library (it backs the future SEC us-gaap bridge + MappingAgent
training corpus), but a tenant graph only needs the curated subset — industry
verticals (oil & gas, insurance, banking, utilities, …) belong in *peer*
frameworks (rs-call-report, rs-statutory, rs-ferc), and XBRL dimension
members/domains are never CoA line-item targets.

This script computes the **MVP-core, zero-rollup-risk** exclusion set and writes
it to ``frameworks/rs-gaap/tenant-exclude/v1.json``. The copy path
(``writer.copy_library_into_tenant`` / ``resync_library_into_tenant``) reads that
artifact and omits the listed concepts from each tenant schema; the public
library is untouched. Promotion is reversible: drop a qname from the list and
re-sync.

Exclusion = (dimension Member/Domain + general-special-disconnected + industry
verticals + general-special LEAVES) **minus** KEEP-CRITICAL (the working set +
its upward rollup ancestors + the calc DAG + every concept referenced by a
library rule). The leaf level is the finest disaggregation detail of the
aggregation lattice; it is inert until the granularity-selection feature ships
(mapping is capped at the renderable working set today), so we defer it and keep
only the high-level aggregates — adding disaggregation levels back later via
resync. The keep-critical subtraction guarantees we never drop a concept the
render ancestor-rollup (``_resolve_renderable_ancestor``) or a rule
(target/operand) could need.

Run against a seeded library DB (mirrors ``generate_rollup_rules.py``):

    uv run python -m robosystems.taxonomy.scripts.generate_tenant_exclude

Re-run whenever the rs-gaap catalog or the default Reporting Styles change; the
artifact is the committed source of truth the copy path consumes.
"""

from __future__ import annotations

import json
import re

from sqlalchemy import text

from robosystems.db.extensions import LIBRARY_GRAPH_ID, extensions_session
from robosystems.operations.operators.implementations.mapping.constants import (
  RS_GAAP_SYNTHESIZED_DETAIL_ALLOW,
)
from robosystems.taxonomy.discovery import FRAMEWORKS_DIR

_ARTIFACT = FRAMEWORKS_DIR / "rs-gaap" / "tenant-exclude" / "v1.json"

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

  # Synthesized-detail mapping grains (PP&E Gross + its accumulated-depreciation
  # contra). The renderer synthesizes PropertyPlantAndEquipmentNet = Gross - AD
  # and the CF Investing derivation reads ΔGross as capex, so a CoA fixed-asset
  # account maps to these even though Net is what the BS presents — they are NOT
  # in the working set's presentation networks (AD is a general-special leaf, so
  # it would otherwise be excluded) but MUST be in every tenant. Mirrors
  # ``mapping/constants.py::RS_GAAP_SYNTHESIZED_DETAIL_ALLOW``.
  synthesized_detail = {
    qname_to_id[q] for q in RS_GAAP_SYNTHESIZED_DETAIL_ALLOW if q in qname_to_id
  }

  keep_critical = working | calc | ancestors | rule_refs | synthesized_detail

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

  # Disaggregation leaves — the MVP curation (Sprint 3). The general-special
  # tree is the aggregation lattice: a preparer picks report granularity by
  # mapping a CoA account to a higher (aggregate) or lower (disaggregated) node.
  # The leaf level is the finest detail. Today it is INERT — mapping candidates
  # are capped at the renderable working set and the renderer only walks the
  # aggregate level (see operations/roboledger/reads/taxonomies.py, the
  # 2026-05-17 narrowing), so a tenant cannot map to or render a leaf. Shipping
  # the leaves buys no capability now but makes them un-deletable once the
  # granularity-selection feature lands and customers map to them. So defer the
  # leaf detail: keep the high-level aggregates (intermediates with children),
  # and add disaggregation levels back later via resync ("we added a deeper
  # level so your reports can be denser"). Add is cheap; delete after use isn't.
  gs_parents = {f for f, _ in gs}
  gs_children = {t for _, t in gs}
  disaggregation_leaves = (gs_children - gs_parents) & rg  # child-only in the lattice

  candidate = members | disconnected | verticals | disaggregation_leaves
  final_drop = candidate - keep_critical  # never drop a rollup-critical concept

  def cat(i: str) -> str:
    if i in members:
      return "dimension_member_domain"
    if i in disconnected:
      return "type_subtype_disconnected"
    if i in verticals:
      return "industry_vertical"
    return "disaggregation_leaf"

  by_category: dict[str, int] = {}
  for i in final_drop:
    by_category[cat(i)] = by_category.get(cat(i), 0) + 1

  excluded = sorted(qname[i] for i in final_drop)
  return {
    "framework": "rs-gaap",
    "version": "v1",
    "policy": "tenant_exclude_mvp_core",
    "description": (
      "rs-gaap concepts kept in the public library but NOT copied into tenant "
      "schemas. The MVP-core curation: drop dimension members/domains, "
      "general-special-disconnected concepts, industry verticals, AND the "
      "general-special leaf level (the finest disaggregation detail, inert "
      "until the granularity-selection feature ships) — minus keep-critical "
      "(working set + rollup ancestors + calc DAG + rule refs). Keeps the "
      "high-level aggregates; disaggregation levels are added back later via "
      "resync (add is cheap; delete after a tenant maps to a concept is not)."
    ),
    "counts": {
      "rs_gaap_total": len(rg),
      "working_set": len(working),
      "keep_critical": len(keep_critical),
      "excluded": len(final_drop),
      "tenant_kept": len(rg) - len(final_drop),
      "by_category": by_category,
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
