"""Generate the per-tenant rs-gaap exclusion list (copy-filter curation).

The rs-gaap catalog is a us-gaap *mapping-target mirror* (~2155 concepts) of
which only ~90 render under the seeded Reporting Styles. The full mirror stays
in the **public** library (it backs the future SEC us-gaap bridge + MappingAgent
training corpus), but a tenant graph only needs the curated subset — industry
verticals (oil & gas, insurance, banking, utilities, …) belong in *peer*
frameworks (rs-call-report, rs-statutory, rs-ferc), and XBRL dimension
members/domains are never CoA line-item targets.

This script computes the **keep-critical, zero-rollup-risk** exclusion
set and writes it to ``frameworks/rs-gaap/tenant-exclude/v1.json``. The copy path
(``writer.copy_library_into_tenant`` / ``resync_library_into_tenant``) reads that
artifact and omits the listed concepts from each tenant schema; the public
library is untouched. Promotion is reversible: drop a qname from the list and
re-sync.

Exclusion = library **minus** KEEP-CRITICAL — i.e. a tenant keeps EXACTLY the
working set (concepts that render under the active Reporting Style's Networks)
plus its structural scaffolding: the upward rollup ancestors, the calc DAG,
every concept a library rule references (target/operand), and the synthesized
PP&E grains. Everything else is inert — mapping candidates are capped at the
renderable working set and the renderer only walks anchored concepts, so an
un-anchored concept renders nowhere and cannot be mapped. "Kept ⟺ used": it is
dropped, and re-added via resync the moment a future Reporting Style or deeper
breakdown wires it (add is cheap; delete after a tenant maps to a concept is
not). The drop is partitioned for audit (members/domains, disconnected,
industry/specialist verticals, general-special leaves, unanchored intermediate
aggregates) but the gate is simply non-membership in keep-critical, so the
subtraction can never drop a concept the render ancestor-rollup
(``_resolve_renderable_ancestor``) or a rule could need.

Run against a seeded library DB:

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

# Industry-vertical / specialist-domain keyword matcher. These are concepts that
# belong in peer frameworks (rs-call-report, rs-statutory, rs-ferc, …) or
# specialist modules, NOT a general-purpose GL framework. Leases (ASC 842) and
# income tax are intentionally NOT here: they apply to every entity.
#
# The base block catches the obvious verticals; the financial-institution /
# insurance / derivative / pension domains are matched here too. Their
# INTERMEDIATE aggregates (non-leaf, so they slipped the disaggregation-leaf
# filter; not vertical-keyword'd, so they slipped this matcher) were being copied
# into every tenant despite rendering in no Reporting Style — e.g. servicing-
# financial-asset fees, leveraged leases, deposit interest, loan-loss provisions,
# derivative/hedge positions, defined-benefit pension liabilities. General
# private-company lines (notes & loans payable/receivable, related-party
# balances, share-based comp, FX gains) are deliberately NOT matched — they stay
# in the tenant as wire-in stock for a future granular/vertical Style. Conservative
# bias: when a concept is plausibly general, leave it unmatched (kept).
_VERTICAL = re.compile(
  r"(OilAndGas|NaturalGas|Aircraft|Airline|Regulated"
  r"|AllowanceForFundsUsedDuringConstruction|PublicUtilit|Mineral|Mining"
  r"|OreReserves?|Drilling|Timber|Insurance|Reinsuranc|Policyholder|Annuit"
  r"|Ceded|NOWAccount|FederalFunds|TimeDeposit|DemandDeposit|LoansAndLeases"
  r"|AllowanceForLoanAndLease|CappingClosure|Landfill|RealEstateInvestmentTrust"
  # banking / depository (→ rs-call-report)
  r"|Deposit(s|or)?|InterestAndFeeIncomeLoans|InterestAndDividendsReceivable"
  r"|ProvisionForLoan|AllowanceForLoan|GainLossOnSalesOfLoans|FinancingReceivable"
  r"|BankOwned|CoreDeposit|TrustFee|FederalHomeLoan|LoansHeldForSale|LoansHeldforsale"
  r"|FederalReserveBank|LoanAndLeaseOrigination|OriginationAndPurchasesOfLoans"
  r"|PurchaseLoansHeldForSale"
  # broker-dealer
  r"|Brokerage|Clearing|TradingGainsLosses|PrincipalTransaction|SecuritiesSoldUnder"
  r"|SecuritiesPurchasedUnder|UnderwritingFee|CommissionsRevenue"
  # insurance (→ rs-statutory; Reinsuranc/Annuit/Policyholder already in the base block)
  r"|ContractHolders|DeferredPolicyAcquisition|PolicyLoan|SeparateAccount"
  r"|Underwriting|UnpaidClaim|RealizedInvestmentGains|NetInvestmentIncome"
  r"|InvestmentIncomeNet|DividendIncomeOperating"
  # mortgage servicing / leveraged & direct-financing lease / securitization
  r"|Servicing.*FinancialAsset|MortgageServic|ServicingFee|ServicingAsset"
  r"|ServicingLiabilit|ContractuallySpecifiedServicing|LeveragedLease"
  r"|DirectFinancingLease|SalesTypeLease|Securitiz|VariableInterestEntity|BeneficialInterest"
  # extractive (beyond the base) / regulated utility
  r"|Exploration|Petroleum|Coal|ProvedProperty|UnprovedProperty|FossilFuel|FuelInventor"
  r"|DeferredFuel|RateDeferral|StormReserve|DecommissioningFund|NuclearFuel|EmissionCredit"
  r"|PowerContract|PurchasedPower|WaterAndWasteWater|EnergyRelated|EnergyRecovery|OverUnderEnergy"
  # real estate / construction operator
  r"|RealEstate|OperativeBuilders|TenantReimbursement|StraightLineRent|AboveMarketLease"
  r"|InPlaceLease|FundsFromOperations|ContractReceivableDue"
  # specialist derivatives / hedging
  r"|Derivative|Hedg|InterestRateSwap|BasisSwap|CurrencySwap|CreditDefault"
  r"|CommodityContract|PriceRiskManagement"
  # defined-benefit pension & OPEB
  r"|DefinedBenefit|Pension|Postretirement|SupplementalRetirement"
  # other specialist (agriculture, airline, hospitality, healthcare, exotic financing)
  r"|Agricultur|Livestock|Cargo|Freight|Vessel|Charter|Franchis|Casino|Gaming"
  r"|Hospitality|DirectCostsOfHotels|Concession|FoodAndBeverage|Malpractice|Medicare"
  r"|Medicaid|ConvertibleSubordinated|MandatorilyRedeemable|BridgeLoan|TrustPreferred)",
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

  # Disaggregation leaves. The general-special tree is the aggregation lattice:
  # a preparer picks report granularity by mapping a CoA account to a higher
  # (aggregate) or lower (disaggregated) node. The leaf level is the finest
  # detail. Today it is INERT — mapping candidates are capped at the renderable
  # working set and the renderer only walks the aggregate level (see
  # operations/roboledger/reads/taxonomies.py), so a tenant cannot map to or
  # render a leaf. Shipping the leaves buys no capability now but makes them
  # un-deletable once a granularity-selection feature lands and customers map to
  # them. So defer the leaf detail: keep the high-level aggregates (intermediates
  # with children), and add disaggregation levels back later via resync ("we
  # added a deeper level so your reports can be denser"). Add is cheap; delete
  # after use isn't.
  gs_parents = {f for f, _ in gs}
  gs_children = {t for _, t in gs}
  disaggregation_leaves = (gs_children - gs_parents) & rg  # child-only in the lattice

  # A tenant keeps EXACTLY keep-critical — the working set that renders under
  # the active Reporting Style plus its structural scaffolding (calc DAG, rollup
  # ancestors, rule operands, synthesized PP&E grains). Everything else is inert
  # today — mapping candidates are capped at
  # the renderable working set and the renderer only walks anchored concepts, so
  # an un-anchored concept renders nowhere and can't be mapped. "Kept ⟺ used":
  # drop it, and re-add via resync the moment a future Reporting Style / deeper
  # breakdown wires it (add is cheap; delete after a tenant maps to a concept is
  # not). The members / disconnected / verticals / leaves sets are retained only
  # to LABEL the drop reason for the audit metadata; the residual — connected
  # non-leaf aggregates outside the active Style (unwired BS/IS disaggregations,
  # finer CF detail, replaced combined leaves) — is ``unanchored_intermediate``.
  final_drop = rg - keep_critical  # keep only what renders + its scaffolding

  def cat(i: str) -> str:
    if i in members:
      return "dimension_member_domain"
    if i in disconnected:
      return "type_subtype_disconnected"
    if i in verticals:
      return "industry_vertical"
    if i in disaggregation_leaves:
      return "disaggregation_leaf"
    return "unanchored_intermediate"

  by_category: dict[str, int] = {}
  for i in final_drop:
    by_category[cat(i)] = by_category.get(cat(i), 0) + 1

  excluded = sorted(qname[i] for i in final_drop)
  return {
    "framework": "rs-gaap",
    "version": "v1",
    "policy": "tenant_exclude_keep_critical",
    "description": (
      "rs-gaap concepts kept in the public library but NOT copied into tenant "
      "schemas. The keep-critical curation: a tenant keeps EXACTLY the "
      "concepts that render under the active Reporting Style (the working set) "
      "plus their structural scaffolding — the calc DAG, rollup ancestors, rule "
      "operands, and the synthesized PP&E grains. Everything else renders nowhere "
      "and cannot be mapped (mapping candidates are capped at the renderable "
      "working set), so it is dropped: 'kept ⟺ used'. The excluded set is the "
      "library minus keep-critical, broken down for audit into dimension members/"
      "domains, general-special-disconnected concepts, industry/specialist "
      "verticals (peer-framework material), general-special leaves, and "
      "unanchored intermediate aggregates (unwired BS/IS disaggregations, finer "
      "CF detail, replaced combined leaves). Any concept is re-added via resync "
      "the moment a future Reporting Style or deeper breakdown wires it (add is "
      "cheap; delete after a tenant maps to a concept is not)."
    ),
    "counts": {
      "rs_gaap_total": len(rg),
      "working_set": len(working),
      "keep_critical": len(keep_critical),
      "excluded": len(final_drop),
      "tenant_kept": len(rg) - len(final_drop),
      "by_category": dict(sorted(by_category.items())),  # sorted → deterministic regen
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
