"""Generate the per-tenant rs-gaap exclusion list,
``frameworks/rs-gaap/tenant-exclude/v1.json``.

The full rs-gaap mirror stays in ``public``; tenants get only KEEP-CRITICAL:
the concepts that render under the active Reporting Style plus their
scaffolding (rollup ancestors, calc DAG, rule operands, synthesized PP&E
grains). Everything else is inert (unmappable and unrendered), so it is
dropped: adding a concept back via resync is cheap, deleting one a tenant has
mapped is not. The drop is partitioned by reason for audit only; the gate is
non-membership in keep-critical, so nothing the renderer's ancestor rollup or
a rule needs can be dropped.

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

# Industry and specialist domains that belong in peer frameworks. Used only to
# label the drop reason. Leases, income tax and general private-company lines
# are deliberately unmatched; when a concept is plausibly general, leave it out.
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

    # Rule targets and operands: dropping one would dangle the rule's FK.
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

  # Ancestors the renderer may roll up to (mirrors fact_grid's
  # _resolve_renderable_ancestor). general-special runs parent→child;
  # equivalence/mapping run child→parent.
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

  # PP&E Gross and accumulated depreciation: not presented (the BS shows Net =
  # Gross - AD, and CF reads ΔGross as capex), but fixed-asset accounts map to
  # them, so every tenant needs them.
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

  # Leaves of the general-special aggregation lattice: unmappable and
  # unrendered today, so deferred until a granularity feature wires them.
  gs_parents = {f for f, _ in gs}
  gs_children = {t for _, t in gs}
  disaggregation_leaves = (gs_children - gs_parents) & rg  # child-only in the lattice

  # The category sets above only label the drop reason; anything else dropped
  # is ``unanchored_intermediate``.
  final_drop = rg - keep_critical

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
