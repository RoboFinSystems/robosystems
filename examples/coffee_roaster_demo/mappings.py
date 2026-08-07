"""CoA → rs-gaap mappings for Driftline Coffee Roasters.

Every target qname below was verified against the rs-gaap library source
(``frameworks/rs-gaap/packages/rs-gaap-presentation/v1``) to be admitted by
the **Default Reporting Style's Networks** — the Classified Balance Sheet and
Multi-step Income Statement. A fact only renders if its concept is admitted
(or rolls up into an admitted leaf via the calc linkbase); mapping to a
concept outside the Networks silently drops the fact as "out of structure".

The leaves that make a roaster's working-capital story renderable, and that a
services business never needs, are:

- **Inventory** → ``rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings``
  (the exact Classified-BS inventory leaf; *not* the shorter ``InventoryNet``).
- **Deferred subscription revenue** → ``rs-gaap:DeferredRevenueCurrent``.
- **COGS** → ``rs-gaap:CostOfGoodsAndServicesSold`` (Multi-step IS leaf; drives
  the ``GrossProfit`` subtotal the "P&L glows" beat leans on).

PP&E gross and accumulated depreciation map to the calc inputs of the
``PropertyPlantAndEquipmentNet`` BS leaf rather than to the net leaf itself;
Net = Gross - AD is synthesized at fact generation. Keeping them separate also
lets the cash-flow derivation read ΔGross as purchases instead of conflating
purchases with depreciation.
"""

# (coa_code, rs_gaap_qname)
MAPPINGS: list[tuple[str, str]] = [
  # Assets
  ("1000", "rs-gaap:CashAndCashEquivalentsAtCarryingValue"),
  ("1100", "rs-gaap:ReceivablesNetCurrent"),  # Wholesale AR — the slow-pay reveal
  ("1200", "rs-gaap:PrepaidExpenseCurrent"),  # Prepaid insurance
  ("1210", "rs-gaap:PrepaidExpenseCurrent"),  # Prepaid software
  ("1220", "rs-gaap:PrepaidExpenseCurrent"),  # Prepaid fulfillment
  # All three inventory stages map to the same Classified-BS leaf — and
  # each ALSO multi-maps to its disclosure member concept (authored in
  # disclosures.py; the member arcs are created by the disclosure-note
  # runner step), so the BS line and the inventory-components note foot
  # over the same facts.
  ("1400", "rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings"),
  ("1410", "rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings"),
  ("1420", "rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings"),
  ("1500", "rs-gaap:PropertyPlantAndEquipmentGross"),  # Roasting & packaging equipment
  (
    "1550",
    "rs-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment",
  ),
  # Liabilities
  ("2000", "rs-gaap:AccountsPayableCurrent"),
  ("2100", "rs-gaap:AccruedLiabilitiesCurrent"),
  ("2300", "rs-gaap:DeferredRevenueCurrent"),  # DTC subscription deferred revenue
  # Equity (corporation form)
  ("3000", "rs-gaap:AdditionalPaidInCapital"),
  ("3100", "rs-gaap:RetainedEarningsAccumulatedDeficit"),
  # Revenue — all ASC 606 revenue from contracts with customers
  ("4000", "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"),  # DTC subs
  ("4100", "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"),  # Wholesale
  ("4200", "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"),  # DTC single
  # COGS — taxed-on-gross-profit reveal needs a clean gross margin line
  ("5000", "rs-gaap:CostOfGoodsAndServicesSold"),
  # Operating expenses — back-office costs are G&A; demand-gen is Selling & Marketing
  ("6000", "rs-gaap:GeneralAndAdministrativeExpense"),  # Salaries & wages
  ("6100", "rs-gaap:GeneralAndAdministrativeExpense"),  # Roastery rent
  ("6200", "rs-gaap:SellingAndMarketingExpense"),  # Fulfillment & shipping
  ("6300", "rs-gaap:SellingAndMarketingExpense"),  # Marketing & advertising
  ("6400", "rs-gaap:GeneralAndAdministrativeExpense"),  # Software & subscriptions
  ("6500", "rs-gaap:GeneralAndAdministrativeExpense"),  # Insurance
  ("6600", "rs-gaap:GeneralAndAdministrativeExpense"),  # Utilities & supplies
  ("7000", "rs-gaap:DepreciationDepletionAndAmortization"),  # Depreciation
]


# Equity maps to the rs-gaap capital concept that matches the entity's legal
# form, so the equity-form Reporting Style renders its native capital line.
# Driftline is a corporation; the other forms are here so the same scenario can
# be re-run as a partnership or LLC.
_EQUITY_BY_FORM: dict[str, list[tuple[str, str]]] = {
  "corporation": [
    ("3000", "rs-gaap:AdditionalPaidInCapital"),
    ("3100", "rs-gaap:RetainedEarningsAccumulatedDeficit"),
  ],
  "partnership": [
    ("3000", "rs-gaap:PartnersCapital"),
    ("3100", "rs-gaap:PartnersCapital"),
  ],
  "llc": [
    ("3000", "rs-gaap:MembersEquity"),
    ("3100", "rs-gaap:MembersEquity"),
  ],
}


def mappings_for(entity_type: str = "corporation") -> list[tuple[str, str]]:
  """CoA→rs-gaap mappings with equity rows tuned to the entity legal form."""
  form = (entity_type or "corporation").strip().lower()
  equity = _EQUITY_BY_FORM.get(form, _EQUITY_BY_FORM["corporation"])
  base = [m for m in MAPPINGS if m[0] not in ("3000", "3100")]
  return base + equity
