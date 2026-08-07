"""mini → rs-gaap concept mapping table.

The bridge from Charlie Hoffman's mini reporting framework to the rs-gaap
canonical reporting taxonomy — the correspondence that lets one set of
postings render as two different sets of financial statements. Two scopes:

1. **BS / IS leaves** — mini's monetary instant and duration concepts that
   appear on the Balance Sheet or Income Statement, mapped to their rs-gaap
   equivalents.
2. **Flow concepts** — the ``TransactionDescriptionCode`` values from the
   source CSV, mapped to rs-gaap cash-flow and equity roll-forward concepts.
   These are what the rollforward filter engine matches on, so they are how a
   balance-sheet movement gets attributed to the events that caused it.

Where mini and rs-gaap genuinely diverge, the choice is flagged inline with
``# NOTE:``; the reconciliation report surfaces any resulting loss of
precision under "Their data quality" or "Methodology gap".

``seed_mappings.py`` seeds these as ``association_type='derivation'`` arcs —
the same arc type a rollforward uses to derive its default change tag.
"""

from __future__ import annotations

# ── BS / IS leaves ────────────────────────────────────────────────────────
#
# (mini qname, rs-gaap qname) — 1:1 mappings for the concepts the dataset
# touches. mini's instant concepts (BS) map to rs-gaap BS concepts, its
# duration concepts (IS) to rs-gaap IS concepts.

BS_IS_MAPPINGS: list[tuple[str, str]] = [
  # Balance sheet — instant / monetary. Targets must be leaves in the
  # rs-gaap BS-Classified presentation network used by the Default
  # Reporting Style — the visible leaves the renderer aggregates into the
  # Assets / L+E subtotals.
  ("mini:CashAndCashEquivalents", "rs-gaap:CashAndCashEquivalentsAtCarryingValue"),
  ("mini:Receivables", "rs-gaap:ReceivablesNetCurrent"),
  (
    "mini:Inventories",
    "rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings",
  ),
  ("mini:PropertyPlantAndEquipment", "rs-gaap:PropertyPlantAndEquipmentNet"),
  # AP and Accrued are separate BS-Classified leaves (split out from the
  # combined AccountsPayableAndAccruedLiabilitiesCurrent line).
  ("mini:AccountsPayable", "rs-gaap:AccountsPayableCurrent"),
  ("mini:AccruedExpenses", "rs-gaap:AccruedLiabilitiesCurrent"),
  ("mini:LongtermDebt", "rs-gaap:LongTermDebtAndCapitalLeaseObligations"),
  ("mini:PaidInCapital", "rs-gaap:AdditionalPaidInCapital"),
  # Income statement — duration / monetary
  ("mini:Sales", "rs-gaap:Revenues"),
  # NOTE: mini:CostsOfSales conflates COGS and services-cost.
  # CostOfGoodsAndServicesSold (ASC 705) is the modern leaf covering both —
  # the right target for Charlie's lemonade-stand fixtures (contractor labor
  # in JE-205 is mis-tagged per the README's data-quality findings).
  ("mini:CostsOfSales", "rs-gaap:CostOfGoodsAndServicesSold"),
  # SG&A — untouched by the 14-JE lemonade stand, but heavily used by the
  # World Online dataset (examples/seattle_method_world_online), which shares
  # this mapping table.
  (
    "mini:SalesGeneralAndAdministrativeExpenses",
    "rs-gaap:GeneralAndAdministrativeExpense",
  ),
  ("mini:DepreciationAndAmortization", "rs-gaap:DepreciationDepletionAndAmortization"),
  ("mini:InterestExpense", "rs-gaap:InterestExpense"),
  (
    "mini:NonoperatingIncomeExpenses",
    "rs-gaap:OtherNonoperatingIncomeExpense",
  ),
  ("mini:IncomeTaxExpenseBenefit", "rs-gaap:IncomeTaxExpenseBenefit"),
]


# ── Flow concepts (TDCs) ──────────────────────────────────────────────────
#
# Every value of the ``TransactionDescriptionCode`` column, mapped to the
# rs-gaap cash-flow or equity flow concept it represents. These are the
# rollforward filter targets.
#
# mini names both sides of a journal entry, so one economic event can arrive
# under two concepts: an owner investment appears as
# ``ProceedsFromInvestmentsByOwner`` on the cash line and
# ``InvestmentsByOwner`` on the equity line. rs-gaap does not draw that
# distinction, so both collapse onto the same target concept.

FLOW_MAPPINGS: list[tuple[str, str]] = [
  # Financing — equity contribution. Both sides of the JE (the cash debit and
  # the PaidInCapital credit) collapse onto one rs-gaap concept.
  ("mini:ProceedsFromInvestmentsByOwner", "rs-gaap:ProceedsFromIssuanceOfCommonStock"),
  ("mini:InvestmentsByOwner", "rs-gaap:ProceedsFromIssuanceOfCommonStock"),
  # Financing — debt, GROSS per ASC 230. The CF-Financing calc rollup
  # carries the signed gross pair (ProceedsFromIssuanceOfLongTermDebt +
  # RepaymentsOfLongTermDebt), so issuances and repayments map to their own
  # leaves and present separately rather than collapsing onto the signed-net
  # concept.
  (
    "mini:ProceedsFromAdditionalLongtermBorrowings",
    "rs-gaap:ProceedsFromIssuanceOfLongTermDebt",
  ),
  (
    "mini:AdditionalLongtermBorrowings",
    "rs-gaap:ProceedsFromIssuanceOfLongTermDebt",
  ),
  (
    "mini:RepaymentLongtermBorrowings",
    "rs-gaap:RepaymentsOfLongTermDebt",
  ),
  (
    "mini:PaymentForReductionOfLongtermBorrowings",
    "rs-gaap:RepaymentsOfLongTermDebt",
  ),
  # Investing — PP&E
  (
    "mini:CapitalAdditionsPropertyPlantAndEquipment",
    "rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment",
  ),
  (
    "mini:PaymentForCapitalAdditionsOfPropertyPlantEquipment",
    "rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment",
  ),
  # Operating — receivables
  (
    "mini:IncreaseInReceivablesFromSalesOnAccount",
    "rs-gaap:IncreaseDecreaseInAccountsReceivable",
  ),
  (
    "mini:ProceedsFromCollectionOfReceivables",
    "rs-gaap:IncreaseDecreaseInAccountsReceivable",
  ),
  ("mini:CollectionOfReceivables", "rs-gaap:IncreaseDecreaseInAccountsReceivable"),
  # Operating — inventory
  ("mini:PurchasesOfInventoryForSale", "rs-gaap:IncreaseDecreaseInInventories"),
  # NOTE: mini:PurchasesInventoryForSaleOnAccount is the AP-side concept for
  # an inventory purchase on account, so it belongs with the other
  # AP-impacting flows below rather than here.
  ("mini:DecreaseInInventoriesFromSales", "rs-gaap:IncreaseDecreaseInInventories"),
  ("mini:InventoryWrittenOff", "rs-gaap:InventoryWriteDown"),
  # Operating — AP. CF-Indirect's leaf is the combined AP+Accrued change
  # (IncreaseDecreaseInAccountsPayableAndAccruedLiabilities); the
  # finer-grained IncreaseDecreaseInAccountsPayable isn't on the BS-Classified
  # CF path, so it never renders.
  (
    "mini:DecreaseFromPaymentAccountsPayable",
    "rs-gaap:IncreaseDecreaseInAccountsPayableAndAccruedLiabilities",
  ),
  (
    "mini:PaymentOfAccountsPayable",
    "rs-gaap:IncreaseDecreaseInAccountsPayableAndAccruedLiabilities",
  ),
  # Inventory bought on account moves AP, so it targets the same combined
  # leaf for the same reason.
  (
    "mini:PurchasesInventoryForSaleOnAccount",
    "rs-gaap:IncreaseDecreaseInAccountsPayableAndAccruedLiabilities",
  ),
  # Operating — interest accruals.
  # NOTE: mini's canonical concept is ``PaymentInterest``, not
  # ``PaymentOfInterest``; ``ingest_transactions.py::_KNOWN_TDC_ALIASES``
  # normalizes the source value so the filter can target the canonical name.
  # NOTE: the loaded rs-gaap subset carries no dedicated interest-paid
  # cash-flow concept, so this maps to ``rs-gaap:InterestExpense`` as the
  # closest available target. The mapping is approximate — the cash-flow
  # rendering conflates the income-statement expense with the cash payment —
  # and the reconciliation report flags it as a library gap.
  ("mini:PaymentInterest", "rs-gaap:InterestExpense"),
  (
    "mini:DecreaseFromPaymentOfInterest",
    "rs-gaap:IncreaseDecreaseInAccruedLiabilities",
  ),
  ("mini:InterestAccrued", "rs-gaap:IncreaseDecreaseInAccruedLiabilities"),
  # Operating — D&A
  (
    "mini:DecreaseFromDepreciationAndAmortization",
    "rs-gaap:DepreciationDepletionAndAmortization",
  ),
  # PPE write-off
  (
    "mini:PropertyPlantAndEquipmentWrittenOff",
    "rs-gaap:GainLossOnDispositionOfAssets",
  ),
  # Net income roll-up — the flow concept every income-statement line carries
  ("mini:NetIncomeLoss", "rs-gaap:NetIncomeLoss"),
]


# Combined list — what ``seed_mappings.py`` walks.
ALL_MAPPINGS: list[tuple[str, str]] = BS_IS_MAPPINGS + FLOW_MAPPINGS
