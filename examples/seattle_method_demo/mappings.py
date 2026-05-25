"""mini → rs-gaap concept mapping table.

The canonical bridge from Charlie Hoffman's mini reporting framework
to RoboSystems' rs-gaap canonical reporting taxonomy. Two scopes:

1. **BS / IS leaves** — mini's monetary instant/duration concepts that
   appear on the Balance Sheet or Income Statement, mapped to their
   rs-gaap equivalents. The 14 leaves needed for Test Case 1's 14 JEs
   plus the surrounding sub-rollup concepts.
2. **Flow concepts (TDCs)** — mini's ``TransactionDescriptionCode``
   values from Charlie's CSV, mapped to rs-gaap CF / SE roll-forward
   concepts. These drive the per-line ``transaction_description_code``
   metadata that the rollforward filter engine matches against.

Mapping decisions where mini and rs-gaap diverge are flagged inline
with ``# NOTE:``. The reconciliation report's "Their data quality" /
"Methodology gap" sections will surface any cases where the bridge
loses precision.

Seeded via ``seed_mappings.py`` as ``association_type='derivation'``
arcs (same arc type Phase 1 rollforward uses for default change tag
derivation).
"""

from __future__ import annotations

# ── BS / IS leaves ────────────────────────────────────────────────────────
#
# (mini qname, rs-gaap qname) — straight 1:1 mappings for the concepts
# Charlie's 14-JE dataset touches. mini's instant concepts (BS) → rs-gaap
# BS concepts; mini's duration concepts (IS) → rs-gaap IS concepts.

BS_IS_MAPPINGS: list[tuple[str, str]] = [
  # Balance sheet — instant / monetary. Targets must be leaves in the
  # rs-gaap BS-Classified presentation network used by the Default
  # Reporting Style. The earlier set targeted leaf-cousins (e.g.
  # rs-gaap:AccountsPayableCurrent) that the renderer rolled up to a
  # parent NOT in the BS path — producing facts that displayed but
  # didn't aggregate into Assets / L+E subtotals.
  ("mini:CashAndCashEquivalents", "rs-gaap:CashCashEquivalentsAndShortTermInvestments"),
  ("mini:Receivables", "rs-gaap:ReceivablesNetCurrent"),
  (
    "mini:Inventories",
    "rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings",
  ),
  ("mini:PropertyPlantAndEquipment", "rs-gaap:PropertyPlantAndEquipmentNet"),
  # AP and Accrued both roll into the same combined BS-Classified leaf
  # (the rs-gaap presentation network doesn't expose them separately
  # at the visible-leaf layer).
  ("mini:AccountsPayable", "rs-gaap:AccountsPayableAndAccruedLiabilitiesCurrent"),
  ("mini:AccruedExpenses", "rs-gaap:AccountsPayableAndAccruedLiabilitiesCurrent"),
  ("mini:LongtermDebt", "rs-gaap:LongTermDebtAndCapitalLeaseObligations"),
  ("mini:PaidInCapital", "rs-gaap:AdditionalPaidInCapital"),
  # Income statement — duration / monetary
  ("mini:Sales", "rs-gaap:Revenues"),
  # NOTE: mini:CostsOfSales conflates COGS and services-cost. rs-gaap
  # splits these; pick CostOfGoodsSold as the dominant case for Charlie's
  # lemonade-stand fixtures (contractor labor in JE-205 is mis-tagged
  # per the README's data-quality findings).
  ("mini:CostsOfSales", "rs-gaap:CostOfGoodsSold"),
  # SG&A — not exercised by the 14-JE lemonade stand, but the World
  # Online dataset (examples/seattle_method_world_online) has 7,822 SG&A
  # lines. Additive: Case 1 simply never touches this concept.
  (
    "mini:SalesGeneralAndAdministrativeExpenses",
    "rs-gaap:SellingGeneralAndAdministrativeExpense",
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
# Every value of mini's ``TransactionDescriptionCode`` column in
# Charlie's CSV, mapped to the rs-gaap CF / SE flow concept it
# represents. These drive the rollforward filter targets.
#
# Some mini flow concepts appear on BOTH sides of a JE (the BS-line
# perspective and the equity-or-flow perspective), e.g. owner
# investment shows up as ``ProceedsFromInvestmentsByOwner`` on the
# Cash line and ``InvestmentsByOwner`` on the equity line — both map
# to ``rs-gaap:ProceedsFromIssuanceOfCommonStock`` (or its
# equity-statement analog).

FLOW_MAPPINGS: list[tuple[str, str]] = [
  # Financing — equity contribution. Both sides of the JE (the Cash
  # debit and the PaidInCapital credit) collapse to the same rs-gaap
  # flow concept since rs-gaap doesn't distinguish the cash-side from
  # the equity-side of an issuance (mini does).
  ("mini:ProceedsFromInvestmentsByOwner", "rs-gaap:ProceedsFromIssuanceOfCommonStock"),
  ("mini:InvestmentsByOwner", "rs-gaap:ProceedsFromIssuanceOfCommonStock"),
  # Financing — debt, GROSS per ASC 230. The CF-Financing calc rollup now
  # carries the signed gross pair (ProceedsFromIssuanceOfLongTermDebt +
  # RepaymentsOfLongTermDebt), so issuances and repayments map to their own
  # leaves and present separately — no longer collapsed onto the signed-net
  # concept (the prior "lands on a leaf that renders" workaround, now obsolete).
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
  # NOTE: mini:PurchasesInventoryForSaleOnAccount is the AP-side TDC
  # for an inventory purchase on account — handled below in the AP
  # operating-flow block alongside other AP-impacting TDCs (all target
  # the combined AP+Accrued leaf).
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
  # PurchasesInventoryForSaleOnAccount above was also targeting the
  # legacy AP-only concept — re-aim it at the combined leaf for the same
  # reason.
  (
    "mini:PurchasesInventoryForSaleOnAccount",
    "rs-gaap:IncreaseDecreaseInAccountsPayableAndAccruedLiabilities",
  ),
  # Operating — interest accruals.
  # NOTE: mini's canonical concept is ``PaymentInterest`` (not
  # ``PaymentOfInterest``); Charlie's CSV has the typo. We normalize
  # at ingest time via ``ingest_transactions.py::_KNOWN_TDC_ALIASES``
  # so the filter targets the canonical name here.
  # NOTE: rs-gaap doesn't carry a clean "InterestPaid" CF concept in
  # our currently-loaded library — the canonical
  # ``rs-gaap:InterestPaidNet`` is part of the broader rs-gaap corpus
  # but not in our subset. We map mini:PaymentInterest to
  # ``rs-gaap:InterestExpense`` as the closest available concept —
  # surfaces as "Library gap" in the reconciliation report (mapping
  # is approximate; the CF rendering will conflate the IS expense
  # and the CF payment).
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
  # PPE write-off (JE-225's nil placeholder)
  (
    "mini:PropertyPlantAndEquipmentWrittenOff",
    "rs-gaap:GainLossOnDispositionOfAssets",
  ),
  # Net income roll-up (the IS-side TDC every IS line carries)
  ("mini:NetIncomeLoss", "rs-gaap:NetIncomeLoss"),
]


# Combined list — what ``seed_mappings.py`` walks.
ALL_MAPPINGS: list[tuple[str, str]] = BS_IS_MAPPINGS + FLOW_MAPPINGS
