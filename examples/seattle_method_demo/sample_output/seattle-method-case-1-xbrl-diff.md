# Seattle Method — XBRL emit-side reconciliation (Test Case 1)

Graph: `kg19e714499a8d055c5458`
Report: `rpt_01KSRMHXMPS6RKA8F3WYT7EVP3`

## Summary

- **Our emit**: 39 facts (rs-gaap)
- **Charlie's reference**: 84 facts (mini)
- **Shared (concept × period)**: 7
- **Exact value match**: 7 / 7 (100.0%)
- **Value mismatch**: 0
- **Our emit only**: 32
- **Charlie's reference only**: 77

## Notes

Charlie's reference uses the `mini:` taxonomy; ours uses `rs-gaap:`. Diff identity is on concept **local-name** so cross-taxonomy parity holds for the canonical BS/IS concepts (the seeded mini→rs-gaap derivation mappings handle the namespace projection upstream in `reconcile.py` step 7). The columns below show the full qname from each side so the namespace difference is visible.

Values are dollars (the underlying XBRL `decimals='INF'` integer amounts ÷ 100).

## Exact matches (7)

| Kind | Concept | Ours (rs-gaap) | Ours period | Charlie's (mini) | Charlie's period | Δ |
|---|---|---:|:--|---:|:--|---:|
| duration | `IncomeTaxExpenseBenefit` | 400.00 | 2024-03-31 | 400.00 | 2024-12-31 | 0 |
| duration | `InterestExpense` | 150.00 | 2024-03-31 | 150.00 | 2024-12-31 | 0 |
| duration | `NetIncomeLoss` | 2,050.00 | 2024-03-31 | 2,050.00 | 2024-12-31 | 0 |
| duration | `OperatingExpenses` | 100.00 | 2024-03-31 | 100.00 | 2024-12-31 | 0 |
| duration | `OperatingIncomeLoss` | 2,600.00 | 2024-03-31 | 2,600.00 | 2024-12-31 | 0 |
| instant | `Assets` | 14,450.00 | 2024-03-31 | 14,450.00 | 2024-12-31 | 0 |
| instant | `Liabilities` | 2,400.00 | 2024-03-31 | 2,400.00 | 2024-12-31 | 0 |

## Value mismatches (0)

_None._

## Our emit only (32)

| Kind | Concept | Ours (rs-gaap) | Ours period | Charlie's (mini) | Charlie's period | Δ |
|---|---|---:|:--|---:|:--|---:|
| duration | `CashAndCashEquivalentsPeriodIncreaseDecrease` | 10,850.00 | 2024-03-31 | — | — |  |
| duration | `CostOfGoodsSold` | 5,300.00 | 2024-03-31 | — | — |  |
| duration | `CostOfRevenue` | 5,300.00 | 2024-03-31 | — | — |  |
| duration | `DepreciationDepletionAndAmortization` | 100.00 | 2024-03-31 | — | — |  |
| duration | `GrossProfit` | 2,700.00 | 2024-03-31 | — | — |  |
| duration | `IncomeLossFromContinuingOperations` | 2,050.00 | 2024-03-31 | — | — |  |
| duration | `IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest` | 2,450.00 | 2024-03-31 | — | — |  |
| duration | `IncreaseDecreaseInAccountsPayableAndAccruedLiabilities` | 1,400.00 | 2024-03-31 | — | — |  |
| duration | `IncreaseDecreaseInInventories` | -2,700.00 | 2024-03-31 | — | — |  |
| duration | `NetCashProvidedByUsedInFinancingActivities` | 11,000.00 | 2024-03-31 | — | — |  |
| duration | `NetCashProvidedByUsedInInvestingActivities` | -1,000.00 | 2024-03-31 | — | — |  |
| duration | `NetCashProvidedByUsedInOperatingActivities` | 850.00 | 2024-03-31 | — | — |  |
| duration | `NonoperatingIncomeExpense` | -150.00 | 2024-03-31 | — | — |  |
| duration | `PaymentsToAcquirePropertyPlantAndEquipment` | -1,000.00 | 2024-03-31 | — | — |  |
| duration | `ProceedsFromIssuanceOfCommonStock` | 10,000.00 | 2024-03-31 | — | — |  |
| duration | `ProceedsFromIssuanceOfLongTermDebt` | 2,000.00 | 2024-03-31 | — | — |  |
| duration | `RepaymentsOfLongTermDebt` | -1,000.00 | 2024-03-31 | — | — |  |
| duration | `Revenues` | 8,000.00 | 2024-03-31 | — | — |  |
| instant | `AccountsPayableAndAccruedLiabilitiesCurrent` | 1,400.00 | 2024-03-31 | — | — |  |
| instant | `AdditionalPaidInCapital` | 10,000.00 | 2024-03-31 | — | — |  |
| instant | `AssetsCurrent` | 13,550.00 | 2024-03-31 | — | — |  |
| instant | `AssetsNoncurrent` | 900.00 | 2024-03-31 | — | — |  |
| instant | `CashCashEquivalentsAndShortTermInvestments` | 10,850.00 | 2024-03-31 | — | — |  |
| instant | `InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings` | 2,700.00 | 2024-03-31 | — | — |  |
| instant | `LiabilitiesAndStockholdersEquity` | 14,450.00 | 2024-03-31 | — | — |  |
| instant | `LiabilitiesCurrent` | 1,400.00 | 2024-03-31 | — | — |  |
| instant | `LiabilitiesNoncurrent` | 1,000.00 | 2024-03-31 | — | — |  |
| instant | `LongTermDebtAndCapitalLeaseObligations` | 1,000.00 | 2024-03-31 | — | — |  |
| instant | `PropertyPlantAndEquipmentNet` | 900.00 | 2024-03-31 | — | — |  |
| instant | `ReceivablesNetCurrent` | 0.00 | 2024-03-31 | — | — |  |
| instant | `RetainedEarningsAccumulatedDeficit` | 2,050.00 | 2024-03-31 | — | — |  |
| instant | `StockholdersEquity` | 12,050.00 | 2024-03-31 | — | — |  |

## Charlie's reference only (77)

| Kind | Concept | Ours (rs-gaap) | Ours period | Charlie's (mini) | Charlie's period | Δ |
|---|---|---:|:--|---:|:--|---:|
| duration | `AdditionalLongtermBorrowings` | — | — | 2,000.00 | 2024-12-31 |  |
| duration | `AdditionsToAllowanceForBadDebts` | — | — | 0.00 | 2024-12-31 |  |
| duration | `BadDebtsWrittenOff` | — | — | 0.00 | 2024-12-31 |  |
| duration | `CapitalAdditionsPropertyPlantAndEquipment` | — | — | 1,000.00 | 2024-12-31 |  |
| duration | `CheckSumChanges` | — | — | 0.00 | 2024-12-31 |  |
| duration | `CollectionOfReceivables` | — | — | 8,000.00 | 2024-12-31 |  |
| duration | `CostsOfSales` | — | — | 5,300.00 | 2024-12-31 |  |
| duration | `DecreaseFromDepreciationAndAmortization` | — | — | 100.00 | 2024-12-31 |  |
| duration | `DecreaseFromPaymentAccountsPayable` | — | — | 7,000.00 | 2024-12-31 |  |
| duration | `DecreaseFromPaymentOfInterest` | — | — | 150.00 | 2024-12-31 |  |
| duration | `DecreaseInInventoriesFromSales` | — | — | 2,000.00 | 2024-12-31 |  |
| duration | `DepreciationAndAmortization` | — | — | 100.00 | 2024-12-31 |  |
| duration | `DistributionsToOwner` | — | — | 0.00 | 2024-12-31 |  |
| duration | `GainLossOnSalePropertyPlantEquipment` | — | — | 0.00 | 2024-12-31 |  |
| duration | `GrossProfitLoss` | — | — | 2,700.00 | 2024-12-31 |  |
| duration | `IncomeLossFromContinuingOperationsBeforeTax` | — | — | 2,450.00 | 2024-12-31 |  |
| duration | `IncreaseInReceivablesFromSalesOnAccount` | — | — | 8,000.00 | 2024-12-31 |  |
| duration | `InterestAccrued` | — | — | 550.00 | 2024-12-31 |  |
| duration | `InventoryWrittenOff` | — | — | 300.00 | 2024-12-31 |  |
| duration | `InvestmentsByOwner` | — | — | 10,000.00 | 2024-12-31 |  |
| duration | `NetCashFlow` | — | — | 10,850.00 | 2024-12-31 |  |
| duration | `NetCashFlowFinancingActivities` | — | — | 10,850.00 | 2024-12-31 |  |
| duration | `NetCashFlowInvestingActivities` | — | — | -1,000.00 | 2024-12-31 |  |
| duration | `NetCashFlowOperatingActivities` | — | — | 1,000.00 | 2024-12-31 |  |
| duration | `NonoperatingIncomeExpenses` | — | — | -150.00 | 2024-12-31 |  |
| duration | `PaymentForCapitalAdditionsOfPropertyPlantEquipment` | — | — | 1,000.00 | 2024-12-31 |  |
| duration | `PaymentForDistributionsToOwner` | — | — | 0.00 | 2024-12-31 |  |
| duration | `PaymentForReductionOfLongtermBorrowings` | — | — | 1,000.00 | 2024-12-31 |  |
| duration | `PaymentInterest` | — | — | 150.00 | 2024-12-31 |  |
| duration | `PaymentOfAccountsPayable` | — | — | 7,000.00 | 2024-12-31 |  |
| duration | `ProceedsFromAdditionalLongtermBorrowings` | — | — | 2,000.00 | 2024-12-31 |  |
| duration | `ProceedsFromCollectionOfReceivables` | — | — | 8,000.00 | 2024-12-31 |  |
| duration | `ProceedsFromInvestmentsByOwner` | — | — | 10,000.00 | 2024-12-31 |  |
| duration | `PropertyPlantAndEquipmentWrittenOff` | — | — | 0.00 | 2024-12-31 |  |
| duration | `PurchasesInventoryForSaleOnAccount` | — | — | 8,000.00 | 2024-12-31 |  |
| duration | `PurchasesOfInventoryForSale` | — | — | 5,000.00 | 2024-12-31 |  |
| duration | `RepaymentLongtermBorrowings` | — | — | 1,000.00 | 2024-12-31 |  |
| duration | `Sales` | — | — | 8,000.00 | 2024-12-31 |  |
| duration | `SalesGeneralAndAdministrativeExpenses` | — | — | 0.00 | 2024-12-31 |  |
| instant | `AccountsPayable` | — | — | 1,000.00 | 2024-12-31 |  |
| instant | `AccruedExpenses` | — | — | 400.00 | 2024-12-31 |  |
| instant | `AccumulatedDepreciation` | — | — | 100.00 | 2024-12-31 |  |
| instant | `Buildings` | — | — | 0.00 | 2024-12-31 |  |
| instant | `Cash` | — | — | 10,850.00 | 2024-12-31 |  |
| instant | `CashAndCashEquivalents` | — | — | 10,850.00 | 2024-12-31 |  |
| instant | `CashEquivalents` | — | — | 0.00 | 2024-12-31 |  |
| instant | `CheckSum` | — | — | 0.00 | 2024-12-31 |  |
| instant | `CurrentAssets` | — | — | 13,550.00 | 2024-12-31 |  |
| instant | `CurrentLiabilities` | — | — | 1,400.00 | 2024-12-31 |  |
| instant | `Equipment` | — | — | 1,000.00 | 2024-12-31 |  |
| instant | `Equity` | — | — | 12,050.00 | 2024-12-31 |  |
| instant | `FinishedGoods` | — | — | 0.00 | 2024-12-31 |  |
| instant | `Inventories` | — | — | 2,700.00 | 2024-12-31 |  |
| instant | `Land` | — | — | 0.00 | 2024-12-31 |  |
| instant | `LiabilitiesAndEquity` | — | — | 14,450.00 | 2024-12-31 |  |
| instant | `LongtermDebt` | — | — | 1,000.00 | 2024-12-31 |  |
| instant | `MaturesInFiveYears` | — | — | 0.00 | 2024-12-31 |  |
| instant | `MaturesInFourYears` | — | — | 0.00 | 2024-12-31 |  |
| instant | `MaturesInOneYear` | — | — | 1,000.00 | 2024-12-31 |  |
| instant | `MaturesInThreeYears` | — | — | 0.00 | 2024-12-31 |  |
| instant | `MaturesInTwoYears` | — | — | 0.00 | 2024-12-31 |  |
| instant | `MaturesThereafter` | — | — | 0.00 | 2024-12-31 |  |
| instant | `MortgageLoans` | — | — | 0.00 | 2024-12-31 |  |
| instant | `NoncurrentAssets` | — | — | 900.00 | 2024-12-31 |  |
| instant | `NoncurrentLiabilities` | — | — | 1,000.00 | 2024-12-31 |  |
| instant | `OtherPayables` | — | — | 0.00 | 2024-12-31 |  |
| instant | `OtherReceivables` | — | — | 0.00 | 2024-12-31 |  |
| instant | `OtherSecuredLoans` | — | — | 1,000.00 | 2024-12-31 |  |
| instant | `PaidInCapital` | — | — | 10,000.00 | 2024-12-31 |  |
| instant | `PropertyPlantAndEquipment` | — | — | 900.00 | 2024-12-31 |  |
| instant | `PropertyPlantAndEquipmentGross` | — | — | 1,000.00 | 2024-12-31 |  |
| instant | `RawMaterial` | — | — | 2,700.00 | 2024-12-31 |  |
| instant | `Receivables` | — | — | 0.00 | 2024-12-31 |  |
| instant | `RetainedEarnings` | — | — | 2,050.00 | 2024-12-31 |  |
| instant | `TradePayables` | — | — | 1,000.00 | 2024-12-31 |  |
| instant | `TradeReceivables` | — | — | 0.00 | 2024-12-31 |  |
| instant | `WorkInProgress` | — | — | 0.00 | 2024-12-31 |  |
