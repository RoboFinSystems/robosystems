# The World Online — Four-Statement Report (rs-gaap projection)

**Graph**: `kg19f310eb3b8272a718bd`
**Report**: `rpt_01KWRGZE0Q574X3Z17TK0T7F69` (published)
**Period**: 2024-01-01 → 2028-12-31 (cumulative; opening balance 12/31/2023)
**Dataset**: Charlie Hoffman's *The World Online* — 22,288 GL lines, 3,389 entries
**Source vocabulary**: `mini` (Seattle Method MINI 2026)
**Render vocabulary**: `rs-gaap` (RoboSystems canonical reporting taxonomy)

This is the **rs-gaap projection** of the same general ledger that
produced the `mini` reconciliation in
[`world-online-reconciliation.md`](world-online-reconciliation.md),
materialized through the Report architecture (one Report, four FactSets,
four Information Blocks). The opening balances ingest as ordinary
transactions tagged `mini:OpeningBalance`; the balance sheet therefore
reflects the cumulative position without any separately-injected opening
number.

## Anchor totals

| Anchor | Value |
|---|---:|
| Total Assets (`rs-gaap:Assets`) | $3,084,325.68 |
| Total Liabilities & Equity (`rs-gaap:LiabilitiesAndStockholdersEquity`) | $3,084,325.68 |
| **Balanced?** | **✓ YES (Δ = $0.00)** |
| Net Income (`rs-gaap:NetIncomeLoss`) | $(1,351,122.32) |
| Retained Earnings (`rs-gaap:RetainedEarningsAccumulatedDeficit`, auto-derived) | $(1,351,122.32) |
| Net Cash Change (`rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease`) | $(1,047,489.70) |
| Ending Stockholders' Equity (`rs-gaap:StockholdersEquity`) | $56,524.32 |

---

## Four Statements

### Balance Sheet

- **Structure**: `rs-gaap — Balance Sheet — Classified`
- **Block type**: `balance_sheet`
- **Row count**: 16
- **Unmapped elements**: 0

| QName | Concept | Current (2024-01-01 → 2028-12-31) | Prior (2018-12-31 → 2023-12-31) |
|---|---|---: | ---:|
| `rs-gaap:CashAndCashEquivalentsAtCarryingValue` |     Cash and Cash Equivalents, at Carrying Value | $(648,551.94) | $398,937.76 |
| `rs-gaap:ReceivablesNetCurrent` |     Receivables, Net, Current | $2,035,468.27 | $1,231,338.47 |
| `rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings` |     Inventory, Net of Allowances, Customer Advances and Progress Billings | $451,842.19 | $467,010.20 |
| `rs-gaap:AssetsCurrent` |   **Assets, Current** | $1,838,758.52 | $2,097,286.43 |
| `rs-gaap:PropertyPlantAndEquipmentNet` |     Property, Plant and Equipment, Net | $1,245,567.16 | $1,266,995.32 |
| `rs-gaap:AssetsNoncurrent` |   **Assets, Noncurrent** | $1,245,567.16 | $1,266,995.32 |
| `rs-gaap:Assets` | **Assets** | $3,084,325.68 | $3,364,281.75 |
| `rs-gaap:AccountsPayableCurrent` |       Accounts Payable, Current | $2,689,452.31 | $1,595,349.42 |
| `rs-gaap:LiabilitiesCurrent` |     **Liabilities, Current** | $2,689,452.31 | $1,595,349.42 |
| `rs-gaap:LongTermDebtAndCapitalLeaseObligations` |       Long-Term Debt and Lease Obligation | $338,349.05 | $361,285.69 |
| `rs-gaap:LiabilitiesNoncurrent` |     **Liabilities, Noncurrent** | $338,349.05 | $361,285.69 |
| `rs-gaap:Liabilities` |   **Liabilities** | $3,027,801.36 | $1,956,635.11 |
| `rs-gaap:AdditionalPaidInCapital` |     Additional Paid in Capital | $1,407,646.64 | $1,407,646.64 |
| `rs-gaap:RetainedEarningsAccumulatedDeficit` |     Retained Earnings (Accumulated Deficit) | $(1,351,122.32) | $0.00 |
| `rs-gaap:StockholdersEquity` |   **Stockholders' Equity Attributable to Parent** | $56,524.32 | $1,407,646.64 |
| `rs-gaap:LiabilitiesAndStockholdersEquity` | **Liabilities and Equity** | $3,084,325.68 | $3,364,281.75 |

---

### Income Statement

- **Structure**: `rs-gaap — Income Statement — Multi-step`
- **Block type**: `income_statement`
- **Row count**: 12
- **Unmapped elements**: 0

| QName | Concept | Current (2024-01-01 → 2028-12-31) | Prior (2018-12-31 → 2023-12-31) |
|---|---|---: | ---:|
| `rs-gaap:Revenues` |   **Revenues** | $2,604,048.36 | $0.00 |
| `rs-gaap:CostOfGoodsAndServicesSold` |     Cost of Product and Service Sold | $886,041.18 | $0.00 |
| `rs-gaap:CostOfRevenue` |   **Cost of Revenue** | $886,041.18 | $0.00 |
| `rs-gaap:GrossProfit` |   **Gross Profit** | $1,718,007.18 | $0.00 |
| `rs-gaap:GeneralAndAdministrativeExpense` |     General and Administrative Expense | $3,049,867.27 | $0.00 |
| `rs-gaap:DepreciationDepletionAndAmortization` |     Depreciation, Depletion and Amortization | $21,428.16 | $0.00 |
| `rs-gaap:OperatingExpenses` |   **Operating Expenses** | $3,071,295.43 | $0.00 |
| `rs-gaap:OperatingIncomeLoss` |   **Operating Income (Loss)** | $(1,353,288.25) | $0.00 |
| `rs-gaap:InterestExpense` |     Interest Expense, Operating and Nonoperating | $(2,165.93) | $0.00 |
| `rs-gaap:NonoperatingIncomeExpense` |   **Nonoperating Income (Expense)** | $2,165.93 | $0.00 |
| `rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest` |   **Income (Loss) from Continuing Operations before Income Taxes, Noncontrolling Interest** | $(1,351,122.32) | $0.00 |
| `rs-gaap:NetIncomeLoss` |   **Net Income (Loss) Attributable to Parent** | $(1,351,122.32) | $0.00 |

---

### Cash Flow Statement

- **Structure**: `rs-gaap — Cash Flow Statement — Indirect`
- **Block type**: `cash_flow_statement`
- **Row count**: 7
- **Unmapped elements**: 0

| QName | Concept | Current (2024-01-01 → 2028-12-31) | Prior (2018-12-31 → 2023-12-31) |
|---|---|---: | ---:|
| `rs-gaap:NetIncomeLoss` |     Net Income (Loss) Attributable to Parent | $(1,351,122.32) | $0.00 |
| `rs-gaap:DepreciationDepletionAndAmortization` |     Depreciation, Depletion and Amortization | $21,428.16 | $0.00 |
| `rs-gaap:IncreaseDecreaseInAccountsReceivable` |     Increase (Decrease) in Accounts Receivable | $(804,129.80) | $0.00 |
| `rs-gaap:IncreaseDecreaseInInventories` |     Increase (Decrease) in Inventories | $15,168.01 | $0.00 |
| `rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet` |     Increase (Decrease) in Other Operating Assets and Liabilities, Net | $1,071,166.25 | $0.00 |
| `rs-gaap:NetCashProvidedByUsedInOperatingActivities` |   **Cash Provided by (Used in) Operating Activity, Including Discontinued Operation** | $(1,047,489.70) | $0.00 |
| `rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease` | **Cash and Cash Equivalents, Period Increase (Decrease)** | $(1,047,489.70) | $0.00 |

---

### Statement of Changes in Equity

- **Structure**: `rs-gaap — Statement of Changes in Equity — Roll Forward (Total)`
- **Block type**: `equity_statement`
- **Row count**: 2
- **Unmapped elements**: 0

| QName | Concept | Current (2024-01-01 → 2028-12-31) | Prior (2018-12-31 → 2023-12-31) |
|---|---|---: | ---:|
| `rs-gaap:NetIncomeLoss` |   Net Income (Loss) Attributable to Parent | $(1,351,122.32) | $0.00 |
| `rs-gaap:StockholdersEquity` | **Stockholders' Equity Attributable to Parent** | $56,524.32 | $1,407,646.64 |

---

## Companion artifacts

- [`world-online-reconciliation.md`](world-online-reconciliation.md) —
  source-vocabulary (`mini`) cell-by-cell reconciliation against Charlie's
  `SummaryOfTransactions.csv` pivot (also the business-event summary).
- [`../README.md`](../README.md) — demo orchestrator + scope notes.

### Reproduce

```bash
just demo-world-online                         # full pipeline
just demo-world-online-create-report kg19f310eb3b8272a718bd  # this report only
```
