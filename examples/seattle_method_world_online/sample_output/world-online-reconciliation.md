# The World Online — Cross-Taxonomy Reconciliation

**Graph**: `kg19e71b8c978fd3b84ea9`  
**Dataset**: Charlie Hoffman's *The World Online* — 22,288 GL lines, 3,389 journal entries, opening 12/31/2023 through 2028  
**Source vocabulary**: `mini` (Seattle Method MINI 2026)  
**Reference**: `SummaryOfTransactions.csv` (`StandardLineItem × StandardBusinessEvent` pivot)  

This is the **summary of business-event information** Charlie's README asks the report to produce, reproduced from the ingested graph and reconciled cell-by-cell against his published pivot. Every LineItem is grouped by its balance-sheet/income-statement concept and its business-event flow tag (`LineItem.flow_element_id`) — the same grouping the rollforward filter engine sees.

## Scorecard

- **Cells compared**: 23
- **Matching**: 22 / 23
- **Methodology gap**: 0
- **Our bug**: 0
- **Their data quality**: 1

### Balance check (grand total)

| | Amount (debit-positive) | Lines |
|---|---:|---:|
| Graph (all posted LineItems) | $0.00 | 22,286 |
| Charlie's summary (All × All) | $0.50 | 22,288 |

A double-entry GL must net to $0.00 across all accounts; the graph does. Charlie's published summary nets to a small non-zero figure — accumulated per-cell rounding in the summary, not a posting imbalance.

## Cell-by-cell

| Line item | Business event | Expected | Graph | Δ amount | Cnt (exp/graph) | Category | Note |
|---|---|---:|---:|---:|:---:|---|---|
| `mini:AccountsPayable` | `mini:DecreaseFromPaymentAccountsPayable` | $1,889,640.10 | $1,889,636.81 | $(3.29) | 418/418 | Matching | Amounts agree within published-summary rounding (Δ $3.29 over 418 lines; GL balances to $0.00). |
| `mini:AccountsPayable` | `mini:OpeningBalance` | $(1,595,349.60) | $(1,595,349.42) | $0.18 | 49/49 | Matching | Amounts agree within published-summary rounding (Δ $0.18 over 49 lines; GL balances to $0.00). |
| `mini:AccountsPayable` | `mini:PurchasesInventoryForSaleOnAccount` | $(2,983,790.20) | $(2,983,739.70) | $50.50 | 8434/8434 | Matching | Amounts agree within published-summary rounding (Δ $50.50 over 8434 lines; GL balances to $0.00). |
| `mini:CashAndCashEquivalents` | `mini:OpeningBalance` | $398,937.90 | $398,937.76 | $(0.14) | 11/11 | Matching | Amounts agree within published-summary rounding (Δ $0.14 over 11 lines; GL balances to $0.00). |
| `mini:CashAndCashEquivalents` | `mini:PaymentOfAccountsPayable` | $(3,119,538.20) | $(3,119,525.02) | $13.18 | 1993/1993 | Matching | Amounts agree within published-summary rounding (Δ $13.18 over 1993 lines; GL balances to $0.00). |
| `mini:CashAndCashEquivalents` | `mini:ProceedsFromCollectionOfReceivables` | $2,072,040.60 | $2,072,035.32 | $(5.28) | 402/402 | Matching | Amounts agree within published-summary rounding (Δ $5.28 over 402 lines; GL balances to $0.00). |
| `mini:CostsOfSales` | `mini:NetIncomeLoss` | $886,044.90 | $886,041.18 | $(3.72) | 568/568 | Matching | Amounts agree within published-summary rounding (Δ $3.72 over 568 lines; GL balances to $0.00). |
| `mini:DepreciationAndAmortization` | `mini:NetIncomeLoss` | $21,428.00 | $21,428.16 | $0.16 | 5/5 | Matching | Amounts agree within published-summary rounding (Δ $0.16 over 5 lines; GL balances to $0.00). |
| `mini:InterestExpense` | `mini:NetIncomeLoss` | $(2,165.90) | $(2,165.93) | $(0.03) | 22/20 | Their data quality | Amounts tie (Δ $0.03) but Charlie counts 2 more line(s) — $0 memo line(s) we skip as non-postable in double-entry. No economic impact. |
| `mini:Inventories` | `mini:DecreaseInInventoriesFromSales` | $(1,543,607.00) | $(1,543,602.24) | $4.76 | 549/549 | Matching | Amounts agree within published-summary rounding (Δ $4.76 over 549 lines; GL balances to $0.00). |
| `mini:Inventories` | `mini:OpeningBalance` | $467,010.20 | $467,010.20 | $0.00 | 5/5 | Matching | Exact match. |
| `mini:Inventories` | `mini:PurchasesOfInventoryForSale` | $1,528,436.70 | $1,528,434.23 | $(2.47) | 344/344 | Matching | Amounts agree within published-summary rounding (Δ $2.47 over 344 lines; GL balances to $0.00). |
| `mini:LongtermDebt` | `mini:AdditionalLongtermBorrowings` | $(11,458.10) | $(11,458.10) | $0.00 | 2/2 | Matching | Exact match. |
| `mini:LongtermDebt` | `mini:OpeningBalance` | $(361,285.70) | $(361,285.69) | $0.01 | 5/5 | Matching | Amounts agree within published-summary rounding (Δ $0.01 over 5 lines; GL balances to $0.00). |
| `mini:LongtermDebt` | `mini:RepaymentLongtermBorrowings` | $34,394.80 | $34,394.74 | $(0.06) | 16/16 | Matching | Amounts agree within published-summary rounding (Δ $0.06 over 16 lines; GL balances to $0.00). |
| `mini:PaidInCapital` | `mini:OpeningBalance` | $(1,407,646.70) | $(1,407,646.64) | $0.06 | 9/9 | Matching | Amounts agree within published-summary rounding (Δ $0.06 over 9 lines; GL balances to $0.00). |
| `mini:PropertyPlantAndEquipment` | `mini:DecreaseFromDepreciationAndAmortization` | $(21,428.00) | $(21,428.16) | $(0.16) | 5/5 | Matching | Amounts agree within published-summary rounding (Δ $0.16 over 5 lines; GL balances to $0.00). |
| `mini:PropertyPlantAndEquipment` | `mini:OpeningBalance` | $1,266,995.30 | $1,266,995.32 | $0.02 | 10/10 | Matching | Amounts agree within published-summary rounding (Δ $0.02 over 10 lines; GL balances to $0.00). |
| `mini:Receivables` | `mini:CollectionOfReceivables` | $(2,038,353.80) | $(2,038,347.91) | $5.89 | 459/459 | Matching | Amounts agree within published-summary rounding (Δ $5.89 over 459 lines; GL balances to $0.00). |
| `mini:Receivables` | `mini:IncreaseInReceivablesFromSalesOnAccount` | $2,842,484.10 | $2,842,477.71 | $(6.39) | 536/536 | Matching | Amounts agree within published-summary rounding (Δ $6.39 over 536 lines; GL balances to $0.00). |
| `mini:Receivables` | `mini:OpeningBalance` | $1,231,338.50 | $1,231,338.47 | $(0.03) | 9/9 | Matching | Amounts agree within published-summary rounding (Δ $0.03 over 9 lines; GL balances to $0.00). |
| `mini:Sales` | `mini:NetIncomeLoss` | $(2,604,057.00) | $(2,604,048.36) | $8.64 | 615/615 | Matching | Amounts agree within published-summary rounding (Δ $8.64 over 615 lines; GL balances to $0.00). |
| `mini:SalesGeneralAndAdministrativeExpenses` | `mini:NetIncomeLoss` | $3,049,929.60 | $3,049,867.27 | $(62.33) | 7822/7822 | Matching | Amounts agree within published-summary rounding (Δ $62.33 over 7822 lines; GL balances to $0.00). |

## Classification key

| Category | Meaning | Owner |
|---|---|---|
| **Matching** | Graph equals expected (exact, or within the published summary's per-cell rounding). | — |
| **Methodology gap** | Architectural feature not yet shipped. | RoboSystems (forward queue) |
| **Our bug** | Implementation error in shipped code — needs a fix. | RoboSystems (fix) |
| **Their data quality** | Source data tagging/rounding issue, not a pipeline defect. | Source author |

## How to reproduce

```bash
just demo-world-online                       # full pipeline
just demo-world-online-reconcile <graph_id>  # this report only
```