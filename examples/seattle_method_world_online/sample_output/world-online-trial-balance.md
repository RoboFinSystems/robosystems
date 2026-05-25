# The World Online — Trial Balance

**Graph**: `kg19e5d418596c8ed2e273`  
**Period**: 2023-01-01 → 2028-12-31 (cumulative; includes the 12/31/2023 opening)  
**Source vocabulary**: `mini` (Seattle Method MINI 2026)  
**Grain**: mini line-item concept (the 239 raw GL accounts collapse to their mini concept at ingest)  

Produced from the ingested graph via the `trialBalance` GraphQL query. A trial balance balances when **total debits = total credits**; the net column is debit-positive and sums to $0.00 for a balanced ledger.

| Account | Trait | Debits | Credits | Net balance (Dr +) |
|---|---|---:|---:|---:|
| Cash and Cash Equivalents (`mini:CashAndCashEquivalents`) | asset | $2,470,973.08 | $3,119,525.02 | $(648,551.94) |
| Inventories (`mini:Inventories`) | asset | $1,995,444.43 | $1,543,602.24 | $451,842.19 |
| Property, Plant and Equipment (`mini:PropertyPlantAndEquipment`) | asset | $2,200,950.98 | $955,383.82 | $1,245,567.16 |
| Receivables (`mini:Receivables`) | asset | $4,101,187.58 | $2,065,719.31 | $2,035,468.27 |
| Accounts Payable (`mini:AccountsPayable`) | liability | $1,899,168.93 | $4,588,621.24 | $(2,689,452.31) |
| Long-term Debt (`mini:LongtermDebt`) | liability | $35,584.50 | $373,933.55 | $(338,349.05) |
| Paid In Capital (`mini:PaidInCapital`) | equity | — | $1,407,646.64 | $(1,407,646.64) |
| Sales (`mini:Sales`) | revenue | $96,487.71 | $2,700,536.07 | $(2,604,048.36) |
| Costs of Sales (`mini:CostsOfSales`) | expense | $1,155,015.47 | $268,974.29 | $886,041.18 |
| Depreciation and Amortization (`mini:DepreciationAndAmortization`) | expense | $21,428.16 | — | $21,428.16 |
| Interest Expense (`mini:InterestExpense`) | expense | $118,210.32 | $120,376.25 | $(2,165.93) |
| Sales, General, and Administrative Expenses (`mini:SalesGeneralAndAdministrativeExpenses`) | expense | $3,123,052.04 | $73,184.77 | $3,049,867.27 |
| **Total** | | **$17,217,503.20** | **$17,217,503.20** | **—** |

**Balanced?** ✓ YES — total debits = total credits (Δ —)

## Companion artifacts

- [`world-online-reconciliation.md`](world-online-reconciliation.md) — business-event summary + reconciliation vs `SummaryOfTransactions.csv`.
- [`world-online-four-statements.md`](world-online-four-statements.md) — rs-gaap 4-statement Report.

### Reproduce

```bash
just demo-world-online-trial-balance kg19e5d418596c8ed2e273
```