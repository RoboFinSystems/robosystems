# Seattle Method Cross-Taxonomy — Test Case 1 Reconciliation

**Graph**: `kg19e5b8b619973b960555`
**Period**: 2024-01-01 → 2024-01-31
**Dataset**: Charlie Hoffman's lemonade-stand 14-JE Q1 2024 fixture
**Expected output reference**: [luca.pacioli.ai/luca/view/0f24fd35…](https://luca.pacioli.ai/luca/view/0f24fd35e961e167a727b663c75a4c5ec9fb7eb86730d6292f46e6e180fc2018980cd52e/index)

---

## Automated Diff vs. Charlie's Published Facts

**Correctness** (every concept we surface matches Charlie to the cent): **18/18 exact match**, |Δ| = **$0.00**.

**Coverage** (concepts we surface vs. Charlie's published set): **18/60** (30%). The remaining 43 concept(s) are forward-work — see the Coverage Gap section below.

Compared against Charlie's published XBRL instance (`local/datasets/seattle_method/report/instance.xml`, fetched by `pull_expected_report.sh` from [`xbrlsite.com/seattlemethod/platinum-testcases/record-to-report/`](http://www.xbrlsite.com/seattlemethod/platinum-testcases/record-to-report/index.html)).

| Concept | Our value | Charlie's value | Δ | |
|---|---:|---:|---:|---|
| `mini:AccountsPayable` | $1,000.00 | $1,000.00 | $0.00 | ✓ |
| `mini:AccruedExpenses` | $400.00 | $400.00 | $0.00 | ✓ |
| `mini:CashAndCashEquivalents` | $10,850.00 | $10,850.00 | $0.00 | ✓ |
| `mini:CostsOfSales` | $5,300.00 | $5,300.00 | $0.00 | ✓ |
| `mini:DepreciationAndAmortization` | $100.00 | $100.00 | $0.00 | ✓ |
| `mini:IncomeTaxExpenseBenefit` | $400.00 | $400.00 | $0.00 | ✓ |
| `mini:InterestExpense` | $150.00 | $150.00 | $0.00 | ✓ |
| `mini:Inventories` | $2,700.00 | $2,700.00 | $0.00 | ✓ |
| `mini:LongtermDebt` | $1,000.00 | $1,000.00 | $0.00 | ✓ |
| `mini:PaidInCapital` | $10,000.00 | $10,000.00 | $0.00 | ✓ |
| `mini:PropertyPlantAndEquipment` | $900.00 | $900.00 | $0.00 | ✓ |
| `mini:Receivables` | $0.00 | $0.00 | $0.00 | ✓ |
| `mini:RetainedEarnings` | $2,050.00 | $2,050.00 | $0.00 | ✓ |
| `mini:Sales` | $8,000.00 | $8,000.00 | $0.00 | ✓ |
| `mini:Assets` | $14,450.00 | $14,450.00 | $0.00 | ✓ |
| `mini:LiabilitiesAndEquity` | $14,450.00 | $14,450.00 | $0.00 | ✓ |
| `mini:NetIncomeLoss` | $2,050.00 | $2,050.00 | $0.00 | ✓ |
| `mini:NetCashFlow` | $10,850.00 | $10,850.00 | $0.00 | ✓ |

## Coverage Gap — Concepts Charlie Publishes We Don't Yet Surface

Charlie's instance contains **43** additional non-zero current-period concept(s) that our pipeline doesn't emit. **This is incompleteness, not incorrectness** — every concept we surface matches Charlie's value to the cent. These represent the gap between our current render and full Record-to-Report parity; each is bucketed below by the architectural lever that would close it.

### Subtotals our pipeline computes but doesn't emit as named facts (13)

**Low effort** — every input is already in our facts; walking the mini calc linkbase and emitting intermediate sums as named facts is a renderer change.

| Concept | Period | Charlie's value |
|---|---|---:|
| `mini:CurrentAssets` | instant | $13,550.00 |
| `mini:CurrentLiabilities` | instant | $1,400.00 |
| `mini:Equity` | instant | $12,050.00 |
| `mini:GrossProfitLoss` | duration | $2,700.00 |
| `mini:IncomeLossFromContinuingOperationsBeforeTax` | duration | $2,450.00 |
| `mini:Liabilities` | instant | $2,400.00 |
| `mini:NetCashFlowFinancingActivities` | duration | $10,850.00 |
| `mini:NetCashFlowInvestingActivities` | duration | $(1,000.00) |
| `mini:NetCashFlowOperatingActivities` | duration | $1,000.00 |
| `mini:NoncurrentAssets` | instant | $900.00 |
| `mini:NoncurrentLiabilities` | instant | $1,000.00 |
| `mini:OperatingExpenses` | duration | $100.00 |
| `mini:OperatingIncomeLoss` | duration | $2,600.00 |

### Hierarchical leaf splits we don't model in our CoA (8)

**Medium effort** — requires CoA element splits (e.g. gross PPE + AccumulatedDepreciation as separate accounts). Customer-facing accounting design decision, not just a renderer fix.

| Concept | Period | Charlie's value |
|---|---|---:|
| `mini:AccumulatedDepreciation` | instant | $100.00 |
| `mini:Cash` | instant | $10,850.00 |
| `mini:Equipment` | instant | $1,000.00 |
| `mini:MaturesInOneYear` | instant | $1,000.00 |
| `mini:OtherSecuredLoans` | instant | $1,000.00 |
| `mini:PropertyPlantAndEquipmentGross` | instant | $1,000.00 |
| `mini:RawMaterial` | instant | $2,700.00 |
| `mini:TradePayables` | instant | $1,000.00 |

### Flow concepts (TDC values) Charlie publishes as standalone facts (22)

**Medium-high effort** — architectural: our model stores TDC on ``LineItem.metadata['transaction_description_code']``; Charlie's model emits each TDC as a published fact. Closes with a materialize-time aggregator that groups by TDC per period.

| Concept | Period | Charlie's value |
|---|---|---:|
| `mini:AdditionalLongtermBorrowings` | duration | $2,000.00 |
| `mini:CapitalAdditionsPropertyPlantAndEquipment` | duration | $1,000.00 |
| `mini:CollectionOfReceivables` | duration | $8,000.00 |
| `mini:DecreaseFromDepreciationAndAmortization` | duration | $100.00 |
| `mini:DecreaseFromPaymentAccountsPayable` | duration | $7,000.00 |
| `mini:DecreaseFromPaymentOfInterest` | duration | $150.00 |
| `mini:DecreaseInInventoriesFromSales` | duration | $2,000.00 |
| `mini:IncreaseInReceivablesFromSalesOnAccount` | duration | $8,000.00 |
| `mini:InterestAccrued` | duration | $550.00 |
| `mini:InventoryWrittenOff` | duration | $300.00 |
| `mini:InvestmentsByOwner` | duration | $10,000.00 |
| `mini:NonoperatingIncomeExpenses` | duration | $(150.00) |
| `mini:PaymentForCapitalAdditionsOfPropertyPlantEquipment` | duration | $1,000.00 |
| `mini:PaymentForReductionOfLongtermBorrowings` | duration | $1,000.00 |
| `mini:PaymentInterest` | duration | $150.00 |
| `mini:PaymentOfAccountsPayable` | duration | $7,000.00 |
| `mini:ProceedsFromAdditionalLongtermBorrowings` | duration | $2,000.00 |
| `mini:ProceedsFromCollectionOfReceivables` | duration | $8,000.00 |
| `mini:ProceedsFromInvestmentsByOwner` | duration | $10,000.00 |
| `mini:PurchasesInventoryForSaleOnAccount` | duration | $8,000.00 |
| `mini:PurchasesOfInventoryForSale` | duration | $5,000.00 |
| `mini:RepaymentLongtermBorrowings` | duration | $1,000.00 |

## Four Anchor Totals

Methodology spec §4.6 exit criterion: these four lines must match Charlie's PoC for the test to pass. All amounts are debit-positive cents internally; presentation flips signs per accounting convention.

| Anchor | Our value |
|---|---:|
| Total Assets | $14,450.00 |
| Total Liabilities & Equity | $14,450.00 |
| Net Income | $2,050.00 |
| Net Cash Change | $10,850.00 |

## Concept-Level Period Totals

Every mini concept with non-zero activity in the period. ``Δ debit-positive`` is the period flow (Σ DR − Σ CR). For instant/asset concepts this equals the period-ending balance (Charlie's data starts from zero). For duration concepts this is the period income/expense.

| QName | Label | Trait | Period | Δ debit-positive |
|---|---|---|---|---:|
| `mini:AccountsPayable` | Accounts Payable | liability | instant | $(1,000.00) |
| `mini:AccruedExpenses` | Accrued Expenses | liability | instant | $(400.00) |
| `mini:CashAndCashEquivalents` | Cash and Cash Equivalents | asset | instant | $10,850.00 |
| `mini:CostsOfSales` | Costs of Sales | expense | duration | $5,300.00 |
| `mini:DepreciationAndAmortization` | Depreciation and Amortization | expense | duration | $100.00 |
| `mini:IncomeTaxExpenseBenefit` | Income Tax Expense (Benefit) | expense | duration | $400.00 |
| `mini:InterestExpense` | Interest Expense | expense | duration | $150.00 |
| `mini:Inventories` | Inventories | asset | instant | $2,700.00 |
| `mini:LongtermDebt` | Long-term Debt | liability | instant | $(1,000.00) |
| `mini:PaidInCapital` | Paid In Capital | equity | instant | $(10,000.00) |
| `mini:PropertyPlantAndEquipment` | Property, Plant and Equipment | asset | instant | $900.00 |
| `mini:RetainedEarnings` | Retained Earnings | equity | instant | $(2,050.00) |
| `mini:Sales` | Sales | revenue | duration | $(8,000.00) |

## Rollforward Attribution (Phase 2 MVP Filter Engine)

Each rollforward IB decomposes its BS source's period delta across declared TDC filters. Where ``Σ filters == Δ BS``, the rollforward is balanced (residual = 0). A non-zero residual indicates either an unattributed flow or a phantom TDC in the source data (logged at author time).

### Accounts Payable (mini:AccountsPayable)

**Δ BS** (debit-positive): $(1,000.00)

| Flow concept | Value | Matched lines | Event ids |
|---|---:|---:|---|
| `mini:PurchasesInventoryForSaleOnAccount` | $(8,000.00) | 2 | evt_01KSDRPZFX4TYDDJ652J3XVJGG, evt_01KSDRPZGGA1B9JD53MTP9QTFE |
| `mini:DecreaseFromPaymentAccountsPayable` | $7,000.00 | 1 | evt_01KSDRPZJJ822PKMV2XM5NZNN4 |

### Accrued Expenses (mini:AccruedExpenses)

**Δ BS** (debit-positive): $(400.00)

| Flow concept | Value | Matched lines | Event ids |
|---|---:|---:|---|
| `mini:InterestAccrued` | $(550.00) | 2 | evt_01KSDRPZKQ22QZN204AP6W5X43, evt_01KSDRPZNSHDRXD30C5GQ5F9MS |
| `mini:DecreaseFromPaymentOfInterest` | $150.00 | 1 | evt_01KSDRPZK2HV2JX8CNG6363CCG |

### Cash and Cash Equivalents (mini:CashAndCashEquivalents)

**Δ BS** (debit-positive): $10,850.00

| Flow concept | Value | Matched lines | Event ids |
|---|---:|---:|---|
| `mini:ProceedsFromInvestmentsByOwner` | $10,000.00 | 1 | evt_01KSDRPZD2ZZFPMYKBKS45FRR3 |
| `mini:ProceedsFromAdditionalLongtermBorrowings` | $2,000.00 | 1 | evt_01KSDRPZED6JBZ14DSVCYX8C7D |
| `mini:PaymentForCapitalAdditionsOfPropertyPlantEquipment` | $(1,000.00) | 1 | evt_01KSDRPZF2AB3GF3KGETH1TR0B |
| `mini:PaymentInterest` | $(150.00) | 1 | evt_01KSDRPZK2HV2JX8CNG6363CCG |
| `mini:ProceedsFromCollectionOfReceivables` | $8,000.00 | 1 | evt_01KSDRPZJ05WRQXNBQ7A4SD4YX |
| `mini:PaymentOfAccountsPayable` | $(7,000.00) | 1 | evt_01KSDRPZJJ822PKMV2XM5NZNN4 |
| `mini:PaymentForReductionOfLongtermBorrowings` | $(1,000.00) | 1 | evt_01KSDRPZK2HV2JX8CNG6363CCG |

### Inventories (mini:Inventories)

**Δ BS** (debit-positive): $2,700.00

| Flow concept | Value | Matched lines | Event ids |
|---|---:|---:|---|
| `mini:InventoryWrittenOff` | $(300.00) | 1 | evt_01KSDRPZM7D7GGZVM49DHESX6G |
| `mini:PurchasesOfInventoryForSale` | $5,000.00 | 1 | evt_01KSDRPZFX4TYDDJ652J3XVJGG |
| `mini:DecreaseInInventoriesFromSales` | $(2,000.00) | 1 | evt_01KSDRPZH8ARYNTK2Z8CK5G6HQ |

### Long-term Debt (mini:LongtermDebt)

**Δ BS** (debit-positive): $(1,000.00)

| Flow concept | Value | Matched lines | Event ids |
|---|---:|---:|---|
| `mini:AdditionalLongtermBorrowings` | $(2,000.00) | 1 | evt_01KSDRPZED6JBZ14DSVCYX8C7D |
| `mini:RepaymentLongtermBorrowings` | $1,000.00 | 1 | evt_01KSDRPZK2HV2JX8CNG6363CCG |

### Paid In Capital (mini:PaidInCapital)

**Δ BS** (debit-positive): $(10,000.00)

| Flow concept | Value | Matched lines | Event ids |
|---|---:|---:|---|
| `mini:InvestmentsByOwner` | $(10,000.00) | 1 | evt_01KSDRPZD2ZZFPMYKBKS45FRR3 |

### Property, Plant and Equipment (mini:PropertyPlantAndEquipment)

**Δ BS** (debit-positive): $900.00

| Flow concept | Value | Matched lines | Event ids |
|---|---:|---:|---|
| `mini:CapitalAdditionsPropertyPlantAndEquipment` | $1,000.00 | 1 | evt_01KSDRPZF2AB3GF3KGETH1TR0B |
| `mini:DecreaseFromDepreciationAndAmortization` | $(100.00) | 1 | evt_01KSDRPZMVTCEWHNKK33TKP2ZC |

### Receivables (mini:Receivables)

**Δ BS** (debit-positive): $0.00

| Flow concept | Value | Matched lines | Event ids |
|---|---:|---:|---|
| `mini:IncreaseInReceivablesFromSalesOnAccount` | $8,000.00 | 1 | evt_01KSDRPZH8ARYNTK2Z8CK5G6HQ |
| `mini:CollectionOfReceivables` | $(8,000.00) | 1 | evt_01KSDRPZJ05WRQXNBQ7A4SD4YX |

## Findings — Classification per Methodology §3.2

**Their data quality** (source CSV inconsistencies):

- **JE-205** — Description "Payment for contractor" but TDC on the AP line is `mini:PurchasesInventoryForSaleOnAccount`. Contractor services aren't inventory; vocabulary misuse.
- **JE-209** — TDC `mini:PaymentOfInterest` on the Cash line is a typo for `mini:PaymentInterest` (the canonical mini.xsd concept name). `ingest_transactions.py::_KNOWN_TDC_ALIASES` normalizes it at ingest time so the rollforward filter engine matches; logged as a warning per JE so the substitution stays transparent. Note that `mini:DecreaseFromPaymentOfInterest` (the AccruedExpenses-side TDC) keeps the "Of" — Charlie's naming is internally inconsistent.
- **JE-226** — Income tax accrual ($400) but TDC is `mini:InterestAccrued` instead of `IncomeTaxAccrued`. Copy-paste-style bug from the JE-210 interest pattern.

**Methodology gap** (architecturally aligned, semantically distinct):

- **JE-225** — "Write off of PPE" with `Amount = 0` on both lines. Boundary test case. Our GL handler rejects nil-amount entries (`must have non-zero D or C`); Charlie's system likely creates `$0` facts. Reconciliation delta is `$0` either way; the four anchor totals are unaffected.
- **rs-gaap library subset** — Two flow concepts in `mappings.py` don't exist in our currently-loaded rs-gaap library: `rs-gaap:InterestPaidNet` (mapped through to the closest available `rs-gaap:InterestExpense`) and `rs-gaap:StockIssuedDuringPeriodValueNewIssues` (mapped through to `rs-gaap:ProceedsFromIssuanceOfCommonStock`). Approximation; future library expansion closes the gap.

**Our bug**: none identified.

**Matching**: see Anchor Totals table above + line-by-line concept totals. Compare manually against Charlie's PoC rendering at the expected-output URL — automated HTML diff is a forward-queue enhancement (methodology §3.1 step 5 stretch goal).

---

*Reconciliation produced by `examples/seattle_method_demo/reconcile.py` against the Phase 2 MVP rollforward filter engine. See `examples/seattle_method_demo/README.md` for the full methodology and `local/docs/specs/cross-taxonomy-projection.md` for the architectural pattern this test validates.*
