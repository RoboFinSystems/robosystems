# Seattle Method Cross-Taxonomy — Test Case 1 Reconciliation

**Graph**: `kg19e468f7c281578a351d`
**Period**: 2024-01-01 → 2024-01-31
**Dataset**: Charlie Hoffman's lemonade-stand 14-JE Q1 2024 fixture
**Expected output reference**: [luca.pacioli.ai/luca/view/0f24fd35…](https://luca.pacioli.ai/luca/view/0f24fd35e961e167a727b663c75a4c5ec9fb7eb86730d6292f46e6e180fc2018980cd52e/index)

---

## Automated Diff vs. Charlie's Published Facts

Compared **17** concept(s) against Charlie's luca.pacioli.ai export (`fixtures/expected_facts_mini.csv`). **17 exact match** • **0 delta**. Total absolute delta: **$0.00**.

| Concept                            |  Our value | Charlie's value |     Δ |     |
| ---------------------------------- | ---------: | --------------: | ----: | --- |
| `mini:AccountsPayable`             |  $1,000.00 |       $1,000.00 | $0.00 | ✓   |
| `mini:AccruedExpenses`             |    $400.00 |         $400.00 | $0.00 | ✓   |
| `mini:CashAndCashEquivalents`      | $10,850.00 |      $10,850.00 | $0.00 | ✓   |
| `mini:CostsOfSales`                |  $5,300.00 |       $5,300.00 | $0.00 | ✓   |
| `mini:DepreciationAndAmortization` |    $100.00 |         $100.00 | $0.00 | ✓   |
| `mini:IncomeTaxExpenseBenefit`     |    $400.00 |         $400.00 | $0.00 | ✓   |
| `mini:InterestExpense`             |    $150.00 |         $150.00 | $0.00 | ✓   |
| `mini:Inventories`                 |  $2,700.00 |       $2,700.00 | $0.00 | ✓   |
| `mini:LongtermDebt`                |  $1,000.00 |       $1,000.00 | $0.00 | ✓   |
| `mini:PaidInCapital`               | $10,000.00 |      $10,000.00 | $0.00 | ✓   |
| `mini:PropertyPlantAndEquipment`   |    $900.00 |         $900.00 | $0.00 | ✓   |
| `mini:Receivables`                 |      $0.00 |           $0.00 | $0.00 | ✓   |
| `mini:Sales`                       |  $8,000.00 |       $8,000.00 | $0.00 | ✓   |
| `mini:Assets`                      | $14,450.00 |      $14,450.00 | $0.00 | ✓   |
| `mini:LiabilitiesAndEquity`        | $14,450.00 |      $14,450.00 | $0.00 | ✓   |
| `mini:NetIncomeLoss`               |  $2,050.00 |       $2,050.00 | $0.00 | ✓   |
| `mini:NetCashFlow`                 | $10,850.00 |      $10,850.00 | $0.00 | ✓   |

## Four Anchor Totals

Methodology spec §4.6 exit criterion: these four lines must match Charlie's PoC for the test to pass. All amounts are debit-positive cents internally; presentation flips signs per accounting convention.

| Anchor                     |  Our value |
| -------------------------- | ---------: |
| Total Assets               | $14,450.00 |
| Total Liabilities & Equity | $14,450.00 |
| Net Income                 |  $2,050.00 |
| Net Cash Change            | $10,850.00 |

## Concept-Level Period Totals

Every mini concept with non-zero activity in the period. `Δ debit-positive` is the period flow (Σ DR − Σ CR). For instant/asset concepts this equals the period-ending balance (Charlie's data starts from zero). For duration concepts this is the period income/expense.

| QName                              | Label                         | Trait     | Period   | Δ debit-positive |
| ---------------------------------- | ----------------------------- | --------- | -------- | ---------------: |
| `mini:AccountsPayable`             | Accounts Payable              | liability | instant  |      $(1,000.00) |
| `mini:AccruedExpenses`             | Accrued Expenses              | liability | instant  |        $(400.00) |
| `mini:CashAndCashEquivalents`      | Cash and Cash Equivalents     | asset     | instant  |       $10,850.00 |
| `mini:CostsOfSales`                | Costs of Sales                | expense   | duration |        $5,300.00 |
| `mini:DepreciationAndAmortization` | Depreciation and Amortization | expense   | duration |          $100.00 |
| `mini:IncomeTaxExpenseBenefit`     | Income Tax Expense (Benefit)  | expense   | duration |          $400.00 |
| `mini:InterestExpense`             | Interest Expense              | expense   | duration |          $150.00 |
| `mini:Inventories`                 | Inventories                   | asset     | instant  |        $2,700.00 |
| `mini:LongtermDebt`                | Long-term Debt                | liability | instant  |      $(1,000.00) |
| `mini:PaidInCapital`               | Paid In Capital               | equity    | instant  |     $(10,000.00) |
| `mini:PropertyPlantAndEquipment`   | Property, Plant and Equipment | asset     | instant  |          $900.00 |
| `mini:Sales`                       | Sales                         | revenue   | duration |      $(8,000.00) |

## Rollforward Attribution (Phase 2 MVP Filter Engine)

Each rollforward IB decomposes its BS source's period delta across declared TDC filters. Where `Σ filters == Δ BS`, the rollforward is balanced (residual = 0). A non-zero residual indicates either an unattributed flow or a phantom TDC in the source data (logged at author time).

### Accounts Payable (mini:AccountsPayable)

**Δ BS** (debit-positive): $(1,000.00)

| Flow concept                              |       Value | Matched lines | Event ids                                                      |
| ----------------------------------------- | ----------: | ------------: | -------------------------------------------------------------- |
| `mini:PurchasesInventoryForSaleOnAccount` | $(8,000.00) |             2 | evt_01KS38Z3CVEQ4SXXYG1GR7G4G3, evt_01KS38Z3D85T6VJ54DQTWKQWFM |
| `mini:DecreaseFromPaymentAccountsPayable` |   $7,000.00 |             1 | evt_01KS38Z3EKC1ANPWFNXC5CBCM3                                 |

### Accrued Expenses (mini:AccruedExpenses)

**Δ BS** (debit-positive): $(400.00)

| Flow concept                         |     Value | Matched lines | Event ids                                                      |
| ------------------------------------ | --------: | ------------: | -------------------------------------------------------------- |
| `mini:InterestAccrued`               | $(550.00) |             2 | evt_01KS38Z3FFD7T2NYKC3DPVW7WE, evt_01KS38Z3H4ZBVHQ69RFNH8MEWN |
| `mini:DecreaseFromPaymentOfInterest` |   $150.00 |             1 | evt_01KS38Z3F02WDVKC42Z5NDMQWB                                 |

### Cash and Cash Equivalents (mini:CashAndCashEquivalents)

**Δ BS** (debit-positive): $10,850.00

| Flow concept                                              |       Value | Matched lines | Event ids                      |
| --------------------------------------------------------- | ----------: | ------------: | ------------------------------ |
| `mini:ProceedsFromInvestmentsByOwner`                     |  $10,000.00 |             1 | evt_01KS38Z3AQZ6RG3W208KJA658Y |
| `mini:ProceedsFromAdditionalLongtermBorrowings`           |   $2,000.00 |             1 | evt_01KS38Z3BQ47YK0469Z2G7T413 |
| `mini:PaymentForCapitalAdditionsOfPropertyPlantEquipment` | $(1,000.00) |             1 | evt_01KS38Z3C99YC78W3EPXW1CABA |
| `mini:PaymentInterest`                                    |   $(150.00) |             1 | evt_01KS38Z3F02WDVKC42Z5NDMQWB |
| `mini:ProceedsFromCollectionOfReceivables`                |   $8,000.00 |             1 | evt_01KS38Z3E6W5BWVDS70S6VQZTP |
| `mini:PaymentOfAccountsPayable`                           | $(7,000.00) |             1 | evt_01KS38Z3EKC1ANPWFNXC5CBCM3 |
| `mini:PaymentForReductionOfLongtermBorrowings`            | $(1,000.00) |             1 | evt_01KS38Z3F02WDVKC42Z5NDMQWB |

### Inventories (mini:Inventories)

**Δ BS** (debit-positive): $2,700.00

| Flow concept                          |       Value | Matched lines | Event ids                      |
| ------------------------------------- | ----------: | ------------: | ------------------------------ |
| `mini:InventoryWrittenOff`            |   $(300.00) |             1 | evt_01KS38Z3G0GN1Y38K1NVJE722N |
| `mini:PurchasesOfInventoryForSale`    |   $5,000.00 |             1 | evt_01KS38Z3CVEQ4SXXYG1GR7G4G3 |
| `mini:DecreaseInInventoriesFromSales` | $(2,000.00) |             1 | evt_01KS38Z3DQBV8YHX9JPGJ8KFNR |

### Long-term Debt (mini:LongtermDebt)

**Δ BS** (debit-positive): $(1,000.00)

| Flow concept                        |       Value | Matched lines | Event ids                      |
| ----------------------------------- | ----------: | ------------: | ------------------------------ |
| `mini:AdditionalLongtermBorrowings` | $(2,000.00) |             1 | evt_01KS38Z3BQ47YK0469Z2G7T413 |
| `mini:RepaymentLongtermBorrowings`  |   $1,000.00 |             1 | evt_01KS38Z3F02WDVKC42Z5NDMQWB |

### Paid In Capital (mini:PaidInCapital)

**Δ BS** (debit-positive): $(10,000.00)

| Flow concept              |        Value | Matched lines | Event ids                      |
| ------------------------- | -----------: | ------------: | ------------------------------ |
| `mini:InvestmentsByOwner` | $(10,000.00) |             1 | evt_01KS38Z3AQZ6RG3W208KJA658Y |

### Property, Plant and Equipment (mini:PropertyPlantAndEquipment)

**Δ BS** (debit-positive): $900.00

| Flow concept                                     |     Value | Matched lines | Event ids                      |
| ------------------------------------------------ | --------: | ------------: | ------------------------------ |
| `mini:CapitalAdditionsPropertyPlantAndEquipment` | $1,000.00 |             1 | evt_01KS38Z3C99YC78W3EPXW1CABA |
| `mini:DecreaseFromDepreciationAndAmortization`   | $(100.00) |             1 | evt_01KS38Z3GDSFX5N77AKBN6RF2K |

### Receivables (mini:Receivables)

**Δ BS** (debit-positive): $0.00

| Flow concept                                   |       Value | Matched lines | Event ids                      |
| ---------------------------------------------- | ----------: | ------------: | ------------------------------ |
| `mini:IncreaseInReceivablesFromSalesOnAccount` |   $8,000.00 |             1 | evt_01KS38Z3DQBV8YHX9JPGJ8KFNR |
| `mini:CollectionOfReceivables`                 | $(8,000.00) |             1 | evt_01KS38Z3E6W5BWVDS70S6VQZTP |

## Findings — Classification per Methodology §3.2

**Their data quality** (source CSV inconsistencies):

- **JE-205** — Description "Payment for contractor" but TDC on the AP line is `mini:PurchasesInventoryForSaleOnAccount`. Contractor services aren't inventory; vocabulary misuse.
- **JE-209** — TDC `mini:PaymentOfInterest` was a typo for `mini:PaymentInterest` (the canonical mini.xsd concept name). Fixed at source in `fixtures/transactions.csv` prior to ingest. Note that `mini:DecreaseFromPaymentOfInterest` (the AccruedExpenses-side TDC) keeps the "Of" — Charlie's naming is internally inconsistent.
- **JE-226** — Income tax accrual ($400) but TDC is `mini:InterestAccrued` instead of `IncomeTaxAccrued`. Copy-paste-style bug from the JE-210 interest pattern.

**Methodology gap** (architecturally aligned, semantically distinct):

- **JE-225** — "Write off of PPE" with `Amount = 0` on both lines. Boundary test case. Our GL handler rejects nil-amount entries (`must have non-zero D or C`); Charlie's system likely creates `$0` facts. Reconciliation delta is `$0` either way; the four anchor totals are unaffected.
- **rs-gaap library subset** — Two flow concepts in `mappings.py` don't exist in our currently-loaded rs-gaap library: `rs-gaap:InterestPaidNet` (mapped through to the closest available `rs-gaap:InterestExpense`) and `rs-gaap:StockIssuedDuringPeriodValueNewIssues` (mapped through to `rs-gaap:ProceedsFromIssuanceOfCommonStock`). Approximation; future library expansion closes the gap.

**Our bug**: none identified.

**Matching**: see Anchor Totals table above + line-by-line concept totals. Compare manually against Charlie's PoC rendering at the expected-output URL — automated HTML diff is a forward-queue enhancement (methodology §3.1 step 5 stretch goal).

---

_Reconciliation produced by `examples/seattle_method_demo/reconcile.py` against the Phase 2 MVP rollforward filter engine. See `examples/seattle_method_demo/README.md` for the full methodology and `local/docs/specs/cross-taxonomy-projection.md` for the architectural pattern this test validates._
