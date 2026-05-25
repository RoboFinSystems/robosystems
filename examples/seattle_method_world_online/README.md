# The World Online — Seattle Method Demo (MINI 2026, at scale)

End-to-end exercise of the **cross-taxonomy projection** methodology on
Charlie Hoffman's [_The World
Online_](https://github.com/seattlemethod/prototypes/tree/main/the-world-online-demo-data)
dataset — a realistic mid-size company with **22,288 general-ledger
lines** across **3,389 journal entries**, a real **239-account chart of
accounts**, and **opening balances**, all tagged against the **MINI
2026** reporting framework.

This is the scaled-up sibling of
[`examples/seattle_method_demo`](../seattle_method_demo/README.md) (Test
Case 1 — Charlie's 14-JE lemonade stand). The methodology is identical;
the dataset is the difference. The two demos share the generic steps
(`load_taxonomy`, `seed_mappings`, the report-rendering helpers) by
import, and diverge on the input pipeline (GL format + CoA layer +
opening balances + scale), the rollforward authoring, and the
reconciliation reference.

## Quick Start

```bash
just start          # stack up
just demo-user      # create/reuse demo credentials (.local/config.json)

# Full pipeline: pull, provision, load MINI 2026, seed mappings,
# ingest 3,389 entries, author rollforwards, reconcile, create-report.
# The 3,389-entry ingest is the intended per-entry path and takes a few
# minutes — there is a live progress readout.
just demo-world-online

# Smoke-test the whole pipeline on a 50-entry subset first
just demo-world-online --limit 50

# Single step (after provisioning)
just demo-world-online --step ingest --graph <graph_id>

# Re-run just the artifacts against an existing graph
just demo-world-online-reconcile <graph_id>
just demo-world-online-create-report <graph_id>
```

## What Gets Created

| Step | Artifact |
| --- | --- |
| `pull` | `local/taxonomies/mini-2026/` (MINI 2026 base taxonomy) + `local/datasets/seattle_method_world_online/` (GeneralLedger.csv, ChartOfAccounts.csv, SummaryOfTransactions.csv) — both gitignored, re-fetched on demand |
| `provision` | A dedicated test graph (slot `world_online_test` in `.local/config.json`) — isolated from the lemonade-stand demo |
| `load` | ~239 MINI 2026 monetary concepts as Elements + the `mini:OpeningBalance` **extension** concept (see below) |
| `seed-mappings` | mini→rs-gaap derivation Associations (shared table; the World Online run additionally exercises `mini:SalesGeneralAndAdministrativeExpenses`) |
| `ingest` | ~3,389 Events / Entries, 22,288 LineItems — each line's GL account collapsed to its mini concept, business-event tag stamped on `flow_element_id`, GL account + provenance preserved on metadata |
| `author-rollforwards` | 7 rollforward IBs (Cash, Receivables, Inventories, PP&E, AP, LongtermDebt, PaidInCapital — no AccruedExpenses in this dataset) |
| `reconcile` | `output/world-online-reconciliation.md` — the `(line-item × business-event)` pivot reproduced from the graph and reconciled cell-by-cell against `SummaryOfTransactions.csv` |
| `create-report` | `output/world-online-four-statements.md` — the rs-gaap 4-statement Report (BS / IS / CF / SE) |

## Opening balances (the load-bearing design decision)

The 98 `BBF` ("beginning balance forward") lines, dated 12/31/2023 and
tagged `mini:OpeningBalance`, are **ingested as ordinary transactions**
— `Transaction → Entry → LineItem`, exactly like the other 22,190 lines.
They are not synthesized as a separate opening number outside the
pipeline.

For an opening line to **render as a fact** (attribute in the rollforward
and reconcile against Charlie's `mini:OpeningBalance` pivot row), the
rollforward filter engine must match it — and that engine matches on
`LineItem.flow_element_id`, the FK populated at ingest by resolving the
line's business-event qname to a loaded Element. `mini:OpeningBalance`
is **not** in the MINI 2026 taxonomy (Charlie flags this), so the load
step adds it as a small **extension concept**. Without it, the opening
lines would land with `flow_element_id = NULL` and silently drop out of
the attribution.

The rollforward period spans the opening date, so the opening is a
genuine **$0 genesis** and `mini:OpeningBalance` is simply the first
business event in the decomposition:

```
0 (genesis) + OpeningBalance + Σ(flow events) = ending balance   (residual 0)
```

This is also a clean answer to Charlie's open "how do I handle opening
balances long-term" question: treat opening balance as a first-class
business-event/flow concept the framework recognizes — ingested as
transactions, rendered as facts.

## Sign convention

The GL's `amount` column is already **debit-positive** signed (verified
against `SummaryOfTransactions.csv`: asset openings positive,
liability/equity openings negative; the `D`/`C` indicator agrees with
the sign on every row). So `debit_amount − credit_amount = round(amount
× 100)` and the reconcile is a direct comparison with no sign flipping.
The GL balances to exactly **$0.00** across all accounts; Charlie's
published summary nets to **$0.50** — accumulated per-cell rounding in
the summary, classified _Their data quality_, not a posting imbalance.

## Reconciliation Classification

Every cell delta is classified into one of four categories — the
classification is what makes the report actionable:

| Category | Definition | Owner |
| --- | --- | --- |
| **Matching** | Graph equals expected (exact, or within the published summary's per-cell rounding). | — |
| **Methodology gap** | Architectural feature not yet shipped. | RoboSystems (forward queue) |
| **Our bug** | Implementation error in shipped code. | RoboSystems (fix) |
| **Their data quality** | Source data tagging / rounding issue, not a pipeline defect. | Source author |

## Cash flow statement: operating-only (a dataset finding)

The four-statement report's cash-flow statement renders the **operating
section** (Net Income + D&A add-back + the AR / Inventory / AP
working-capital changes) and foots to the operating subtotal. The
report is generated with `comparative=True` — the indirect method
derives the working-capital changes from period-over-period balance-sheet
deltas, so a prior period is required (the 12/31/2023 opening balances
land in that prior period, which is correct: the opening is a position,
not a flow).

It does **not** render a financing or investing section, and the net
cash change therefore does not tie to the actual cash movement (it is
short by ~$23K — the net long-term-debt repayment). This is a
**characteristic of the World Online dataset, not a platform limit**:

- The platform emits investing/financing CF facts from flow concepts
  that **hit the cash line** (`fact_grid.py` — a financing flow must move
  cash to be a financing cash flow).
- World Online tags its long-term-debt borrowings/repayments on the
  **LongtermDebt line** (`mini:AdditionalLongtermBorrowings` /
  `mini:RepaymentLongtermBorrowings`), and the cash side of those entries
  is tagged with an **operating** concept (`mini:PaymentOfAccountsPayable`)
  or doesn't touch cash at all (many are LTD-only reclassifications).
  There are no cash-side financing/investing tags anywhere in the dataset.

So the indirect CF cannot reconstruct a financing section from this
tagging. The activity is not lost — it is fully captured in the
**LongtermDebt rollforward IB** and the reconciliation pivot
(`mini:LongtermDebt × mini:{Additional,Repayment}LongtermBorrowings`).
This is exactly the kind of source-tagging gap the cross-taxonomy
projection is meant to surface (a "Their data quality" finding for the
CF presentation). Contrast Test Case 1, whose borrowing entries tag the
cash line with a financing concept and therefore render a full
financing section.

## Out of Scope (Charlie's wishlist — forward work)

Charlie's dataset README lists ten things a full implementation could
show. This demo delivers the core (ingest → rollforwards → 4 statements
→ the business-event summary, which is the reconciliation pivot). The
rest are noted here as forward work, not built:

- **Trial balance** (item 9) — MINI 2026 ships a TrialBalance support
  network; a `trialBalance` GraphQL query already exists. A dedicated
  trial-balance artifact is a small follow-on.
- **Policies & text disclosures** (item 2) — appended by a separate
  process; not derivable from transactions.
- **Subclassifications** (item 3) — we collapse GL accounts to the
  lowest-level mini line item per Charlie's own guidance; the 239-account
  detail is preserved on `LineItem.metadata` but not rendered as
  subclassification disclosures.
- **Traceability / provenance** (items 4–5) — `transactionId`,
  `enteredBy`, `enteredDateTime` are captured on metadata; surfacing them
  as navigable lineage is future work.
- **LLM-enabled audit, interconnections, subsidiary ledgers,
  verification** (items 6–8, 10) — future.

The methodology is the durable artifact; specific test cases are
scheduled by external forcing functions.

## Period note

The GL runs from the 12/31/2023 opening through activity in 2028, and
`SummaryOfTransactions.csv` aggregates the entire dataset, so the
four-statement report is the **cumulative** position (BS as of period
end, flows over the whole span). Charlie's reference report
([`index2.html`](https://xbrlsite.azurewebsites.net/2026/reporting-framework/mini/ref-num/index2.html))
does not publicly expose its exact period; if it turns out to be a single
fiscal year, narrow `PERIOD_START`/`PERIOD_END` in `create_report.py`.
The reconciliation (the primary artifact) is period-independent — it sums
every posted line.
