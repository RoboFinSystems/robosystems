# Seattle Method Cross-Taxonomy Demo

End-to-end exercise of the **cross-taxonomy projection** methodology
(Test Case 1 in [`METHODOLOGY.md`](METHODOLOGY.md)) using
Charlie Hoffman's *Seattle Method* `mini` reporting framework and his
published 14-transaction Q1 2024 lemonade-stand dataset.

The demo proves that the RoboSystems three-block architecture
(TaxonomyBlock + EventBlock + InformationBlock) can ingest an external
XBRL reporting taxonomy, ingest transactions tagged with that
taxonomy's flow concepts, decompose period changes via filter-based
attribution (the `rollforward` block-type, Phase 2 MVP), and render
the result in both the source vocabulary AND our canonical `rs-gaap`
vocabulary from the same fact set.

The reconciliation report — comparing our output to Charlie's
expected output at
[`luca.pacioli.ai/luca/view/0f24fd35…`](https://luca.pacioli.ai/luca/view/0f24fd35e961e167a727b663c75a4c5ec9fb7eb86730d6292f46e6e180fc2018980cd52e/index) —
is the primary external-facing artifact.

## Quick Start

```bash
# Make sure the stack is running
just start

# Run the demo end-to-end (provisions test graph, pulls mini, ingests
# the 14 JEs, authors rollforwards, renders, reconciles)
just demo-seattle-method

# Or run a single step
just demo-seattle-method --step pull
just demo-seattle-method --step load
just demo-seattle-method --step seed-mappings
just demo-seattle-method --step ingest
just demo-seattle-method --step author-rollforwards

# Reconcile (not a --step of the orchestrator; runs separately
# because it consumes the graph the orchestrator just built).
just demo-seattle-method-reconcile <graph_id>
```

## What Gets Created

| Step | Artifact |
|---|---|
| `pull` | `local/taxonomies/mini/` — 30 XBRL artifacts curled from `xbrlsite.azurewebsites.net` (gitignored — re-fetched on demand) |
| `provision` | A dedicated test graph (e.g. `kg_seattle_method_<timestamp>`) — isolated from real customer data |
| `load` | 239 mini concepts as Elements + presentation/calculation/definition/formula linkbase arcs as Associations |
| `seed-mappings` | ~36 mini→rs-gaap derivation Associations (BS/IS/CF/SE leaves touched by the 14-JE dataset) |
| `ingest` | 14 Events (one per JournalEntryID), 14 Entries, ~32 LineItems — each LineItem.metadata_['transaction_description_code'] stamped from CSV |
| `author-rollforwards` | 8 rollforward IBs (one per BS leaf with activity) — Cash, Receivables, Inventories, PP&E, AP, Accrued, LTD, PaidInCapital |
| `reconcile` | `local/reports/seattle-method-case-1.md` — line-by-line comparison of our output vs. Charlie's PoC, classified per the methodology spec §3.2 |

## Fixtures

- **`fixtures/transactions.csv`** — Charlie's 14-JE lemonade-stand dataset (JE-201 through JE-226, Q1 2024, single entity). Committed; sourced from Charlie Hoffman directly on 2026-05-19.

The mini taxonomy artifacts themselves are NOT committed — `pull_mini.sh` fetches them on demand from `xbrlsite.azurewebsites.net` into `local/taxonomies/mini/` (gitignored). This keeps the repo small and lets us pick up upstream taxonomy changes without re-vendoring.

## Known Data-Quality Findings (Pre-Reconciliation)

These are identified during input review on 2026-05-19, NOT
introduced by the reconciliation. They become part of the report's
"Their data quality" section per the methodology classification.

| JE | Issue |
|---|---|
| **JE-205** | Description "Payment for contractor"; TDC on the AP line is `mini:PurchasesInventoryForSaleOnAccount` — but contractor services aren't inventory. Vocabulary misuse. |
| **JE-209** | TDC `mini:PaymentOfInterest` on the Cash line was a typo — mini.xsd's canonical concept is `mini:PaymentInterest` (no "Of"). Fixed at source in `fixtures/transactions.csv`; note that the sibling concept `mini:DecreaseFromPaymentOfInterest` on the AccruedExpenses line *does* keep the "Of" (Charlie's own naming is internally inconsistent). |
| **JE-225** | Boundary test case: "Write off of PPE" with `Amount = 0` on both lines — an entry with no economic substance. Our GL handler correctly rejects it (`must have non-zero D or C`); Charlie's system likely creates $0 facts. The reconciliation delta is $0 either way (no economic activity to attribute), so the four anchor totals are unaffected. Classified as **Methodology gap** (neither side has a bug — both correctly handle a nil entry under their respective semantics). |
| **JE-226** | Income tax accrual ($400). TDC on the AccruedExpenses line is `mini:InterestAccrued` — should be `IncomeTaxAccrued`. Copy-paste-style bug from the JE-210 interest accrual pattern. |

## Methodology

See [`METHODOLOGY.md`](METHODOLOGY.md) for the full methodology spec —
the 5-step pipeline, reconciliation report format, finding
classification rubric (matching / methodology gap / our bug / their
data quality), and forward queue of future test cases.

## Architectural Dependencies

- **Phase 2 MVP rollforward block-type** — `block_type='rollforward'`,
  `RollforwardMechanics` Pydantic, single-predicate filter engine
  (`line_item_metadata_field`). See
  `robosystems/operations/information_block/rollforward.py` and
  `robosystems/operations/roboledger/reports/rollforward_filters.py`.
- **Per-line metadata plumbing** —
  `JournalEntryLineItemInput.metadata` flows through to
  `LineItem.metadata_`, where the filter engine reads it.
- **TaxonomyBlock + Element + Association infrastructure** — used as-is
  (this demo authors per-tenant, not library-scoped).

## Out of Scope

- Phase 2.5 enrichment engine (the rules + Operator + manual layer
  that populates `LineItem.metadata_['transaction_description_code']`
  for legacy QB data without flow tags). Charlie's data ships with
  flow tags pre-stamped; we use them as-is. For real QB data the
  enrichment layer is the load-bearing piece.
- Manual attribution (Tier 3) — this dataset is fully
  transaction-attributable.
- Test Cases 1.1 (1-transaction), 1.2 (22k-transaction), 2 (SEC us-gaap),
  3 (IFRS), 4 (FDTA) — queued in [`METHODOLOGY.md`](METHODOLOGY.md) §5.
