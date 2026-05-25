# Seattle Method Cross-Taxonomy Demo

End-to-end exercise of the **cross-taxonomy projection** methodology
(Test Case 1) using Charlie Hoffman's _Seattle Method_ `mini`
reporting framework and his published 14-transaction Q1 2024
lemonade-stand dataset.

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

# Run the demo end-to-end. Provisions a fresh test graph, pulls the
# mini taxonomy, ingests the 14 JEs, authors rollforwards, runs the
# mini reconciliation against Charlie's published facts, AND
# materializes the 4-IB rs-gaap Report — two markdown artifacts land in
# output/ at the end.
just demo-seattle-method

# Or run a single step
just demo-seattle-method --step pull
just demo-seattle-method --step load
just demo-seattle-method --step seed-mappings
just demo-seattle-method --step ingest
just demo-seattle-method --step author-rollforwards
just demo-seattle-method --step reconcile        # mini reconciliation only
just demo-seattle-method --step create-report    # rs-gaap 4-IB Report only

# Reconcile + create-report can also be run via their dedicated recipes
# (handy when iterating on the markdown rendering without re-provisioning):
just demo-seattle-method-reconcile <graph_id>
just demo-seattle-method-create-report <graph_id>
```

## What Gets Created

| Step                  | Artifact                                                                                                                                     |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `pull`                | `local/taxonomies/mini/` — 30 XBRL artifacts curled from `xbrlsite.azurewebsites.net` AND `local/datasets/seattle_method/GeneralJournal.csv` from Charlie's `seattlemethod/prototypes` GitHub repo (both gitignored — re-fetched on demand) |
| `provision`           | A dedicated test graph (e.g. `kg_seattle_method_<timestamp>`) — isolated from real customer data                                             |
| `load`                | 239 mini concepts as Elements + presentation/calculation/definition/formula linkbase arcs as Associations                                    |
| `seed-mappings`       | ~36 mini→rs-gaap derivation Associations (BS/IS/CF/SE leaves touched by the 14-JE dataset)                                                   |
| `ingest`              | 14 Events (one per JournalEntryID), 14 Entries, ~32 LineItems — each LineItem.metadata\_['transaction_description_code'] stamped from CSV    |
| `author-rollforwards` | 8 rollforward IBs (one per BS leaf with activity) — Cash, Receivables, Inventories, PP&E, AP, Accrued, LTD, PaidInCapital                    |
| `reconcile`           | `output/seattle-method-case-1.md` — mini-vocab line-by-line comparison vs. Charlie's PoC, classified per methodology spec §3.2               |
| `create-report`       | `output/seattle-method-case-1-four-statements.md` — rs-gaap 4-IB Report (BS / IS / CF / SE) materialized via create-report + reportPackage   |

Steps write to `output/` (gitignored — each run stamps fresh graph/report
IDs, so committing it would churn). Committed reference copies live in
[`sample_output/`](sample_output/); refresh them with
`cp output/*.md sample_output/`.

## Inputs (all fetched from Charlie's published artifacts, none committed)

Charlie maintains his test artifacts on GitHub and xbrlsite. We pull
directly from those canonical sources rather than vendoring copies — a
deliberate "ingest the published artifact" stance so the demo stays
faithful to his upstream as it evolves.

| Artifact | Upstream | Local destination (gitignored) |
| --- | --- | --- |
| `GeneralJournal.csv` (14 JEs) | [github.com/seattlemethod/prototypes/.../journal-entries-csv](https://github.com/seattlemethod/prototypes/blob/main/record-to-report/journal-entries-csv/GeneralJournal.csv) | `local/datasets/seattle_method/GeneralJournal.csv` |
| `mini` base taxonomy + linkbases | [xbrlsite.azurewebsites.net/.../mini/base-taxonomy](https://xbrlsite.azurewebsites.net/2026/reporting-framework/mini/base-taxonomy/mini_ModelStructure.html) | `local/taxonomies/mini/` |
| Record-to-Report `instance.xml` (reconciliation reference) | [xbrlsite.com/seattlemethod/platinum-testcases/record-to-report/report.zip](http://www.xbrlsite.com/seattlemethod/platinum-testcases/record-to-report/) | `local/datasets/seattle_method/report/instance.xml` |

`pull_mini.sh`, `pull_general_journal.sh`, and `pull_expected_report.sh`
are idempotent (`step_pull` runs all three); re-runs overwrite the
local copy with the current upstream. `reconcile.py` parses
`instance.xml` directly as the source of truth — strictly stronger
than the earlier hand-derived CSV fixture since it consumes the same
artifact Arelle validates. Charlie's two alternative input formats —
the [XBRL Global Ledger instance](https://github.com/seattlemethod/prototypes/tree/main/record-to-report/journal-entries-xbrl-global-ledger)
and the [XBRL typed-members representation](https://github.com/seattlemethod/prototypes/tree/main/record-to-report/journal-entries-typed-members)
— are forward work (new ingest adapters with the same downstream pipeline).

## Reconciliation Classification

Every delta in a reconciliation report is classified into one of four
categories. The classification is what makes the report actionable —
it tells the reader who needs to act on what.

| Category               | Definition                                                                                          | Owner                       |
| ---------------------- | --------------------------------------------------------------------------------------------------- | --------------------------- |
| **Matching**           | Our output equals expected output.                                                                  | —                           |
| **Methodology gap**    | Architectural feature not yet shipped; documented in a known spec.                                  | RoboSystems (forward queue) |
| **Our bug**            | Implementation error in shipped code. Needs a fix.                                                  | RoboSystems (fix)           |
| **Their data quality** | Source data has a tagging error, vocabulary misuse, or inconsistency. Not a defect in our pipeline. | Source author               |

A reconciliation report with all four categories represented is more
informative than an all-green one — it surfaces what each party owns.

## Known Data-Quality Findings (Pre-Reconciliation)

These are identified during input review on 2026-05-19, NOT
introduced by the reconciliation. They land in the report's
**Their data quality** category per the classification above.

| JE         | Issue                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **JE-205** | Description "Payment for contractor"; TDC on the AP line is `mini:PurchasesInventoryForSaleOnAccount` — but contractor services aren't inventory. Vocabulary misuse.                                                                                                                                                                                                                                                                                                                 |
| **JE-209** | TDC `mini:PaymentOfInterest` on the Cash line is a typo — mini.xsd's canonical concept is `mini:PaymentInterest` (no "Of"). `ingest_transactions.py::_KNOWN_TDC_ALIASES` normalizes it at ingest time so the rollforward filter engine matches; logged as a warning per JE so the substitution stays transparent. The sibling concept `mini:DecreaseFromPaymentOfInterest` on the AccruedExpenses line _does_ keep the "Of" (Charlie's own naming is internally inconsistent).                                                                                                                                  |
| **JE-225** | Boundary test case: "Write off of PPE" with `Amount = 0` on both lines — an entry with no economic substance. Our GL handler correctly rejects it (`must have non-zero D or C`); Charlie's system likely creates $0 facts. The reconciliation delta is $0 either way (no economic activity to attribute), so the four anchor totals are unaffected. Classified as **Methodology gap** (neither side has a bug — both correctly handle a nil entry under their respective semantics). |
| **JE-226** | Income tax accrual ($400). TDC on the AccruedExpenses line is `mini:InterestAccrued` — should be `IncomeTaxAccrued`. Copy-paste-style bug from the JE-210 interest accrual pattern.                                                                                                                                                                                                                                                                                                  |

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

The methodology is the durable artifact; specific test cases are
scheduled by external forcing functions (a customer, a published
reference, a regulatory deadline).
