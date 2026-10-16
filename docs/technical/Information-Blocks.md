# Information Blocks

An **Information Block** is the platform's unit of financial meaning: the numbers, plus the concepts, structure, and rules that make them facts. On its own, `83333` could be cents of depreciation, a share count, or a typo; it only becomes a fact once you know which concept it measures, which period it covers, which statement it rolls into, and which rules it has to satisfy. The block carries all of that together, and every block, whatever its type, is read and written in the same envelope.

This page is the concept. The field-by-field envelope, the per-type mechanics, and the rule and registry matrices are in [Information Block Reference](Information-Block-Reference.md).

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development.md).

## Table of Contents

- [Atomic vs. Molecular](#atomic-vs-molecular)
- [Block Types and Construction Modes](#block-types-and-construction-modes)
- [The Envelope](#the-envelope)
- [FactSets — Block = Fact Set](#factsets--block--fact-set)
- [Rules and Verification](#rules-and-verification)
- [View Projections](#view-projections)
- [Live and Pinned Reads](#live-and-pinned-reads)
- [Authoring an Information Block](#authoring-an-information-block)
- [How It Ties Together](#how-it-ties-together)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Atomic vs. Molecular

The platform stores two kinds of thing. An **atom** is a single bare row: one element (a chart-of-accounts account or a reporting concept), one association (an arc between two elements), one fact (a value for a period). Atoms are inert. A fact of `833.33` against a depreciation element is just a number until you know whose it is, how it was measured, and what it must reconcile to.

An **Information Block** is a **molecule**: the atoms assembled with the wiring that interprets them. The rule that follows from this:

> **What crosses a system boundary is always a molecule, never a bare atom.** Atoms live inside the store. The API, the SDKs, the MCP tools, and the AI operators hand a consumer a complete block.

```text
 ATOMS (rows in the store)              THE BLOCK
 ─────────────────────────              ─────────────────────────────────────────────

 elements ─────┐
               ├──▶  Structure        created once: information model, mechanics, rules
 associations ─┘         │
                         │  instantiated per period / report / scenario
 facts ───────────────▶  FactSet · FactSet · FactSet · …
                         │
                         ▼
                    Envelope          Structure + one FactSet + verification results
                         │
                         ▼
                    Views             rendering · chart · facts · elements · rules · validation
```

## Block Types and Construction Modes

`block_type` says what a block **is**, not what it is about. `schedule` is a kind of molecule; "Office Building Depreciation" is the name of one schedule. The API, the SDKs, and the operators speak `block_type`.

Every type belongs to one of three **construction modes**, and the mode decides how the block comes into being:

| Mode | How it is made | Block types |
| --- | --- | --- |
| **declarative** | You declare the mechanics and parameters; the platform generates the facts. You **author** these. | `schedule`, `rollforward`, `forecast` |
| **compositional** | The facts already exist from the ledger; the block is assembled when you read it. You **render** these, through a Report. | `balance_sheet`, `income_statement`, `cash_flow_statement`, `equity_statement`, `comprehensive_income`, `regulatory_disclosure` |
| **derivative** | Facts are computed from other blocks, asserted from outside, or observed from a source outside the ledger. | `metric`, `reconciliation` |

The practical consequence: **you author a Schedule; you render a Balance Sheet.** Calling `create-information-block` with a compositional or derivative type returns HTTP 501 with a pointer to the right path (`create-report` for statements, `create-taxonomy-block` for a disclosure or metric structure, `refresh-reconciliations` for a reconciliation). The full type-by-operation matrix is in [Information Block Reference § The Registry](Information-Block-Reference.md#the-registry).

## The Envelope

Every block returns the same wire shape, the `InformationBlockEnvelope`. A tool can handle any block without special-casing its type, because the per-type detail rides in one typed field: `artifact.mechanics`, whose `kind` tells you its shape.

A trimmed schedule envelope, as `create-information-block` returns it (snake_case on REST and MCP; GraphQL serves the same fields in camelCase):

```jsonc
{
  "id": "struct_01K9R3M2QH7Z8W1V5T4X6Y0NBC",   // the Structure id; use it for every later read
  "block_type": "schedule",
  "name": "Loan Discount Amortization",
  "display_name": "Schedule",
  "category": "Close",
  "taxonomy_id": "…",
  "disclosure_id": null,                        // set only when a named Disclosure maps to the block

  "information_model": {                        // the block's shape
    "concept_arrangement": "roll_forward",
    "member_arrangement": null
  },
  "artifact": {
    "topic": "Effective-interest amortization of the term-loan discount",
    "mechanics": {                              // typed by `kind`; one arm per block family
      "kind": "closing_entry_generator",
      "entry_template": { "debit_element_id": "elem_…", "credit_element_id": "elem_…",
                          "entry_type": "adjusting", "memo_template": "{structure_name}",
                          "auto_reverse": false },
      "schedule_metadata": { "method": "custom", "original_amount": 130140,
                             "periodic_amounts": [21340, 21480, 21620, 21760, 21900, 22040] },
      "periods_with_entries": 0
    }
  },

  "elements":    [ /* the accounts and concepts the block touches */ ],
  "connections": [ /* associations between them */ ],
  "facts": [
    { "id": "fact_…", "element_id": "elem_…", "value": 213.4,      // facts carry the unit's value (USD), not cents
      "period_start": "2026-01-01", "period_end": "2026-01-31",
      "period_type": "duration", "unit": "USD", "fact_scope": "in_scope", "fact_set_id": "fs_…" }
  ],
  "rules": [ { "rule_pattern": "SumEquals", "rule_expression": "sum($periodic_amount) = 1301.4", "…": "…" } ],

  "fact_set": {                                 // which instantiation of the Structure you are reading
    "id": "fs_…", "structure_id": "struct_…", "period_start": "2026-01-01", "period_end": "2026-06-30",
    "factset_type": "schedule", "scenario_id": null,
    "provenance": { "origin": "asserted", "source_system": "custom_amortization_curve" }
  },
  "verification_results": [],
  "verification_summary": null,                 // null until the rules have been evaluated
  "view": { "rendering": null, "chart": null }  // server projections; statements and metrics fill these
}
```

Every field, and what each mechanics arm carries, is in [Information Block Reference § The Envelope](Information-Block-Reference.md#the-envelope).

## FactSets — Block = Fact Set

A block separates a **persistent skeleton** from its **instantiations**:

- A **Structure** is the skeleton: elements, associations, mechanics, rules. It is created once, and its id is the block's `id`.
- A **FactSet** is one instantiation of that skeleton: a set of facts stamped with its own bounds, type, and `provenance` (how its facts were made: pivoted from the ledger, generated by a schedule, derived, asserted, bound from a document, forecast, filed, or observed from an outside source for a reconciliation).

A Structure accumulates FactSets over time. Each period close stamps a new statement FactSet for the month, each published Report pins its own, and each forecast scenario keeps a parallel set identified by `scenario_id` (`null` means actuals). A Schedule is the simple case: one FactSet covering its whole window, with one fact per element per month.

The envelope you read is the Structure plus **one** FactSet. A library-seeded statement Structure with no FactSet yet returns `facts: []` and `fact_set: null`. That is normal, not an error.

## Rules and Verification

Rules are not a global corpus run against a graph. **Every rule hangs off a block**, and ships in the block's envelope next to the facts it checks. Some arrive with the block: a schedule with a cost basis gets a `SumEquals` rule proving its periodic amounts add back to that basis, and the seeded statement structures carry their calculation arcs as `RollUp` and `EqualTo` rules. Others are authored with the structure.

Evaluating writes one result per rule and fills `verification_results` and `verification_summary`. Call `evaluate-rules` with a `structure_id` to run one block by hand; `close-period` runs the same engine over the period's schedules and the statements it stamps, and returns the tallies on its receipt. Each result is `pass`, `fail`, `error`, or `skipped`; a pattern without an evaluator reports `skipped` rather than failing. Which patterns evaluate today is in [Information Block Reference § Rule Patterns](Information-Block-Reference.md#rule-patterns); a walk through the rules a demo tenant carries is in [RoboLedger Demo Walkthrough § The Rule Engine](RoboLedger-Demo-Walkthrough.md#the-rule-engine).

Rule-engine verification is not the same as a statement's render-time footing check. See [Reporting and Rendering § Guard Rails](Reporting-and-Rendering.md#guard-rails).

## View Projections

A **view** is one way of looking at the same block, with no second query. Two views are computed on the server and ride in the envelope's `view` field:

- **`rendering`**: ordered, footed rows with one value per period column. Filled for the statement family, disclosures, metrics, forecast blocks, and reconciliations.
- **`chart`**: panel and series configuration over those rows. Filled for metric blocks.

The rest are projections of the envelope's own lists, done by the client: the facts table (`facts`), the elements (`elements`), the rules (`rules`), and the verification results (`verification_results`, `verification_summary`). This is how the RoboLedger Explorer offers Rendered, Chart, Facts, Elements, Validation, and Rules views over one envelope. The rendering payload and how statements are laid out, including the per-entity Reporting Style that picks which presentation a statement uses, are in [Reporting and Rendering](Reporting-and-Rendering.md).

## Live and Pinned Reads

Two GraphQL reads return envelopes, and they answer different questions:

- **`informationBlock(id)`** is the **live** read. It binds the block's latest FactSet, so it reflects the books as they stand now. `scenarioId` selects a forecast scenario instead of actuals, and `series: true` renders a statement as its whole monthly time series.
- **`reportPackage(reportId)`** is the **pinned** read. It rehydrates a saved Report as a package of envelopes, each pinned to the FactSet the Report was published with, so it reads the same next year as it did the day it was filed.

The snapshot unit is the FactSet, not the Structure. Use the live read for the closing book and the pinned read for anything you file, share, or compare against later.

> **Same noun, different call.** The analytical surface also has operations named **`disclosures`** and **`information-block`** (`POST /extensions/roboledger/{graph_id}/operations/information-block`, and the MCP tool of the same name). Those do **not** read the blocks on this page. They read **one section of a filing or report whole**: `disclosures` lists a report's sections, and `information-block` returns one of them with its rows, its breakdown by its own axes, its calculation footing, and its text blocks. They run xbrlkit over the report held whole (the published filing on `sec`, the report bundle on a tenant), not over the graph and not over this envelope. To read an authored or rendered block, use `informationBlock` / `get-information-block`. See [RoboLedger Operations](RoboLedger-Operations.md#analytical-view-operations).

## Authoring an Information Block

Statements are rendered and metrics are derived, so the block you author is a declarative one. A **Schedule** is the canonical example: a generator of recurring closing entries for depreciation, amortization, prepaid drawdowns, and accruals.

All calls authenticate with `X-API-Key`, using a key from **Settings → API keys** at robosystems.ai. `GRAPH_ID` is a graph with the RoboLedger extension. See [Quick Start](Quick-Start.md) for an account, a key, and a graph.

### Create a schedule with an uneven curve

Cadence Labs amortizes a $1,301.40 discount on a term loan over six months using the effective-interest method, so the monthly amounts rise. `periodic_amounts` supplies one amount per month, in cents; it must have exactly one entry per month in the window and sum exactly to `original_amount` less `residual_value` (here zero). Leave it out and the schedule is straight-line from `monthly_amount`, which is still required.

Element ids are **chart-of-accounts element ids** (the `id` from `get-unmapped-elements`), not taxonomy qnames.

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai
export GRAPH_ID=kg...               # from GET /v1/graphs or the app's graph selector

curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/create-information-block" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{
    "block_type": "schedule",
    "payload": {
      "name": "Loan Discount Amortization",
      "element_ids": ["<interest_expense_id>", "<loan_discount_id>"],
      "period_start": "2026-01-01",
      "period_end": "2026-06-30",
      "monthly_amount": 21690,
      "entry_template": {
        "debit_element_id": "<interest_expense_id>",
        "credit_element_id": "<loan_discount_id>",
        "entry_type": "adjusting",
        "memo_template": "{structure_name}"
      },
      "schedule_metadata": {
        "method": "custom",
        "original_amount": 130140,
        "periodic_amounts": [21340, 21480, 21620, 21760, 21900, 22040]
      }
    }
  }'
```

The response is an `OperationEnvelope` whose `result` is the new block's envelope. Creating the schedule also writes its facts, a `SumEquals` rule proving the six amounts total `original_amount`, and one obligation event per month that drafts that month's entry when its period arrives (see [Event-Driven Ledger](Event-Driven-Ledger.md)). Keep the `id`.

### Read it back and verify it

Reads go to GraphQL at `/extensions/{graph_id}/graphql`. The graph is the URL, not an argument.

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ informationBlock(id: \"STRUCTURE_ID\") { blockType name facts { elementName value periodEnd } rules { rulePattern ruleExpression } verificationSummary { total passed failed } } }"}'

curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/evaluate-rules" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"structure_id": "STRUCTURE_ID"}'
```

`informationBlocks(blockType: "schedule")` lists schedules; from an MCP client the same reads are `get-information-block` and `list-information-blocks`.

### Where to go next

- The other declarative types: [Information Block Reference § Per-Type Mechanics](Information-Block-Reference.md#per-type-mechanics) for rollforwards; [Forecasting and Metrics](Forecasting-and-Metrics.md) for forecast scenarios and metric blocks.
- How schedule drafts post: [Period Close](Period-Close.md).
- Reconciliation blocks, which the close maintains rather than you authoring them: [Period Close § Reconciliations](Period-Close.md#reconciliations).
- Producing statements: [Reporting and Rendering](Reporting-and-Rendering.md).
- Gotchas (cents in, dollars out; 501s; null summaries): [Information Block Reference § Gotchas](Information-Block-Reference.md#gotchas).

## How It Ties Together

The Information Block is where the two halves of the platform meet:

- **Taxonomy content** supplies the structural atoms, the elements and associations. See [Taxonomy and Frameworks](Taxonomy-and-Frameworks.md).
- **Ledger content** supplies the measured atoms, the facts derived from transactions, entries, and line items. See [Event-Driven Ledger](Event-Driven-Ledger.md).

A taxonomy without facts is an empty skeleton, and facts without a taxonomy are loose numbers. The block binds them into one molecule that is assembled once, read the same way everywhere, and projected into [Reporting and Rendering](Reporting-and-Rendering.md) and [Serialization and Export](Serialization-and-Export.md).

## Related Documentation

**Guides:**
- **[Information Block Reference](Information-Block-Reference.md)** - Envelope fields, per-type mechanics, the registry, rule patterns, and gotchas
- **[Reporting and Rendering](Reporting-and-Rendering.md)** - Statement rendering, Reporting Style, and the rendering payload
- **[Forecasting and Metrics](Forecasting-and-Metrics.md)** - Forecast scenarios and metric blocks
- **[Period Close](Period-Close.md)** - How schedule drafts post and the close runs the rule engine
- **[Taxonomy and Frameworks](Taxonomy-and-Frameworks.md)** - The elements and associations that supply a block's structure
- **[Event-Driven Ledger](Event-Driven-Ledger.md)** - The events and entries that supply a block's facts

**Codebase Documentation:**
- **[API reference](https://robosystems.ai/docs/extensions)** - Full request and response schemas
- **[GraphQL Extensions](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graphql/README.md)** - The GraphQL read surface
- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - The operations kernel

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Technical docs](Home.md)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
