# Information Blocks

An Information Block is a typed, self-describing envelope that bundles raw graph atoms — elements, associations, and facts — together with the molecular wiring that makes them meaningful: an information model, mechanics, verification rules, and a per-period fact set. It is the unifying join between taxonomy content and ledger content, projected into financial reports.

## Table of Contents

- [Atomic vs. Molecular](#atomic-vs-molecular)
- [The block_type Axis](#the-block_type-axis)
- [The Envelope](#the-envelope)
- [FactSets — Block = Fact Set](#factsets--block--fact-set)
- [The Registry — One Construction Machine](#the-registry--one-construction-machine)
- [Reporting Style](#reporting-style)
- [View Projections](#view-projections)
- [Authoring an Information Block](#authoring-an-information-block)
- [How It Ties Together](#how-it-ties-together)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Atomic vs. Molecular

The platform stores two kinds of thing. An **atom** is a single bare row — one element (a chart-of-accounts concept), one association (an edge between two elements), one fact (a measured value for a period). Atoms are data: they are inert and mean almost nothing on their own. A fact of `1500000` against `us-gaap:Depreciation` for a period is just a number until you know which entity it belongs to, which statement it rolls into, how it was measured, and which rules it must satisfy.

An **Information Block** is a **molecule**: an assembly that carries its own context. It packages the atoms together with the wiring needed to interpret, query, audit, or export them unambiguously. The governing rule is simple:

> **What crosses a system boundary is always a molecule, never a bare atom.** Atoms live inside the store; molecules are what the API, the SDKs, the MCP tools, and the operators hand to a consumer.

That is why every read of an Information Block returns a complete envelope — identity, model, atoms, fact set, verification, and rendering — rather than a loose collection of rows you would have to reassemble yourself.

## The block_type Axis

`block_type` describes what a block **is**, not what domain it touches. A `schedule` is a kind of molecule; "Office Building Depreciation" is the domain name of one particular schedule. The frontend speaks domain nouns; the API, the backend, and the operators speak `block_type`.

There are ten registered block types, grouped by the construction role they play:

| `block_type` | Display | Category | Construction | Mechanics |
| --- | --- | --- | --- | --- |
| `schedule` | Schedule | Close | declarative | `ScheduleMechanics` |
| `rollforward` | Rollforward | Reporting | declarative | `RollforwardMechanics` |
| `forecast` | Forecast | Planning | declarative | `ForecastMechanics` |
| `balance_sheet` | Balance Sheet | Reporting | compositional | `StatementMechanics` |
| `income_statement` | Income Statement | Reporting | compositional | `StatementMechanics` |
| `cash_flow_statement` | Cash Flow Statement | Reporting | compositional | `StatementMechanics` |
| `equity_statement` | Equity Statement | Reporting | compositional | `StatementMechanics` |
| `comprehensive_income` | Statement of Comprehensive Income | Reporting | compositional | `StatementMechanics` |
| `regulatory_disclosure` | Disclosure | Reporting | compositional | `StatementMechanics` |
| `metric` | Metric | Reporting | derivative | `MetricMechanics` |

The three construction roles determine how a block comes into being:

- **declarative** — you declare the mechanics and seed parameters, and the system generates the atoms. Schedules, rollforwards, and forecasts are declarative: you author them.
- **compositional** — the atoms already exist from ingestion, and the block is a view assembled at read time. The five financial statements and the disclosure note are compositional: you render them, you do not author them.
- **derivative** — facts are computed from other blocks at read time. Metrics are derivative.

The practical consequence is the single most important thing to know about authoring: **you author a Schedule; you render a Balance Sheet.**

## The Envelope

Every Information Block, regardless of type, returns the same wire shape — the `InformationBlockEnvelope`. One block, many shapes is the contract: a tool, SDK, or operator can work with any block without special-casing its type, because the envelope is uniform and the per-type detail is carried in a typed `mechanics` union.

The envelope carries:

| Field group | Contents |
| --- | --- |
| **Identity** | `id`, `block_type`, `name`, `display_name`, `category` |
| **Taxonomy linkage** | `taxonomy_id`, `taxonomy_name`, `disclosure_id` (a qname like `disclosures:BalanceSheet`; null for tenant-authored blocks) |
| **Information model** | `information_model.concept_arrangement`, `information_model.member_arrangement` |
| **Artifact** | `artifact.topic`, `artifact.renderer_note`, `artifact.mechanics` (a typed union keyed on `block_type`) |
| **Atoms** | `elements[]`, `connections[]` (associations), `facts[]`, `rules[]`, `dimensions[]` |
| **Fact set** | `fact_set` — the per-period instance, carrying typed `provenance` |
| **Verification** | `verification_results[]`, `verification_summary` (null until rules run) |
| **View** | `view` — server-side projections over the envelope |

GraphQL field names are camelCase (`blockType`, `displayName`, `factSet`); the Pydantic and SDK names are snake_case (`block_type`, `display_name`, `fact_set`). The `artifact.mechanics` union is what makes the molecule genuinely self-describing: a consumer knows a block's shape **from its type alone**, with no runtime shape-guessing. A `schedule` carries `ScheduleMechanics`, a statement carries `StatementMechanics`, a `metric` carries `MetricMechanics`.

The full field-by-field schema is published in the OpenAPI spec at [https://api.robosystems.ai/docs](https://api.robosystems.ai/docs) — this page covers the concepts; the spec is the source of truth for exact types.

## FactSets — Block = Fact Set

A block separates a **persistent skeleton** from a **per-period instantiation**:

- A **Structure** is the skeleton: the elements, the associations, the mechanics, the rules. It is created once.
- A **FactSet** is one period's instantiation of that skeleton: the actual measured facts for a specific period.

The envelope you read back is the Structure plus the FactSet for a period. Over time a single Structure accumulates **many FactSets** — one Office Building Depreciation schedule (the Structure) gains a new FactSet every month it runs. This is why a block's `id` is its `structure_id`, and why `fact_set` carries its own period bounds and `provenance` independent of the structure.

A `FactSetLite` carries `id`, `structure_id`, `period_start`, `period_end`, `factset_type`, `entity_id`, `report_id`, `scenario_id` (the forecast scenario it belongs to; `null` means actuals), and `provenance`. A library-seeded statement Structure has no tenant facts until a report publishes one, so its envelope returns `facts=[]` and `fact_set=null` — that is normal, not an error.

## The Registry — One Construction Machine

Rather than a bespoke operation per block type, a single registry maps each `block_type` to a handler. New block types plug in by writing the same row shape — they do not inherit a producer or fork the write path. The registry holds, per type, the default concept-arrangement pattern, the default member-arrangement pattern, the construction mode, the typed mechanics class, and whether the type surfaces in the public library.

Because mechanics are typed (a discriminated union, not free-form JSONB), the construction machine can validate a write against the exact shape its type demands before it ever touches the graph. That typing is what lets one generic dispatch serve ten different block kinds without losing precision.

The construction mode also dictates which write operations are real:

- **declarative** types (`schedule`, `rollforward`, `forecast`) have real, typed create / update / delete handlers.
- **compositional** types (the five statements and `regulatory_disclosure`) return **HTTP 501** on `create-information-block` — their atoms come from ingestion and their envelopes are assembled at read time. Statements are produced through the report-rendering path (`create-report`), not the block-create path.
- **derivative** types (`metric`) likewise return **501** on the write path.

## Reporting Style

A block does not exist in a vacuum — it renders within a framework and a presentation style. Two layers cooperate:

- A **Framework** (the basis of accounting) decides recognition and measurement — what a number means and when it is recognized.
- A **Reporting Style** decides presentation **within** a framework — how the recognized numbers are laid out.

Reporting Style is expressed as a compositional four-segment code, `{BS-layout}-{equity-form}-{IS-layout}-{CF-method}`, for example `BSC-CORP-IS02-CF1`:

| Segment | Shipped values | Reserved | Selects |
| --- | --- | --- | --- |
| BS-layout | `BSC` | `BSU`, `NET` | Balance-sheet layout |
| equity-form | `CORP`, `PART`, `LLC` | `SOLE`, `NFP` | Equity presentation form |
| IS-layout | `IS02` | `IS01` | Income-statement layout |
| CF-method | `CF1` | `CF2` | Cash-flow method |

The grammar has room for far more than currently ships. **The equity-form axis is the only one with shipped variants**: v1 ships the default family `BSC-{CORP|PART|LLC}-IS02-CF1` — three styles in total. The other segments are fixed at the classified balance sheet, multi-step income statement, and indirect cash flow; additional BS layouts, a single-step income statement, and the direct cash-flow method are reserved and land as package content when prioritized.

A Reporting Style is not a `Classification` — it is a **`Structure`** with `block_type='reporting_style'`, composing one presentation Network per statement type through the `reporting_style_networks` table. It is pinned **per Entity**, not per graph: `entities.reporting_style_id` is non-nullable and defaults from the entity's legal form at creation (corporation → Default, partnership → PART, llc → LLC), so heterogeneous subsidiaries can each carry their own style while resolving to the same canonical calc-DAG subtotals. Switch it later with `POST /extensions/roboledger/{graph_id}/operations/change-reporting-style` (a `reporting_style_id` plus an optional `entity_id`; omit the entity to target the graph's primary one). The style constrains **which statement structures are eligible to render**, so the same recognized facts can project into different statement layouts — without changing a single underlying atom.

## View Projections

The `view` field on the envelope holds server-side **lenses** over the same molecule — different ways of looking at one block without re-querying. Two arms are populated today:

- **`rendering`** — the laid-out, footed presentation of the statement family, ready for display. This lens is what turns a Balance Sheet Structure plus a period's FactSet into rows, subtotals, and totals in presentation order.
- **`chart`** — panel and series configuration computed over the rendering's rows and periods, populated for metric blocks.

Rendering and the broader reporting surface — including the multi-dimensional fact grid over the XBRL hypercube — are covered in [Reporting and Rendering](Reporting-and-Rendering).

## Authoring an Information Block

Because statements are rendered and metrics are derived, the block you actually **author** is a declarative one — a **Schedule** is the canonical worked example. Authoring goes through the RoboLedger command surface; reading back goes through the always-on GraphQL surface.

All authenticated calls use the `X-API-Key` header, read from `.local/config.json` (created by `just demo-user`). The base URL for local testing is `http://localhost:8000`, and `GRAPH_ID` is your tenant graph id (for example `kg1a2b3c...`).

### Step 1: Create a Schedule (the write path)

The `create-information-block` operation takes a body discriminated on `block_type`. The `schedule` arm carries a fully typed payload. Note that **amounts are integer cents** — `monthly_amount: 83333` means $833.33 per month.

`element_ids` and the `entry_template` sides take **chart-of-accounts element ids**, not taxonomy qnames — the `id` values returned by `get-unmapped-elements` or `get-graph-schema`. Resolve the two ids you need first and substitute them for the placeholders below; passing `us-gaap:Depreciation` here will not resolve.

```bash
API_KEY=$(jq -r .api_key .local/config.json)
GRAPH_ID=<your graph id>

curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/create-information-block" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(date +%s)" \
  -d '{
    "block_type": "schedule",
    "payload": {
      "name": "Office Building Depreciation",
      "element_ids": ["<depreciation_expense_element_id>", "<accumulated_depreciation_element_id>"],
      "period_start": "2026-01-01",
      "period_end": "2030-12-31",
      "monthly_amount": 83333,
      "entry_template": {
        "debit_element_id": "<depreciation_expense_element_id>",
        "credit_element_id": "<accumulated_depreciation_element_id>",
        "entry_type": "adjusting"
      }
    }
  }'
```

The response is the full `InformationBlockEnvelope` for the freshly created block. Its `id` is the `structure_id` — keep it for the read-back and rule-evaluation steps. Every write returns an `OperationEnvelope` with a ULID `operationId` and accepts an `Idempotency-Key` header for safe retries.

### Step 2: Read the block back via GraphQL

Reads go to `POST /extensions/{graph_id}/graphql`. **The graph is the URL scope, not a query argument** — there is no `graphId` argument; passing one is wrong. GraphQL field names are camelCase.

```bash
curl -X POST "http://localhost:8000/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ informationBlock(id: \"STRUCTURE_ID_FROM_STEP_1\") { id blockType name displayName category informationModel { conceptArrangement memberArrangement } artifact { topic mechanics } facts { elementQname value periodEnd } factSet { id factsetType provenance } verificationSummary { passed failed } } }"}'
```

### Step 3: List schedules

The `informationBlocks` query takes optional `blockType` and `category` filters plus `limit` / `offset`. This is always-on regardless of product flags.

```bash
curl -X POST "http://localhost:8000/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ informationBlocks(blockType: \"schedule\", category: \"Close\", limit: 20) { id name displayName facts { value } } }"}'
```

### Step 4: Run the verification rules

Verification rules live as rows on the block — each carries a category (what it checks) and a mechanism (how it checks). Running the rule engine writes one `VerificationResult` per rule and returns a status-keyed summary.

```bash
curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/evaluate-rules" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"structure_id": "STRUCTURE_ID_FROM_STEP_1"}'
```

The arithmetic patterns that evaluate today include `EqualTo`, `RollUp`, `RollForward`, `Exists`, `CoExists`, and `SumEquals`; patterns without an implementation return `skipped`. The `verification_summary` on the envelope stays null until you run this at least once, which is why the viewer hides the verification panel rather than render zeros.

### MCP access

The same blocks are reachable from any MCP client. `get-information-block` (params `id`, `scenario_id`, `series`, `series_history`, `series_forecast`) and `list-information-blocks` (params `block_type`, `category`, `limit`, `offset`, `include_atoms`, `scenario_id`) read; `create-information-block` writes. Per platform policy, exercise blocks through the GraphQL, REST, and MCP surfaces — not direct database access — so you are always going through the same business rules a real consumer hits.

### Common gotchas

- **Statements are rendered, not created.** `create-information-block` with a statement or metric `block_type` returns **HTTP 501**. Author a `schedule` (or `rollforward`); produce statements through `create-report`.
- **Amounts are integer cents.** `monthly_amount`, `original_amount`, and `residual_value` are integers in cents, not dollars.
- **GraphQL scope is the URL.** `informationBlock` / `informationBlocks` take no `graphId` argument.
- **`fact_set = null` and `facts = []` are normal** for a statement Structure that has not been instantiated by a report yet.
- **`verification_summary` is null until rules run.** Call `evaluate-rules` first.
- **`disclosure_id` is a derived qname**, present only when a Disclosure mapping exists; tenant-authored schedules carry null.

## How It Ties Together

The Information Block is the seam where the two halves of the platform meet:

- **Taxonomy content** — elements and associations — supplies the structural atoms. These come from the frameworks and reporting styles described in [Taxonomy and Frameworks](Taxonomy-and-Frameworks).
- **Event / ledger content** — facts derived from transactions, entries, and line items — supplies the measured atoms. These come from the three-level ledger described in [Event-Driven Ledger](Event-Driven-Ledger).

Neither half means anything in isolation: a taxonomy without facts is an empty skeleton, and facts without a taxonomy are loose numbers. The Information Block is the envelope that bundles both with meaning — a typed model, mechanics, rules, and a per-period fact set — and the view projections render that bundle into financial reports. One molecule, assembled once, read uniformly, projected into [Reporting and Rendering](Reporting-and-Rendering).

## Related Documentation

**Wiki Guides:**
- **[Taxonomy and Frameworks](Taxonomy-and-Frameworks)** - Elements, associations, frameworks, and reporting styles that supply a block's structural atoms
- **[Event-Driven Ledger](Event-Driven-Ledger)** - Transactions, entries, and line items that supply a block's measured facts
- **[Reporting and Rendering](Reporting-and-Rendering)** - View projections, statement rendering, and the multi-dimensional fact grid
- **[Architecture Overview](Architecture-Overview)** - How the extensions GraphQL, command, and analytical surfaces fit the platform

**Codebase Documentation:**
- **[API Documentation](https://api.robosystems.ai/docs)** - Full request/response schema and the live OpenAPI spec
- **[GraphQL Extensions](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graphql/README.md)** - Strawberry GraphQL surface and Pydantic auto-derivation
- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business workflow orchestration and the operations kernel
- **[API Models](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/models/api/README.md)** - Pydantic request/response models including the envelope shapes

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
